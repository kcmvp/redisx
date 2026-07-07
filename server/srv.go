package server

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kcmvp/respx/internal"
	"github.com/kcmvp/respx/storage"
	"github.com/kcmvp/respx/x"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
)

const (
	privateAddr = "127.0.0.1:6380"

	cmdAuth       = "auth"
	cmdHello      = "hello"
	cmdPing       = "ping"
	cmdQuit       = "quit"
	cmdSet        = "set"
	cmdSetEx      = "setex"
	cmdGet        = "get"
	cmdSetNX      = "setnx"
	cmdDel        = "del"
	cmdKeys       = "keys"
	cmdQueryIndex = "queryindex"
	cmdQueryKey   = "querykey"
	cmdPublish    = "publish"
	cmdSubscribe  = "subscribe"
	cmdPSubscribe = "psubscribe"
	cmdClient     = "client"
)

var (
	// internalAuthKey is generated per-process and used only for internal bootstrap auth.
	internalAuthKey = internal.AuthKey()
	// authKey is for external client AUTH and can be configured via environment.
	authKey = ""

	activeExternalConns int
	connCountMu         sync.Mutex
	externalMaxConns    = 1
	serverMu            sync.Mutex
	serverListener      net.Listener
	shutdownOnce        sync.Once
	shutdownMu          sync.Mutex
	shutdownSignalCh    chan os.Signal
	shutdownDoneCh      chan struct{}

	globalMu         sync.RWMutex
	listenAndServeFn = listenAndServeWithStop
	signalNotifyFn   = signal.Notify
	signalStopFn     = signal.Stop
	osExitFn         = os.Exit
	currentDB        storage.DB
	srvOnce          sync.Once
)

func getOsExitFn() func(int) {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return osExitFn
}

func getListenAndServeFn() func(string, func(redcon.Conn, redcon.Command), func(redcon.Conn) bool, func(redcon.Conn, error)) error {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return listenAndServeFn
}

func resolveExternalAuthKey(getenv func(string) string) string {
	if v := getenv(internal.RespxAuthKeyEnv); v != "" {
		return v
	}
	return rand.Text()
}

func ensureExternalAuthKey() {
	if authKey != "" {
		return
	}
	authKey = resolveExternalAuthKey(os.Getenv)
	if os.Getenv(internal.RespxAuthKeyEnv) == "" {
		slog.Warn("auth key env is not set, generated random external auth key", "env", internal.RespxAuthKeyEnv)
	}
}

func acceptCon(conn redcon.Conn) bool {
	connCountMu.Lock()
	if activeExternalConns >= externalMaxConns {
		connCountMu.Unlock()
		slog.Warn("reject connection", "remote", conn.RemoteAddr(), "active", activeExternalConns, "external_max_conns", externalMaxConns)
		return false
	}
	activeExternalConns++
	connCountMu.Unlock()

	conn.SetContext("")
	return true
}

func closeCon(conn redcon.Conn, err error) {
	ctx := conn.Context()
	var prevAuth string
	if ctx != nil {
		prevAuth, _ = ctx.(string)
	}

	connCountMu.Lock()
	if ctx != nil && prevAuth != internalAuthKey && activeExternalConns > 0 {
		activeExternalConns--
	}
	current := activeExternalConns
	connCountMu.Unlock()

	if err != nil {
		slog.Info("connection closed", "remote", conn.RemoteAddr(), "active", current, "error", err)
	} else {
		slog.Info("connection closed", "remote", conn.RemoteAddr(), "active", current)
	}
}
func listenAndServeWithStop(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	serverMu.Lock()
	serverListener = ln
	serverMu.Unlock()

	// cleanup on exit
	defer func() {
		serverMu.Lock()
		if serverListener == ln {
			serverListener = nil
		}
		serverMu.Unlock()
	}()

	return redcon.Serve(ln, handler, accept, closed)
}

// parseFilter recursively parses a MongoDB-style JSON string into an x.Filter.
func parseFilter(jsonStr string) (x.Filter, error) {
	if jsonStr == "" || jsonStr == "{}" {
		return nil, nil // empty filter passes everything
	}

	if !gjson.Valid(jsonStr) {
		return nil, errors.New("invalid JSON filter format")
	}

	root := gjson.Parse(jsonStr)
	return parseNode(root)
}

func parseNode(node gjson.Result) (x.Filter, error) {
	if node.Type != gjson.JSON {
		return nil, errors.New("expected JSON object")
	}

	var filters []x.Filter
	var parseErr error

	node.ForEach(func(key, value gjson.Result) bool {
		k := key.String()

		switch k {
		case "$and":
			if !value.IsArray() {
				parseErr = errors.New("$and must be an array")
				return false
			}
			var subFilters []x.Filter
			value.ForEach(func(_, subNode gjson.Result) bool {
				f, err := parseNode(subNode)
				if err != nil {
					parseErr = err
					return false
				}
				subFilters = append(subFilters, f)
				return true
			})
			if parseErr != nil {
				return false
			}
			filters = append(filters, x.And(subFilters...))
		case "$or":
			if !value.IsArray() {
				parseErr = errors.New("$or must be an array")
				return false
			}
			var subFilters []x.Filter
			value.ForEach(func(_, subNode gjson.Result) bool {
				f, err := parseNode(subNode)
				if err != nil {
					parseErr = err
					return false
				}
				subFilters = append(subFilters, f)
				return true
			})
			if parseErr != nil {
				return false
			}
			filters = append(filters, x.Or(subFilters...))
		default:
			// Field comparison. E.g. "age": {"$gt": 18} or "status": "active"
			if value.Type == gjson.JSON {
				// Object with operators
				value.ForEach(func(opKey, opVal gjson.Result) bool {
					op := opKey.String()
					switch op {
					case "$eq":
						filters = append(filters, x.Eq(k, opVal.Value()))
					case "$neq":
						filters = append(filters, x.Neq(k, opVal.Value()))
					case "$gt":
						filters = append(filters, x.Gt(k, opVal.Float()))
					case "$gte":
						filters = append(filters, x.Gte(k, opVal.Float()))
					case "$lt":
						filters = append(filters, x.Lt(k, opVal.Float()))
					case "$lte":
						filters = append(filters, x.Lte(k, opVal.Float()))
					case "$contains":
						filters = append(filters, x.Contains(k, opVal.String()))
					case "$in":
						if !opVal.IsArray() {
							parseErr = fmt.Errorf("$in operator requires an array for field %s", k)
							return false
						}
						var inValues []any
						opVal.ForEach(func(_, v gjson.Result) bool {
							inValues = append(inValues, v.Value())
							return true
						})
						filters = append(filters, x.In(k, inValues...))
					default:
						parseErr = fmt.Errorf("unsupported operator: %s", op)
						return false
					}
					return true
				})
			} else {
				// Implicit $eq. E.g. "status": "active"
				filters = append(filters, x.Eq(k, value.Value()))
			}
		}

		return parseErr == nil
	})

	if parseErr != nil {
		return nil, parseErr
	}

	if len(filters) == 0 {
		return nil, nil
	}
	if len(filters) == 1 {
		return filters[0], nil
	}
	// Implicit AND for multiple keys in the same object
	return x.And(filters...), nil
}
func isServerShutdownErr(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")
}

func handleShutdownSignals(sigCh <-chan os.Signal, doneCh <-chan struct{}, stopFn func() error) {
	select {
	case <-sigCh:
		if err := stopFn(); err != nil {
			slog.Warn("graceful shutdown failed", "error", err)
		}
	case <-doneCh:
	}
}

func watchShutdownSignals() {
	shutdownOnce.Do(func() {
		sigCh := make(chan os.Signal, 1)
		doneCh := make(chan struct{})
		shutdownMu.Lock()
		shutdownSignalCh = sigCh
		shutdownDoneCh = doneCh
		shutdownMu.Unlock()

		signalNotifyFn(sigCh, os.Interrupt, syscall.SIGTERM)
		go handleShutdownSignals(sigCh, doneCh, stop)
	})
}

func handleCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}

	if db == nil {
		conn.WriteError("ERR storage not initialized")
		return
	}

	cmdName := strings.ToLower(string(cmd.Args[0]))

	// Enforce strict auth: only AUTH is allowed before a connection is authenticated.
	if cmdName != cmdAuth && cmdName != cmdHello && cmdName != cmdClient {
		connAuthKey, _ := conn.Context().(string)
		if conn.Context() == nil || connAuthKey == "" {
			conn.WriteError("NOAUTH authentication required")
			slog.Warn("unauthenticated command attempt", "remote", conn.RemoteAddr(), "cmd", cmdName)
			_ = conn.Close()
			return
		}
	}

	switch cmdName {
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	case cmdAuth:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR authentication failed")
			slog.Warn("auth format error", "remote", conn.RemoteAddr())
			_ = conn.Close()
			return
		}
		providedKey := string(cmd.Args[1])
		if providedKey == internalAuthKey || (authKey != "" && providedKey == authKey) {
			prevAuth, _ := conn.Context().(string)
			if providedKey == internalAuthKey && prevAuth != internalAuthKey {
				connCountMu.Lock()
				activeExternalConns--
				connCountMu.Unlock()
			} else if providedKey != internalAuthKey && prevAuth == internalAuthKey {
				connCountMu.Lock()
				activeExternalConns++
				connCountMu.Unlock()
			}
			conn.SetContext(providedKey)
			conn.WriteString("OK")
			slog.Info("connection authenticated", "remote", conn.RemoteAddr())
		} else {
			conn.WriteError("ERR authentication failed")
			slog.Warn("auth failed (invalid key)", "remote", conn.RemoteAddr())
			_ = conn.Close()
			return
		}
	case cmdHello:
		conn.WriteAny(map[string]any{
			"server":  "mresp",
			"version": "1.0.0",
			"proto":   2,
			"id":      1,
			"mode":    "standalone",
			"role":    "master",
			"modules": []any{},
		})
	case cmdClient:
		conn.WriteString("OK")
	case cmdPing:
		conn.WriteString("PONG")
	case cmdQuit:
		conn.WriteString("OK")
		_ = conn.Close()
	case cmdSet:
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		var ttl time.Duration
		if len(cmd.Args) > 3 {
			for i := 3; i < len(cmd.Args); i++ {
				arg := strings.ToUpper(string(cmd.Args[i]))
				if arg == "EX" && i+1 < len(cmd.Args) {
					secs, err := strconv.Atoi(string(cmd.Args[i+1]))
					if err != nil {
						conn.WriteError("ERR value is not an integer or out of range")
						return
					}
					ttl = time.Duration(secs) * time.Second
					i++
				} else if arg == "PX" && i+1 < len(cmd.Args) {
					msecs, err := strconv.Atoi(string(cmd.Args[i+1]))
					if err != nil {
						conn.WriteError("ERR value is not an integer or out of range")
						return
					}
					ttl = time.Duration(msecs) * time.Millisecond
					i++
				}
			}
		}

		if err := db.SetWithTtl(string(cmd.Args[1]), string(cmd.Args[2]), ttl); err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case cmdSetEx:
		if len(cmd.Args) != 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		secs, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		ttl := time.Duration(secs) * time.Second
		if err := db.SetWithTtl(string(cmd.Args[1]), string(cmd.Args[3]), ttl); err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case cmdSetNX:
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		set, err := db.SetNX(string(cmd.Args[1]), string(cmd.Args[2]))
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else if set {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	case cmdGet:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		res := db.Get(string(cmd.Args[1]))
		if res.IsError() {
			if errors.Is(res.Error(), buntdb.ErrNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError("ERR " + res.Error().Error())
			}
		} else {
			conn.WriteBulk([]byte(res.MustGet()))
		}
	case cmdDel:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		var deleted bool
		deleted, err := db.Delete(string(cmd.Args[1]))
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else if deleted {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	case cmdKeys:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		res := db.Keys(string(cmd.Args[1]))
		if res.IsError() {
			conn.WriteError("ERR " + res.Error().Error())
		} else {
			keys := res.MustGet()
			conn.WriteArray(len(keys))
			for _, key := range keys {
				conn.WriteBulk([]byte(key))
			}
		}
	case cmdQueryIndex:
		if len(cmd.Args) < 4 || len(cmd.Args) > 5 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		schemaName := string(cmd.Args[1])
		indexAttr := string(cmd.Args[2])
		filterJSON := string(cmd.Args[3])

		desc := false
		if len(cmd.Args) == 5 {
			order := strings.ToUpper(string(cmd.Args[4]))
			if order == "DESC" {
				desc = true
			} else if order != "ASC" {
				conn.WriteError("ERR invalid order: " + order)
				return
			}
		}

		// Parse the MongoDB-style JSON string into an x.Filter
		filter, err := parseFilter(filterJSON)
		if err != nil {
			conn.WriteError("ERR invalid query: " + err.Error())
			return
		}

		res := db.QueryIndex(schemaName, indexAttr, filter, desc)
		if res.IsError() {
			if errors.Is(res.Error(), buntdb.ErrNotFound) {
				conn.WriteArray(0)
			} else {
				conn.WriteError("ERR " + res.Error().Error())
			}
		} else {
			results := res.MustGet()
			conn.WriteArray(len(results))
			for _, val := range results {
				conn.WriteBulk([]byte(val))
			}
		}
	case cmdQueryKey:
		if len(cmd.Args) < 4 || len(cmd.Args) > 5 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		schemaName := string(cmd.Args[1])
		pattern := string(cmd.Args[2])
		filterJSON := string(cmd.Args[3])

		desc := false
		if len(cmd.Args) == 5 {
			order := strings.ToUpper(string(cmd.Args[4]))
			if order == "DESC" {
				desc = true
			} else if order != "ASC" {
				conn.WriteError("ERR invalid order: " + order)
				return
			}
		}

		filter, err := parseFilter(filterJSON)
		if err != nil {
			conn.WriteError("ERR invalid query: " + err.Error())
			return
		}

		res := db.QueryKey(schemaName, pattern, filter, desc)
		if res.IsError() {
			if errors.Is(res.Error(), buntdb.ErrNotFound) {
				conn.WriteArray(0)
			} else {
				conn.WriteError("ERR " + res.Error().Error())
			}
		} else {
			results := res.MustGet()
			conn.WriteArray(len(results))
			for _, val := range results {
				conn.WriteBulk([]byte(val))
			}
		}
	case cmdPublish:
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
	case cmdSubscribe, cmdPSubscribe:
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		command := strings.ToLower(string(cmd.Args[0]))
		for i := 1; i < len(cmd.Args); i++ {
			if command == cmdPSubscribe {
				ps.Psubscribe(conn, string(cmd.Args[i]))
			} else {
				ps.Subscribe(conn, string(cmd.Args[i]))
			}
		}
	}
}

func stop() error {
	serverMu.Lock()
	ln := serverListener
	serverListener = nil
	serverMu.Unlock()

	shutdownMu.Lock()
	sigCh := shutdownSignalCh
	doneCh := shutdownDoneCh
	shutdownSignalCh = nil
	shutdownDoneCh = nil
	shutdownMu.Unlock()

	if sigCh != nil {
		signalStopFn(sigCh)
	}
	if doneCh != nil {
		close(doneCh)
	}

	var err error
	if ln != nil {
		err = ln.Close()
		// Sleep a bit more to ensure TCP TIME_WAIT is not an issue locally during rapid test restarts.
		time.Sleep(50 * time.Millisecond)
	}

	globalMu.Lock()
	dbToClose := currentDB
	currentDB = nil
	globalMu.Unlock()

	if dbToClose != nil {
		if closeErr := dbToClose.Close(); closeErr != nil && err == nil {
			err = closeErr
		} else if closeErr != nil {
			slog.Warn("failed to close storage", "error", closeErr)
		}
	}

	srvOnce = sync.Once{}
	return err
}

// Start initializes and starts the Redis-compatible server on the given address.
// This function returns immediately after the server has successfully started binding
// to the port, as the server runs in a background goroutine.
// The service itself is long-running and will remain active until interrupted by
// a system signal (SIGINT/SIGTERM) or a programmatic shutdown.
func Start(address string, maxConn int, persistent bool, schemas ...storage.Schema) storage.DB {
	watchShutdownSignals()

	srvOnce.Do(func() {
		db := storage.Open(persistent, schemas...)
		if db == nil {
			slog.Error("failed to open storage")
			if exitFn := getOsExitFn(); exitFn != nil {
				exitFn(1)
			}
			return
		}

		globalMu.Lock()
		currentDB = db
		globalMu.Unlock()

		ensureExternalAuthKey()

		resolvedAddr := address
		if resolvedAddr == "" {
			resolvedAddr = privateAddr
		}
		if maxConn < 1 {
			maxConn = 1
		}
		externalMaxConns = maxConn

		slog.Info("generated internal bootstrap token")
		var ps redcon.PubSub

		startedCh := make(chan struct{})

		go func() {
			listenFn := getListenAndServeFn()
			err := listenFn(resolvedAddr,
				func(conn redcon.Conn, cmd redcon.Command) {
					handleCommand(conn, cmd, db, &ps)
				},
				acceptCon,
				closeCon,
			)

			if err != nil && !isServerShutdownErr(err) {
				slog.Error("resp server stopped", "error", err)
				if exitFn := getOsExitFn(); exitFn != nil {
					exitFn(1)
				}
			} else if err != nil {
				slog.Info("resp server stopped")
			}
			close(startedCh)
		}()

		// Wait for server to bind or error
		time.Sleep(10 * time.Millisecond)
	})

	globalMu.RLock()
	db := currentDB
	globalMu.RUnlock()

	return db
}
