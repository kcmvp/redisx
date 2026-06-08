package server

import (
	"crypto/rand"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/kcmvp/respx/internal"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

const (
	privateAddr = "127.0.0.1:6380"

	cmdAuth       = "auth"
	cmdHello      = "hello"
	cmdPing       = "ping"
	cmdQuit       = "quit"
	cmdSet        = "set"
	cmdGet        = "get"
	cmdSetNX      = "setnx"
	cmdDel        = "del"
	cmdPublish    = "publish"
	cmdSubscribe  = "subscribe"
	cmdPSubscribe = "psubscribe"
	cmdClient     = "client"
)

var (
	srvOnce sync.Once

	// internalAuthKey is generated per-process and used only for internal bootstrap auth.
	internalAuthKey = internal.AuthKey()
	// authKey is for external client AUTH and can be configured via environment.
	authKey = ""

	connAuthState    = make(map[string]string)
	authStateMu      sync.RWMutex
	externalMaxConns = 1
	serverMu         sync.Mutex
	serverListener   net.Listener
	shutdownOnce     sync.Once
	shutdownMu       sync.Mutex
	shutdownSignalCh chan os.Signal
	shutdownDoneCh   chan struct{}

	listenAndServeFn = listenAndServeWithStop
	storage          *buntdb.DB
	userHomeDirFn    = os.UserHomeDir
	signalNotifyFn   = signal.Notify
	signalStopFn     = signal.Stop
)

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

// getConnKey returns a unique key for the connection
func getConnKey(conn redcon.Conn) string {
	return conn.RemoteAddr()
}

func getConnState(conn redcon.Conn) (string, bool) {
	connKey := getConnKey(conn)
	authStateMu.RLock()
	defer authStateMu.RUnlock()
	connAuthKey, ok := connAuthState[connKey]
	return connAuthKey, ok
}

// externalConnection counts connections that are not authenticated with internalAuthKey.
// Caller must hold authStateMu (read or write lock).
func externalConnection() int {
	count := 0
	for _, stateKey := range connAuthState {
		if stateKey != internalAuthKey {
			count++
		}
	}
	return count
}

// setConnectionAuthState stores auth state for a connection.
func setConnectionAuthState(conn redcon.Conn, connAuthKey string) {
	connKey := getConnKey(conn)

	authStateMu.Lock()
	defer authStateMu.Unlock()

	connAuthState[connKey] = connAuthKey
}

// clearConnectionAuthState removes authentication state for a connection.
func clearConnectionAuthState(conn redcon.Conn) {
	connKey := getConnKey(conn)

	authStateMu.Lock()
	delete(connAuthState, connKey)
	authStateMu.Unlock()
}

func listenAndServeWithStop(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	serverMu.Lock()
	serverListener = ln
	serverMu.Unlock()

	defer func() {
		serverMu.Lock()
		if serverListener == ln {
			serverListener = nil
		}
		serverMu.Unlock()
	}()

	return redcon.Serve(ln, handler, accept, closed)
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

func handleCommand(conn redcon.Conn, cmd redcon.Command, db *buntdb.DB, ps *redcon.PubSub) {
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
		if connAuthKey, ok := getConnState(conn); !ok || connAuthKey == "" {
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
			setConnectionAuthState(conn, providedKey)
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
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		err := db.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(string(cmd.Args[1]), string(cmd.Args[2]), nil)
			return err
		})
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case cmdGet:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		var val string
		err := db.View(func(tx *buntdb.Tx) error {
			v, e := tx.Get(string(cmd.Args[1]))
			val = v
			return e
		})
		if err != nil {
			if errors.Is(err, buntdb.ErrNotFound) {
				conn.WriteNull()
				return
			}
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteBulk([]byte(val))
	case cmdSetNX:
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		var set bool
		err := db.Update(func(tx *buntdb.Tx) error {
			_, e := tx.Get(string(cmd.Args[1]))
			if e == nil {
				set = false
				return nil
			}
			if !errors.Is(e, buntdb.ErrNotFound) {
				return e
			}
			_, _, e = tx.Set(string(cmd.Args[1]), string(cmd.Args[2]), nil)
			if e != nil {
				return e
			}
			set = true
			return nil
		})
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if set {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	case cmdDel:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		var deleted bool
		err := db.Update(func(tx *buntdb.Tx) error {
			_, e := tx.Delete(string(cmd.Args[1]))
			if errors.Is(e, buntdb.ErrNotFound) {
				deleted = false
				return nil
			}
			if e == nil {
				deleted = true
			}
			return e
		})
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if deleted {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
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

func acceptCon(conn redcon.Conn) bool {
	authStateMu.Lock()
	defer authStateMu.Unlock()

	current := externalConnection()
	if current >= externalMaxConns {
		slog.Warn("reject connection", "remote", conn.RemoteAddr(), "active", current, "external_max_conns", externalMaxConns)
		return false
	}

	connAuthState[getConnKey(conn)] = ""
	return true
}

func closeCon(conn redcon.Conn, err error) {
	clearConnectionAuthState(conn)
	authStateMu.RLock()
	current := externalConnection()
	authStateMu.RUnlock()
	if err != nil {
		slog.Info("connection closed", "remote", conn.RemoteAddr(), "active", current, "error", err)
	} else {
		slog.Info("connection closed", "remote", conn.RemoteAddr(), "active", current)
	}
}

func startDB() {
	var err error

	homeDir, err := userHomeDirFn()
	if err != nil {
		slog.Error("failed to resolve user home directory", "error", err)
		os.Exit(1)
	}

	dataDir := filepath.Join(homeDir, ".respx")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		slog.Error("failed to create data directory", "path", dataDir, "error", err)
		os.Exit(1)
	}

	dbPath := filepath.Join(dataDir, "data.db")
	storage, err = buntdb.Open(dbPath)
	if err != nil {
		slog.Error("failed to open buntdb", "path", dbPath, "error", err)
		os.Exit(1)
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
	}

	if storage != nil {
		if closeErr := storage.Close(); closeErr != nil && err == nil {
			err = closeErr
		} else if closeErr != nil {
			slog.Warn("failed to close buntdb", "error", closeErr)
		}
		storage = nil
	}

	srvOnce = sync.Once{}
	return err
}

func Start(address string, maxConn int) {
	watchShutdownSignals()

	srvOnce.Do(func() {
		startDB()
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
		go func() {
			err := listenAndServeFn(resolvedAddr,
				func(conn redcon.Conn, cmd redcon.Command) {
					handleCommand(conn, cmd, storage, &ps)
				},
				acceptCon,
				closeCon,
			)
			if err != nil && !isServerShutdownErr(err) {
				slog.Error("resp server stopped", "error", err)
				panic(err)
			} else if err != nil {
				slog.Info("resp server stopped")
			}
		}()
	})
}
