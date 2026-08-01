package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kcmvp/redisx/internal"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/x"
	"github.com/kcmvp/redisx/x/contract"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

const (
	privateAddr        = "127.0.0.1:6380"
	unlimitedAuthConns = -1
	cmdAuth            = "auth"
	cmdHello           = "hello"
	cmdPing            = "ping"
	cmdQuit            = "quit"
	cmdSet             = "set"
	cmdSetEx           = "setex"
	cmdGet             = "get"
	cmdSetNX           = "setnx"
	cmdDel             = "del"
	cmdKeys            = "keys"
	cmdPublish         = "publish"
	cmdSubscribe       = "subscribe"
	cmdPSubscribe      = "psubscribe"
	cmdClient          = "client"
)

var (
	cmdUpdate      = strings.ToLower(proto.CmdUpdate)
	cmdSearchIndex = strings.ToLower(proto.CmdSearchIndex)
	cmdSearchKey   = strings.ToLower(proto.CmdSearchKey)
)

type commandHandler func(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub)

var commandRegistry = map[string]commandHandler{
	cmdAuth:       authCommand,
	cmdHello:      helloCommand,
	cmdClient:     clientCommand,
	cmdPing:       pingCommand,
	cmdQuit:       quitCommand,
	cmdSet:        setCommand,
	cmdSetEx:      setExCommand,
	cmdSetNX:      setNxCommand,
	cmdGet:        getCommand,
	cmdDel:        delCommand,
	cmdKeys:       keysCommand,
	cmdPublish:    publishCommand,
	cmdSubscribe:  subscribeCommand,
	cmdPSubscribe: pSubscribeCommand,
	// x commands
	cmdUpdate:      updateCommand,
	cmdSearchIndex: searchIndexCommand,
	cmdSearchKey:   searchKeyCommand,
}

var (
	// internalAuthKey is generated per-process and used only for internal bootstrap auth.
	internalAuthKey = internal.AuthKey()

	authStateMu       sync.Mutex
	authKeyMaxConns   = map[string]int{}
	authKeyConnCounts = map[string]int{}
	serverMu          sync.Mutex
	serverListener    net.Listener
	shutdownOnce      sync.Once
	shutdownMu        sync.Mutex
	shutdownSignalCh  chan os.Signal
	shutdownDoneCh    chan struct{}

	globalMu         sync.RWMutex
	listenAndServeFn = listenAndServeWithStop
	signalNotifyFn   = signal.Notify
	signalStopFn     = signal.Stop
	osExitFn         = os.Exit
	currentDB        *DB
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

func authLimitStoreKey(key string) string {
	return contract.AuthKeyPrefix + contract.StorageKeySeparator + key
}

func loadAuthKeyLimits(db *DB) error {
	limits := map[string]int{}

	keysRes := db.Keys(contract.AuthKeyPrefix + contract.StorageKeySeparator + "*")
	if keysRes.IsError() {
		return keysRes.Error()
	}

	for _, storeKey := range keysRes.MustGet() {
		key := strings.TrimPrefix(storeKey, contract.AuthKeyPrefix+contract.StorageKeySeparator)
		valRes := db.Get(storeKey)
		if valRes.IsError() {
			if errors.Is(valRes.Error(), buntdb.ErrNotFound) {
				slog.Info("skip expired auth key limit while loading", "auth_key", key)
				continue
			}
			return valRes.Error()
		}

		limit, err := strconv.Atoi(valRes.MustGet())
		if err != nil {
			slog.Info("skip unavailable auth key limit while loading", "auth_key", key)
			continue
		}
		limits[key] = limit
	}

	authStateMu.Lock()
	authKeyMaxConns = limits
	authKeyConnCounts = map[string]int{}
	authStateMu.Unlock()
	return nil
}

func refreshAuthLimit(db *DB, key string) (int, bool, error) {
	if key == internalAuthKey {
		return unlimitedAuthConns, true, nil
	}

	res := db.Get(authLimitStoreKey(key))
	if res.IsError() {
		if errors.Is(res.Error(), buntdb.ErrNotFound) {
			authStateMu.Lock()
			delete(authKeyMaxConns, key)
			authStateMu.Unlock()
			return 0, false, nil
		}
		return 0, false, res.Error()
	}

	limit, err := strconv.Atoi(res.MustGet())
	if err != nil {
		authStateMu.Lock()
		delete(authKeyMaxConns, key)
		authStateMu.Unlock()
		slog.Info("treat unavailable auth key limit as expired", "auth_key", key)
		return 0, false, nil
	}

	authStateMu.Lock()
	authKeyMaxConns[key] = limit
	authStateMu.Unlock()
	return limit, true, nil
}

func releaseAuthConn(key string) {
	if key == "" || key == internalAuthKey {
		return
	}

	authStateMu.Lock()
	defer authStateMu.Unlock()

	if current := authKeyConnCounts[key]; current > 1 {
		authKeyConnCounts[key] = current - 1
	} else {
		delete(authKeyConnCounts, key)
	}
}

func acquireAuthConn(db *DB, key string) error {
	if key == internalAuthKey {
		return nil
	}

	limit, ok, err := refreshAuthLimit(db, key)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("auth key not found")
	}
	if limit != unlimitedAuthConns && limit < 0 {
		return fmt.Errorf("invalid auth limit for key %s", key)
	}

	authStateMu.Lock()
	defer authStateMu.Unlock()

	if limit != unlimitedAuthConns && authKeyConnCounts[key] >= limit {
		return fmt.Errorf("auth key connection limit exceeded")
	}

	authKeyConnCounts[key]++
	return nil
}

func acceptCon(conn redcon.Conn) bool {
	conn.SetContext("")
	return true
}

// PrivateIPs returns all non-loopback private IPs on the current host.
func PrivateIPs() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	return collectPrivateIPs(addrs), nil
}

func collectPrivateIPs(addrs []net.Addr) []string {
	seen := map[string]struct{}{}
	ips := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		default:
			continue
		}

		if ip == nil || ip.IsLoopback() || !ip.IsPrivate() {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
		}

		s := ip.String()
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		ips = append(ips, s)
	}

	sort.Strings(ips)
	return ips
}

func closeCon(conn redcon.Conn, err error) {
	ctx := conn.Context()
	var prevAuth string
	if ctx != nil {
		prevAuth, _ = ctx.(string)
	}

	releaseAuthConn(prevAuth)

	authStateMu.Lock()
	current := authKeyConnCounts[prevAuth]
	authStateMu.Unlock()

	if err != nil {
		slog.Info("connection closed", "remote", conn.RemoteAddr(), "auth_key", prevAuth, "active", current, "error", err)
	} else {
		slog.Info("connection closed", "remote", conn.RemoteAddr(), "auth_key", prevAuth, "active", current)
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

func handleCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
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

	if handler, ok := commandRegistry[cmdName]; ok {
		handler(conn, cmd, db, ps)
	} else {
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
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

	authStateMu.Lock()
	authKeyMaxConns = map[string]int{}
	authKeyConnCounts = map[string]int{}
	authStateMu.Unlock()

	srvOnce = sync.Once{}
	return err
}

// Start initializes and starts the Redis-compatible server on the given
// address.
//
// redisx always opens two underlying BuntDB instances:
//   - one primary layer from dbPath
//   - one dedicated memory-only layer for keys prefixed with "_m_"
//
// dbPath configures only the primary layer. Use one real database file path
// such as "/tmp/redisx.db" for on-disk persistence. Missing parent
// directories are created automatically, and the database file itself is
// created on first open when it does not already exist. Directory paths are
// rejected. The special value ":memory:" is also rejected because redisx
// already has a dedicated memory-only layer for "_m_" keys.
//
// This function returns immediately after the server has successfully started
// binding to the port, as the server runs in a background goroutine. The
// service itself is long-running and will remain active until interrupted by a
// system signal (SIGINT/SIGTERM) or a programmatic shutdown.
func Start(address string, dbPath string, indexes ...x.Index) *DB {
	watchShutdownSignals()

	srvOnce.Do(func() {
		db := openDB(dbPath)
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

		if err := loadAuthKeyLimits(db); err != nil {
			slog.Error("failed to load auth key limits", "error", err)
			_ = db.Close()
			globalMu.Lock()
			currentDB = nil
			globalMu.Unlock()
			if exitFn := getOsExitFn(); exitFn != nil {
				exitFn(1)
			}
			return
		}
		if err := db.registerIndexes(indexes...); err != nil {
			slog.Error("failed to register indexes", "error", err)
			_ = db.Close()
			globalMu.Lock()
			currentDB = nil
			globalMu.Unlock()
			if exitFn := getOsExitFn(); exitFn != nil {
				exitFn(1)
			}
			return
		}

		resolvedAddr := address
		if resolvedAddr == "" {
			resolvedAddr = privateAddr
		}

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
