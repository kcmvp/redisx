package server

import (
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

	"github.com/kcmvp/redisx/internal"
	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/x"
	"github.com/samber/lo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

const (
	unlimitedAuthConns = -1
)

var (
	cmdAuth        = strings.ToLower(proto.CmdAuth)
	cmdHello       = strings.ToLower(proto.CmdHello)
	cmdPing        = strings.ToLower(proto.CmdPing)
	cmdQuit        = strings.ToLower(proto.CmdQuit)
	cmdSet         = strings.ToLower(proto.CmdSet)
	cmdSetEx       = strings.ToLower(proto.CmdSetEx)
	cmdGet         = strings.ToLower(proto.CmdGet)
	cmdSetNX       = strings.ToLower(proto.CmdSetNX)
	cmdDel         = strings.ToLower(proto.CmdDel)
	cmdKeys        = strings.ToLower(proto.CmdKeys)
	cmdPublish     = strings.ToLower(proto.CmdPublish)
	cmdSubscribe   = strings.ToLower(proto.CmdSubscribe)
	cmdPSubscribe  = strings.ToLower(proto.CmdPSubscribe)
	cmdClient      = strings.ToLower(proto.CmdClient)
	cmdUpdate      = strings.ToLower(proto.CmdUpdate)
	cmdSearchIndex = strings.ToLower(proto.CmdSearchIndex)
	cmdSearchKey   = strings.ToLower(proto.CmdSearchKey)
	cmdRegSch      = strings.ToLower(proto.CmdRegisterSchema)
	cmdDropSch     = strings.ToLower(proto.CmdDropSchema)
	cmdRegIdx      = strings.ToLower(proto.CmdRegisterIndex)
	cmdDropIdx     = strings.ToLower(proto.CmdDropIndex)
)

type commandHandler func(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub)

var commandRegistry = map[string]commandHandler{
	cmdAuth:        authCommand,
	cmdHello:       helloCommand,
	cmdClient:      clientCommand,
	cmdPing:        pingCommand,
	cmdQuit:        quitCommand,
	cmdSet:         setCommand,
	cmdSetEx:       setExCommand,
	cmdSetNX:       setNxCommand,
	cmdGet:         getCommand,
	cmdDel:         delCommand,
	cmdKeys:        keysCommand,
	cmdPublish:     publishCommand,
	cmdSubscribe:   subscribeCommand,
	cmdPSubscribe:  pSubscribeCommand,
	cmdUpdate:      updateCommand,
	cmdSearchIndex: searchIndexCommand,
	cmdSearchKey:   searchKeyCommand,
	cmdRegSch:      regSchemaCommand,
	cmdDropSch:     dropSchemaCommand,
	cmdRegIdx:      regIdxCommand,
	cmdDropIdx:     dropIndexCommand,
}

var (
	internalAuthKey = internal.AuthKey()

	authStateMu       sync.Mutex
	authKeyMaxConns   = map[string]int{}
	authKeyConnCounts = map[string]int{}
	serverMu          sync.Mutex
	serverListeners   = map[portRole]net.Listener{}
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

// StartWithConfig boots a redisx dual-port server from a Go-native Config
// struct and eagerly registers any passed doc schemas before listeners
// accept traffic. It is the single Go-API counterpart to Start (which loads
// redisx.yaml from disk).
//
// Config is always run through Config.validate: defaults are populated,
// security gates (equal auth / non-loopback admin / port range / duplicate
// ports) trigger a hard exit, and the database path is created and probed
// for writability before any listeners open. Passing nil is equivalent to
// the empty Config{} (pure system defaults).
//
// On boot, redisx automatically rebuilds its BuntDB index registry by
// scanning all "_idx_:*" meta keys stored on disk and in the volatile
// mem-layer. Indexes are NO LONGER declared as Go parameters passed to any
// Start variant; they must be created via the admin CLI (regidx command).
//
// Example (harness-style direct embed):
//
//	cfg := &server.Config{
//	    App: server.AppConfig{Bind: "127.0.0.1", Port: 7379},
//	    Admin: server.AdminConfig{Bind: "127.0.0.1", Port: 7381, DangerBindAny: true},
//	    DataPath: "/tmp/redisx.test.db",
//	}
//	db := server.StartWithConfig(cfg, UserDoc(""))
//	_ = db
//	defer server.Stop()
func StartWithConfig(cfg *Config, schemas ...x.Schema) *DB {
	if cfg == nil {
		cfg = &Config{}
	}
	if err := cfg.validate(); err != nil {
		slog.Error(err.Error())
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil
	}

	appAddr := cfg.App.Addr()
	adminAddr := cfg.Admin.Addr()
	appAuthLabel := lo.Ternary(cfg.App.Auth != "", "set", "unset")
	adminAuthLabel := lo.Ternary(cfg.Admin.Auth != "", "set", "unset")
	slog.Info("redisx dual-port config",
		"app_addr", appAddr,
		"admin_addr", adminAddr,
		"app_auth", appAuthLabel,
		"admin_auth", adminAuthLabel,
		"doc_schemas", len(schemas),
	)

	watchShutdownSignals()
	var db *DB
	srvOnce.Do(func() {
		opened, ok := setupDB(cfg.DataPath, schemas)
		if !ok {
			return
		}
		db = opened
	})

	configureAuthKeys(cfg.App.Auth, cfg.Admin.Auth)
	if db == nil {
		globalMu.RLock()
		db = currentDB
		globalMu.RUnlock()
	}
	if db != nil {
		var ps redcon.PubSub
		go bootListener(portRoleApp, appAddr, db, &ps)
		go bootListener(portRoleAdmin, adminAddr, db, &ps)
		time.Sleep(10 * time.Millisecond)
	}
	return db
}

// Start boots a redisx dual-port server using the redisx.yaml configuration
// file located in the current working directory, and eagerly registers any
// passed doc schemas before listeners accept traffic.
//
// Call site idiom (zero-value string alias of your Document types):
//
//	db := server.Start(
//	    UserDoc(""),
//	    OrderDoc(""),
//	)
//
// Schemas are registered exactly once per process lifetime, and written to
// the "_doc_:<storage_ns>" SSoT keys on the matching storage layer (disk or
// in-memory, according to Schema.Mem). Conflicting namespace registrations
// cause a hard process exit with a descriptive error.
//
// When redisx.yaml is missing or unreadable (except YAML syntax errors) the
// zero-config defaults kick in: App 7379 on auto-selected RFC1918 bind,
// Admin 7381 on 127.0.0.1, and database file at ~/.redisx/redisx.db.
//
// To supply a Config struct directly (e.g. from tests or harnesses) instead
// of loading a file, use StartWithConfig.
func Start(schemas ...x.Schema) *DB {
	cfg, err := LoadConfig("redisx.yaml")
	if err != nil {
		slog.Error("redisx startup failed", "phase", "load_config", "error", err)
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil
	}
	return StartWithConfig(cfg, schemas...)
}

func Stop() error {
	serverMu.Lock()
	listeners := make([]net.Listener, 0, len(serverListeners))
	for _, ln := range serverListeners {
		listeners = append(listeners, ln)
	}
	clear(serverListeners)
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
	for _, ln := range listeners {
		if closeErr := ln.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if len(listeners) > 0 {
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

func getOsExitFn() func(int) {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return osExitFn
}

func getListenAndServeFn() func(portRole, string, func(redcon.Conn, redcon.Command), func(redcon.Conn) bool, func(redcon.Conn, error)) error {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return listenAndServeFn
}

func authLimitStoreKey(key string) string {
	return naming.AuthStorageKey(key)
}

func loadAuthKeyLimits(db *DB) error {
	limits := map[string]int{}

	keysRes := db.Keys(naming.AuthStorageGlob())
	if keysRes.IsError() {
		return keysRes.Error()
	}

	for _, storeKey := range keysRes.MustGet() {
		key, _ := naming.StripAuthPrefixIfAuth(storeKey)
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
	appAuth, adminAuth, configured := getAuthConfig()
	if configured {
		if (appAuth != "" && key == appAuth) || (adminAuth != "" && key == adminAuth) {
			return
		}
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
	appAuth, adminAuth, configured := getAuthConfig()
	if configured {
		if (appAuth != "" && key == appAuth) || (adminAuth != "" && key == adminAuth) {
			return nil
		}
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

func closeCon(conn redcon.Conn, err error) {
	clearConnPortRole(conn)

	ctx := conn.Context()
	var prevAuth string
	if ctx != nil {
		prevAuth, _ = ctx.(string)
	}

	releaseAuthConn(prevAuth)

	authStateMu.Lock()
	current := authKeyConnCounts[prevAuth]
	authStateMu.Unlock()

	role := connPortRole(conn)
	if err != nil {
		slog.Info("connection closed", "port_role", role.String(), "remote", conn.RemoteAddr(), "auth_key", prevAuth, "active", current, "error", err)
	} else {
		slog.Info("connection closed", "port_role", role.String(), "remote", conn.RemoteAddr(), "auth_key", prevAuth, "active", current)
	}
}

func listenAndServeWithStop(role portRole, addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	serverMu.Lock()
	serverListeners[role] = ln
	serverMu.Unlock()

	defer func() {
		serverMu.Lock()
		if cur, ok := serverListeners[role]; ok && cur == ln {
			delete(serverListeners, role)
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
	case sig := <-sigCh:
		if sig != nil {
			slog.Info("redisx server caught shutdown signal", "signal", sig.String())
		} else {
			slog.Info("redisx server caught shutdown signal")
		}
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
		go handleShutdownSignals(sigCh, doneCh, Stop)
	})
}

func handleCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if db == nil {
		conn.WriteError("ERR storage not initialized")
		return
	}

	cmdName := strings.ToLower(string(cmd.Args[0]))

	if gate0Auth(conn, cmdName) {
		return
	}
	if gate1CommandWordByPortRole(conn, cmdName) {
		return
	}
	if gate2MTLSAndSourceIP(conn, cmdName) {
		return
	}
	// Note: per-command argc validation lives inside each handler. Registry
	// commands (REGSCH/REGIDX/DROPSCH/DROPIDX) are ordinary commands with
	// exactly the same argc discipline as SET/GET/DEL.

	if cmdName != cmdAuth && cmdName != cmdClient {
		connAuthKey, _ := conn.Context().(string)
		if conn.Context() == nil || connAuthKey == "" {
			appAuth, adminAuth, _ := getAuthConfig()
			role := connPortRole(conn)
			var authRequired bool
			switch role {
			case portRoleAdmin:
				authRequired = adminAuth != ""
			case portRoleApp:
				authRequired = appAuth != ""
			default:
				authRequired = (appAuth != "") || (adminAuth != "")
			}
			if authRequired {
				if cmdName == cmdHello || cmdName == cmdPing || cmdName == cmdQuit {
					conn.WriteError("NOAUTH authentication required")
					slog.Warn("unauthenticated handshake/status command on port that requires AUTH",
						"remote", conn.RemoteAddr(), "cmd", cmdName, "port_role", role.String())
					return
				}
				conn.WriteError("NOAUTH authentication required")
				slog.Warn("unauthenticated command attempt on port that requires AUTH",
					"remote", conn.RemoteAddr(), "cmd", cmdName, "port_role", role.String())
				_ = conn.Close()
				return
			}
		}
	}

	if handler, ok := commandRegistry[cmdName]; ok {
		handler(conn, cmd, db, ps)
	} else {
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	}
}

func setupDB(dbPath string, schemas []x.Schema) (*DB, bool) {
	db := openDB(dbPath)
	if db == nil {
		slog.Error("failed to open storage")
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil, false
	}
	if err := db.loadIndexes(); err != nil {
		slog.Error("failed to load indexes from _idx_:* records", "error", err)
		_ = db.Close()
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil, false
	}
	if err := db.loadDocSpecs(); err != nil {
		slog.Error("failed to load doc specs from _doc_:* records", "error", err)
		_ = db.Close()
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil, false
	}
	if err := loadAuthKeyLimits(db); err != nil {
		slog.Error("failed to load auth key limits", "error", err)
		_ = db.Close()
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil, false
	}
	globalMu.Lock()
	currentDB = db
	globalMu.Unlock()
	if err := db.registerSchemas(schemas...); err != nil {
		slog.Error("failed to register schemas", "error", err)
		_ = db.Close()
		globalMu.Lock()
		currentDB = nil
		globalMu.Unlock()
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil, false
	}
	slog.Info("generated internal bootstrap token")
	return db, true
}

func bootListener(role portRole, address string, db *DB, ps *redcon.PubSub) {
	listenFn := getListenAndServeFn()
	acceptFn := func(conn redcon.Conn) bool {
		setConnPortRole(conn, role)
		conn.SetContext("")
		return true
	}
	slog.Info("starting listener", "port_role", role.String(), "addr", address)
	err := listenFn(role, address,
		func(conn redcon.Conn, cmd redcon.Command) {
			handleCommand(conn, cmd, db, ps)
		},
		acceptFn,
		closeCon,
	)
	if err != nil && !isServerShutdownErr(err) {
		slog.Error("redisx listener stopped", "port_role", role.String(), "error", err)
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
	} else if err != nil {
		slog.Info("redisx listener stopped", "port_role", role.String())
	}
}
