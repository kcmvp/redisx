package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kcmvp/redisx/internal"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/x"
	"github.com/samber/lo"
	"github.com/tidwall/redcon"
)

func printBootstrapAuthBanner(appAuth, ctrlAuth string) {
	appPort := defaultAppPort
	ctrlPort := defaultCtrlPort
	if _, configured, curr := getEffectivePort(portRoleApp); configured {
		appPort = curr
	}
	if _, configured, curr := getEffectivePort(portRoleCtrl); configured {
		ctrlPort = curr
	}
	width := 80
	bar := strings.Repeat("═", width)
	head := "REDISX AUTO-GENERATED CREDENTIALS  (BOOTSTRAP — FIRST RUN ONLY)"
	headPadL := (width - len(head)) / 2
	if headPadL < 0 {
		headPadL = 0
	}
	headPadR := width - len(head) - headPadL
	if headPadR < 0 {
		headPadR = 0
	}
	save1 := "SAVE THESE NOW."
	save2 := "They are stored ONLY under the _auth_ namespace"
	save3 := "and will NOT be reprinted on subsequent restarts."
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n%s\n", bar)
	fmt.Fprintf(&sb, "%s%s%s\n", strings.Repeat(" ", headPadL), head, strings.Repeat(" ", headPadR))
	fmt.Fprintf(&sb, "  app   : %s    # redisx -p %d -a %s\n", appAuth, appPort, appAuth)
	fmt.Fprintf(&sb, "  ctrl  : %s    # redisx -p %d -a %s\n", ctrlAuth, ctrlPort, ctrlAuth)
	fmt.Fprintf(&sb, "  %s\n", save1)
	fmt.Fprintf(&sb, "  %s\n", save2)
	fmt.Fprintf(&sb, "  %s\n", save3)
	fmt.Fprintf(&sb, "%s\n", bar)
	_, _ = fmt.Fprint(os.Stdout, sb.String())
}

var (
	cmdAuth        = proto.LowerCmdAuth
	cmdHello       = proto.LowerCmdHello
	cmdPing        = proto.LowerCmdPing
	cmdQuit        = proto.LowerCmdQuit
	cmdSet         = proto.LowerCmdSet
	cmdSetEx       = proto.LowerCmdSetEx
	cmdGet         = proto.LowerCmdGet
	cmdSetNX       = proto.LowerCmdSetNX
	cmdDel         = proto.LowerCmdDel
	cmdKeys        = proto.LowerCmdKeys
	cmdPublish     = proto.LowerCmdPublish
	cmdSubscribe   = proto.LowerCmdSubscribe
	cmdPSubscribe  = proto.LowerCmdPSubscribe
	cmdClient      = proto.LowerCmdClient
	cmdUpdate      = proto.LowerCmdUpdate
	cmdSearchIndex = proto.LowerCmdSearchIndex
	cmdSearchKey   = proto.LowerCmdSearchKey
	cmdRegSch      = proto.LowerCmdRegisterSchema
	cmdDropSch     = proto.LowerCmdDropSchema
	cmdRegIdx      = proto.LowerCmdRegisterIndex
	cmdDropIdx     = proto.LowerCmdDropIndex
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

var supportedCommandList = func() string {
	keys := lo.Keys(commandRegistry)
	sort.Strings(keys)
	return strings.Join(keys, ",")
}()

var (
	internalAuthKey = internal.AuthKey()

	serverMu          sync.Mutex
	serverListeners   = map[portRole]net.Listener{}
	shutdownOnce      sync.Once
	shutdownMu        sync.Mutex
	globalMu         sync.RWMutex
	listenAndServeFn = listenAndServeWithStop
	osExitFn         = os.Exit
	currentDB        *DB
	srvOnce          sync.Once
)

// StartWith boots a redisx dual-port server from a Go-native Config
// struct and eagerly registers any passed doc schemas before listeners
// accept traffic. It is the single Go-API counterpart to Start (which loads
// redisx.yaml from disk).
//
// Config is always run through Config.validate: defaults are populated,
// security gates (equal auth / non-loopback ctrl / port range / duplicate
// ports) trigger a hard exit, and the database path is created and probed
// for writability before any listeners open. Passing nil is equivalent to
// the empty Config{} (pure system defaults).
//
// AUTH bootstrap flow — SSoT lives ONLY inside `_auth_:app` + `_auth_:ctrl`
// two KV entries on the disk layer of the DB itself. There is NO per-process
// duplicate state: every AUTH gate round-trips to the storage layer via a
// buntdb View tx (BuntDB is mmap-backed; read cost is O(log n) in memory):
//
//   - cfg.App.Auth / cfg.Ctrl.Auth EMPTY (zero-value default)
//     → BootstrapAuth(seedApp="", seedCtrl="") is called.
//     · Both slots MISSING → generate 2× crypto/rand 128-bit hex pair, write
//     them atomically inside a single BuntDB Update tx using SETNX-style
//     presence-check. firstBoot=true ONLY when our write actually went
//     through; if another caller won the race, reload winner's values and
//     continue silently. Banner is printed ONCE.
//     · Both slots PRESENT → silent load, zero logs.
//     · Exactly ONE slot present → hard FATAL "_auth_ namespace corrupted"
//     requiring operator intervention (delete partial key or wipe db file).
//   - cfg.App.Auth / cfg.Ctrl.Auth set explicitly (yaml or StartWith)
//     → BootstrapAuth's seed parameters ALWAYS win. Non-empty seeds are
//     written unconditionally to their respective storage slots, with a
//     slog.Info when an existing DB value is being overwritten. If the
//     resolved pair would end up equal after merge → hard error.
//
// On boot, redisx automatically rebuilds its BuntDB index registry by
// scanning all "_idx_:*" meta keys stored on disk and in the volatile
// mem-layer. Indexes are NO LONGER declared as Go parameters passed to any
// Start variant; they must be created via the ctrl CLI (regidx command).
//
// Example (harness-style direct embed):
//
//	cfg := &server.Config{
//	    App: server.AppConfig{Bind: "127.0.0.1", Port: 7379},
//	    Ctrl: server.CtrlConfig{Bind: "127.0.0.1", Port: 7381, DangerBindAny: true},
//	    DataPath: "/tmp/redisx.test.db",
//	}
//	db := server.StartWith(cfg, UserDoc(""))
//	_ = db
//	defer server.Stop()
func StartWith(cfg *Config, schemas ...x.Schema) *DB {
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
	ctrlAddr := cfg.Ctrl.Addr()
	recordEffectivePort(portRoleApp, cfg.App.Port)
	recordEffectivePort(portRoleCtrl, cfg.Ctrl.Port)

	watchShutdownSignals()
	var db *DB
	srvOnce.Do(func() {
		opened, ok := setupDB(cfg.DataPath, schemas)
		if !ok {
			return
		}
		db = opened
	})
	if db == nil {
		globalMu.RLock()
		db = currentDB
		globalMu.RUnlock()
	}
	if db != nil {
		finalApp, finalCtrl, anyGenerated, berr := db.bootstrapAuth(cfg.App.Auth, cfg.Ctrl.Auth)
		if berr != nil {
			slog.Error("redisx startup failed", "phase", "bootstrap_auth", "error", berr)
			if exitFn := getOsExitFn(); exitFn != nil {
				exitFn(1)
			}
			return nil
		}
		if finalApp == "" || finalCtrl == "" {
			slog.Error("redisx startup failed", "phase", "bootstrap_auth_final",
				"error", "resolved AUTH pair contains empty string; refusing to start")
			if exitFn := getOsExitFn(); exitFn != nil {
				exitFn(1)
			}
			return nil
		}
		appAuthLabel := lo.Ternary(cfg.App.Auth != "", "explicit", lo.Ternary(anyGenerated && cfg.App.Auth == "", "generated", "persisted"))
		ctrlAuthLabel := lo.Ternary(cfg.Ctrl.Auth != "", "explicit", lo.Ternary(anyGenerated && cfg.Ctrl.Auth == "", "generated", "persisted"))
		slog.Info("redisx dual-port config",
			"app_addr", appAddr,
			"ctrl_addr", ctrlAddr,
			"app_auth", appAuthLabel,
			"ctrl_auth", ctrlAuthLabel,
			"doc_schemas", len(schemas),
		)
		if anyGenerated {
			printBootstrapAuthBanner(finalApp, finalCtrl)
		}
		internal.SetAddr(ctrlAddr, appAddr)
		var ps redcon.PubSub
		go bootListener(portRoleApp, appAddr, db, &ps)
		go bootListener(portRoleCtrl, ctrlAddr, db, &ps)
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
// Ctrl 7381 on 127.0.0.1, and database file at ~/.redisx/redisx.db.
//
// To supply a Config struct directly (e.g. from tests or harnesses) instead
// of loading a file, use StartWith.
func Start(schemas ...x.Schema) *DB {
	cfg, err := LoadConfig("redisx.yaml")
	if err != nil {
		slog.Error("redisx startup failed", "phase", "load_config", "error", err)
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return nil
	}
	return StartWith(cfg, schemas...)
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

	effPortMu.Lock()
	clear(effPorts)
	clear(effPortsSet)
	effPortMu.Unlock()

	srvOnce = sync.Once{}
	internal.ResetAddrs()
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

func closeCon(conn redcon.Conn, err error, db *DB) {
	clearConnPortRole(conn)

	ctx := conn.Context()
	var prevAuth string
	if ctx != nil {
		prevAuth, _ = ctx.(string)
	}

	releaseAuthConn(prevAuth, db)

	authStateMu.Lock()
	current := authKeyConnCounts[prevAuth]
	authStateMu.Unlock()

	role := connPortRole(conn)
	if err != nil {
		slog.Info("connection closed", "port_role", role.string(), "remote", conn.RemoteAddr(), "auth_key", prevAuth, "active", current, "error", err)
	} else {
		slog.Info("connection closed", "port_role", role.string(), "remote", conn.RemoteAddr(), "auth_key", prevAuth, "active", current)
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

func handleCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if db == nil {
		conn.WriteError("ERR storage not initialized")
		return
	}

	cmdName := strings.ToLower(string(cmd.Args[0]))

	if gate1_NoAuthNsAccessOnAppPort(conn, cmd) {
		return
	}
	if gate2_AuthKeyMatch(conn, cmdName, db) {
		return
	}
	if gate3_CommandByPortRole(conn, cmdName) {
		return
	}
	if gate4_SourceAndMTLS(conn, cmdName) {
		return
	}
	// Note: per-command argc validation lives inside each handler. Registry
	// commands (REGSCH/REGIDX/DROPSCH/DROPIDX) are ordinary commands with
	// exactly the same argc discipline as SET/GET/DEL.

	if cmdName != cmdAuth && cmdName != cmdClient {
		connAuthKey, _ := conn.Context().(string)
		if conn.Context() == nil || connAuthKey == "" {
			appValues, ctrlValues, _, _, err := loadAllAuthPortKeys(db)
			role := connPortRole(conn)
			var authRequired bool
			switch {
			case err != nil:
				// If storage read fails we fail-closed (require AUTH) so
				// unauthenticated clients can't slip through on transient
				// storage I/O hiccups.
				authRequired = true
			case role == portRoleCtrl:
				authRequired = len(ctrlValues) > 0
			case role == portRoleApp:
				authRequired = len(appValues) > 0
			default:
				authRequired = len(appValues) > 0 || len(ctrlValues) > 0
			}
			if authRequired {
				if cmdName == cmdHello || cmdName == cmdPing || cmdName == cmdQuit {
					conn.WriteError("NOAUTH authentication required")
					slog.Warn("unauthenticated handshake/status command on port that requires AUTH",
						"remote", conn.RemoteAddr(), "cmd", cmdName, "port_role", role.string(), "storage_error", err)
					return
				}
				conn.WriteError("NOAUTH authentication required")
				slog.Warn("unauthenticated command attempt on port that requires AUTH",
					"remote", conn.RemoteAddr(), "cmd", cmdName, "port_role", role.string(), "storage_error", err)
				_ = conn.Close()
				return
			}
		}
	}

	if handler, ok := commandRegistry[cmdName]; ok {
		handler(conn, cmd, db, ps)
	} else {
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "' | supported: " + supportedCommandList)
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
	slog.Info("starting listener", "port_role", role.string(), "addr", address)
	err := listenFn(role, address,
		func(conn redcon.Conn, cmd redcon.Command) {
			handleCommand(conn, cmd, db, ps)
		},
		acceptFn,
		func(conn redcon.Conn, closeErr error) {
			closeCon(conn, closeErr, db)
		},
	)
	if err != nil && !isServerShutdownErr(err) {
		slog.Error("redisx listener stopped", "port_role", role.string(), "error", err)
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
	} else if err != nil {
		slog.Info("redisx listener stopped", "port_role", role.string())
	}
}
