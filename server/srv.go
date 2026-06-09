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
	storage          *buntdb.DB
	userHomeDirFn    = os.UserHomeDir
	signalNotifyFn   = signal.Notify
	signalStopFn     = signal.Stop
	osExitFn         = os.Exit
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
	defer connCountMu.Unlock()

	if activeExternalConns >= externalMaxConns {
		slog.Warn("reject connection", "remote", conn.RemoteAddr(), "active", activeExternalConns, "external_max_conns", externalMaxConns)
		return false
	}

	activeExternalConns++
	conn.SetContext("")
	return true
}

func closeCon(conn redcon.Conn, err error) {
	ctx := conn.Context()
	if ctx == nil {
		return
	}
	prevAuth, _ := ctx.(string)

	connCountMu.Lock()
	if prevAuth != internalAuthKey {
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
		if errors.Is(err, buntdb.ErrNotFound) {
			conn.WriteNull()
		} else if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else {
			conn.WriteBulk([]byte(val))
		}
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
			if errors.Is(e, buntdb.ErrNotFound) {
				_, _, e = tx.Set(string(cmd.Args[1]), string(cmd.Args[2]), nil)
				if e == nil {
					set = true
				}
				return e
			}
			return e
		})
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else if set {
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
		} else if deleted {
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

func startDB() {
	var err error

	globalMu.RLock()
	homeFn := userHomeDirFn
	globalMu.RUnlock()

	homeDir, err := homeFn()
	if err != nil {
		slog.Error("failed to resolve user home directory", "error", err)
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return
	}

	dataDir := filepath.Join(homeDir, ".respx")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		slog.Error("failed to create data directory", "path", dataDir, "error", err)
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return
	}

	dbPath := filepath.Join(dataDir, "data.db")
	storage, err = buntdb.Open(dbPath)
	if err != nil {
		slog.Error("failed to open buntdb", "path", dbPath, "error", err)
		if exitFn := getOsExitFn(); exitFn != nil {
			exitFn(1)
		}
		return
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

// Start initializes and starts the Redis-compatible server on the given address.
// This function returns immediately after the server has successfully started binding
// to the port, as the server runs in a background goroutine.
// The service itself is long-running and will remain active until interrupted by
// a system signal (SIGINT/SIGTERM) or a programmatic shutdown.
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

		startedCh := make(chan struct{})

		go func() {
			close(startedCh)
			listenFn := getListenAndServeFn()
			err := listenFn(resolvedAddr,
				func(conn redcon.Conn, cmd redcon.Command) {
					handleCommand(conn, cmd, storage, &ps)
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
		}()

		<-startedCh
	})
}
