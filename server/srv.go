package server

import (
	"crypto/rand"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kcmvp/indx/internal"
	"github.com/kcmvp/indx/storage"
	"github.com/tidwall/redcon"
)

const (
	privateAddr = "127.0.0.1:6380"

	cmdAuth        = "auth"
	cmdHello       = "hello"
	cmdPing        = "ping"
	cmdQuit        = "quit"
	cmdSet         = "set"
	cmdSetEx       = "setex"
	cmdGet         = "get"
	cmdSetNX       = "setnx"
	cmdDel         = "del"
	cmdKeys        = "keys"
	cmdUpdate      = "update"
	cmdSearchIndex = "searchindex"
	cmdSearchKey   = "searchkey"
	cmdPublish     = "publish"
	cmdSubscribe   = "subscribe"
	cmdPSubscribe  = "psubscribe"
	cmdClient      = "client"
)

type commandHandler func(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub)

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
