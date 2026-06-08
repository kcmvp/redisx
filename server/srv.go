package server

import (
	"crypto/rand"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/kcmvp/respx/internal"
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

	listenAndServeFn = redcon.ListenAndServe
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

func handleCommand(conn redcon.Conn, cmd redcon.Command, items map[string][]byte, mu *sync.RWMutex, ps *redcon.PubSub) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
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
		mu.Lock()
		items[string(cmd.Args[1])] = cmd.Args[2]
		mu.Unlock()
		conn.WriteString("OK")
	case cmdGet:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		mu.RLock()
		val, ok := items[string(cmd.Args[1])]
		mu.RUnlock()
		if !ok {
			conn.WriteNull()
		} else {
			conn.WriteBulk(val)
		}
	case cmdSetNX:
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		mu.RLock()
		_, ok := items[string(cmd.Args[1])]
		mu.RUnlock()
		if ok {
			conn.WriteInt(0)
			return
		}
		mu.Lock()
		items[string(cmd.Args[1])] = cmd.Args[2]
		mu.Unlock()
		conn.WriteInt(1)
	case cmdDel:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		mu.Lock()
		_, ok := items[string(cmd.Args[1])]
		delete(items, string(cmd.Args[1]))
		mu.Unlock()
		if !ok {
			conn.WriteInt(0)
		} else {
			conn.WriteInt(1)
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

func Start(address string, maxConn int) {
	srvOnce.Do(func() {
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
		var mu sync.RWMutex
		var items = make(map[string][]byte)
		var ps redcon.PubSub
		go func() {
			err := listenAndServeFn(resolvedAddr,
				func(conn redcon.Conn, cmd redcon.Command) {
					handleCommand(conn, cmd, items, &mu, &ps)
				},
				acceptCon,
				closeCon,
			)
			if err != nil {
				slog.Error("resp server stopped", "error", err)
				panic(err)
			}
		}()
	})
}
