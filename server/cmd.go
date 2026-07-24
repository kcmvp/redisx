package server

import (
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/kcmvp/redisx/storage"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

func authCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
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
}

func helloCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	conn.WriteAny(map[string]any{
		"server":  "mresp",
		"version": "1.0.0",
		"proto":   2,
		"id":      1,
		"mode":    "standalone",
		"role":    "master",
		"modules": []any{},
	})
}

func clientCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	conn.WriteString("OK")
}

func pingCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	conn.WriteString("PONG")
}

func quitCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	conn.WriteString("OK")
	_ = conn.Close()
}

func setCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
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
}

func setExCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
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
}

func setNxCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
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
}

func getCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
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
}

func delCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
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
}

func keysCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
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
}

func publishCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
}

func subscribeCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	for i := 1; i < len(cmd.Args); i++ {
		ps.Subscribe(conn, string(cmd.Args[i]))
	}
}

func pSubscribeCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	for i := 1; i < len(cmd.Args); i++ {
		ps.Psubscribe(conn, string(cmd.Args[i]))
	}
}
