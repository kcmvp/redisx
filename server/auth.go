package server

import (
	"log/slog"
	"strings"
	"sync"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

type portRole uint8

const (
	portRoleUnknown portRole = iota
	portRoleApp
	portRoleCtrl
)

func (p portRole) string() string {
	switch p {
	case portRoleApp:
		return "app"
	case portRoleCtrl:
		return "ctrl"
	}
	return "unknown"
}

var (
	connRoleMu  sync.RWMutex
	connRoleMap = map[redcon.Conn]portRole{}
)

func setConnPortRole(conn redcon.Conn, role portRole) {
	if conn == nil {
		return
	}
	connRoleMu.Lock()
	connRoleMap[conn] = role
	connRoleMu.Unlock()
}

func clearConnPortRole(conn redcon.Conn) {
	if conn == nil {
		return
	}
	connRoleMu.Lock()
	delete(connRoleMap, conn)
	connRoleMu.Unlock()
}

func connPortRole(conn redcon.Conn) portRole {
	if conn == nil {
		return portRoleCtrl
	}
	connRoleMu.RLock()
	r, ok := connRoleMap[conn]
	connRoleMu.RUnlock()
	if !ok || r == portRoleUnknown {
		return portRoleCtrl
	}
	return r
}

// loadAllAuthPortKeys scans the _auth_ namespace for all account slots that
// follow the `_auth_:app_N` / `_auth_:ctrl_N` naming convention (N zero or
// more decimal digits — account sequence IDs). It returns:
//
//   - `appValues`: all VALUES stored under `_auth_:app_*` slots → AUTH keys
//     allowed on the app data port.
//   - `ctrlValues`: same for `_auth_:ctrl_*` slots → AUTH keys allowed on the
//     ctrl admin port.
//   - `defaultApp` / `defaultCtrl`: the values specifically under the default
//     account-0 keys (`_auth_:app_0` / `_auth_:ctrl_0`) — used for the startup
//     banner and the WRONGPASS diagnostic hint.
//
// `_auth_:ext-<name>` keys (external per-key connection limits) are NOT part
// of either set; they continue to flow through the separate refreshAuthLimit
// path with per-key conn counting.
func loadAllAuthPortKeys(db *DB) (appValues, ctrlValues map[string]struct{}, defaultApp, defaultCtrl string, err error) {
	if db == nil || db.disk == nil {
		return nil, nil, "", "", nil
	}
	appPrefix := naming.AuthStorageKey("app_")
	ctrlPrefix := naming.AuthStorageKey("ctrl_")
	app0 := naming.AuthStorageKey(installedAppAuth)
	ctrl0 := naming.AuthStorageKey(installedCtrlAuth)
	appValues = map[string]struct{}{}
	ctrlValues = map[string]struct{}{}
	terr := db.disk.View(func(tx *buntdb.Tx) error {
		return tx.Ascend("", func(key, value string) bool {
			switch {
			case strings.HasPrefix(key, appPrefix):
				appValues[value] = struct{}{}
				if key == app0 {
					defaultApp = value
				}
			case strings.HasPrefix(key, ctrlPrefix):
				ctrlValues[value] = struct{}{}
				if key == ctrl0 {
					defaultCtrl = value
				}
			}
			return true
		})
	})
	if terr != nil {
		return nil, nil, "", "", terr
	}
	return appValues, ctrlValues, defaultApp, defaultCtrl, nil
}

// gate2_AuthKeyMatch rejects connections that have already issued a successful
// AUTH (gate #1 lets the AUTH command pass) but whose provided key is NOT
// valid for the port they connected to.
//
// Rules:
//   - IPC internal key: always pass, any port.
//   - app port: only AUTH keys whose value was stored under ANY `_auth_:app_N`
//     slot are accepted. Even if the key IS a valid ctrl key, further cmds
//     on the app port are rejected with WRONGPASS.
//   - ctrl port: symmetric — only values under `_auth_:ctrl_N` slots accepted.
func gate2_AuthKeyMatch(conn redcon.Conn, cmdName string, db *DB) (reject bool) {
	cmdLower := cmdName
	if len(cmdName) > 0 {
		cmdLower = toLowerInPlace(cmdName)
	}
	switch cmdLower {
	case proto.LowerCmdAuth, proto.LowerCmdHello, proto.LowerCmdClient:
		return false
	}
	appValues, ctrlValues, defaultApp, defaultCtrl, err := loadAllAuthPortKeys(db)
	if err != nil {
		slog.Warn("Gate2 reject: failed to read _auth_ port-role keys from storage",
			"port_role", connPortRole(conn).string(), "remote", conn.RemoteAddr(), "cmd", cmdName, "error", err)
		conn.WriteError("ERR internal auth state unavailable")
		return true
	}
	configured := len(appValues) > 0 || len(ctrlValues) > 0
	if !configured {
		return false
	}
	ctx := conn.Context()
	var authedKey string
	if ctx != nil {
		authedKey, _ = ctx.(string)
	}
	if authedKey != "" && authedKey == internalAuthKey {
		return false
	}
	if authedKey == "" {
		return false
	}
	// If the key the client used for AUTH does NOT match ANY port-role-bound
	// value set (i.e. it's an external limit-based key or a legacy key), we
	// don't enforce role binding via gate2 (role binding is for _auth_:app_N /
	// ctrl_N values only). In that case let the request through and let later
	// gates (command-word, mtls, internal write-guard) enforce if needed.
	_, inApp := appValues[authedKey]
	_, inCtrl := ctrlValues[authedKey]
	if !inApp && !inCtrl {
		return false
	}
	role := connPortRole(conn)
	switch role {
	case portRoleApp:
		if !inApp {
			slog.Warn("Gate2 reject: app-port conn authenticated with wrong-role key",
				"port_role", role.string(), "remote", conn.RemoteAddr(), "cmd", cmdName)
			msg := "WRONGPASS invalid or wrong auth key for app port"
			if defaultCtrl != "" && authedKey == defaultCtrl {
				msg += " (looks like you supplied the ctrl_0 key to the app port)"
			}
			conn.WriteError(msg)
			return true
		}
	case portRoleCtrl:
		if !inCtrl {
			slog.Warn("Gate2 reject: ctrl-port conn authenticated with wrong-role key",
				"port_role", role.string(), "remote", conn.RemoteAddr(), "cmd", cmdName)
			msg := "WRONGPASS invalid or wrong auth key for ctrl port"
			if defaultApp != "" && authedKey == defaultApp {
				msg += " (looks like you supplied the app_0 key to the ctrl port)"
			}
			conn.WriteError(msg)
			return true
		}
	}
	return false
}

// gate1_NoAuthNsAccessOnAppPort is the #1 filter in the command pipeline.
// If the connection is the app port and the command touches any `_auth_:*`
// key — either as a literal key (Arg1 has the _auth_: prefix) OR via a
// sweeping KEYS/SCAN glob pattern that contains wildcards — the request is
// immediately rejected with "No Privilege".
func gate1_NoAuthNsAccessOnAppPort(conn redcon.Conn, cmd redcon.Command) bool {
	if connPortRole(conn) != portRoleApp {
		return false
	}
	// Case A: literal key command with at least one key argument (GET/SET/DEL/...)
	if len(cmd.Args) >= 2 && strings.HasPrefix(string(cmd.Args[1]), naming.AuthNsPrefix()+naming.StorageKeySeparator()) {
		conn.WriteError("No Privilege")
		return true
	}
	// Case B: KEYS or SCAN pattern commands that could sweep the _auth_
	// namespace (any glob wildcards). App port is data-only, not admin-scan.
	switch len(cmd.Args) {
	case 0, 1:
		return false
	}
	switch toLowerInPlace(string(cmd.Args[0])) {
	case "keys":
		if len(cmd.Args) < 2 {
			return false
		}
		if strings.ContainsAny(string(cmd.Args[1]), "*?[") {
			conn.WriteError("No Privilege")
			return true
		}
	case "scan":
		if len(cmd.Args) < 4 || toLowerInPlace(string(cmd.Args[2])) != "match" {
			return false
		}
		if strings.ContainsAny(string(cmd.Args[3]), "*?[") {
			conn.WriteError("No Privilege")
			return true
		}
	}
	return false
}

func toLowerInPlace(s string) string {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			out := make([]byte, len(s))
			copy(out, s)
			out[i] = c + 32
			for j := i + 1; j < len(s); j++ {
				c2 := out[j]
				if c2 >= 'A' && c2 <= 'Z' {
					out[j] = c2 + 32
				}
			}
			return string(out)
		}
	}
	return s
}

// gate3_CommandByPortRole blocks CTRL-only command words from reaching the
// app-port listener. Fail-closed: allowlist map for app-port (13 handshake +
// shared biz cmds), ctrl-port = pass everything.
//
// MVP before D3 dual-port: default conn role = ctrl-port → every cmd passes.
func gate3_CommandByPortRole(conn redcon.Conn, cmdName string) (reject bool) {
	role := connPortRole(conn) // ctrl/app; MVP defaults to "ctrl"
	if role == portRoleCtrl {
		return false
	}
	// app-port → only allow allowlist. Build once at init time from proto.
	if !appPortCmdAllowlist[cmdName] {
		slog.Warn("app-port attempted ctrl-only command (Gate1 reject)",
			"remote", conn.RemoteAddr(), "cmd", cmdName)
		conn.WriteError("ERR No Privilege: '" + cmdName + "' is a Meta Management command, only allowed on the ctrl port. Connect via the ctrl port and use --ctrl-auth, or run equivalent data operations on this app port.")
		return true
	}
	return false
}

// gate4_SourceAndMTLS validates Caddy forward PROXY v2 real source IP +
// cert OU/CN. MVP structure only: always returns reject=false until D3+mTLS is wired.
func gate4_SourceAndMTLS(conn redcon.Conn, cmdName string) (reject bool) {
	return false
}

// appPortCmdAllowlist is the set of command words (lowercase) allowed through
// Gate 1 when a connection's role == portRoleApp. Built once at init().
// Commands intentionally omitted from here are rejected on the app port
// (fail-closed security).
var appPortCmdAllowlist = map[string]bool{}

func init() {
	combined := []string{
		// Handshake & base transport commands
		proto.LowerCmdAuth, proto.LowerCmdHello, proto.LowerCmdClient, proto.LowerCmdPing, proto.LowerCmdQuit, proto.LowerCmdSelect, proto.LowerCmdCommand, proto.LowerCmdReset,
		proto.LowerCmdSubscribe, proto.LowerCmdUnsubscribe, proto.LowerCmdPSubscribe, proto.LowerCmdPUnsubscribe, proto.LowerCmdPublish,
		// Shared business commands: KV, KV-extensions
		proto.LowerCmdSet, proto.LowerCmdSetEx, proto.LowerCmdSetNX, proto.LowerCmdGet, proto.LowerCmdDel, proto.LowerCmdExists,
		proto.LowerCmdSearchKey, proto.LowerCmdSearchIndex, proto.LowerCmdUpdate,
	}
	for _, c := range combined {
		appPortCmdAllowlist[c] = true
	}
}
