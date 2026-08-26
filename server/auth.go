package server

import (
	"log/slog"
	"strings"
	"sync"

	"github.com/kcmvp/redisx/internal/proto"
	"github.com/tidwall/redcon"
)

type portRole uint8

const (
	portRoleUnknown portRole = iota
	portRoleApp
	portRoleCtrl
)

func (p portRole) String() string {
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

	authCfgMu      sync.RWMutex
	activeAppAuth  = ""
	activeCtrlAuth = ""
	authConfigured = false
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

func configureAuthKeys(appAuth, ctrlAuth string) {
	authCfgMu.Lock()
	activeAppAuth = appAuth
	activeCtrlAuth = ctrlAuth
	authConfigured = (appAuth != "") || (ctrlAuth != "")
	authCfgMu.Unlock()
}

func getAuthConfig() (appAuth, ctrlAuth string, configured bool) {
	authCfgMu.RLock()
	defer authCfgMu.RUnlock()
	return activeAppAuth, activeCtrlAuth, authConfigured
}

func gate0Auth(conn redcon.Conn, cmdName string) (reject bool) {
	cmdLower := strings.ToLower(cmdName)
	switch cmdLower {
	case proto.LowerCmdAuth, proto.LowerCmdHello, proto.LowerCmdClient:
		return false
	}
	appAuth, ctrlAuth, configured := getAuthConfig()
	if !configured {
		return false
	}
	role := connPortRole(conn)
	ctx := conn.Context()
	var authedKey string
	if ctx != nil {
		authedKey, _ = ctx.(string)
	}
	// Internal per-process bootstrap auth key (used exclusively by embedded
	// client ↔ embedded server traffic) bypasses the port-role vs. configured
	// user-key match entirely. It is accepted on both App and Ctrl ports
	// unconditionally regardless of whether app/ctrl user keys are set.
	if authedKey != "" && authedKey == internalAuthKey {
		return false
	}
	anyConfigured := (appAuth != "") || (ctrlAuth != "")
	if !anyConfigured || authedKey == "" {
		return false
	}
	switch role {
	case portRoleApp:
		if appAuth == "" {
			return false
		}
		if authedKey != appAuth {
			slog.Warn("Gate0 reject: app-port conn authenticated with wrong key",
				"port_role", role.String(), "remote", conn.RemoteAddr(), "cmd", cmdName)
			conn.WriteError("WRONGPASS invalid password for app port. AUTH with the --auth key, not the --ctrl-auth key, then retry.")
			return true
		}
	case portRoleCtrl:
		if ctrlAuth == "" {
			return false
		}
		if authedKey != ctrlAuth {
			slog.Warn("Gate0 reject: ctrl-port conn authenticated with wrong key",
				"port_role", role.String(), "remote", conn.RemoteAddr(), "cmd", cmdName)
			conn.WriteError("WRONGPASS invalid password for ctrl port. AUTH with the --ctrl-auth key, not the --auth key, then retry.")
			return true
		}
	}
	return false
}

// gate1CommandWordByPortRole blocks CTRL-only command words from reaching the
// app-port listener. Fail-closed: allowlist map for app-port (13 handshake +
// shared biz cmds), ctrl-port = pass everything.
//
// MVP before D3 dual-port: default conn role = ctrl-port → every cmd passes.
func gate1CommandWordByPortRole(conn redcon.Conn, cmdName string) (reject bool) {
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

// gate2MTLSAndSourceIP validates Caddy forward PROXY v2 real source IP +
// cert OU/CN. MVP structure only: always returns reject=false until D3+mTLS is wired.
func gate2MTLSAndSourceIP(conn redcon.Conn, cmdName string) (reject bool) {
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
