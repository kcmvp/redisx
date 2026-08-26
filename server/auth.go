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
	portRoleAdmin
)

func (p portRole) String() string {
	switch p {
	case portRoleApp:
		return "app"
	case portRoleAdmin:
		return "admin"
	}
	return "unknown"
}

var (
	connRoleMu  sync.RWMutex
	connRoleMap = map[redcon.Conn]portRole{}

	authCfgMu       sync.RWMutex
	activeAppAuth   = ""
	activeAdminAuth = ""
	authConfigured  = false
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
		return portRoleAdmin
	}
	connRoleMu.RLock()
	r, ok := connRoleMap[conn]
	connRoleMu.RUnlock()
	if !ok || r == portRoleUnknown {
		return portRoleAdmin
	}
	return r
}

func configureAuthKeys(appAuth, adminAuth string) {
	authCfgMu.Lock()
	activeAppAuth = appAuth
	activeAdminAuth = adminAuth
	authConfigured = (appAuth != "") || (adminAuth != "")
	authCfgMu.Unlock()
}

func getAuthConfig() (appAuth, adminAuth string, configured bool) {
	authCfgMu.RLock()
	defer authCfgMu.RUnlock()
	return activeAppAuth, activeAdminAuth, authConfigured
}

func gate0Auth(conn redcon.Conn, cmdName string) (reject bool) {
	cmdLower := strings.ToLower(cmdName)
	switch cmdLower {
	case strings.ToLower(proto.CmdAuth), strings.ToLower(proto.CmdHello), strings.ToLower(proto.CmdClient):
		return false
	}
	appAuth, adminAuth, configured := getAuthConfig()
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
	// user-key match entirely. It is accepted on both App and Admin ports
	// unconditionally regardless of whether app/admin user keys are set.
	if authedKey != "" && authedKey == internalAuthKey {
		return false
	}
	anyConfigured := (appAuth != "") || (adminAuth != "")
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
			conn.WriteError("WRONGPASS invalid password for app port. AUTH with the --auth key, not the --admin-auth key, then retry.")
			return true
		}
	case portRoleAdmin:
		if adminAuth == "" {
			return false
		}
		if authedKey != adminAuth {
			slog.Warn("Gate0 reject: admin-port conn authenticated with wrong key",
				"port_role", role.String(), "remote", conn.RemoteAddr(), "cmd", cmdName)
			conn.WriteError("WRONGPASS invalid password for admin port. AUTH with the --admin-auth key, not the --auth key, then retry.")
			return true
		}
	}
	return false
}

// gate1CommandWordByPortRole blocks ADMIN-only command words from reaching the
// app-port listener. Fail-closed: allowlist map for app-port (13 handshake +
// shared biz cmds), admin-port = pass everything.
//
// MVP before D3 dual-port: default conn role = admin-port → every cmd passes.
func gate1CommandWordByPortRole(conn redcon.Conn, cmdName string) (reject bool) {
	role := connPortRole(conn) // admin/app; MVP defaults to "admin"
	if role == portRoleAdmin {
		return false
	}
	// app-port → only allow allowlist. Build once at init time from proto.
	if !appPortCmdAllowlist[cmdName] {
		slog.Warn("app-port attempted admin-only command (Gate1 reject)",
			"remote", conn.RemoteAddr(), "cmd", cmdName)
		conn.WriteError("ERR No Privilege: '" + cmdName + "' is a Meta Management command, only allowed on the admin port. Connect via the admin port and use --admin-auth, or run equivalent data operations on this app port.")
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
		proto.CmdAuth, proto.CmdHello, proto.CmdClient, proto.CmdPing, proto.CmdQuit, proto.CmdSelect, proto.CmdCommand, proto.CmdReset,
		proto.CmdSubscribe, proto.CmdUnsubscribe, proto.CmdPSubscribe, proto.CmdPUnsubscribe, proto.CmdPublish,
		// Shared business commands: KV, KV-extensions
		proto.CmdSet, proto.CmdSetEx, proto.CmdSetNX, proto.CmdGet, proto.CmdDel, proto.CmdExists,
		proto.CmdSearchKey, proto.CmdSearchIndex, proto.CmdUpdate,
	}
	for _, c := range combined {
		appPortCmdAllowlist[strings.ToLower(c)] = true
	}
}
