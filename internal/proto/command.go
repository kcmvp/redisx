package proto

import "strings"

const (
	CmdAuth         = "AUTH"
	CmdHello        = "HELLO"
	CmdClient       = "CLIENT"
	CmdPing         = "PING"
	CmdQuit         = "QUIT"
	CmdSelect       = "SELECT"
	CmdCommand      = "COMMAND"
	CmdReset        = "RESET"
	CmdSubscribe    = "SUBSCRIBE"
	CmdUnsubscribe  = "UNSUBSCRIBE"
	CmdPSubscribe   = "PSUBSCRIBE"
	CmdPUnsubscribe = "PUNSUBSCRIBE"
	CmdPublish      = "PUBLISH"

	CmdSet    = "SET"
	CmdSetEx  = "SETEX"
	CmdSetNX  = "SETNX"
	CmdGet    = "GET"
	CmdDel    = "DEL"
	CmdExists = "EXISTS"
	CmdKeys   = "KEYS"

	CmdUpdate      = "UPDATE"
	CmdSearchIndex = "SEARCHINDEX"
	CmdSearchKey   = "SEARCHKEY"

	// CmdSchema carries one client-side schema declaration — the doc schema
	// plus its inline secondary indexes (client.RegisterSchema). The server
	// applies it immediately and idempotently; it is not a replay channel, and
	// the server keeps no persistent registry of schemas.
	CmdSchema = "SCHEMA"
)

// LowerName returns the canonical lowercase form of a RedisX command word
// (the exact key used by the server-side command registry and the app-port
// allowlist). Callers should prefer this inline helper over ad-hoc
// strings.ToLower(cmd) so the casing rule lives in exactly one place.
func LowerName(cmd string) string { return strings.ToLower(cmd) }

var (
	LowerCmdAuth         = LowerName(CmdAuth)
	LowerCmdHello        = LowerName(CmdHello)
	LowerCmdClient       = LowerName(CmdClient)
	LowerCmdPing         = LowerName(CmdPing)
	LowerCmdQuit         = LowerName(CmdQuit)
	LowerCmdSelect       = LowerName(CmdSelect)
	LowerCmdCommand      = LowerName(CmdCommand)
	LowerCmdReset        = LowerName(CmdReset)
	LowerCmdSubscribe    = LowerName(CmdSubscribe)
	LowerCmdUnsubscribe  = LowerName(CmdUnsubscribe)
	LowerCmdPSubscribe   = LowerName(CmdPSubscribe)
	LowerCmdPUnsubscribe = LowerName(CmdPUnsubscribe)
	LowerCmdPublish      = LowerName(CmdPublish)
	LowerCmdSet          = LowerName(CmdSet)
	LowerCmdSetEx        = LowerName(CmdSetEx)
	LowerCmdSetNX        = LowerName(CmdSetNX)
	LowerCmdGet          = LowerName(CmdGet)
	LowerCmdDel          = LowerName(CmdDel)
	LowerCmdExists       = LowerName(CmdExists)
	LowerCmdKeys         = LowerName(CmdKeys)
	LowerCmdUpdate       = LowerName(CmdUpdate)
	LowerCmdSearchIndex  = LowerName(CmdSearchIndex)
	LowerCmdSearchKey    = LowerName(CmdSearchKey)
	LowerCmdSchema       = LowerName(CmdSchema)
)

// AllLowerCmdNames returns the complete lowercase command set, keyed by the
// original upper-case constant name. It is intended for allowlist/blocklist
// builders that want to iterate over every known RedisX command word without
// re-listing them by hand.
func AllLowerCmdNames() map[string]string {
	return map[string]string{
		CmdAuth:         LowerCmdAuth,
		CmdHello:        LowerCmdHello,
		CmdClient:       LowerCmdClient,
		CmdPing:         LowerCmdPing,
		CmdQuit:         LowerCmdQuit,
		CmdSelect:       LowerCmdSelect,
		CmdCommand:      LowerCmdCommand,
		CmdReset:        LowerCmdReset,
		CmdSubscribe:    LowerCmdSubscribe,
		CmdUnsubscribe:  LowerCmdUnsubscribe,
		CmdPSubscribe:   LowerCmdPSubscribe,
		CmdPUnsubscribe: LowerCmdPUnsubscribe,
		CmdPublish:      LowerCmdPublish,
		CmdSet:          LowerCmdSet,
		CmdSetEx:        LowerCmdSetEx,
		CmdSetNX:        LowerCmdSetNX,
		CmdGet:          LowerCmdGet,
		CmdDel:          LowerCmdDel,
		CmdExists:       LowerCmdExists,
		CmdKeys:         LowerCmdKeys,
		CmdUpdate:       LowerCmdUpdate,
		CmdSearchIndex:  LowerCmdSearchIndex,
		CmdSearchKey:    LowerCmdSearchKey,
		CmdSchema:       LowerCmdSchema,
	}
}
