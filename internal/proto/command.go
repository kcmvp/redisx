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

	// 4 registry pair commands.
	// They are ordinary commands treated identically to SET/GET at the
	// dispatcher level; the only semantic differences live inside their
	// handler functions (e.g. DROPSCH refuses to drop a schema while
	// indexes are still attached to it).
	CmdRegisterSchema = "REGSCH"
	CmdRegisterIndex  = "REGIDX"
	CmdDropSchema     = "DROPSCH"
	CmdDropIndex      = "DROPIDX"
)

// LowerName returns the canonical lowercase form of a RedisX command word
// (the exact key used by the server-side command registry and the app-port
// allowlist). Callers should prefer this inline helper over ad-hoc
// strings.ToLower(cmd) so the casing rule lives in exactly one place.
func LowerName(cmd string) string { return strings.ToLower(cmd) }

var (
	LowerCmdAuth           = LowerName(CmdAuth)
	LowerCmdHello          = LowerName(CmdHello)
	LowerCmdClient         = LowerName(CmdClient)
	LowerCmdPing           = LowerName(CmdPing)
	LowerCmdQuit           = LowerName(CmdQuit)
	LowerCmdSelect         = LowerName(CmdSelect)
	LowerCmdCommand        = LowerName(CmdCommand)
	LowerCmdReset          = LowerName(CmdReset)
	LowerCmdSubscribe      = LowerName(CmdSubscribe)
	LowerCmdUnsubscribe    = LowerName(CmdUnsubscribe)
	LowerCmdPSubscribe     = LowerName(CmdPSubscribe)
	LowerCmdPUnsubscribe   = LowerName(CmdPUnsubscribe)
	LowerCmdPublish        = LowerName(CmdPublish)
	LowerCmdSet            = LowerName(CmdSet)
	LowerCmdSetEx          = LowerName(CmdSetEx)
	LowerCmdSetNX          = LowerName(CmdSetNX)
	LowerCmdGet            = LowerName(CmdGet)
	LowerCmdDel            = LowerName(CmdDel)
	LowerCmdExists         = LowerName(CmdExists)
	LowerCmdKeys           = LowerName(CmdKeys)
	LowerCmdUpdate         = LowerName(CmdUpdate)
	LowerCmdSearchIndex    = LowerName(CmdSearchIndex)
	LowerCmdSearchKey      = LowerName(CmdSearchKey)
	LowerCmdRegisterSchema = LowerName(CmdRegisterSchema)
	LowerCmdRegisterIndex  = LowerName(CmdRegisterIndex)
	LowerCmdDropSchema     = LowerName(CmdDropSchema)
	LowerCmdDropIndex      = LowerName(CmdDropIndex)
)

// AllLowerCmdNames returns the complete lowercase command set, keyed by the
// original upper-case constant name. It is intended for allowlist/blocklist
// builders that want to iterate over every known RedisX command word without
// re-listing them by hand.
func AllLowerCmdNames() map[string]string {
	return map[string]string{
		CmdAuth:           LowerCmdAuth,
		CmdHello:          LowerCmdHello,
		CmdClient:         LowerCmdClient,
		CmdPing:           LowerCmdPing,
		CmdQuit:           LowerCmdQuit,
		CmdSelect:         LowerCmdSelect,
		CmdCommand:        LowerCmdCommand,
		CmdReset:          LowerCmdReset,
		CmdSubscribe:      LowerCmdSubscribe,
		CmdUnsubscribe:    LowerCmdUnsubscribe,
		CmdPSubscribe:     LowerCmdPSubscribe,
		CmdPUnsubscribe:   LowerCmdPUnsubscribe,
		CmdPublish:        LowerCmdPublish,
		CmdSet:            LowerCmdSet,
		CmdSetEx:          LowerCmdSetEx,
		CmdSetNX:          LowerCmdSetNX,
		CmdGet:            LowerCmdGet,
		CmdDel:            LowerCmdDel,
		CmdExists:         LowerCmdExists,
		CmdKeys:           LowerCmdKeys,
		CmdUpdate:         LowerCmdUpdate,
		CmdSearchIndex:    LowerCmdSearchIndex,
		CmdSearchKey:      LowerCmdSearchKey,
		CmdRegisterSchema: LowerCmdRegisterSchema,
		CmdRegisterIndex:  LowerCmdRegisterIndex,
		CmdDropSchema:     LowerCmdDropSchema,
		CmdDropIndex:      LowerCmdDropIndex,
	}
}

// Usage strings shown by the Shell's handshake meta group and the server's
// built-in "wrong number of arguments" errors.
const (
	UsageRegisterSchema = "REGSCH <spec_json>  — atomically register a Doc schema (versioning semantics: identical MD5 = no-op, different MD5 = live upgrade)"
	UsageDropSchema     = "DROPSCH <logical_ns>  — drop a Doc schema (covers both disk and mem versions; rejected if any indexes are still attached to this namespace — drop them first via DROPIDX)"
	UsageRegisterIndex  = "REGIDX <spec_json>  — register a secondary index on an already-registered Doc schema (versioning semantics: identical MD5 = no-op, different MD5 = live swap)"
	UsageDropIndex      = "DROPIDX <fullName>  —or—  DROPIDX <owner_ns> <logical>  — drop an index (plain DEL-style command; the ns+logical form automatically covers both disk and mem variants)"
)
