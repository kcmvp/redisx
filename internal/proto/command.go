package proto

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

	// 4 registry / admin pair commands.
	// They are ordinary commands treated identically to SET/GET at the
	// dispatcher level; the only semantic differences live inside their
	// handler functions (e.g. DROPSCH refuses to drop a schema while
	// indexes are still attached to it).
	CmdRegisterSchema = "REGSCH"
	CmdRegisterIndex  = "REGIDX"
	CmdDropSchema     = "DROPSCH"
	CmdDropIndex      = "DROPIDX"
)

// Usage strings shown by the Shell's handshake meta group and the server's
// built-in "wrong number of arguments" errors.
const (
	UsageRegisterSchema = "REGSCH <spec_json>  — atomically register a Doc schema (versioning semantics: identical MD5 = no-op, different MD5 = live upgrade)"
	UsageDropSchema     = "DROPSCH <logical_ns>  — drop a Doc schema (covers both disk and mem versions; rejected if any indexes are still attached to this namespace — drop them first via DROPIDX)"
	UsageRegisterIndex  = "REGIDX <spec_json>  — register a secondary index on an already-registered Doc schema (versioning semantics: identical MD5 = no-op, different MD5 = live swap)"
	UsageDropIndex      = "DROPIDX <fullName>  —or—  DROPIDX <owner_ns> <logical>  — drop an index (plain DEL-style command; the ns+logical form automatically covers both disk and mem variants)"
)
