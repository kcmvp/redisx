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

	CmdRegisterDoc   = "regdoc"
	CmdListDocs      = "lsdoc"
	CmdDescribeDoc   = "desdoc"
	CmdListIndexes   = "lsidx"
	CmdRegisterIndex = "regidx"
	CmdDropIndex     = "delidx"
)

// CommandRole 定义命令的端口角色授权范围（Gate1 用）
type CommandRole uint8

const (
	RoleBaseHandshake CommandRole = iota // 握手级：PING/AUTH/QUIT/HELLO，App+Admin 都必须放行
	RoleSharedBiz                        // 业务共享：SET/GET/DEL/KEYS/SEARCH*/UPDATE，App+Admin 都过 Strict Gate
	RoleAdminOnly                        // Admin-only：X* + REGISTER + DROP，App-port 命令字级拒绝
)

// ArgcConstraint 命令参数个数约束（SSoT 两边共享：Server argc pre-check + Shell dispatcher client-side pre-check）
type ArgcConstraint struct {
	MinArgs int // 最小值（不含命令字本身，因此 ADMREGDOC 只有 spec_json 1 arg，Min=1）
	MaxArgs int // 最大值；-1 表示 unlimited（DEL 多个 key / SET 多 OPTION 场景）
}

// CommandSpec = 命令字 SSoT 规范，Server & Shell 两端都读同一份
type CommandSpec struct {
	CmdWord string         // 大写原始命令字，如 SET/ADMREGDOC
	Role    CommandRole    // 端口角色：握手/共享业务/Admin 独占
	Argc    ArgcConstraint // argc 合同约束
	Usage   string         // 帮助文本（Shell !HELP / Server 报 ERR wrong number of arguments 时拼文本用）
}

// Registry —— Server & Shell 都遍历这个 map（Server 注册 handler、Shell 注册 dispatcher）
// Key = 命令字大写（Server 端 cmdName := strings.ToLower(args[0]) 后再 strings.ToUpper 匹配）
var Registry = map[string]CommandSpec{
	// ==================== Admin-only：Doc 族（*doc 后缀）====================
	CmdRegisterDoc: {
		CmdWord: CmdRegisterDoc,
		Role:    RoleAdminOnly,
		Argc:    ArgcConstraint{MinArgs: 1, MaxArgs: 1},
		Usage:   "regdoc <spec_json>  — 原子注册一个 Doc schema（落盘 DocMetaNsPrefix:<ns>）",
	},
	CmdListDocs: {
		CmdWord: CmdListDocs,
		Role:    RoleAdminOnly,
		Argc:    ArgcConstraint{MinArgs: 0, MaxArgs: 1},
		Usage:   "lsdoc [pattern]  — 列出所有已注册 Doc（可带 GLOB pattern）",
	},
	CmdDescribeDoc: {
		CmdWord: CmdDescribeDoc,
		Role:    RoleAdminOnly,
		Argc:    ArgcConstraint{MinArgs: 1, MaxArgs: 1},
		Usage:   "desdoc <namespace>  — 返回 Doc 完整 spec（KeyAttrs 字段、是否 Mem 等）",
	},
	// ==================== Admin-only：Index 族（*idx 后缀）====================
	CmdListIndexes: {
		CmdWord: CmdListIndexes,
		Role:    RoleAdminOnly,
		Argc:    ArgcConstraint{MinArgs: 0, MaxArgs: 1},
		Usage:   "lsidx [namespace]  — 列出指定 ns（或全部）已注册 Index",
	},
	CmdRegisterIndex: {
		CmdWord: CmdRegisterIndex,
		Role:    RoleAdminOnly,
		Argc:    ArgcConstraint{MinArgs: 4, MaxArgs: 6}, // regidx ns name path [UNIQUE] [TYPE type]
		Usage:   "regidx <ns> <logicalName> <jsonPath> [UNIQUE] [TYPE <type>]  — 动态创建索引（Shell UI 糖：REGISTER INDEX ON ...）",
	},
	CmdDropIndex: {
		CmdWord: CmdDropIndex,
		Role:    RoleAdminOnly,
		Argc:    ArgcConstraint{MinArgs: 2, MaxArgs: 2}, // delidx ns logicalName（UI 糖 DROP INDEX ns.name → wire 拆成 2 args）
		Usage:   "delidx <namespace> <logicalName>  — 删除索引（Shell UI 糖：DROP INDEX ns.name）",
	},
}

// BaseHandshakeCommands —— Base 握手命令（已在 proto 之前有了，这里只列名称给 Gate1 Allowlist 用，不在 Registry 放 argc，因为每一条 argc 历史上是 handler 自己判）
var BaseHandshakeCommands = []string{
	CmdAuth, CmdHello, CmdClient, CmdPing, CmdQuit, CmdSelect, CmdCommand, CmdReset,
	CmdSubscribe, CmdUnsubscribe, CmdPSubscribe, CmdPUnsubscribe, CmdPublish,
}

// SharedBizCommands —— Shared 业务命令（Strict Gate 共享）
var SharedBizCommands = []string{
	CmdSet, CmdSetEx, CmdSetNX, CmdGet, CmdDel, CmdExists, CmdKeys, CmdSearchKey, CmdSearchIndex, CmdUpdate,
}

// IsAdminOnly 辅助：Shell dispatcher / Server Gate1 Allowlist 构建时都用
func (s CommandSpec) IsAdminOnly() bool { return s.Role == RoleAdminOnly }
