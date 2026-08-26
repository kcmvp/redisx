package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/samber/lo"
)

type commandEntry struct {
	Name  string
	Usage string
}

type commandGroups struct {
	GroupOrder []string
	Groups     map[string][]commandEntry
}

func defaultCommandGroups() *commandGroups {
	return &commandGroups{
		GroupOrder: []string{"Extended", "Meta Management"},
		Groups: map[string][]commandEntry{
			"Extended": {
				{Name: proto.LowerCmdUpdate, Usage: "UPDATE key <json_patch>  — merge-UPDATE a JSON value (typed doc enabled: writes validated)"},
				{Name: proto.LowerCmdSearchKey, Usage: "SEARCHKEY <predicate_json>  — typed doc+index search: filter → pick key"},
				{Name: proto.LowerCmdSearchIndex, Usage: "SEARCHINDEX <ns> <idx_name> <query>  — query a named typed index"},
			},
			"Meta Management": {
				{Name: proto.LowerCmdRegisterSchema, Usage: proto.UsageRegisterSchema},
				{Name: proto.LowerCmdDropSchema, Usage: proto.UsageDropSchema},
				{Name: proto.LowerCmdRegisterIndex, Usage: proto.UsageRegisterIndex},
				{Name: proto.LowerCmdDropIndex, Usage: proto.UsageDropIndex},
			},
		},
	}
}

func sessFromCtx(ctx tui.RunContext) *session.Session {
	data, _ := ctx.UserData.(*AppData)
	if data == nil {
		return nil
	}
	return *data.Session
}

func runRedisxDiagnose(ctx tui.RunContext, sess *session.Session, data *AppData, args []string) error {
	_ = ctx
	_ = args
	out := func(k string, v any) {
		switch x := v.(type) {
		case string:
			fmt.Printf("  %-36s %s\n", cBold(k), x)
		default:
			fmt.Printf("  %-36s %#v\n", cBold(k), v)
		}
	}
	fmt.Println(cBold("=== redisx DIAGNOSE ==="))
	h, p := "127.0.0.1", "7381"
	if data != nil {
		h, p = data.HostPort()
	}
	out("host:port", h+":"+p)
	fmt.Println(cBold("-- probe HELLO (server identity) --"))
	{
		raw := sess.RawDo([]any{"HELLO", 2})
		if raw.Err() != nil {
			out("HELLO error", raw.Err().Error())
		} else if v, e := raw.Result(); e != nil {
			out("HELLO decode error", e.Error())
		} else {
			m, ok := v.(map[string]any)
			if !ok {
				fmt.Printf("  %-36s %T: %#v\n", cBold("HELLO reply"), v, v)
			} else {
				if srv, _ := m["server"].(string); srv != "" {
					out("server", srv)
				}
			}
		}
	}
	fmt.Println(cBold("-- client-builtin command catalogue (SSoT: decoupled from server) --"))
	groups := defaultCommandGroups()
	out("commands.group_order", strings.Join(groups.GroupOrder, " | "))
	for gName, entries := range groups.Groups {
		out(fmt.Sprintf("commands.group[%s] items", gName), len(entries))
	}
	fmt.Println(cBold("=== end DIAGNOSE ==="))
	return nil
}

func cacheFromCtx(ctx tui.RunContext) *session.Cache {
	data, _ := ctx.UserData.(*AppData)
	if data == nil {
		return nil
	}
	return data.Cache
}

var examples = map[string][]string{
	"example": {
		"example (no args)   — list example targets",
		"example <target>    — copy-paste a full template for a command",
		"",
		"Targets:",
		"  regsch | regidx | dropsch | dropidx",
		"  update | searchkey | searchindex",
	},
	"regsch": {
		"# REGSCH accepts a SINGLE JSON argument.",
		"# Copy the following, edit the parts you need, then paste into the shell.",
		"# JSON tips: pass the blob as one argument — a single-quoted string in the CLI works.",
		"",
		"REGSCH '{" +
			`"name":"user"` +
			"," +
			`"ttl_ms":0` +
			"," +
			`"mem":false` +
			"," +
			`"pk":{"path":"id","auto":false}` +
			"," +
			`"paths":[` +
			`{"path":"id","type":"string"}` +
			"," +
			`{"path":"age","type":"int"}` +
			"," +
			`{"path":"profile.email","type":"string"}` +
			"]}",
		"",
		"# Fields:",
		"#   name       — required, logical namespace (ValidateDocLogicalNamespace rules)",
		"#   ttl_ms     — 0 = no expiry; >0 = every key of this schema expires after N ms",
		"#   mem        — false = disk-backed storage (default); true = _m_: memory-only layer",
		"#   pk.path    — required, primary key field path inside JSON (where to extract id)",
		"#   pk.auto    — true = let server assign a UUID string; false = caller writes pk explicitly",
		"#   paths      — required, array of {path, type}: all typed fields of the document",
		"#                  type ∈ {string, int, float, bool, time, json}",
		"#                  paths with dots (profile.email) are automatically flattened to profile_email internally",
		"# Notes:",
		"#   - server disallows the reserved key \"indexes\" inside REGSCH payload — put indexes in REGIDX, not here",
		"#   - identical MD5(canonical JSON) → no-op OK (idempotent upgrade guard)",
		"#   - different MD5 vs existing schema → live upgrade (see DROPSCH for attached-index guard before dropping)",
	},
	"regidx": {
		"# REGIDX accepts a SINGLE JSON argument. Three owner addressing forms — pick ONE:",
		"#   A) { full_name: \"<ownerNs>!_!<logical>\" }  ← canonical full name",
		"#   B) { owner_ns: \"<storageNs>\", logical: \"age_idx\", paths: [...] }",
		"#   C) { owner_doc_logical_ns: \"user\", owner_mem: false, logical: \"age_idx\", paths: [...] }",
		"# paths come as a single string \"age\" OR an array [\"age\",\"profile.email\"] OR [{path:\"age\"}].",
		"",
		"# Shortcut C — shortest form for typical indexes:",
		"REGIDX '{" +
			`"owner_doc_logical_ns":"user"` +
			"," +
			`"owner_mem":false` +
			"," +
			`"logical":"age_email_idx"` +
			"," +
			`"paths":["age","profile.email"]` +
			"," +
			`"case_sensitive":true` +
			"}",
		"",
		"# Full form A — reusing a full_name you already got from a diagnostic/listing:",
		"REGIDX '{" +
			`"full_name":"user!_!age_idx"` +
			"," +
			`"paths":["age"]` +
			"}",
		"",
		"# Fields:",
		"#   owner_doc_logical_ns — no storageNs prefix, pair with owner_mem: true/false",
		"#   owner_ns             — already-prefixed storage ns (\"user\" or \"_m_:user\")",
		"#   full_name            — takes precedence over owner_ns + logical",
		"#   logical              — required (index handle, ValidateLogicalIndexName rules)",
		"#   paths                — required, one or more doc paths to index",
		"#   case_sensitive       — optional bool; true = string comparison is byte-exact (default false = case-insensitive)",
		"#   comparator           — optional string for custom ordering on typed values; usually not needed",
		"#   key_pattern          — optional, defaults to owner_ns:* (entire namespace)",
		"# Notes:",
		"#   - identical canonical MD5 → no-op OK (idempotent); different MD5 vs existing → live swap",
		"#   - owner namespace MUST already be registered via REGSCH, otherwise REGIDX rejects with \"owner not found\"",
	},
	"dropsch": {
		"# DROPSCH takes a SINGLE plain-key-style argument: logical_ns (not JSON).",
		"# Attached indexes → DROPSCH refuses with a list of their names — DROPIDX them first.",
		"# Both disk + mem layers are cleaned up symmetrically.",
		"",
		"DROPSCH user",
		"",
		"# Argument:",
		"#   logical_ns — ValidateDocLogicalNamespace rules (no storage prefix, no colons, no underscore prefix)",
	},
	"dropidx": {
		"# DROPIDX is a plain key-style command, two call forms:",
		"#   (A) DROPIDX <fullName>              — canonical name, e.g. \"user!_!age_idx\" or \"_m_:user!_!age_idx\"",
		"#   (B) DROPIDX <logicalNs> <idxName>   — shortcut; server tries BOTH disk and _m_: variants automatically",
		"# The (B) shortcut lets operators type without knowing the fullName.",
		"",
		"DROPIDX user!_!age_email_idx",
		"",
		"DROPIDX user age_email_idx",
		"",
		"# Notes:",
		"#   - not-registered on one layer (disk-only / mem-only) is silently OK; only fails when NEITHER layer has it",
		"#   - irreversible — drops the btree handle + meta key + composite store",
	},
	"update": {
		"# UPDATE: key + JSON patch object. The patch is MERGED into the existing document.",
		"# For typed-doc namespaces, server validates field types against REGSCH schema before writing.",
		"# key pattern rules are the same as SET/GET — for typed docs the key's anchor selects the ns.",
		"",
		"UPDATE user:leto '{\"age\":46, \"profile\":{\"email\":\"leto@atreides.frf\"}}'",
		"",
		"# Behavior:",
		"#   - top-level keys in the patch OVERWRITE existing doc keys of the same name",
		"#   - nested objects are merged recursively (\"profile.email\" = patch replaces whole profile.value if both present, nested value is the one on patch)",
		"#   - for non-typed plain KV keys: JSON still parsed, SET replaces the whole value (no schema verification)",
	},
	"searchkey": {
		"# SEARCHKEY: single JSON predicate argument. Returns matching PRIMARY KEYS (anchors) from the filtered range.",
		"# Works on any registered typed-doc namespace.",
		"",
		"SEARCHKEY '{" +
			`"ns":"user"` +
			"," +
			`"mem":false` +
			"," +
			`"filter":{"age":{"gte":18},"profile.email":{"ends_with":"@atreides.frf"}}` +
			"," +
			`"limit":100` +
			"," +
			`"offset":0` +
			"}",
		"",
		"# Top-level keys:",
		"#   ns/mem          — optional namespace selector; if omitted, filter's first-level anchor(s) drive it",
		"#   filter          — required, object of {path: {OP: value}} pairs",
		"#   filter.OPs      — eq / ne / gt / gte / lt / lte / between / in / like / starts_with / ends_with / contains / regex",
		"#   order           — optional [{path:\"age\", dir:\"asc|desc\"}, ...]",
		"#   limit / offset  — pagination (server default caps apply)",
	},
	"searchindex": {
		"# SEARCHINDEX: 3 plain-key args — <storageNs OR logical_ns> <idx_logical_name> <raw_lower_bound>",
		"# The third arg is the raw scan start key inside the chosen index — exact semantics depend on comparator/case flags of the REGIDX.",
		"",
		"SEARCHINDEX user age_email_idx 18",
		"",
		"# For a 2-path index (age, profile.email), query prefix works too — give enough args in lower_bound to",
		"# encode the first path, then server walks the btree, e.g. for strings:",
		"SEARCHINDEX user age_email_idx '18::leto'",
		"",
		"# Arguments:",
		"#   arg1 = ns or storageNs — \"user\"  or \"_m_:user\" (no prefix = disk)",
		"#   arg2 = logical index name — the REGIDX \"logical\" handle",
		"#   arg3 = start value — scalar or composite joined with the index's native joiner",
		"# Notes:",
		"#   - if the index's namespace + logical combo doesn't exist → ERR \"index not found\"",
		"#   - result is a flat list of PKs matching the index scan (not doc rows themselves — follow up with GET/MGET)",
	},
}

func printExample(which string) error {
	target := strings.ToLower(strings.TrimSpace(which))
	if target == "" || target == "list" || target == "help" {
		list := []string{"regsch", "regidx", "dropsch", "dropidx", "update", "searchkey", "searchindex"}
		fmt.Println(cBold("Usage: example <target>"))
		fmt.Println("Available targets:")
		for _, n := range list {
			fmt.Printf("  example %-12s   — %s\n", n, strings.ToUpper(n)+" copy-paste template")
		}
		return nil
	}
	lines, ok := examples[target]
	if !ok {
		available := lo.Keys(examples)
		sort.Strings(available)
		return fmt.Errorf("no example for %q — try one of: %s", target, strings.Join(available, ", "))
	}
	for i, l := range lines {
		_ = i
		fmt.Println(l)
	}
	return nil
}

func registerAllCommands(app *tui.CLIApp) {
	ping := &tui.CLICommand{
		Use:     "ping",
		Short:   "send PING to the connected server and print PONG",
		MinArgs: 0, MaxArgs: 0,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if sess == nil {
				return fmt.Errorf("shell not connected")
			}
			v, err := sess.RawDo([]any{"PING"}).Result()
			if err != nil {
				return err
			}
			fmt.Println(v)
			return nil
		},
	}
	app.Register(ping)
	trackCmd("ping", ping)

	ver := &tui.CLICommand{
		Use:     "!version",
		Aliases: []string{"version"},
		Short:   "print the redisx shell version",
		MinArgs: 0, MaxArgs: 0,
		Run: func(_ tui.RunContext, _ []string) error {
			fmt.Printf("redisx %s (go %s, %s/%s)\n", Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
			return nil
		},
	}
	app.Register(ver)
	trackCmd("!version", ver)

	clr := &tui.CLICommand{
		Use:     "clear",
		Aliases: []string{"!clear", "cls"},
		Short:   "clear the terminal screen",
		MinArgs: 0, MaxArgs: 0,
		Run: func(_ tui.RunContext, _ []string) error {
			printScreenClear()
			return nil
		},
	}
	app.Register(clr)
	trackCmd("clear", clr)

	redisxCmd := &tui.CLICommand{
		Use:   "redisx [subcommand [args...]]",
		Short: "Local shell introspection. Subcommands: HELP | VERSION | CAPS | COMMANDS; DIAGNOSE = local probe debug; no args → HELP",
		Long: `Subcommands are ALL local (SSoT: client-builtin catalogue decoupled from server per the upgrade rule):
  redisx HELP          — subcommand list
  redisx VERSION       — client SDK version info
  redisx CAPS          — client-side identity (SSoT: all decisions server-side)
  redisx COMMANDS      — client-builtin command list (Extended/Meta Management)
  redisx DIAGNOSE      — local HELLO probe + catalogue summary

With 0 args, redisx is equivalent to "redisx HELP".

IMPORTANT: any command NOT listed above is forwarded VERBATIM as a raw RESP wire
call to the connected peer. This means the redisx CLI operates as a generic
Redis-compatible client: SET / GET / DEL / KEYS / AUTH / REGSCH / REGIDX /
UPDATE / SEARCHKEY / SEARCHINDEX / any-new-server-command all work without a
client rebuild. Support, availability and privilege enforcement are entirely
server-side — the CLI never pre-judges whether a command is "allowed".`,
		MinArgs: 0, MaxArgs: -1,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if sess == nil {
				return fmt.Errorf("shell not connected")
			}
			data, _ := ctx.UserData.(*AppData)
			useArgs := args
			if len(useArgs) == 0 {
				useArgs = []string{"HELP"}
			}
			sub := strings.ToUpper(useArgs[0])
			switch sub {
			case "DIAGNOSE":
				return runRedisxDiagnose(ctx, sess, data, useArgs[1:])
			case "HELP":
				fmt.Println(cBold("redisx HELP (client-builtin SSoT; decoupled from server)"))
				items := []string{
					"redisx HELP          — this text",
					"redisx VERSION       — client SDK version info",
					"redisx CAPS          — client identity (all decisions server-side)",
					"redisx COMMANDS      — client-builtin command list (Extended/Meta Management)",
					"redisx DIAGNOSE      — local HELLO probe + catalogue summary",
					"                     — everything else = raw RESP passthrough (server rejects unknowns)",
				}
				for i, it := range items {
					fmt.Printf("  %d) %s\n", i+1, it)
				}
				return nil
			case "VERSION":
				fmt.Println("redisx-sdk (support and privileges are fully server-side; use client package version above)")
				return nil
			case "CAPS", "CAPABILITIES":
				out := map[string]any{
					"server": "redisx",
					"note":   "client makes zero assumptions — support and privileges are fully server-side",
				}
				wizardPrintAny(os.Stdout, out)
				return nil
			case "COMMANDS", "CMDS", "COMMAND":
				cmdGroups := defaultCommandGroups()
				groups := map[string][][2]string{}
				for g, entries := range cmdGroups.Groups {
					rows := make([][2]string, 0, len(entries))
					for _, e := range entries {
						rows = append(rows, [2]string{e.Name, e.Usage})
					}
					groups[g] = rows
				}
				h, p := "127.0.0.1", "7381"
				if data != nil {
					h, p = data.HostPort()
				}
				roleLine := cBold("Connected: ") + cCyan(h+":"+p) + "  (raw RESP passthrough — support & privilege enforcement handled server-side)"
				printServerCommandsList(data, groups, cmdGroups.GroupOrder, roleLine)
				return nil
			default:
				return fmt.Errorf("unknown redisx subcommand %q; try `redisx HELP`", useArgs[0])
			}
		},
	}
	app.Register(redisxCmd)
	trackCmd("redisx", redisxCmd)

	exampleCmd := &tui.CLICommand{
		Use:   "example [target]",
		Short: "Copy-paste templates (type `example` to list targets)",
		Long: `No args or "example list" = show targets. "example regsch" = paste-ready REGSCH JSON
template + field-by-field notes; same form for the other targets.

Examples are intentionally client-side so they ship with the shell binary
(SSoT decoupling: server can add commands later without forcing a client
upgrade — examples are refreshed on shell upgrade, not on handshake).`,
		MinArgs: 0, MaxArgs: 1,
		Run: func(_ tui.RunContext, args []string) error {
			if len(args) == 0 {
				return printExample("help")
			}
			return printExample(args[0])
		},
	}
	app.Register(exampleCmd)
	trackCmd("example", exampleCmd)
}

func wizardPrintAny(out *os.File, v any) {
	switch t := v.(type) {
	case nil:
		_, _ = fmt.Fprintln(out, "(nil)")
	case string:
		_, _ = fmt.Fprintln(out, t)
	case []any:
		for i, e := range t {
			_, _ = fmt.Fprintf(out, "[%d] %v\n", i, e)
		}
	case []string:
		for i, e := range t {
			_, _ = fmt.Fprintf(out, "[%d] %s\n", i, e)
		}
	case error:
		_, _ = fmt.Fprintln(out, cRed(t.Error()))
	default:
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			_, _ = fmt.Fprintf(out, "%v\n", v)
			return
		}
		_, _ = fmt.Fprintln(out, string(b))
	}
}
