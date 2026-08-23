package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/cmd/redisx/internal/wizard"
)

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
	fmt.Println(cBold("-- probe HELLO 2 (SSoT: identity + caps + commands in one reply) --"))
	var (
		probeIsRedisx   bool
		probeAdminRole  bool
		probeServerVer  string
		probeFeaturesOk bool
		probeCommandsOk bool
		probeGroupCount int
	)
	{
		raw := sess.RawDo([]any{"HELLO", 2})
		if raw.Err() != nil {
			out("HELLO 2 error", raw.Err().Error())
		} else if v, e := raw.Result(); e != nil {
			out("HELLO 2 decode error", e.Error())
		} else {
			m, ok := v.(map[string]any)
			if !ok {
				fmt.Printf("  %-36s %T: %#v\n", cBold("HELLO 2 reply"), v, v)
			} else {
				if srv, _ := m["server"].(string); srv != "" {
					out("server", srv)
					probeIsRedisx = (srv == "redisx")
				}
				if ver, _ := m["version"].(string); ver != "" {
					out("version (server-side)", ver)
					probeServerVer = ver
				}
				if proto, _ := m["proto"].(int64); proto != 0 {
					out("proto", proto)
				}
				if role, _ := m["role"].(string); role != "" {
					out("role", role)
				}
				if ar, _ := m["admin_role"].(bool); ar {
					out("admin_role", ar)
					probeAdminRole = ar
				}
				if fm, okG := m["features"].(map[string]any); okG {
					b, _ := json.MarshalIndent(fm, "", "  ")
					probeFeaturesOk = true
					fmt.Printf("  %-36s\n%s\n", cBold("features"), string(b))
				}
				if cm, okG := m["commands"].(map[string]any); okG {
					probeCommandsOk = true
					if gorder, okG2 := cm["group_order"].([]any); okG2 {
						names := make([]string, 0, len(gorder))
						for _, n := range gorder {
							if s, _ := n.(string); s != "" {
								names = append(names, s)
							}
						}
						probeGroupCount = len(names)
						out("commands.group_order", strings.Join(names, " | "))
					}
					if groups, okG2 := cm["groups"].(map[string]any); okG2 {
						for _, g := range []string{"Basic", "Extended", "Meta Management"} {
							list, _ := groups[g].([]any)
							out(fmt.Sprintf("commands.group[%s] items", g), len(list))
						}
					}
				}
			}
		}
	}
	fmt.Println(cBold("-- shell caps caches (read vs probe) --"))
	var sessCaps session.Capabilities
	var frozen session.Capabilities
	if sess != nil {
		sessCaps = sess.Capabilities()
	}
	if data != nil {
		frozen = data.FrozenCaps
	}
	out("Session.Capabilities.IsRedisx", sessCaps.IsRedisx)
	out("Session.Capabilities.AdminRole", sessCaps.AdminRole)
	out("Session.Capabilities.ServerVer", sessCaps.ServerVer)
	out("FrozenCaps.IsRedisx", frozen.IsRedisx)
	out("FrozenCaps.AdminRole", frozen.AdminRole)
	out("FrozenCaps.ServerVer", frozen.ServerVer)
	needRefresh := !frozen.IsRedisx && (probeIsRedisx || !sessCaps.IsRedisx)
	out("need RefreshCapabilities (recommended)", needRefresh)
	if needRefresh && sess != nil {
		prev, now, ok := sess.RefreshCapabilities()
		out("RefreshCapabilities called", ok)
		if ok {
			_ = prev
			if data != nil {
				data.FrozenCaps = now
			}
			out("FrozenCaps after refresh.IsRedisx", now.IsRedisx)
			out("FrozenCaps after refresh.AdminRole", now.AdminRole)
			out("FrozenCaps after refresh.ServerVer", now.ServerVer)
		}
	}
	fmt.Println(cBold("-- HELLO commands groups summary --"))
	out("commands.Commands present", probeCommandsOk)
	out("commands.group count", probeGroupCount)
	_ = probeIsRedisx
	_ = probeAdminRole
	_ = probeServerVer
	_ = probeFeaturesOk
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

func requireRedisx(sess *session.Session) error {
	if sess == nil {
		return fmt.Errorf("shell not connected")
	}
	caps := sess.Capabilities()
	if !caps.IsRedisx {
		return fmt.Errorf("connected peer is not a redisx server — Extended Commands / Meta Management are only available on redisx; try raw RESP forwarding for standard commands")
	}
	return nil
}

func requireRedisxDoc(sess *session.Session) error {
	if err := requireRedisx(sess); err != nil {
		return err
	}
	caps := sess.Capabilities()
	if !caps.TypedDocs {
		return fmt.Errorf("server reports typed_doc feature disabled")
	}
	return nil
}

func requireRedisxIndex(sess *session.Session) error {
	if err := requireRedisx(sess); err != nil {
		return err
	}
	caps := sess.Capabilities()
	if !caps.TypedIndexes {
		return fmt.Errorf("server reports typed_indexes feature disabled")
	}
	return nil
}

func registerBuiltins(app *tui.CLIApp) {
	ping := &tui.CLICommand{
		Use:     "ping",
		Short:   "send PING to the admin port and print PONG",
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
		Short:   "print the redisx admin shell version",
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
		Short: "Local shell introspection (handshake via HELLO). Subcommands: HELP | VERSION | CAPS | COMMANDS; DIAGNOSE = local probe debug; no args → HELP",
		Long: `Subcommands are ALL local (read from the handshake HELLO reply; no extra wire commands):
  redisx HELP          — subcommand list
  redisx VERSION       — server version string (from HELLO)
  redisx CAPS          — server capabilities + features + live stats (from HELLO)
  redisx COMMANDS      — server-side live command list (Basic/Extended/Meta Management) (from HELLO)
  redisx DIAGNOSE      — debug handshake probe + Banner FrozenCaps vs. Session.Capabilities vs. server reply

With 0 args, redisx is equivalent to "redisx HELP".`,
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
				fmt.Println(cBold("redisx HELP (all read locally from handshake HELLO)"))
				items := []string{
					"redisx HELP          — this text",
					"redisx VERSION       — server version string (HELLO version)",
					"redisx CAPS          — server capabilities + features + live stats (HELLO features/stats/storage)",
					"redisx COMMANDS      — server-side live command list (Basic/Extended/Meta Management) (HELLO commands)",
					"redisx DIAGNOSE      — local probe + cache compare + force RefreshCapabilities if stale",
				}
				for i, it := range items {
					fmt.Printf("  %d) %s\n", i+1, it)
				}
				return nil
			case "VERSION":
				caps := data.FrozenCaps
				if caps.ServerVer == "" {
					caps = sess.Capabilities()
				}
				if caps.ServerVer == "" {
					return fmt.Errorf("server version not available (HELLO handshake did not provide version)")
				}
				fmt.Println(caps.ServerVer)
				return nil
			case "CAPS", "CAPABILITIES":
				caps := data.FrozenCaps
				if !caps.IsRedisx {
					if _, now, ok := sess.RefreshCapabilities(); ok {
						data.FrozenCaps = now
						caps = now
					}
				}
				if !caps.IsRedisx {
					return fmt.Errorf("no capabilities available (peer is not a redisx server or HELLO failed)")
				}
				out := map[string]any{
					"server":         "redisx",
					"server_version": caps.ServerVer,
					"dual_port":      caps.DualPort,
					"admin_role":     caps.AdminRole,
					"features": map[string]any{
						"typed_docs":    caps.TypedDocs,
						"typed_indexes": caps.TypedIndexes,
						"live_rebuild":  caps.LiveRebuild,
						"write_hooks":   caps.WriteHooks,
						"search_index":  caps.SearchIndex,
						"pubsub":        caps.PubSub,
					},
					"stats": map[string]any{
						"namespaces": caps.StatsNs,
						"indexes":    caps.StatsIdx,
					},
					"storage": map[string]any{
						"mode": caps.StorageMode,
					},
				}
				wizard.PrintAny(os.Stdout, out)
				return nil
			case "COMMANDS", "CMDS", "COMMAND":
				caps := data.FrozenCaps
				if !caps.IsRedisx {
					if _, now, ok := sess.RefreshCapabilities(); ok {
						data.FrozenCaps = now
						caps = now
					}
				}
				if caps.Commands == nil {
					return fmt.Errorf("no command list available (peer is not a redisx server or HELLO did not include commands)")
				}
				groups := map[string][][2]string{}
				for g, entries := range caps.Commands.Groups {
					rows := make([][2]string, 0, len(entries))
					for _, e := range entries {
						rows = append(rows, [2]string{e.Name, e.Usage})
					}
					groups[g] = rows
				}
				roleLine := ""
				{
					h, p := "127.0.0.1", "7381"
					if data != nil {
						h, p = data.HostPort()
					}
					role := "app"
					mode := "app mode — meta commands may be rejected by server (No Privilege)"
					if caps.AdminRole {
						role = "admin"
						mode = "admin shell — typed docs & indexes available"
					}
					roleLine = cBold("Connected: ") + cCyan(role+" "+h+":"+p) + "  (" + mode + ")"
					if caps.ServerVer != "" {
						roleLine += "  [" + cDim("server="+caps.ServerVer) + "]"
					}
				}
				printServerCommandsList(data, groups, caps.Commands.GroupOrder, roleLine)
				return nil
			default:
				return fmt.Errorf("unknown redisx subcommand %q; try `redisx HELP`", useArgs[0])
			}
		},
	}
	app.Register(redisxCmd)
	trackCmd("redisx", redisxCmd)
}

func registerDocCmds(app *tui.CLIApp) {
	regdoc := &tui.CLICommand{
		Use:   "regdoc [spec_json]",
		Short: "register a Doc schema — no args = step-by-step wizard; 1 JSON arg = raw wire script mode",
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			cache := cacheFromCtx(ctx)
			if err := requireRedisxDoc(sess); err != nil {
				return err
			}
			if len(args) == 0 {
				return wizard.RunCreateDocWizard(sess, cache)
			}
			if len(args) > 1 {
				return fmt.Errorf("regdoc accepts 0 or 1 arg(s) (got %d)", len(args))
			}
			if !json.Valid([]byte(args[0])) {
				return errors.New("spec_json is not valid JSON")
			}
			v, err := sess.RawDo([]any{"regdoc", args[0]}).Result()
			if err != nil {
				return err
			}
			cache.Invalidate()
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
		MinArgs: 0, MaxArgs: 1,
	}
	app.Register(regdoc)
	trackCmd("regdoc", regdoc)

	lsdoc := &tui.CLICommand{
		Use:     "lsdoc [pattern]",
		Short:   "list registered Doc namespaces (optionally filtered by GLOB pattern)",
		MinArgs: 0, MaxArgs: 1,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisxDoc(sess); err != nil {
				return err
			}
			callArgs := []any{"lsdoc"}
			for _, arg := range args {
				callArgs = append(callArgs, arg)
			}
			v, err := sess.RawDo(callArgs).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(lsdoc)
	trackCmd("lsdoc", lsdoc)

	desdoc := &tui.CLICommand{
		Use:     "desdoc <namespace>",
		Short:   "describe a single registered Doc schema (fields, KeyAttrs, schemaVersion)",
		MinArgs: 1, MaxArgs: 1,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisxDoc(sess); err != nil {
				return err
			}
			v, err := sess.RawDo([]any{"desdoc", args[0]}).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(desdoc)
	trackCmd("desdoc", desdoc)

	listdocs := &tui.CLICommand{
		Use:     "!listdocs [pattern]",
		Aliases: []string{"listdocs"},
		Short:   "alias for lsdoc",
		MinArgs: 0, MaxArgs: 1,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisxDoc(sess); err != nil {
				return err
			}
			callArgs := []any{"lsdoc"}
			for _, arg := range args {
				callArgs = append(callArgs, arg)
			}
			v, err := sess.RawDo(callArgs).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(listdocs)
	trackCmd("!listdocs", listdocs)

	describeddoc := &tui.CLICommand{
		Use:     "!describedoc <namespace>",
		Aliases: []string{"describedoc"},
		Short:   "alias for desdoc",
		MinArgs: 1, MaxArgs: 1,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisxDoc(sess); err != nil {
				return err
			}
			v, err := sess.RawDo([]any{"desdoc", args[0]}).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(describeddoc)
	trackCmd("!describedoc", describeddoc)

	createdoc := &tui.CLICommand{
		Use:     "!createdoc",
		Aliases: []string{"createdoc"},
		Short:   "(REPL sugar) 5-step wizard to register a new Doc schema — avoids one-shot JSON typos",
		MinArgs: 0, MaxArgs: 0,
		Run: func(ctx tui.RunContext, _ []string) error {
			sess := sessFromCtx(ctx)
			cache := cacheFromCtx(ctx)
			if err := requireRedisxDoc(sess); err != nil {
				return err
			}
			return wizard.RunCreateDocWizard(sess, cache)
		},
	}
	app.Register(createdoc)
	trackCmd("!createdoc", createdoc)
}

func registerIdxCmds(app *tui.CLIApp) {
	regidx := &tui.CLICommand{
		Use:   "regidx [owner_ns logical_name json_path [UNIQUE] [TYPE <type>]]",
		Short: "register a secondary index — no args = step-by-step wizard; 3..6 args = raw wire script mode",
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			cache := cacheFromCtx(ctx)
			if err := requireRedisxIndex(sess); err != nil {
				return err
			}
			if len(args) == 0 {
				return wizard.RunCreateIdxWizard(sess, cache)
			}
			if len(args) < 3 || len(args) > 6 {
				return fmt.Errorf("regidx accepts 0 args (wizard) or 3..6 args (owner_ns logical_name json_path [UNIQUE] [TYPE <type>]); received %d", len(args))
			}
			callArgs := make([]any, 0, 1+len(args))
			callArgs = append(callArgs, "regidx")
			for _, arg := range args {
				callArgs = append(callArgs, arg)
			}
			v, err := sess.RawDo(callArgs).Result()
			if err != nil {
				return err
			}
			cache.Invalidate()
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
		MinArgs: -1, MaxArgs: -1,
	}
	app.Register(regidx)
	trackCmd("regidx", regidx)

	lsidx := &tui.CLICommand{
		Use:     "lsidx [owner_ns]",
		Short:   "list registered indexes (optionally filtered by owner namespace)",
		MinArgs: 0, MaxArgs: 1,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisxIndex(sess); err != nil {
				return err
			}
			callArgs := []any{"lsidx"}
			for _, arg := range args {
				callArgs = append(callArgs, arg)
			}
			v, err := sess.RawDo(callArgs).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(lsidx)
	trackCmd("lsidx", lsidx)

	listidx := &tui.CLICommand{
		Use:     "!listindexes [owner_ns]",
		Aliases: []string{"listindexes"},
		Short:   "alias for lsidx",
		MinArgs: 0, MaxArgs: 1,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisxIndex(sess); err != nil {
				return err
			}
			callArgs := []any{"lsidx"}
			for _, arg := range args {
				callArgs = append(callArgs, arg)
			}
			v, err := sess.RawDo(callArgs).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(listidx)
	trackCmd("!listindexes", listidx)

	delidx := &tui.CLICommand{
		Use:     "delidx <owner_ns> <logical_name>",
		Short:   "(raw wire, script-only) delete an index — prefer `!dropindex` for the 2-phase confirm",
		MinArgs: 2, MaxArgs: 2,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			cache := cacheFromCtx(ctx)
			if err := requireRedisxIndex(sess); err != nil {
				return err
			}
			v, err := sess.RawDo([]any{"delidx", args[0], args[1]}).Result()
			if err != nil {
				return err
			}
			cache.Invalidate()
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(delidx)
	trackCmd("delidx", delidx)

	createidx := &tui.CLICommand{
		Use:     "!createindex",
		Aliases: []string{"createindex"},
		Short:   "(REPL sugar) 4-step wizard to create a secondary index — avoids one-shot arg mistakes",
		MinArgs: 0, MaxArgs: 0,
		Run: func(ctx tui.RunContext, _ []string) error {
			sess := sessFromCtx(ctx)
			cache := cacheFromCtx(ctx)
			if err := requireRedisxIndex(sess); err != nil {
				return err
			}
			return wizard.RunCreateIdxWizard(sess, cache)
		},
	}
	app.Register(createidx)
	trackCmd("!createindex", createidx)

	dropidx := &tui.CLICommand{
		Use:     "!dropindex",
		Aliases: []string{"dropindex"},
		Short:   "(REPL sugar) 2-phase destroy confirm: type exact full-name, then type DESTROY — irreversible",
		MinArgs: 0, MaxArgs: 0,
		Run: func(ctx tui.RunContext, _ []string) error {
			sess := sessFromCtx(ctx)
			cache := cacheFromCtx(ctx)
			if err := requireRedisxIndex(sess); err != nil {
				return err
			}
			return wizard.RunDropIdxWizard(sess, cache)
		},
	}
	app.Register(dropidx)
	trackCmd("!dropindex", dropidx)
}

func registerExtendedCmds(app *tui.CLIApp) {
	sk := &tui.CLICommand{
		Use:     "searchkey <key_prefix_pattern> [filter_json] [desc]",
		Aliases: []string{"sk"},
		Short:   "(Extended) SEARCHKEY — range scan by key prefix/pattern with JSON field filter",
		Long: `SEARCHKEY uses the document key range (prefix / keys() / GLOB pattern) plus a
JSON filter for field equality/range and returns matching keys.
Examples:
  searchkey user:*                                  # key GLOB, no filter
  searchkey '{"pattern":"user:*"}'                  # explicit JSON keyrange
  searchkey user:* '{"bucket":"A"}'                 # GLOB + filter
  searchkey '{"gt":"user:u010","lt":"user:u020"}' '{"amt":{"gt":100}}' desc`,
		MinArgs: 1, MaxArgs: 3,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisx(sess); err != nil {
				return err
			}
			call := make([]any, 0, 1+len(args))
			call = append(call, "SEARCHKEY")
			for _, arg := range args {
				call = append(call, arg)
			}
			v, err := sess.RawDo(call).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(sk)
	trackCmd("searchkey", sk)

	si := &tui.CLICommand{
		Use:     "searchindex <namespace.index_name> <keyrange> [filter_json] [desc]",
		Aliases: []string{"si"},
		Short:   "(Extended) SEARCHINDEX — secondary-index ordered scan with JSON field filter",
		Long: `SEARCHINDEX looks up a typed secondary index registered via regidx/!createindex
and returns document keys ordered by the index score.
Examples:
  searchindex user.score '{"prefix":"user:""}'
  searchindex user.score '{"gt":"user:u000","lt":"user:u100"}' '{"bucket":"A"}' desc`,
		MinArgs: 2, MaxArgs: 4,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisx(sess); err != nil {
				return err
			}
			call := make([]any, 0, 1+len(args))
			call = append(call, "SEARCHINDEX")
			for _, arg := range args {
				call = append(call, arg)
			}
			v, err := sess.RawDo(call).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(si)
	trackCmd("searchindex", si)

	upd := &tui.CLICommand{
		Use:     "update <kr_json> <filter_json> <update_json> [LIMIT <count>]",
		Aliases: []string{"upd"},
		Short:   "(Extended) UPDATE — bulk mutate JSON fields on KeyRange-scoped keys filtered by JSON condition",
		Long: `UPDATE applies a mutation JSON (SET field=value, INC numeric, PUSH list, etc.) to every key
matched by the key range and filter, returning the updated key list as an array of strings.

key range JSON examples:
  {"prefix":"user:"}                         # namespace prefix scan
  {"gt":"user:u010","lt":"user:u100"}        # exclusive lexicographic range
  {"gte":"user:u010","lte":"user:u099"}      # inclusive lexicographic range

filter JSON examples:
  {}                                         # no filter
  {"bucket":"A"}                             # field equality
  {"amt":{"gte":100,"lt":1000}}              # numeric range
  {"tags":{"contains":"vip"}}                # array contains

update JSON examples (full shape via xcmd.ParseUpdate):
  {"ops":[{"op":"set","path":"status","val":"banned"},{"op":"inc","path":"strikes","val":1}]}

Use the trailing optional "LIMIT <count>" to cap how many keys are touched on the server side
(count must be a positive integer; server default = unlimited).

Examples:
  update '{"prefix":"user:"}' '{"country":"US"}' '{"ops":[{"op":"set","path":"region","val":"NA"}]}'
  upd '{"gt":"u:0","lt":"u:1000"}' '{"score":{"lt":0}}' '{"ops":[{"op":"inc","path":"score","val":10}]}' LIMIT 50`,
		MinArgs: 3, MaxArgs: 5,
		Run: func(ctx tui.RunContext, args []string) error {
			sess := sessFromCtx(ctx)
			if err := requireRedisx(sess); err != nil {
				return err
			}
			if len(args) == 4 {
				return fmt.Errorf("update accepts 3 args or 5 args (<kr> <filter> <update> [LIMIT <count>]); got 4. Did you mean 'LIMIT <count>'?")
			}
			if len(args) == 5 {
				if strings.ToUpper(args[3]) != "LIMIT" {
					return fmt.Errorf("update 4th token must be 'LIMIT' (case-insensitive); got %q", args[3])
				}
				n, err := strconv.Atoi(args[4])
				if err != nil || n <= 0 {
					return fmt.Errorf("update LIMIT count must be a positive integer; got %q", args[4])
				}
				_ = n
			}
			call := make([]any, 0, 1+len(args))
			call = append(call, "UPDATE")
			if len(args) == 5 {
				call = append(call, args[0], args[1], args[2], strings.ToUpper(args[3]), args[4])
			} else {
				call = append(call, args[0], args[1], args[2])
			}
			v, err := sess.RawDo(call).Result()
			if err != nil {
				return err
			}
			wizard.PrintAny(os.Stdout, v)
			return nil
		},
	}
	app.Register(upd)
	trackCmd("update", upd)
}
