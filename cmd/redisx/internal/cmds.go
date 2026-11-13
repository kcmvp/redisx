package internal

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
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
		GroupOrder: []string{"Extended"},
		Groups: map[string][]commandEntry{
			"Extended": {
				{Name: proto.LowerCmdUpdate, Usage: "UPDATE <keyRange> <filter> <updateJSON> [LIMIT N]  — RFC6902-style patch on typed docs matching the filter"},
				{Name: proto.LowerCmdSearchKey, Usage: "SEARCHKEY <keyRange> <filter> [ASC|DESC] [LIMIT N]  — filter → pick keys"},
				{Name: proto.LowerCmdSearchIndex, Usage: "SEARCHINDEX <fullIndexName> <keyRange> <filter> [ASC|DESC] [LIMIT N]  — query a named typed index"},
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

var examples = map[string][]string{
	"example": {
		"example (no args)   — list example targets",
		"example <target>    — copy-paste a full template for a command",
		"",
		"Targets:",
		"  update | searchkey | searchindex",
	},
	"update": {
		"# UPDATE works on typed documents ONLY (namespace must be declared in code via x.RegisterSchema).",
		"# Wire form: UPDATE <keyRange> <filter> <updateJSON> [LIMIT N]",
		"#   keyRange   — JSON object or bare pattern shorthand (\"user:*\"), anchored to one namespace",
		"#   filter     — Mongo-style JSON ({} = all docs in range)",
		"#   updateJSON — array of {op, path, value} patch ops (RFC6902-style, SSoT: x.ParseUpdate)",
		"",
		"UPDATE 'user:*' '{\"name\":{\"$eq\":\"leto\"}}' '[{\"op\":\"replace\",\"path\":\"/age\",\"value\":46}]'",
		"",
		"# Ops:",
		"#   replace — set the value at path (creates it if missing)",
		"#   remove  — delete the value at path",
		"#   path uses JSON-pointer form: /age, /profile/email",
	},
	"searchkey": {
		"# SEARCHKEY: <keyRange> <filter> [ASC|DESC] [LIMIT N]. Returns matching keys from the filtered range.",
		"# Works on any registered typed-doc namespace (and raw KV paths with a ':').",
		"",
		"SEARCHKEY 'user:*' '{\"age\":{\"$gte\":18},\"profile.email\":{\"$contains\":\"@atreides\"}}'",
		"",
		"# keyRange forms (JSON object or bare pattern shorthand):",
		"#   user:*                                        — shorthand for {\"op\":\"pattern\",\"p\":\"user:*\"}",
		"#   {\"op\":\"bt\",\"ge\":\"user:a\",\"lt\":\"user:n\"}   — ops: bt gt gte lt lte pattern",
		"# Filter operators (multiple keys AND implicitly; $and/$or arrays combine):",
		"#   $eq $neq $gt $gte $lt $lte $contains $in — \"field\": <scalar> is shorthand for implicit $eq",
		"#   {} = empty filter passes everything",
		"# Notes:",
		"#   - key-range must be anchored (no leading wildcard); doc namespace must be declared in code via x.RegisterSchema",
		"#   - LIMIT is a trailing arg: SEARCHKEY 'user:*' '{}' DESC LIMIT 10",
	},
	"searchindex": {
		"# SEARCHINDEX: <fullIndexName> <keyRange> <filter> [ASC|DESC] [LIMIT N] — query a named typed index.",
		"# fullIndexName format: <ownerNs>:<logical> (SSoT: naming.ParseIdxFullName), owner ns must be declared in code via x.RegisterSchema.",
		"",
		"SEARCHINDEX 'user:age_email_idx' '{\"op\":\"pattern\",\"p\":\"user:*\"}' '{\"age\":{\"$gte\":18}}'",
		"",
		"# Arguments:",
		"#   keyRange — MUST be a JSON object here (no shorthand): {\"op\":\"pattern\",\"p\":\"user:*\"} or bt/gt/gte/lt/lte forms",
		"#   filter   — same Mongo-style grammar as SEARCHKEY ({} = everything)",
		"# Notes:",
		"#   - if the index doesn't exist → ERR \"index not found\"",
		"#   - result is a flat list of PKs matching the index scan (not doc rows themselves — follow up with GET/MGET)",
	},
}

func printExample(which string) error {
	target := strings.ToLower(strings.TrimSpace(which))
	if target == "" || target == "list" || target == "help" {
		list := []string{"update", "searchkey", "searchindex"}
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

	con := &tui.CLICommand{
		Use:        "con [-h host] [-p port] [-a auth] | con [host] [port] [auth]",
		Aliases:    []string{"connect"},
		Short:      "connect to a server (redis-cli style flags or positional; no args reuses the -h/-p/-a startup values)",
		MinArgs:    0,
		MaxArgs:    -1,
		AllowFlags: true,
		Run: func(ctx tui.RunContext, args []string) error {
			data, _ := ctx.UserData.(*AppData)
			if data == nil {
				return fmt.Errorf("no app data")
			}
			host, port, auth := data.Opts.Host, data.Opts.Port, data.Opts.Auth
			fs := flag.NewFlagSet("con", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			fs.StringVar(&host, "h", host, "server host")
			fs.StringVar(&host, "host", host, "server host")
			fs.IntVar(&port, "p", port, "server port")
			fs.IntVar(&port, "port", port, "server port")
			fs.StringVar(&auth, "a", auth, "AUTH key")
			fs.StringVar(&auth, "auth", auth, "AUTH key")
			if err := fs.Parse(args); err != nil {
				return fmt.Errorf("con: %v (usage: con [-h host] [-p port] [-a auth] | con [host] [port] [auth])", err)
			}
			// Positional fallback on top of the parsed flags.
			switch rest := fs.Args(); len(rest) {
			case 0:
			case 1:
				host = rest[0]
			case 2, 3:
				p, err := strconv.Atoi(rest[1])
				if err != nil {
					return fmt.Errorf("invalid port %q: %v", rest[1], err)
				}
				host, port = rest[0], p
				if len(rest) == 3 {
					auth = rest[2]
				}
			default:
				return fmt.Errorf("con accepts at most 3 positional arg(s) (got %d)", len(rest))
			}
			if err := connectSession(data, host, port, auth); err != nil {
				return err
			}
			fmt.Printf("connected to %s\n", cCyan(fmt.Sprintf("%s:%d", host, port)))
			return nil
		},
	}
	app.Register(con)
	trackCmd("con", con)

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
Redis-compatible client: SET / GET / DEL / KEYS / AUTH /
UPDATE / SEARCHKEY / SEARCHINDEX / any-new-server-command all work without a
client rebuild. Support, availability and privilege enforcement are entirely
server-side — the CLI never pre-judges whether a command is "allowed".`,
		MinArgs: 0, MaxArgs: -1,
		Run: func(ctx tui.RunContext, args []string) error {
			data, _ := ctx.UserData.(*AppData)
			useArgs := args
			if len(useArgs) == 0 {
				useArgs = []string{"HELP"}
			}
			sub := strings.ToUpper(useArgs[0])
			switch sub {
			case "DIAGNOSE":
				sess := sessFromCtx(ctx)
				if sess == nil {
					return fmt.Errorf("shell not connected")
				}
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
				printCatalogue(data)
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
		Long: `No args or "example list" = show targets. "example searchindex" = paste-ready
searchindex template + field-by-field notes; same form for the other targets.

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
