package internal

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/cmd/redisx/internal/wizard"
)

func noShellCmds(name string) bool {
	switch name {
	case "clear", "!clear", "cls", "version", "!version":
		return true
	}
	return false
}

func BuildApp(sessPtr **session.Session) *tui.CLIApp {
	ud := &AppData{Session: sessPtr, Cache: session.NewCache()}
	var app *tui.CLIApp
	hooks := tui.AppHooks{
		BeforeRun: func(ctx tui.RunContext, name string, args []string) error {
			data, _ := ctx.UserData.(*AppData)
			if data == nil {
				return nil
			}
			if noShellCmds(name) {
				return nil
			}
			if *data.Session != nil {
				return nil
			}
			created, err := session.New(session.Options{
				Host:      data.Opts.Host,
				Port:      data.Opts.Port,
				AdminAuth: data.Opts.AdminAuth,
				TimeoutMs: data.Opts.TimeoutMs,
			})
			if err != nil {
				return err
			}
			*data.Session = created
			data.FrozenCaps = created.Capabilities()
			return nil
		},
		OnUnknown: func(ctx tui.RunContext, args []string) (handled bool, err error) {
			data, _ := ctx.UserData.(*AppData)
			if data == nil || *data.Session == nil {
				return false, nil
			}
			if len(args) == 0 {
				return false, nil
			}
			w := args[0]
			if strings.HasPrefix(w, "!") {
				return false, fmt.Errorf("unknown sugar command %q — run !help to list available wizards", w)
			}
			sess := *data.Session
			call := make([]any, 0, len(args))
			for _, a := range args {
				call = append(call, a)
			}
			fmt.Fprintln(os.Stderr, cYellow("forwarding as raw RESP command (not a registered sub-command)"))
			v, err := sess.RawDo(call).Result()
			if err != nil {
				return true, err
			}
			wizard.PrintAny(os.Stdout, v)
			return true, nil
		},
		NeedsContext: func(name string, args []string) bool {
			return !noShellCmds(name)
		},
		Banner: func(ctx tui.RunContext) string {
			data, _ := ctx.UserData.(*AppData)
			host, port := "127.0.0.1", "7381"
			var caps session.Capabilities
			if data != nil {
				host, port = data.HostPort()
				if !data.FrozenCaps.IsRedisx {
					if *data.Session != nil {
						_, now, ok := (*data.Session).RefreshCapabilities()
						if ok {
							data.FrozenCaps = now
						}
					}
				}
				caps = data.FrozenCaps
			}
			return BannerFor(caps, host, port)
		},
		EnterREPL: func(ctx tui.RunContext) error {
			data, _ := ctx.UserData.(*AppData)
			if data == nil {
				return fmt.Errorf("no app data")
			}
			if app == nil {
				return fmt.Errorf("app not ready")
			}
			if !data.FrozenCaps.IsRedisx {
				if *data.Session != nil {
					_, now, ok := (*data.Session).RefreshCapabilities()
					if ok {
						data.FrozenCaps = now
					}
				}
			}
			caps := data.FrozenCaps
			host, port := data.HostPort()
			fmt.Print(BannerFor(caps, host, port))
			return RunREPL(app, data, caps)
		},
		GlobalHelp: func(ctx tui.RunContext, groups map[string][]tui.GroupEntry, flagHelpText string) string {
			var sb strings.Builder
			data, _ := ctx.UserData.(*AppData)
			var caps session.Capabilities
			if data != nil {
				if !data.FrozenCaps.IsRedisx {
					if *data.Session != nil {
						_, now, ok := (*data.Session).RefreshCapabilities()
						if ok {
							data.FrozenCaps = now
						}
					}
				}
				caps = data.FrozenCaps
			}
			isRedisx := caps.IsRedisx
			hasTypedDocs := isRedisx && caps.TypedDocs
			hasTypedIndexes := isRedisx && caps.TypedIndexes
			hasSearch := isRedisx && caps.SearchIndex
			if isRedisx {
				sb.WriteString(`redisx is the admin command-line client for the redisx embedded JSON+KV database.

It connects ONLY via the RESP wire protocol to the redisx ADMIN PORT
(default 127.0.0.1:7381). It never opens the database files directly.

Run without sub-commands to enter the interactive REPL. Pass sub-commands
to execute a single action and exit (e.g. "redisx regsch").

`)
			} else {
				sb.WriteString(`redisx CLI — generic RESP client mode.

Connected peer is NOT a redisx server; this tool operates as a plain
Redis RESP shell (SET / GET / KEYS / raw forwarding). Extended and Meta
commands are provided only by redisx and are hidden in this mode.

Run without sub-commands to enter the interactive REPL. Pass sub-commands
to execute a single action and exit.

`)
			}
			sb.WriteString("USAGE:\n")
			fmt.Fprintf(&sb, "  redisx [flags] <subcommand> [args...]\n")
			fmt.Fprintf(&sb, "  redisx [flags]                       (enter REPL)\n\n")
			sb.WriteString("GLOBAL FLAGS (must come before any subcommand):\n")
			sb.WriteString(flagHelpText)
			sb.WriteString("\n")
			order := []string{"Basic Commands"}
			if isRedisx {
				if hasSearch {
					order = append(order, "Extended Commands")
				}
				if hasTypedDocs || hasTypedIndexes {
					order = append(order, "Meta Management")
				}
			}
			regrouped := map[string][]tui.GroupEntry{
				"Basic Commands":    {},
				"Extended Commands": {},
				"Meta Management":   {},
			}
			_ = groups
			for _, name := range currentCmdOrder {
				c, ok := currentCmds[name]
				if !ok || c.Hidden {
					continue
				}
				g := groupOf(name)
				switch g {
				case "Extended Commands":
					if !hasSearch {
						continue
					}
				case "Meta Management":
					if !isRedisx {
						continue
					}
					switch name {
					case "regsch", "dropsch", "!createsch":
						if !hasTypedDocs {
							continue
						}
					case "regidx", "dropidx", "!createidx", "!dropidx":
						if !hasTypedIndexes {
							continue
						}
					}
				}
				regrouped[g] = append(regrouped[g], tui.GroupEntry{Use: c.Use, Short: c.Short})
			}
			for _, g := range order {
				list := regrouped[g]
				if len(list) == 0 {
					continue
				}
				sort.Slice(list, func(i, j int) bool { return list[i].Use < list[j].Use })
				fmt.Fprintf(&sb, "%s:\n", cBold(g))
				width := 0
				for _, e := range list {
					if len(e.Use) > width {
						width = len(e.Use)
					}
				}
				for _, e := range list {
					pad := strings.Repeat(" ", width-len(e.Use)+2)
					fmt.Fprintf(&sb, "  %s%s%s\n", e.Use, pad, e.Short)
				}
				sb.WriteString("\n")
			}
			if !isRedisx {
				sb.WriteString(cYellow("notice: connected peer is not a redisx server (REDISX CAPABILITIES returned unknown command).\n"))
				sb.WriteString(cYellow("        only Basic Commands + raw RESP forwarding are available.\n\n"))
			}
			promptWord := "redisx"
			if isRedisx && !caps.AdminRole {
				promptWord = "redisx (app)"
			} else if !isRedisx {
				promptWord = "redis"
			}
			_, _ = fmt.Fprintf(&sb, "REPL-only shortcuts (type these inside the %s> prompt):\n", promptWord)
			sb.WriteString("  <enter> with empty = no-op; quit / exit / logout / !quit = exit REPL\n")
			sb.WriteString("  unrecognised non-! words are forwarded verbatim as raw RESP commands (e.g. SET, GET, KEYS)\n")
			sb.WriteString("  unrecognised !words error out and hint at !help\n\n")
			return sb.String()
		},
		DispatchError: func(ctx tui.RunContext, err error) {
			fmt.Fprintln(os.Stderr, cRed("error: "+err.Error()))
		},
	}
	app = tui.NewCLIApp("redisx", Version, hooks)
	app.SetUserData(ud)
	app.Flags().StringVar(&ud.Opts.Host, "host", "H", "127.0.0.1", "admin-port host (never bind public; cross-machine access via Caddy+mTLS)")
	app.Flags().IntVar(&ud.Opts.Port, "port", "p", 7381, "admin-port port")
	app.Flags().StringVar(&ud.Opts.AdminAuth, "admin-auth", "a", "", "admin AUTH key (required unless server was started without --admin-auth)")
	app.Flags().IntVar(&ud.Opts.TimeoutMs, "timeout-ms", "", 3000, "dial / read / write timeout in milliseconds")
	app.InstallHelpVersionFlags()
	currentCmds = map[string]*tui.CLICommand{}
	currentCmdOrder = []string{}
	registerAllCommands(app)
	_ = runtime.GOOS
	return app
}

var (
	currentCmds     = map[string]*tui.CLICommand{}
	currentCmdOrder = []string{}
)

func trackCmd(name string, c *tui.CLICommand) {
	currentCmds[name] = c
	currentCmdOrder = append(currentCmdOrder, name)
}

func groupOf(name string) string {
	switch name {
	case "searchkey", "searchindex", "update":
		return "Extended Commands"
	case "regsch", "dropsch", "!createsch",
		"regidx", "dropidx", "!createidx", "!dropidx":
		return "Meta Management"
	}
	return "Basic Commands"
}

func registerAllCommands(app *tui.CLIApp) {
	registerBuiltins(app)
	registerExtendedCmds(app)
	registerDocCmds(app)
	registerIdxCmds(app)
}
