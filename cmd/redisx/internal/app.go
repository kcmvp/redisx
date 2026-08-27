package internal

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
)

func noShellCmds(name string) bool {
	switch name {
	case "clear", "!clear", "cls", "version", "!version", "con", "connect":
		return true
	}
	return false
}

// notConnectedErr is the error every connection-needing command returns when
// the shell has no session yet.
func notConnectedErr(data *AppData) error {
	hint := "not connected — connect with `con -h <host> -p <port> [-a <auth>]` or `con <host> <port> [auth]`"
	if data != nil && len(data.Endpoints) > 0 {
		hint += " or " + ShortcutNames(data.Endpoints) + " (from ./" + LocalConfigFile + ")"
	}
	return fmt.Errorf("%s", hint)
}

// connectSession dials a new session and swaps it in as the shell's current
// session, closing the previous one when present.
func connectSession(data *AppData, host string, port int, auth string) error {
	if data == nil {
		return fmt.Errorf("no app data")
	}
	timeoutMs := data.Opts.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 3000
	}
	created, err := session.New(session.Options{
		Host:      host,
		Port:      port,
		Auth:      auth,
		TimeoutMs: timeoutMs,
	})
	if err != nil {
		return err
	}
	if old := *data.Session; old != nil {
		_ = old.Close()
	}
	*data.Session = created
	data.ConnHost = host
	data.ConnPort = port
	data.Cache.Invalidate()
	return nil
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
			// Entering the REPL (name == "") never auto-dials; the user
			// connects explicitly via con / !app / !ctrl.
			if name == "" {
				return nil
			}
			if noShellCmds(name) {
				return nil
			}
			if *data.Session != nil {
				return nil
			}
			if data.InREPL {
				return notConnectedErr(data)
			}
			// One-shot mode (subcommand on the command line) keeps the lazy
			// dial from the -h/-p/-a flags for script usage.
			created, err := session.New(session.Options{
				Host:      data.Opts.Host,
				Port:      data.Opts.Port,
				Auth:      data.Opts.Auth,
				TimeoutMs: data.Opts.TimeoutMs,
			})
			if err != nil {
				return err
			}
			*data.Session = created
			data.ConnHost = data.Opts.Host
			data.ConnPort = data.Opts.Port
			return nil
		},
		OnUnknown: func(ctx tui.RunContext, args []string) (handled bool, err error) {
			data, _ := ctx.UserData.(*AppData)
			if data == nil {
				return false, nil
			}
			if len(args) == 0 {
				return false, nil
			}
			w := args[0]
			if strings.HasPrefix(w, "!") {
				if ep := FindEndpoint(data.Endpoints, strings.TrimPrefix(w, "!")); ep != nil {
					if err := connectSession(data, ep.Host, ep.Port, ep.Auth); err != nil {
						return true, err
					}
					fmt.Printf("connected to %s (%s)\n", cCyan(ep.Addr()), ep.Name)
					return true, nil
				}
				return false, fmt.Errorf("unknown sugar command %q — run !help to list available wizards", w)
			}
			if *data.Session == nil {
				return true, notConnectedErr(data)
			}
			call := make([]any, 0, len(args))
			for _, a := range args {
				call = append(call, a)
			}
			v, err := (*data.Session).RawDo(call).Result()
			if err != nil {
				return true, err
			}
			wizardPrintAny(os.Stdout, v)
			return true, nil
		},
		NeedsContext: func(name string, args []string) bool {
			return !noShellCmds(name)
		},
		Banner: func(ctx tui.RunContext) string {
			data, _ := ctx.UserData.(*AppData)
			if data != nil && *data.Session != nil {
				return BannerFor(data.ConnHost, fmt.Sprintf("%d", data.ConnPort))
			}
			var eps []LocalEndpoint
			if data != nil {
				eps = data.Endpoints
			}
			return BannerDisconnectedFor(eps)
		},
		EnterREPL: func(ctx tui.RunContext) error {
			data, _ := ctx.UserData.(*AppData)
			if data == nil {
				return fmt.Errorf("no app data")
			}
			if app == nil {
				return fmt.Errorf("app not ready")
			}
			var eps []LocalEndpoint
			if *data.Session != nil {
				fmt.Print(BannerFor(data.ConnHost, fmt.Sprintf("%d", data.ConnPort)))
			} else {
				eps = data.Endpoints
				fmt.Print(BannerDisconnectedFor(eps))
			}
			return RunREPL(app, data)
		},
		GlobalHelp: func(ctx tui.RunContext, groups map[string][]tui.GroupEntry, flagHelpText string) string {
			var sb strings.Builder
			sb.WriteString(`redisx is the command-line client for the redisx embedded JSON+KV database.

It connects ONLY via the RESP wire protocol to the configured peer
(default 127.0.0.1:7381). It never opens the database files directly.

Support, availability and privilege enforcement are entirely server-side:
any command not listed below is forwarded VERBATIM as a raw RESP wire call,
and the connected peer decides whether to accept, reject, or ignore it.

Run without sub-commands to enter the interactive REPL. Pass sub-commands
to execute a single action and exit (e.g. "redisx regsch").

`)
			sb.WriteString("USAGE:\n")
			fmt.Fprintf(&sb, "  redisx [flags] <subcommand> [args...]\n")
			fmt.Fprintf(&sb, "  redisx [flags]                       (enter REPL)\n\n")
			sb.WriteString("GLOBAL FLAGS (must come before any subcommand):\n")
			sb.WriteString(flagHelpText)
			sb.WriteString("\n")
			order := []string{"Basic Commands", "Extended Commands", "Meta Management"}
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
				regrouped[g] = append(regrouped[g], tui.GroupEntry{Use: c.Use, Short: c.Short})
			}
			for _, g := range order {
				list := regrouped[g]
				extra := ""
				var hints []string
				switch g {
				case "Extended Commands":
					hints = []string{"example searchkey", "example searchindex", "example update"}
				case "Meta Management":
					hints = []string{"example regsch", "example regidx", "example dropsch", "example dropidx"}
				}
				if len(hints) > 0 {
					extra += "  (no locally-registered shortcuts here — entries are raw RESP passthrough."
					extra += " Run `redisx COMMANDS` for the full catalogue;\n"
					extra += "   type `example <target>` for a paste-ready template, e.g. `" + strings.Join(hints, "` / `") + "`.)\n"
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
				if extra != "" {
					sb.WriteString(extra)
				}
				sb.WriteString("\n")
			}
			fmt.Fprintf(&sb, "REPL-only shortcuts (type these inside the redisx> prompt):\n")
			sb.WriteString("  con [-h host] [-p port] [-a auth] | con [host] [port] [auth] — connect to a server, redis-cli style (no args reuses the -h/-p/-a startup values)\n")
			ud2, _ := ctx.UserData.(*AppData)
			if ud2 != nil && len(ud2.Endpoints) > 0 {
				fmt.Fprintf(&sb, "  %s — connect to the matching ./%s endpoint\n", ShortcutNames(ud2.Endpoints), LocalConfigFile)
			}
			sb.WriteString("  <enter> with empty = no-op; quit / exit / logout / !quit = exit REPL\n")
			sb.WriteString("  unrecognised non-! words are forwarded verbatim as raw RESP commands (e.g. SET, GET, KEYS)\n")
			sb.WriteString("  unrecognised !words error out and hint at !help\n")
			sb.WriteString("  note: server-side ERR replies, privilege checks and unknown-command listings are authoritative.\n\n")
			return sb.String()
		},
		DispatchError: func(ctx tui.RunContext, err error) {
			fmt.Fprintln(os.Stderr, cRed("error: "+err.Error()))
		},
	}
	app = tui.NewCLIApp("redisx", Version, hooks)
	app.SetUserData(ud)
	// Discover ./redisx.yaml endpoint shortcuts (top-level keys → !app/!ctrl/...).
	if eps, err := LoadLocalConfig("."); err != nil {
		fmt.Fprintln(os.Stderr, cYellow("warning: "+err.Error()))
	} else if len(eps) > 0 {
		ud.Endpoints = eps
	}
	app.Flags().StringVar(&ud.Opts.Host, "host", "h", "127.0.0.1", "server host (never expose publicly; cross-machine access via Caddy+mTLS)")
	app.Flags().IntVar(&ud.Opts.Port, "port", "p", 7381, "server port")
	app.Flags().StringVar(&ud.Opts.Auth, "auth", "a", "", "AUTH key (required only if the target server was started with --auth enabled)")
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
	case "!createsch", "!createidx", "!dropidx":
		return "Meta Management"
	}
	return "Basic Commands"
}
