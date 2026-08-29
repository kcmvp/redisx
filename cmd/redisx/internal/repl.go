package internal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/internal/proto"
)

func RunREPL(app *tui.CLIApp, data *AppData) error {
	if app == nil {
		return fmt.Errorf("RunREPL: app is nil")
	}
	if data == nil {
		return fmt.Errorf("RunREPL: no app data")
	}
	// REPL commands must not auto-dial from flags; they require an explicit
	// `con` / `!app` / `!ctrl` connection first.
	data.InREPL = true
	hp := app.Hooks()
	ioctx := tui.RunContext{UserData: data, IO: tui.DefaultIO()}
	_ = hp
	_ = ioctx
	hf := HistoryFileInternal()
	in := ioctx.IO.In
	_ = hf
	reader := bufio.NewReader(in)
	for {
		prompt := promptFor(data)
		_, _ = fmt.Fprint(ioctx.IO.Out, prompt)
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if line != "" {
					_, _ = handleREPLLine(app, data, line)
				}
				return nil
			}
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			appendHistoryInternal(trimmed)
		}
		quit, herr := handleREPLLine(app, data, line)
		if herr != nil {
			if hp.DispatchError != nil {
				hp.DispatchError(tui.RunContext{UserData: data, IO: tui.DefaultIO()}, herr)
			} else {
				fmt.Fprintln(os.Stderr, cRed("error: "+herr.Error()))
			}
		}
		if quit {
			return nil
		}
	}
}

func promptFor(data *AppData) string {
	if data == nil {
		return "redisx> "
	}
	if *data.Session == nil {
		return cYellow("redisx") + " (not connected)> "
	}
	return cCyan("redisx") + " " + data.ConnHost + ":" + fmt.Sprintf("%d", data.ConnPort) + "> "
}

func handleREPLLine(app *tui.CLIApp, data *AppData, line string) (bool, error) {
	trim := strings.TrimSpace(line)
	if trim == "" {
		return false, nil
	}
	parts, ok := shlexSplit(trim)
	if !ok {
		return false, fmt.Errorf("unterminated quote in line: %s", trim)
	}
	if len(parts) == 0 {
		return false, nil
	}
	name := parts[0]
	rest := parts[1:]
	switch strings.ToLower(name) {
	case "commands", "help", "?", "!help":
		want := ""
		if len(rest) > 0 {
			want = rest[0]
		}
		printCommands(app, data, want)
		return false, nil
	case "clear", "cls", "!clear":
		printScreenClear()
		return false, nil
	case "version", "!version":
		fmt.Printf("redisx %s\n", Version)
		return false, nil
	}
	return app.DispatchREPLLine(parts)
}

func printCommands(app *tui.CLIApp, data *AppData, want string) {
	if want != "" {
		c, canonical := app.Resolve(want)
		if c != nil {
			fmt.Println(app.HelpForCmd(c, canonical))
			return
		}
		if usage, lines, ok := wireCmdHelp(want); ok {
			fmt.Println(usage)
			fmt.Println()
			for _, l := range lines {
				fmt.Println(l)
			}
			return
		}
		fmt.Fprintln(os.Stderr, cRed(fmt.Sprintf("unknown command %q — for local commands try `help` / `commands`; for wire commands see the catalogue above", want)))
		return
	}
	printCatalogue(data)
}

// wireCmdHelp resolves a wire-command name or short alias (sk/si/upd) to its
// catalogue usage line and the paste-ready example block.
func wireCmdHelp(want string) (string, []string, bool) {
	target := strings.ToLower(strings.TrimSpace(want))
	switch target {
	case "sk":
		target = proto.LowerCmdSearchKey
	case "si":
		target = proto.LowerCmdSearchIndex
	case "upd":
		target = proto.LowerCmdUpdate
	}
	for _, entries := range defaultCommandGroups().Groups {
		for _, e := range entries {
			if e.Name != target {
				continue
			}
			lines, hasEx := examples[target]
			if !hasEx {
				return e.Usage, nil, true
			}
			return e.Usage, lines, true
		}
	}
	return "", nil, false
}

// printCatalogue renders the client-builtin SSoT command catalogue (Basic /
// Extended / Meta Management) plus the connection status line. It works with
// or without a session — the catalogue is decoupled from the server.
func printCatalogue(data *AppData) {
	if data == nil {
		return
	}
	groups := map[string][][2]string{}
	order := []string{"Basic", "Extended", "Meta Management"}
	for _, g := range order {
		groups[g] = nil
	}
	for _, name := range currentCmdOrder {
		c, ok := currentCmds[name]
		if !ok || c == nil || c.Hidden {
			continue
		}
		g := "Basic"
		if _, exists := groups[g]; !exists {
			groups[g] = nil
			order = append(order, g)
		}
		use := c.Use
		if use == "" {
			use = name
		}
		groups[g] = append(groups[g], [2]string{use, c.Short})
	}
	cmdGroups := defaultCommandGroups()
	for g, entries := range cmdGroups.Groups {
		for _, e := range entries {
			if e.Name == "" {
				continue
			}
			groups[g] = append(groups[g], [2]string{e.Name, e.Usage})
		}
	}
	h, p := data.HostPort()
	mode := "raw RESP passthrough — support & privilege enforcement handled server-side"
	if *data.Session == nil {
		fmt.Println(cBold("Not connected: ") + cYellow(h+":"+p) + "  (" + mode + " — connect with `con -h <host> -p <port> [-a <auth>]` or `con <host> <port> [auth]`)")
	} else {
		fmt.Println(cBold("Connected: ") + cCyan(h+":"+p) + "  (" + mode + ")")
	}
	fmt.Println()
	for _, g := range order {
		rows := groups[g]
		if len(rows) == 0 {
			continue
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
		fmt.Println(cBold(g) + ":")
		maxUse := 0
		for _, r := range rows {
			if len(r[0]) > maxUse {
				maxUse = len(r[0])
			}
		}
		for _, r := range rows {
			pad := strings.Repeat(" ", maxUse-len(r[0])+2)
			short := r[1]
			if short == "" {
				short = "—"
			}
			fmt.Println("  " + r[0] + pad + short)
		}
		fmt.Println()
	}
	fmt.Println(cDim("(commands list above is the client-builtin SSoT catalogue; server command set upgrades ship with shell binary per the decoupling rule)"))
	fmt.Println("Hint: type `commands <name>` for detailed help; type `example` to list paste-ready templates.")
}
