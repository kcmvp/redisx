package internal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/tui"
)

func RunREPL(app *tui.CLIApp, data *AppData) error {
	if app == nil {
		return fmt.Errorf("RunREPL: app is nil")
	}
	if data == nil {
		return fmt.Errorf("RunREPL: no app data")
	}
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
	host, port := data.HostPort()
	return cCyan("redisx") + " " + host + ":" + port + "> "
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
	case "commands", "help", "?":
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
		if c == nil {
			fmt.Fprintln(os.Stderr, cRed(fmt.Sprintf("unknown command %q — server-side |supported:| list will be returned for wire commands; for local commands try `help` / `commands`", want)))
			return
		}
		fmt.Println(app.HelpForCmd(c, canonical))
		return
	}
	if serverGroups, serverOrder, serverRole, ok := fetchServerCommandsGroups(data); ok {
		printServerCommandsList(data, serverGroups, serverOrder, serverRole)
		return
	}
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
		g := groupOfForCommands(name)
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
	h, p := data.HostPort()
	mode := "raw RESP passthrough — support & privilege enforcement handled server-side"
	fmt.Println(cBold("Connected: ") + cCyan(h+":"+p) + "  (" + mode + ")")
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
	fmt.Println("Hint: type `commands <name>` for detailed help; type `example` to list paste-ready templates.")
}

func fetchServerCommandsGroups(data *AppData) (groups map[string][][2]string, order []string, roleLine string, ok bool) {
	if data == nil || *data.Session == nil {
		return nil, nil, "", false
	}
	cmdGroups := defaultCommandGroups()
	groups = map[string][][2]string{}
	order = append(order, cmdGroups.GroupOrder...)
	h, p := data.HostPort()
	roleLine = cBold("Connected: ") + cCyan(h+":"+p) + "  (raw RESP passthrough — support & privilege enforcement handled server-side)"
	for g, entries := range cmdGroups.Groups {
		rows := make([][2]string, 0, len(entries))
		for _, e := range entries {
			if e.Name == "" {
				continue
			}
			rows = append(rows, [2]string{e.Name, e.Usage})
		}
		groups[g] = rows
	}
	return groups, order, roleLine, true
}

func printServerCommandsList(data *AppData, groups map[string][][2]string, order []string, roleLine string) {
	fmt.Println(roleLine)
	fmt.Println()
	for _, g := range order {
		rows, ok := groups[g]
		if !ok || len(rows) == 0 {
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

func groupOfForCommands(name string) string {
	switch name {
	case "!createsch", "!createidx", "!dropidx":
		return "Meta Management"
	}
	return "Basic"
}
