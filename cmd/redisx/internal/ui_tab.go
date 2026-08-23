package internal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/cmd/redisx/internal/wizard"
)

func buildTabShell(app *tui.CLIApp, data *AppData, caps session.Capabilities) *tui.TabShell {
	isRedisx := caps.IsRedisx
	hasTypedDocs := isRedisx && caps.TypedDocs
	hasTypedIndexes := isRedisx && caps.TypedIndexes
	hasSearch := isRedisx && caps.SearchIndex
	_ = app
	basic := []tui.CmdEntry{
		makeTabCmd(app, "ping", data),
		makeTabCmd(app, "!version", data),
		makeTabCmd(app, "clear", data),
	}
	var extended []tui.CmdEntry
	if hasSearch {
		extended = []tui.CmdEntry{
			makeTabCmd(app, "searchkey", data),
			makeTabCmd(app, "searchindex", data),
			makeTabCmd(app, "update", data),
		}
	}
	var meta []tui.CmdEntry
	if isRedisx {
		if hasTypedDocs {
			meta = append(meta,
				makeTabCmd(app, "regdoc", data),
				makeTabCmd(app, "lsdoc", data),
				makeTabCmd(app, "desdoc", data),
				makeTabCmd(app, "!createdoc", data),
				makeTabCmd(app, "!listdocs", data),
				makeTabCmd(app, "!describedoc", data),
			)
		}
		if hasTypedIndexes {
			meta = append(meta,
				makeTabCmd(app, "regidx", data),
				makeTabCmd(app, "lsidx", data),
				makeTabCmd(app, "delidx", data),
				makeTabCmd(app, "!listindexes", data),
				makeTabCmd(app, "!createindex", data),
				makeTabCmd(app, "!dropindex", data),
			)
		}
	}
	tabs := []tui.TabDef{
		{Name: "Basic", Visible: func() bool { return true }, Entries: basic},
		{Name: "Extended", Visible: func() bool { return len(extended) > 0 }, Entries: extended},
		{Name: "Meta Management", Visible: func() bool { return len(meta) > 0 }, Entries: meta},
	}
	return tui.NewTabShell(tabs)
}

func makeTabCmd(app *tui.CLIApp, name string, data *AppData) tui.CmdEntry {
	canonical := name
	short := ""
	if c, ok := currentCmds[name]; ok {
		short = c.Short
	}
	return tui.CmdEntry{
		Use:   name,
		Short: short,
		Run: func(args []string) (string, error) {
			if name == "!version" || name == "version" {
				return fmt.Sprintf("redisx %s (go %s, %s/%s)", Version, runtime.Version(), runtime.GOOS, runtime.GOARCH), nil
			}
			if name == "clear" || name == "!clear" || name == "cls" {
				return "\x1b[H\x1b[2J", nil
			}
			if name == "ping" {
				sess := *data.Session
				if sess == nil {
					return "", fmt.Errorf("shell not connected")
				}
				v, err := sess.RawDo([]any{"PING"}).Result()
				if err != nil {
					return "", err
				}
				return sprintAny(v), nil
			}
			return dispatchAsTabCmd(name, canonical, args, data)
		},
	}
}

func dispatchAsTabCmd(name, canonical string, args []string, data *AppData) (string, error) {
	sess := *data.Session
	cache := data.Cache
	switch name {
	case "searchkey", "sk":
		if err := requireRedisx(sess); err != nil {
			return "", err
		}
		if len(args) < 1 {
			return "", fmt.Errorf("searchkey requires at least 1 arg")
		}
		call := make([]any, 0, 1+len(args))
		call = append(call, "SEARCHKEY")
		for _, a := range args {
			call = append(call, a)
		}
		v, err := sess.RawDo(call).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "searchindex", "si":
		if err := requireRedisx(sess); err != nil {
			return "", err
		}
		if len(args) < 2 {
			return "", fmt.Errorf("searchindex requires at least 2 args")
		}
		call := make([]any, 0, 1+len(args))
		call = append(call, "SEARCHINDEX")
		for _, a := range args {
			call = append(call, a)
		}
		v, err := sess.RawDo(call).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "update", "upd":
		if err := requireRedisx(sess); err != nil {
			return "", err
		}
		if len(args) != 3 && len(args) != 5 {
			return "", fmt.Errorf("update accepts 3 args or 5 args (<kr> <filter> <update> [LIMIT <count>]); got %d", len(args))
		}
		if len(args) == 5 {
			if strings.ToUpper(args[3]) != "LIMIT" {
				return "", fmt.Errorf("update 4th token must be 'LIMIT'; got %q", args[3])
			}
			n, err := strconv.Atoi(args[4])
			if err != nil || n <= 0 {
				return "", fmt.Errorf("update LIMIT count must be positive integer; got %q", args[4])
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
			return "", err
		}
		return sprintAny(v), nil
	case "regdoc":
		if err := requireRedisxDoc(sess); err != nil {
			return "", err
		}
		if len(args) == 0 {
			outBuf, errBuf := &bytes.Buffer{}, &bytes.Buffer{}
			saved := swapStdio(outBuf, errBuf)
			herr := wizard.RunCreateDocWizard(sess, cache)
			restoreStdio(saved)
			out := outBuf.String() + errBuf.String()
			if herr != nil {
				return strings.TrimRight(out, "\n"), herr
			}
			return strings.TrimRight(out, "\n"), nil
		}
		if len(args) != 1 {
			return "", fmt.Errorf("regdoc accepts 0 or 1 arg(s); got %d", len(args))
		}
		if !json.Valid([]byte(args[0])) {
			return "", errors.New("spec_json is not valid JSON")
		}
		v, err := sess.RawDo([]any{"regdoc", args[0]}).Result()
		if err != nil {
			return "", err
		}
		cache.Invalidate()
		return sprintAny(v), nil
	case "lsdoc":
		if err := requireRedisxDoc(sess); err != nil {
			return "", err
		}
		call := []any{"lsdoc"}
		for _, a := range args {
			call = append(call, a)
		}
		v, err := sess.RawDo(call).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "desdoc":
		if err := requireRedisxDoc(sess); err != nil {
			return "", err
		}
		if len(args) != 1 {
			return "", fmt.Errorf("desdoc requires 1 arg <namespace>")
		}
		v, err := sess.RawDo([]any{"desdoc", args[0]}).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "!createdoc":
		if err := requireRedisxDoc(sess); err != nil {
			return "", err
		}
		outBuf, errBuf := &bytes.Buffer{}, &bytes.Buffer{}
		saved := swapStdio(outBuf, errBuf)
		herr := wizard.RunCreateDocWizard(sess, cache)
		restoreStdio(saved)
		out := outBuf.String() + errBuf.String()
		if herr != nil {
			return strings.TrimRight(out, "\n"), herr
		}
		return strings.TrimRight(out, "\n"), nil
	case "!listdocs":
		if err := requireRedisxDoc(sess); err != nil {
			return "", err
		}
		call := []any{"lsdoc"}
		for _, a := range args {
			call = append(call, a)
		}
		v, err := sess.RawDo(call).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "!describedoc":
		if err := requireRedisxDoc(sess); err != nil {
			return "", err
		}
		if len(args) != 1 {
			return "", fmt.Errorf("!describedoc requires 1 arg <namespace>")
		}
		v, err := sess.RawDo([]any{"desdoc", args[0]}).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "regidx":
		if err := requireRedisxIndex(sess); err != nil {
			return "", err
		}
		if len(args) == 0 {
			outBuf, errBuf := &bytes.Buffer{}, &bytes.Buffer{}
			saved := swapStdio(outBuf, errBuf)
			herr := wizard.RunCreateIdxWizard(sess, cache)
			restoreStdio(saved)
			out := outBuf.String() + errBuf.String()
			if herr != nil {
				return strings.TrimRight(out, "\n"), herr
			}
			return strings.TrimRight(out, "\n"), nil
		}
		if len(args) < 3 || len(args) > 6 {
			return "", fmt.Errorf("regidx accepts 0 or 3..6 args; got %d", len(args))
		}
		call := make([]any, 0, 1+len(args))
		call = append(call, "regidx")
		for _, a := range args {
			call = append(call, a)
		}
		v, err := sess.RawDo(call).Result()
		if err != nil {
			return "", err
		}
		cache.Invalidate()
		return sprintAny(v), nil
	case "lsidx":
		if err := requireRedisxIndex(sess); err != nil {
			return "", err
		}
		call := []any{"lsidx"}
		for _, a := range args {
			call = append(call, a)
		}
		v, err := sess.RawDo(call).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "delidx":
		if err := requireRedisxIndex(sess); err != nil {
			return "", err
		}
		if len(args) != 2 {
			return "", fmt.Errorf("delidx requires 2 args <owner_ns> <logical_name>")
		}
		v, err := sess.RawDo([]any{"delidx", args[0], args[1]}).Result()
		if err != nil {
			return "", err
		}
		cache.Invalidate()
		return sprintAny(v), nil
	case "!listindexes":
		if err := requireRedisxIndex(sess); err != nil {
			return "", err
		}
		call := []any{"lsidx"}
		for _, a := range args {
			call = append(call, a)
		}
		v, err := sess.RawDo(call).Result()
		if err != nil {
			return "", err
		}
		return sprintAny(v), nil
	case "!createindex":
		if err := requireRedisxIndex(sess); err != nil {
			return "", err
		}
		outBuf, errBuf := &bytes.Buffer{}, &bytes.Buffer{}
		saved := swapStdio(outBuf, errBuf)
		herr := wizard.RunCreateIdxWizard(sess, cache)
		restoreStdio(saved)
		out := outBuf.String() + errBuf.String()
		if herr != nil {
			return strings.TrimRight(out, "\n"), herr
		}
		return strings.TrimRight(out, "\n"), nil
	case "!dropindex":
		if err := requireRedisxIndex(sess); err != nil {
			return "", err
		}
		outBuf, errBuf := &bytes.Buffer{}, &bytes.Buffer{}
		saved := swapStdio(outBuf, errBuf)
		herr := wizard.RunDropIdxWizard(sess, cache)
		restoreStdio(saved)
		out := outBuf.String() + errBuf.String()
		if herr != nil {
			return strings.TrimRight(out, "\n"), herr
		}
		return strings.TrimRight(out, "\n"), nil
	}
	return "", fmt.Errorf("unknown tab command %q", canonical)
}

func sprintAny(v any) string {
	buf := &bytes.Buffer{}
	switch t := v.(type) {
	case nil:
		fmt.Fprintln(buf, "(nil)")
	case string:
		fmt.Fprintln(buf, t)
	case []any:
		for i, e := range t {
			fmt.Fprintf(buf, "[%d] %v\n", i, e)
		}
	case []string:
		for i, e := range t {
			fmt.Fprintf(buf, "[%d] %s\n", i, e)
		}
	case []byte:
		fmt.Fprintln(buf, string(t))
	default:
		b, err := json.MarshalIndent(v, "", "  ")
		if err == nil {
			fmt.Fprintln(buf, string(b))
		} else {
			fmt.Fprintf(buf, "%v\n", v)
		}
	}
	return strings.TrimRight(buf.String(), "\n")
}

type savedIO struct {
	in, out, err any
}

func swapStdio(outBuf, errBuf *bytes.Buffer) savedIO {
	_ = outBuf
	_ = errBuf
	return savedIO{}
}
func restoreStdio(_ savedIO) {}

func runTabShellProgram(m *tui.TabShell) error {
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
