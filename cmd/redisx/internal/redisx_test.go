package internal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
)

func TestShlexSplitBasic(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		want   []string
		wantOK bool
	}{
		{"empty", "  \t ", nil, true},
		{"simple", "regsch foo", []string{"regsch", "foo"}, true},
		{"doubleQuotes", `regsch "hello world" foo`, []string{"regsch", "hello world", "foo"}, true},
		{"singleQuotes", `dropsch 'a:b:c'`, []string{"dropsch", "a:b:c"}, true},
		{"escapeInDouble", `"a\"b"`, []string{`a"b`}, true},
		{"escapeOutDouble", `a\ b c`, []string{"a b", "c"}, true},
		{"unterminatedDouble", `"abc`, nil, false},
		{"unterminatedSingle", `'abc`, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := shlexSplit(c.input)
			if ok != c.wantOK {
				t.Fatalf("ok=%v wantOK=%v got=%v", ok, c.wantOK, got)
			}
			if len(got) != len(c.want) {
				t.Fatalf("len=%d want=%d got=%v", len(got), len(c.want), got)
			}
			for i := range c.want {
				if got[i] != c.want[i] {
					t.Fatalf("[%d]=%q want %q got=%v", i, got[i], c.want[i], got)
				}
			}
		})
	}
}

func TestTruncateMiddle(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"abcdefghij", 10, "abcdefghij"},
		{"abcdefghijk", 6, "a...jk"},
		{"abc", 5, "abc"},
		{"longstring123", 3, "lon"},
		{"anything", 0, "anything"},
	}
	for _, c := range cases {
		got := TruncateMiddle(c.in, c.n)
		if got != c.want {
			t.Errorf("TruncateMiddle(%q,%d)=%q want %q", c.in, c.n, got, c.want)
		}
	}
}

func TestHistoryFileNotEmptyWhenHomePresent(t *testing.T) {
	home, herr := os.UserHomeDir()
	got := HistoryFile()
	if herr != nil || home == "" {
		if got != "" {
			t.Fatalf("expected empty when no home, got %q", got)
		}
		return
	}
	if !strings.HasSuffix(got, ".redisx_history") || !strings.HasPrefix(got, home) {
		t.Fatalf("HistoryFile=%q want prefix=%q suffix .redisx_history", got, home)
	}
}

func TestBannerStartsWithBox(t *testing.T) {
	b := BannerFor("127.0.0.1", "7381")
	if !strings.Contains(b, "Redisx  RESP Shell") {
		t.Fatalf("Banner should include Redisx RESP Shell head: %q", b)
	}
	if !strings.Contains(b, "connected: 127.0.0.1:7381") {
		t.Fatalf("Banner should include connected host:port line: %q", b)
	}
	lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("Banner too short lines=%d", len(lines))
	}
	stripped := make([]string, len(lines))
	for i, ln := range lines {
		stripped[i] = stripANSIStrict(ln)
	}
	borderW := len([]rune(stripped[0]))
	if borderW != len([]rune(stripped[len(stripped)-1])) {
		t.Fatalf("top/bottom border width mismatch top=%d bottom=%d\ntop=%q\nbot=%q",
			borderW, len([]rune(stripped[len(stripped)-1])), stripped[0], stripped[len(stripped)-1])
	}
	if runes := []rune(stripped[0]); len(runes) == 0 || runes[0] != ' ' || runes[len(runes)-1] != '─' {
		t.Fatalf("top border should start with ' ' and end with rune ─; got %q (first=%c last=%c)", stripped[0], runes[0], runes[len(runes)-1])
	}
	bodyExpected := borderW + 1
	for i := 1; i < len(stripped)-1; i++ {
		ln := stripped[i]
		got := len([]rune(ln))
		if got != bodyExpected {
			t.Fatalf("body line %d box width mismatch want=%d got=%d line=%q\nborder=%d runes %q",
				i, bodyExpected, got, ln, borderW, []rune(stripped[0]))
		}
		if !strings.HasPrefix(ln, "│") || !strings.HasSuffix(ln, "│") {
			t.Fatalf("body line %d should start and end with │: %q", i, ln)
		}
	}
}

func TestBannerBoxWidthConsistentAllStates(t *testing.T) {
	cases := []struct {
		name string
		host string
		port string
	}{
		{"PlainShort", "h", "1"},
		{"PlainLong", "very.long.host.name.with.many.subdomain.parts", "65535"},
		{"LocalDefault", "127.0.0.1", "7381"},
		{"AltPort", "127.0.0.1", "7379"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := BannerFor(c.host, c.port)
			lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
			if len(lines) < 3 {
				t.Fatalf("too few lines")
			}
			stripped := make([]string, len(lines))
			for i, ln := range lines {
				stripped[i] = stripANSIStrict(ln)
			}
			wBorder := len([]rune(stripped[0]))
			if len([]rune(stripped[len(stripped)-1])) != wBorder {
				t.Fatalf("top/bottom border width mismatch top=%d bottom=%d", wBorder, len([]rune(stripped[len(stripped)-1])))
			}
			bodyExpected := wBorder + 1
			for i := 1; i < len(stripped)-1; i++ {
				got := len([]rune(stripped[i]))
				if got != bodyExpected {
					t.Fatalf("body line %d width got=%d want=%d (border=%d) line=%q", i, got, bodyExpected, wBorder, stripped[i])
				}
				if !strings.HasPrefix(stripped[i], "│") || !strings.HasSuffix(stripped[i], "│") {
					t.Fatalf("body line %d should be │…│ got %q", i, stripped[i])
				}
			}
		})
	}
}

func TestPrintTabWriterNoHeadersWritesAllRows(t *testing.T) {
	var buf bytes.Buffer
	PrintTabWriter(&buf, nil, [][]string{{"a", "b"}, {"c", "d"}})
	out := buf.String()
	if !strings.Contains(out, "a") || !strings.Contains(out, "b") {
		t.Fatalf("row content missing in %q", out)
	}
	if !strings.Contains(out, "c") || !strings.Contains(out, "d") {
		t.Fatalf("row2 content missing in %q", out)
	}
}

func TestPrintTabWriterWithHeaders(t *testing.T) {
	var buf bytes.Buffer
	PrintTabWriter(&buf, []string{"A", "B"}, [][]string{{"x", "y"}})
	out := buf.String()
	if !strings.Contains(out, "A") || !strings.Contains(out, "B") {
		t.Fatalf("header missing: %q", out)
	}
	if !strings.Contains(out, "─") {
		t.Fatalf("separator line missing: %q", out)
	}
}

func TestNewShellFailsOnBadPort(t *testing.T) {
	_, err := session.New(session.Options{Host: "127.0.0.1", Port: 9, Auth: "x", TimeoutMs: 1})
	if err == nil {
		t.Fatalf("expected error connecting to invalid port")
	}
	if err.Error() == "" {
		t.Fatalf("error should not be empty string on dial failure")
	}
}

func buildStubRoot(t *testing.T) (*tui.CLIApp, **session.Session) {
	t.Helper()
	var sess *session.Session
	return BuildApp(&sess), &sess
}

func TestIsKnownCommandMatchesNameAndAlias(t *testing.T) {
	app, _ := buildStubRoot(t)
	if !app.IsKnownCommand("ping") {
		t.Fatalf("ping should be known")
	}
	if !app.IsKnownCommand("PING") {
		t.Fatalf("PING case insensitive")
	}
	if app.IsKnownCommand("banner") {
		t.Fatalf("banner alias must NOT be known after removal; found known")
	}
	if app.IsKnownCommand("!banner") {
		t.Fatalf("!banner canonical must NOT be known after removal; found known")
	}
	if app.IsKnownCommand("") {
		t.Fatalf("empty should not be known")
	}
	if app.IsKnownCommand("nosuchcmd_xyz") {
		t.Fatalf("unknown cmd should not be known")
	}
}

func TestForwardUnknownAsRawBangErrorsBang(t *testing.T) {
	app, sessPtr := buildStubRoot(t)
	hooks := app.Hooks()
	if hooks.OnUnknown == nil {
		t.Fatalf("app should have OnUnknown hook installed")
	}
	*sessPtr = &session.Session{}
	ud, _ := app.UserData().(*AppData)
	*ud.Session = *sessPtr
	ctx := tui.RunContext{UserData: app.UserData()}
	handled, err := hooks.OnUnknown(ctx, []string{"!nope"})
	if handled {
		t.Fatalf("should not handle !unknown")
	}
	if err == nil || !strings.Contains(err.Error(), "!help") {
		t.Fatalf("error should hint !help: %v", err)
	}
	handled, err = hooks.OnUnknown(ctx, nil)
	if handled || err != nil {
		t.Fatalf("empty args noop")
	}
}

func TestOptionsDefaultsAppliedInNewShellBadPort(t *testing.T) {
	sh, err := session.New(session.Options{Port: 9, TimeoutMs: 1})
	if err == nil {
		t.Fatalf("expected dial err")
	}
	if sh != nil {
		_ = sh.Close()
		t.Fatalf("sh should be nil on err")
	}
	if err.Error() == "" {
		t.Fatalf("error should not be empty string on dial failure")
	}
}

func TestRootCmdHasExpectedFlags(t *testing.T) {
	app, _ := buildStubRoot(t)
	ud, ok := app.UserData().(*AppData)
	if !ok {
		t.Fatalf("UserData not *AppData, got %T", app.UserData())
	}
	if ud.Opts.Host != "127.0.0.1" {
		t.Fatalf("default host want 127.0.0.1 got %q", ud.Opts.Host)
	}
	if ud.Opts.Port != 7381 {
		t.Fatalf("default port want 7381 got %d", ud.Opts.Port)
	}
	if err := app.Execute([]string{"--host", "1.2.3.4", "!version"}); err != nil {
		t.Fatalf("execute with --host failed: %v", err)
	}
	if ud.Opts.Host != "1.2.3.4" {
		t.Fatalf("--host write want 1.2.3.4 got %q", ud.Opts.Host)
	}
	app2, _ := buildStubRoot(t)
	ud2, _ := app2.UserData().(*AppData)
	if err := app2.Execute([]string{"-H", "0.0.0.0", "-p", "9999", "-a", "sekret", "!version"}); err != nil {
		t.Fatalf("execute with shorthands failed: %v", err)
	}
	if ud2.Opts.Host != "0.0.0.0" {
		t.Fatalf("-H shorthand write want 0.0.0.0 got %q", ud2.Opts.Host)
	}
	if ud2.Opts.Port != 9999 {
		t.Fatalf("-p shorthand write want 9999 got %d", ud2.Opts.Port)
	}
	if ud2.Opts.Auth != "sekret" {
		t.Fatalf("-a shorthand write want sekret got %q", ud2.Opts.Auth)
	}
	if ud.Help != false {
		t.Fatalf("default help want false got %v", ud.Help)
	}
}

func runGlobalHelpFor(t *testing.T) string {
	t.Helper()
	app, sessPtr := buildStubRoot(t)
	*sessPtr = &session.Session{}
	ud, _ := app.UserData().(*AppData)
	*ud.Session = *sessPtr
	hooks := app.Hooks()
	if hooks.GlobalHelp == nil {
		t.Fatalf("no GlobalHelp hook")
	}
	ctx := tui.RunContext{UserData: app.UserData()}
	return hooks.GlobalHelp(ctx, map[string][]tui.GroupEntry{}, app.FlagHelpText())
}

func TestGlobalHelpShowsAllGroupsStatic(t *testing.T) {
	out := runGlobalHelpFor(t)
	if !strings.Contains(out, cBold("Extended Commands")+":") {
		t.Fatalf("global help should always advertise Extended Commands group (SSoT passthrough, server-side privilege gating): %q", out)
	}
	if !strings.Contains(out, cBold("Meta Management")+":") {
		t.Fatalf("global help should always advertise Meta Management group (SSoT passthrough, server-side privilege gating): %q", out)
	}
	if !strings.Contains(out, cBold("Basic Commands")+":") {
		t.Fatalf("global help must still show Basic Commands group: %q", out)
	}
	if !strings.Contains(out, "example regsch") || !strings.Contains(out, "example regidx") || !strings.Contains(out, "example dropsch") || !strings.Contains(out, "example dropidx") {
		t.Fatalf("Meta Management zero-length list should hint ALL 4 example templates (docs+indexes; server enforces privilege): %q", out)
	}
	if !strings.Contains(out, "example searchkey") || !strings.Contains(out, "example searchindex") || !strings.Contains(out, "example update") {
		t.Fatalf("Extended Commands zero-length list should hint ALL 3 search/update example templates: %q", out)
	}
	if strings.Contains(out, "!createsch") || strings.Contains(out, "!createidx") || strings.Contains(out, "!dropidx") {
		t.Fatalf("wizard shortcuts (!createsch/!createidx/!dropidx) were removed — must NOT appear in help any more: %q", out)
	}
	if !strings.Contains(out, "raw RESP passthrough") {
		t.Fatalf("help should explain command entries are raw RESP passthrough, not locally-wizard routes: %q", out)
	}
	if strings.Contains(out, "connected peer is not a redisx server") {
		t.Fatalf("global help should NOT gate Extended/Meta on IsRedisx — they are always shown, server decides: %q", out)
	}
	if !strings.Contains(out, "Support, availability and privilege enforcement are entirely server-side") {
		t.Fatalf("global help should mention zero-client-assumption / all decisions server-side: %q", out)
	}
}

func TestBeforeRunEmptyNameTriggersBuildSession(t *testing.T) {
	wantHost, wantPort, wantAuth, wantTimeoutMs := "10.0.0.1", 9999, "k", 2000
	prev := session.SetNewForTest(func(opts session.Options) (*session.Session, error) {
		if opts.Host != wantHost || opts.Port != wantPort || opts.Auth != wantAuth || opts.TimeoutMs != wantTimeoutMs {
			t.Fatalf("session.New opts mismatch got=%+v want host=%s port=%d auth=%s ms=%d", opts, wantHost, wantPort, wantAuth, wantTimeoutMs)
		}
		return &session.Session{}, nil
	})
	defer func() { session.SetNewForTest(prev) }()

	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = wantHost
	ud.Opts.Port = wantPort
	ud.Opts.Auth = wantAuth
	ud.Opts.TimeoutMs = wantTimeoutMs
	ctx := tui.RunContext{UserData: app.UserData()}
	hooks := app.Hooks()
	if hooks.BeforeRun == nil {
		t.Fatal("BeforeRun hook not set")
	}
	if *sessPtr != nil {
		t.Fatal("before BeforeRun, *Session should be nil")
	}
	if err := hooks.BeforeRun(ctx, "", nil); err != nil {
		t.Fatalf("BeforeRun(empty REPL name) should not error: %v", err)
	}
	if *sessPtr == nil {
		t.Fatal("BeforeRun(empty REPL name) must build session; it's nil — the 'noShellCmds(\"\")==true' regression is back")
	}
}

func TestBannerStaticContent(t *testing.T) {
	t.Run("AllHosts", func(t *testing.T) {
		out := BannerFor("1.2.3.4", "6379")
		// Banner must be single-mode per client zero-assumption (no role branching).
		// Static content is enforced by the positive assertion below.
		if !strings.Contains(out, "Redisx  RESP Shell") {
			t.Fatalf("banner must say 'Redisx  RESP Shell': %q", out)
		}
		if !strings.Contains(out, "connected: 1.2.3.4:6379") {
			t.Fatalf("banner should include host:port connected line: %q", out)
		}
		if !strings.Contains(out, "docs:  regsch / dropsch") {
			t.Fatalf("Meta docs list missing: %q", out)
		}
		if !strings.Contains(out, "idx:   regidx / dropidx") {
			t.Fatalf("Meta idx list missing: %q", out)
		}
		if !strings.Contains(out, "searchkey(sk)  /  searchindex(si)  /  update(upd)") {
			t.Fatalf("Extended search/update list missing: %q", out)
		}
		if strings.Contains(out, "generic-redis") || strings.Contains(out, "not a redisx server") {
			t.Fatalf("banner should NOT mention generic-redis / not-redisx fallback (client zero assumptions): %q", out)
		}
		if strings.Contains(out, "!createsch") || strings.Contains(out, "!createidx") || strings.Contains(out, "!dropidx") {
			t.Fatalf("wizard shortcuts removed — must not remain in banner: %q", out)
		}
	})
}

func TestEnterREPLBannerUsesHostPortFromAppData(t *testing.T) {
	prev := session.SetNewForTest(func(_ session.Options) (*session.Session, error) {
		return &session.Session{}, nil
	})
	defer func() { session.SetNewForTest(prev) }()

	var buf bytes.Buffer
	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "9.9.9.9"
	ud.Opts.Port = 7381
	ud.Opts.Auth = "X"
	ud.Opts.TimeoutMs = 100
	ctx := tui.RunContext{UserData: app.UserData()}
	ctx.IO = tui.IO{In: os.Stdin, Out: &buf, Err: os.Stderr, FSOut: os.Stderr}
	h := app.Hooks()
	if h.BeforeRun == nil {
		t.Fatal("BeforeRun nil")
	}
	if err := h.BeforeRun(ctx, "", nil); err != nil {
		t.Fatalf("BeforeRun err: %v", err)
	}
	if *sessPtr == nil {
		t.Fatal("session not built")
	}
	if h.Banner == nil {
		t.Fatal("Banner hook not installed")
	}
	txt := h.Banner(ctx)
	if !strings.Contains(txt, "connected: 9.9.9.9:7381") {
		t.Fatalf("Banner(ctx) should pick host/port from AppData after BuildApp; got: %q", txt)
	}
	if strings.Contains(txt, "generic-redis") || strings.Contains(txt, "not a redisx server") {
		t.Fatalf("Banner(ctx) should NOT fall back to generic-redis — client zero assumptions: %q", txt)
	}
	if !strings.Contains(txt, "Redisx  RESP Shell") {
		t.Fatalf("Banner(ctx) should print 'Redisx  RESP Shell' head: %q", txt)
	}
	if ok, diag := bannerIsPerfectRect(txt); !ok {
		t.Fatalf("Banner(ctx) box is NOT a perfect rect (user-visible right-side zigzag): %s\nraw=%q", diag, txt)
	}
}

func TestBannerForBoxPerfectRect(t *testing.T) {
	cases := []struct {
		name string
		host string
		port string
	}{
		{"Min", "h", "1"},
		{"Long", "very-long.hostname.with-many.subdomains.example.internal", "65535"},
		{"LocalShort", "127.0.0.1", "7381"},
		{"LongSubdomain", "a.very.long.subdomain.chain.for.redisx.port.internal", "7381"},
		{"AltPort", "10.0.0.22", "7379"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			txt := BannerFor(c.host, c.port)
			ok, diag := bannerIsPerfectRect(txt)
			if !ok {
				t.Fatalf("Banner box NOT perfect rect: %s\nraw=%q", diag, txt)
			}
			lines := strings.Split(strings.TrimRight(txt, "\n"), "\n")
			if len(lines) < 3 {
				t.Fatalf("too few lines %d", len(lines))
			}
			first := stripANSIStrict(lines[0])
			last := stripANSIStrict(lines[len(lines)-1])
			if first != last {
				t.Fatalf("top/bottom borders differ:\ntop=%q\nbot=%q", first, last)
			}
		})
	}
}

func TestCommandsListStaticAllGroups(t *testing.T) {
	prev := session.SetNewForTest(func(_ session.Options) (*session.Session, error) {
		return &session.Session{}, nil
	})
	defer func() { session.SetNewForTest(prev) }()
	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "127.0.0.1"
	ud.Opts.Port = 7381
	ctx := tui.RunContext{UserData: app.UserData()}
	if err := app.Hooks().BeforeRun(ctx, "", nil); err != nil {
		t.Fatalf("BeforeRun err: %v", err)
	}
	if *sessPtr == nil {
		t.Fatal("session nil after BeforeRun")
	}
	var buf bytes.Buffer
	oldOut := os.Stdout
	rp, wp, _ := os.Pipe()
	os.Stdout = wp
	quit, herr := handleREPLLine(app, ud, "commands")
	_ = wp.Close()
	_, _ = io.Copy(&buf, rp)
	os.Stdout = oldOut
	if quit {
		t.Fatalf("commands should not quit")
	}
	if herr != nil {
		t.Fatalf("commands err: %v", herr)
	}
	out := buf.String()
	outPlain := stripANSIStrict(out)
	mustContain := []string{
		"Extended:", "Meta Management:",
		"regsch", "regidx", "searchkey",
		"127.0.0.1:7381",
	}
	for _, m := range mustContain {
		if !strings.Contains(outPlain, m) {
			t.Fatalf("commands output missing %q — plain output:\n%s", m, outPlain)
		}
	}
	if strings.Contains(outPlain, "Basic:") {
		t.Fatalf("commands output should NO LONGER include 'Basic:' group (generic Redis commands are universally known) — plain output:\n%s", outPlain)
	}
	if strings.Contains(outPlain, "generic-redis") || strings.Contains(outPlain, "not a redisx server") {
		t.Fatalf("commands header should never use generic-redis fallback / client zero assumptions:\n%s", outPlain)
	}
}

func TestBannerVsCommandsConsistentHeader(t *testing.T) {
	var realSess *session.Session
	prev := session.SetNewForTest(func(_ session.Options) (*session.Session, error) {
		realSess = &session.Session{}
		return realSess, nil
	})
	defer func() { session.SetNewForTest(prev) }()
	app, _ := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "127.0.0.1"
	ud.Opts.Port = 7381
	ud.Opts.Auth = "X"
	ctx := tui.RunContext{UserData: app.UserData()}
	if err := app.Hooks().BeforeRun(ctx, "", nil); err != nil {
		t.Fatalf("BeforeRun err: %v", err)
	}
	if realSess == nil {
		t.Fatal("realSess nil; seam not triggered")
	}
	var combo bytes.Buffer
	if app.Hooks().Banner != nil {
		combo.WriteString(app.Hooks().Banner(ctx))
	}
	var cmdBuf bytes.Buffer
	oldOut := os.Stdout
	rp, wp, _ := os.Pipe()
	os.Stdout = wp
	_, _ = handleREPLLine(app, ud, "commands")
	_ = wp.Close()
	_, _ = io.Copy(&cmdBuf, rp)
	os.Stdout = oldOut
	combo.WriteString(cmdBuf.String())
	plain := stripANSIStrict(combo.String())
	headGeneric := strings.Contains(plain, "Generic-redis mode") || strings.Contains(plain, "connected: generic-redis ")
	bodyConnLine := ""
	for _, ln := range strings.Split(plain, "\n") {
		if strings.HasPrefix(strings.TrimLeft(ln, " \t"), "Connected:") {
			bodyConnLine = ln
			break
		}
	}
	bodyRedisx := strings.Contains(bodyConnLine, "127.0.0.1:7381")
	if headGeneric && bodyRedisx {
		t.Fatalf(
			"BUG — Banner head said Generic-redis mode fallback, commands Connected header includes expected host:port\n\n%s",
			plain,
		)
	}
	if !bodyRedisx {
		t.Fatalf("Expected body Connected line to contain '127.0.0.1:7381', got %q", bodyConnLine)
	}
}

func bannerIsPerfectRect(txt string) (bool, string) {
	lines := strings.Split(strings.TrimRight(txt, "\n"), "\n")
	if len(lines) < 3 {
		return false, fmt.Sprintf("less than 3 lines (%d)", len(lines))
	}
	stripped := make([][]rune, len(lines))
	for i, ln := range lines {
		stripped[i] = []rune(stripANSIStrict(ln))
	}
	borderRunes := len(stripped[0])
	if len(stripped[len(stripped)-1]) != borderRunes {
		return false, fmt.Sprintf("top/bottom border widths differ top=%d bottom=%d",
			borderRunes, len(stripped[len(stripped)-1]))
	}
	if borderRunes == 0 || stripped[0][0] != ' ' {
		return false, "top border does not start with ' '"
	}
	for _, r := range stripped[0] {
		if r != '─' && r != ' ' {
			return false, fmt.Sprintf("top border contains non-border rune %q (%v)", string(r), r)
		}
	}
	if stripped[0][borderRunes-1] != '─' {
		return false, fmt.Sprintf("top border last rune should be ─ got %q", string(stripped[0][borderRunes-1]))
	}
	bodyExpected := borderRunes + 1
	for i := 1; i < len(stripped)-1; i++ {
		got := len(stripped[i])
		if got != bodyExpected {
			return false, fmt.Sprintf("body line %d width=%d expected=%d line=%q",
				i, got, bodyExpected, string(stripped[i]))
		}
		if stripped[i][0] != '│' {
			return false, fmt.Sprintf("body line %d first rune should be │ got %q", i, string(stripped[i][0]))
		}
		if stripped[i][got-1] != '│' {
			return false, fmt.Sprintf("body line %d last rune should be │ got %q", i, string(stripped[i][got-1]))
		}
	}
	return true, ""
}
