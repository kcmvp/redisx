package internal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	if !strings.Contains(stripANSIStrict(b), "Redisx (Compatible with Redis Shell)") {
		t.Fatalf("Banner should include Redisx (Compatible with Redis Shell) head: %q", b)
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
	if err := app2.Execute([]string{"-h", "0.0.0.0", "-p", "9999", "-a", "sekret", "!version"}); err != nil {
		t.Fatalf("execute with shorthands failed: %v", err)
	}
	if ud2.Opts.Host != "0.0.0.0" {
		t.Fatalf("-h shorthand write want 0.0.0.0 got %q", ud2.Opts.Host)
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

func TestBeforeRunEmptyNameDoesNotDial(t *testing.T) {
	dialed := false
	prev := session.SetNewForTest(func(_ session.Options) (*session.Session, error) {
		dialed = true
		return &session.Session{}, nil
	})
	defer func() { session.SetNewForTest(prev) }()

	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "10.0.0.1"
	ud.Opts.Port = 9999
	ud.Opts.Auth = "k"
	ud.Opts.TimeoutMs = 2000
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
	if *sessPtr != nil || dialed {
		t.Fatal("entering the REPL must NOT auto-dial; the user connects explicitly via con / !app / !ctrl")
	}
	// In-REPL commands before connecting return the not-connected error.
	ud.InREPL = true
	err := hooks.BeforeRun(ctx, "ping", nil)
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("in-REPL unconnected command should error 'not connected ...': %v", err)
	}
	if !strings.Contains(err.Error(), "con <host> <port> [auth]") {
		t.Fatalf("not-connected error should advertise the con command: %v", err)
	}
	ud.Endpoints = []LocalEndpoint{{Name: "app", Host: "127.0.0.1", Port: 7379}}
	err = hooks.BeforeRun(ctx, "ping", nil)
	if err == nil || !strings.Contains(err.Error(), "!app") {
		t.Fatalf("not-connected error should advertise local shortcuts: %v", err)
	}
	// One-shot mode still lazy-dials from flags.
	ud.InREPL = false
	if err := hooks.BeforeRun(ctx, "ping", nil); err != nil {
		t.Fatalf("one-shot mode should lazy-dial from flags: %v", err)
	}
	if *sessPtr == nil {
		t.Fatal("one-shot mode should have built the session")
	}
}

func TestConRedisCliStyleFlags(t *testing.T) {
	var got session.Options
	prev := session.SetNewForTest(func(o session.Options) (*session.Session, error) {
		got = o
		return &session.Session{}, nil
	})
	defer func() { session.SetNewForTest(prev) }()

	app, _ := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "127.0.0.1"
	ud.Opts.Port = 7381
	ud.Opts.Auth = ""
	ud.InREPL = true
	*ud.Session = nil

	// Short flags, dispatched like a REPL line (guards against the
	// "unexpected flag after subcommand" rejection).
	quit, err := app.DispatchREPLLine([]string{"con", "-h", "127.0.0.1", "-p", "7397", "-a", "123"})
	if quit {
		t.Fatal("con must not quit the REPL")
	}
	if err != nil {
		t.Fatalf("con with redis-cli style flags should connect: %v", err)
	}
	if got.Host != "127.0.0.1" || got.Port != 7397 || got.Auth != "123" {
		t.Fatalf("short flags not applied: %+v", got)
	}

	// Long forms.
	_, err = app.DispatchREPLLine([]string{"con", "--host", "10.1.2.3", "--port", "6380", "--auth", "s3cret"})
	if err != nil {
		t.Fatalf("con with long flags should connect: %v", err)
	}
	if got.Host != "10.1.2.3" || got.Port != 6380 || got.Auth != "s3cret" {
		t.Fatalf("long flags not applied: %+v", got)
	}

	// Positional form still works.
	_, err = app.DispatchREPLLine([]string{"con", "10.2.3.4", "6381", "pw"})
	if err != nil {
		t.Fatalf("con with positional args should connect: %v", err)
	}
	if got.Host != "10.2.3.4" || got.Port != 6381 || got.Auth != "pw" {
		t.Fatalf("positional args not applied: %+v", got)
	}

	// Unknown flags produce a usage error instead of a dial (the REPL
	// DispatchError hook consumes the error, so call Run directly).
	conCmd, _ := app.Resolve("con")
	err = conCmd.Run(tui.RunContext{UserData: app.UserData()}, []string{"-x"})
	if err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("unknown flag should return a usage error: %v", err)
	}
}

func TestBannerStaticContent(t *testing.T) {
	t.Run("AllHosts", func(t *testing.T) {
		out := BannerFor("1.2.3.4", "6379")
		// Banner must be single-mode per client zero-assumption (no role branching).
		// Static content is enforced by the positive assertion below.
		if !strings.Contains(stripANSIStrict(out), "Redisx (Compatible with Redis Shell)") {
			t.Fatalf("banner must say 'Redisx (Compatible with Redis Shell)': %q", out)
		}
		if !strings.Contains(out, "connected: 1.2.3.4:6379") {
			t.Fatalf("banner should include host:port connected line: %q", out)
		}
		stripped := stripANSIStrict(out)
		if !strings.Contains(stripped, "Extended: sk, si, upd, regsch, dropsch, regidx, dropidx") {
			t.Fatalf("Extended command list missing: %q", out)
		}
		if !strings.Contains(stripped, `Type "help <command>" for help`) {
			t.Fatalf("help hint line missing: %q", out)
		}
		if strings.Contains(out, "generic-redis") || strings.Contains(out, "not a redisx server") {
			t.Fatalf("banner should NOT mention generic-redis / not-redisx fallback (client zero assumptions): %q", out)
		}
		if strings.Contains(out, "!createsch") || strings.Contains(out, "!createidx") || strings.Contains(out, "!dropidx") {
			t.Fatalf("wizard shortcuts removed — must not remain in banner: %q", out)
		}
	})
}

func TestEnterREPLBannerDisconnectedAndConnected(t *testing.T) {
	var buf bytes.Buffer
	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "9.9.9.9"
	ud.Opts.Port = 7381
	ud.Opts.Auth = "X"
	ud.Opts.TimeoutMs = 100
	ud.Endpoints = []LocalEndpoint{
		{Name: "app", Host: "127.0.0.1", Port: 7379},
		{Name: "ctrl", Host: "127.0.0.1", Port: 7381},
	}
	ctx := tui.RunContext{UserData: app.UserData()}
	ctx.IO = tui.IO{In: os.Stdin, Out: &buf, Err: os.Stderr, FSOut: os.Stderr}
	h := app.Hooks()
	if h.BeforeRun == nil {
		t.Fatal("BeforeRun nil")
	}
	if err := h.BeforeRun(ctx, "", nil); err != nil {
		t.Fatalf("BeforeRun err: %v", err)
	}
	if *sessPtr != nil {
		t.Fatal("REPL entry must not build a session")
	}
	if h.Banner == nil {
		t.Fatal("Banner hook not installed")
	}
	// Disconnected startup banner: con hint + !app/!ctrl shortcuts from ./redisx.yaml.
	txt := h.Banner(ctx)
	if !strings.Contains(txt, "con <host> <port> [auth]") {
		t.Fatalf("disconnected banner should advertise the con command: %q", txt)
	}
	if !strings.Contains(txt, "!app") || !strings.Contains(txt, "!ctrl") {
		t.Fatalf("disconnected banner should list !app / !ctrl shortcuts from redisx.yaml: %q", txt)
	}
	if !strings.Contains(txt, "127.0.0.1:7379") {
		t.Fatalf("disconnected banner should show the !app endpoint addr: %q", txt)
	}
	if ok, diag := bannerIsPerfectRect(txt); !ok {
		t.Fatalf("disconnected banner box is NOT a perfect rect: %s\nraw=%q", diag, txt)
	}
	// Connected banner (after con): shows the dialled endpoint.
	*ud.Session = &session.Session{}
	ud.ConnHost, ud.ConnPort = "9.9.9.9", 7381
	txt = h.Banner(ctx)
	if !strings.Contains(txt, "connected: 9.9.9.9:7381") {
		t.Fatalf("Banner(ctx) should show the dialled host:port: %q", txt)
	}
	if !strings.Contains(stripANSIStrict(txt), "Redisx (Compatible with Redis Shell)") {
		t.Fatalf("Banner(ctx) should print 'Redisx (Compatible with Redis Shell)' head: %q", txt)
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
	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "127.0.0.1"
	ud.Opts.Port = 7381
	// commands is a local catalogue; it works without a connection, but the
	// header shows the flag host:port, so give it a stub session-free path.
	ctx := tui.RunContext{UserData: app.UserData()}
	if err := app.Hooks().BeforeRun(ctx, "", nil); err != nil {
		t.Fatalf("BeforeRun err: %v", err)
	}
	if *sessPtr != nil {
		t.Fatal("REPL entry must not dial")
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
		"Not connected:",
		"con [host] [port] [auth]",
	}
	for _, m := range mustContain {
		if !strings.Contains(outPlain, m) {
			t.Fatalf("commands output missing %q — plain output:\n%s", m, outPlain)
		}
	}
	if strings.Contains(outPlain, "generic-redis") || strings.Contains(outPlain, "not a redisx server") {
		t.Fatalf("commands header should never use generic-redis fallback / client zero assumptions:\n%s", outPlain)
	}
}

func TestBannerVsCommandsConsistentHeader(t *testing.T) {
	app, _ := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "127.0.0.1"
	ud.Opts.Port = 7381
	ud.Opts.Auth = "X"
	// Simulate the connected state (post-`con`) for banner/commands checks.
	*ud.Session = &session.Session{}
	ud.ConnHost, ud.ConnPort = "127.0.0.1", 7381
	ctx := tui.RunContext{UserData: app.UserData()}
	if err := app.Hooks().BeforeRun(ctx, "", nil); err != nil {
		t.Fatalf("BeforeRun err: %v", err)
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

func TestLoadLocalConfig(t *testing.T) {
	dir := t.TempDir()
	// Missing file → nil, nil.
	eps, err := LoadLocalConfig(dir)
	if err != nil || eps != nil {
		t.Fatalf("missing file: eps=%v err=%v", eps, err)
	}
	// Sample-shaped yaml → endpoints in file order, empty bind → localhost.
	content := `app:
  port: 7379
  bind: ""
  auth: ""
ctrl:
  port: 7381
  bind: "127.0.0.1"
  auth: sekret
  trust_proxy: false
notanendpoint:
  description: no port here
`
	if err := os.WriteFile(filepath.Join(dir, LocalConfigFile), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	eps, err = LoadLocalConfig(dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(eps) != 2 {
		t.Fatalf("want 2 endpoints, got %d: %+v", len(eps), eps)
	}
	if eps[0].Name != "app" || eps[0].Host != "127.0.0.1" || eps[0].Port != 7379 || eps[0].Auth != "" {
		t.Fatalf("app endpoint mismatch: %+v", eps[0])
	}
	if eps[1].Name != "ctrl" || eps[1].Host != "127.0.0.1" || eps[1].Port != 7381 || eps[1].Auth != "sekret" {
		t.Fatalf("ctrl endpoint mismatch: %+v", eps[1])
	}
	// FindEndpoint case-insensitive.
	if ep := FindEndpoint(eps, "APP"); ep == nil || ep.Name != "app" {
		t.Fatalf("FindEndpoint(APP) mismatch: %+v", ep)
	}
	if FindEndpoint(eps, "nope") != nil {
		t.Fatal("FindEndpoint(nope) should be nil")
	}
	if got := ShortcutNames(eps); got != "!app / !ctrl" {
		t.Fatalf("ShortcutNames=%q", got)
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
