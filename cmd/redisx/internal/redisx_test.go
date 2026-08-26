package internal

import (
	"bytes"
	"errors"
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
	adminCaps := session.Capabilities{IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true}
	b := BannerFor(adminCaps, "127.0.0.1", "7381")
	if !strings.Contains(b, "Admin Shell") {
		t.Fatalf("Banner should include admin-shell head when admin role: %q", b)
	}
	if !strings.Contains(b, "connected: admin 127.0.0.1:7381") {
		t.Fatalf("Banner should include connected admin host:port line: %q", b)
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
		caps session.Capabilities
		host string
		port string
	}{
		{"PlainShort", session.Capabilities{}, "h", "1"},
		{"PlainLong", session.Capabilities{}, "very.long.host.name.with.many.subdomain.parts", "65535"},
		{"AdminFull", session.Capabilities{IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true}, "127.0.0.1", "7381"},
		{"AppMode", session.Capabilities{IsRedisx: true, AdminRole: false, TypedDocs: true, TypedIndexes: true, SearchIndex: true}, "127.0.0.1", "7379"},
		{"PartialDocs", session.Capabilities{IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: false, SearchIndex: false}, "127.0.0.1", "7381"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := BannerFor(c.caps, c.host, c.port)
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
	_, err := session.New(session.Options{Host: "127.0.0.1", Port: 9, AdminAuth: "x", TimeoutMs: 1})
	if err == nil {
		t.Fatalf("expected error connecting to invalid port")
	}
	if !strings.Contains(err.Error(), "admin-port") {
		t.Fatalf("error should mention admin-port: %v", err)
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
		t.Fatalf("banner alias must NOT be known after removal in Task3; found known")
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

func TestWrapAdminErrBranches(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		authProvided bool
		wantSub      string
		wantNil      bool
	}{
		{"nil", nil, false, "", true},
		{"noauth_no_a", errors.New("NOAUTH authentication required"), false, "Pass the admin-auth key via", false},
		{"noauth_gave_a_stale_server", errors.New("NOAUTH authentication required"), true, "admin-port still returned NOAUTH after AUTH attempt", false},
		{"wrongpass", errors.New("WRONGPASS invalid username-password pair"), true, "AUTH key rejected (WRONGPASS)", false},
		{"err_auth_failed", errors.New("ERR authentication failed"), true, "ERR authentication failed", false},
		{"random_passthrough", errors.New("i/o timeout"), false, "connect redisx admin-port failed: i/o timeout", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := session.WrapAdminErr(c.err, c.authProvided)
			if c.wantNil {
				if got != nil {
					t.Fatalf("want nil got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("want error, got nil")
			}
			if !strings.Contains(got.Error(), c.wantSub) {
				t.Fatalf("substring %q not in %q", c.wantSub, got.Error())
			}
		})
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
	emsg := err.Error()
	if !strings.Contains(emsg, "admin-port") {
		t.Fatalf("error should mention admin-port: %v", emsg)
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
	if ud2.Opts.AdminAuth != "sekret" {
		t.Fatalf("-a shorthand write want sekret got %q", ud2.Opts.AdminAuth)
	}
	if ud.Help != false {
		t.Fatalf("default help want false got %v", ud.Help)
	}
}

func stubSessionWithCaps(t *testing.T, caps session.Capabilities) *session.Session {
	t.Helper()
	s := &session.Session{}
	s.SetCapabilitiesForTest(caps)
	return s
}

func runGlobalHelpFor(t *testing.T, caps session.Capabilities) string {
	t.Helper()
	app, sessPtr := buildStubRoot(t)
	*sessPtr = stubSessionWithCaps(t, caps)
	ud, _ := app.UserData().(*AppData)
	*ud.Session = *sessPtr
	ud.FrozenCaps = caps
	hooks := app.Hooks()
	if hooks.GlobalHelp == nil {
		t.Fatalf("no GlobalHelp hook")
	}
	ctx := tui.RunContext{UserData: app.UserData()}
	return hooks.GlobalHelp(ctx, map[string][]tui.GroupEntry{}, app.FlagHelpText())
}

func TestGlobalHelpPlainRedisShowsOnlyBasic(t *testing.T) {
	out := runGlobalHelpFor(t, session.Capabilities{})
	if strings.Contains(out, cBold("Extended Commands")+":") {
		t.Fatalf("plain-redis should not show Extended Commands group: %q", out)
	}
	if strings.Contains(out, cBold("Meta Management")+":") {
		t.Fatalf("plain-redis should not show Meta Management group: %q", out)
	}
	if !strings.Contains(out, cBold("Basic Commands")+":") {
		t.Fatalf("plain-redis must still show Basic Commands group: %q", out)
	}
	if !strings.Contains(out, "connected peer is not a redisx server") {
		t.Fatalf("plain-redis should show peer-not-redisx notice: %q", out)
	}
	if strings.Contains(out, "  regsch ") || strings.Contains(out, "  searchkey ") || strings.Contains(out, "  update ") || strings.Contains(out, "  regidx ") {
		t.Fatalf("plain-redis help must not list redisx-only command rows (regsch/searchkey/update/regidx): %q", out)
	}
}

func TestGlobalHelpSearchIndexOffHidesExtended(t *testing.T) {
	caps := session.Capabilities{
		IsRedisx:     true,
		AdminRole:    true,
		TypedDocs:    true,
		TypedIndexes: true,
		SearchIndex:  false,
	}
	out := runGlobalHelpFor(t, caps)
	if strings.Contains(out, cBold("Extended Commands")+":") {
		t.Fatalf("SearchIndex=false should hide Extended Commands group: %q", out)
	}
	if strings.Contains(out, "  searchkey ") || strings.Contains(out, "  searchindex ") || strings.Contains(out, "  update ") {
		t.Fatalf("SearchIndex=false must not list search/update entry rows: %q", out)
	}
	if !strings.Contains(out, cBold("Meta Management")+":") {
		t.Fatalf("with TypedDocs+TypedIndexes true, Meta Management must still appear: %q", out)
	}
}

func TestGlobalHelpTypedDocsAndIndexesDrilldown(t *testing.T) {
	docsOnly := session.Capabilities{
		IsRedisx:     true,
		AdminRole:    true,
		TypedDocs:    true,
		TypedIndexes: false,
		SearchIndex:  true,
	}
	out := runGlobalHelpFor(t, docsOnly)
	if !strings.Contains(out, "  regsch ") || !strings.Contains(out, "  dropsch ") {
		t.Fatalf("docs=true must list Doc management entry rows (regsch/dropsch): %q", out)
	}
	if strings.Contains(out, "  regidx ") || strings.Contains(out, "  dropidx ") || strings.Contains(out, "  !createidx ") {
		t.Fatalf("indexes=false must NOT list Index management entry rows: %q", out)
	}
	if !strings.Contains(out, cBold("Extended Commands")+":") {
		t.Fatalf("SearchIndex=true must show Extended Commands group: %q", out)
	}
	if !strings.Contains(out, cBold("Meta Management")+":") {
		t.Fatalf("Docs alone should still show Meta Management group: %q", out)
	}

	idxOnly := session.Capabilities{
		IsRedisx:     true,
		AdminRole:    true,
		TypedDocs:    false,
		TypedIndexes: true,
		SearchIndex:  true,
	}
	out = runGlobalHelpFor(t, idxOnly)
	if strings.Contains(out, "  regsch ") || strings.Contains(out, "  dropsch ") || strings.Contains(out, "  !createsch ") {
		t.Fatalf("docs=false must NOT list Doc management entry rows: %q", out)
	}
	if !strings.Contains(out, "  regidx ") || !strings.Contains(out, "  dropidx ") {
		t.Fatalf("indexes=true must list Index management entry rows (regidx/dropidx): %q", out)
	}
}

func TestGlobalHelpAppRoleShowsMetaCommands(t *testing.T) {
	caps := session.Capabilities{
		IsRedisx:     true,
		AdminRole:    false,
		TypedDocs:    true,
		TypedIndexes: true,
		SearchIndex:  true,
	}
	out := runGlobalHelpFor(t, caps)
	if !strings.Contains(out, cBold("Meta Management")+":") {
		t.Fatalf("AdminRole=false should still show Meta Management in help (No Privilege handled server-side): %q", out)
	}
	if !strings.Contains(out, "  regsch ") || !strings.Contains(out, "  regidx ") {
		t.Fatalf("AdminRole=false help entries for Meta should still list command rows: %q", out)
	}
	if !strings.Contains(out, cBold("Extended Commands")+":") {
		t.Fatalf("AdminRole=false should still show Extended Commands: %q", out)
	}
}

func TestBeforeRunEmptyNameTriggersBuildSession(t *testing.T) {
	wantHost, wantPort, wantAuth, wantTimeoutMs := "10.0.0.1", 9999, "k", 2000
	prev := session.SetNewForTest(func(opts session.Options) (*session.Session, error) {
		if opts.Host != wantHost || opts.Port != wantPort || opts.AdminAuth != wantAuth || opts.TimeoutMs != wantTimeoutMs {
			t.Fatalf("session.New opts mismatch got=%+v want host=%s port=%d auth=%s ms=%d", opts, wantHost, wantPort, wantAuth, wantTimeoutMs)
		}
		s := &session.Session{}
		s.SetCapabilitiesForTest(session.Capabilities{
			IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true,
		})
		return s, nil
	})
	defer func() { session.SetNewForTest(prev) }()

	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = wantHost
	ud.Opts.Port = wantPort
	ud.Opts.AdminAuth = wantAuth
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
	caps := (*sessPtr).Capabilities()
	if !caps.IsRedisx || !caps.AdminRole {
		t.Fatalf("mock caps should flow through: %+v", caps)
	}
}

func TestBannerForThreeStates(t *testing.T) {
	t.Run("PlainRedis", func(t *testing.T) {
		out := BannerFor(session.Capabilities{}, "1.2.3.4", "6379")
		if strings.Contains(out, "Admin Shell") || strings.Contains(out, "App Mode") {
			t.Fatalf("plain-redis banner should not mention admin/app mode: %q", out)
		}
		if !strings.Contains(out, "Generic-redis mode") {
			t.Fatalf("plain-redis banner must mark Generic-redis mode: %q", out)
		}
		if !strings.Contains(out, "connected: generic-redis 1.2.3.4:6379") {
			t.Fatalf("plain-redis banner should include host:port / generic-redis role label: %q", out)
		}
		if !strings.Contains(out, "not a redisx server — only raw RESP forwarding available") {
			t.Fatalf("plain-redis banner should show yellow 'not a redisx server' notice: %q", out)
		}
		if strings.Contains(out, "regsch") || strings.Contains(out, "searchkey") {
			t.Fatalf("plain-redis banner should NOT list any redisx-only command names (regsch/searchkey/...): %q", out)
		}
	})
	t.Run("RedisxAdminFullFeatures", func(t *testing.T) {
		caps := session.Capabilities{
			IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true,
		}
		out := BannerFor(caps, "127.0.0.1", "7381")
		if !strings.Contains(out, "Admin Shell") {
			t.Fatalf("admin role banner must say 'Admin Shell': %q", out)
		}
		if !strings.Contains(out, "connected: admin 127.0.0.1:7381") {
			t.Fatalf("admin banner role label + host:port missing: %q", out)
		}
		if !strings.Contains(out, "docs:  regsch / dropsch / !createsch") {
			t.Fatalf("with TypedDocs=true docs list missing: %q", out)
		}
		if !strings.Contains(out, "idx:   regidx / dropidx / !createidx / !dropidx") {
			t.Fatalf("with TypedIndexes=true idx list missing: %q", out)
		}
		if !strings.Contains(out, "searchkey(sk)  /  searchindex(si)  /  update(upd)") {
			t.Fatalf("with SearchIndex=true extended list missing: %q", out)
		}
		if strings.Contains(out, "generic-redis") || strings.Contains(out, "not a redisx server") {
			t.Fatalf("admin-role banner should NOT mention generic-redis / not-redisx: %q", out)
		}
	})
	t.Run("RedisxAppRole", func(t *testing.T) {
		caps := session.Capabilities{
			IsRedisx: true, AdminRole: false, TypedDocs: true, TypedIndexes: true, SearchIndex: true,
		}
		out := BannerFor(caps, "127.0.0.1", "7379")
		if !strings.Contains(out, "App Mode") {
			t.Fatalf("app-role banner must say 'App Mode': %q", out)
		}
		if !strings.Contains(out, "No Privilege") {
			t.Fatalf("app-role banner should warn about No Privilege: %q", out)
		}
		if !strings.Contains(out, "connected: app 127.0.0.1:7379") {
			t.Fatalf("app-role banner should have 'app' role label and host:port: %q", out)
		}
		if strings.Contains(out, "generic-redis") {
			t.Fatalf("app-role banner should NOT say generic-redis (it IS redisx, just app port): %q", out)
		}
	})
	t.Run("PartialFeaturesDocsOnlySearchOff", func(t *testing.T) {
		caps := session.Capabilities{
			IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: false, SearchIndex: false,
		}
		out := BannerFor(caps, "127.0.0.1", "7381")
		if !strings.Contains(out, "docs:  regsch / dropsch / !createsch") {
			t.Fatalf("with TypedDocs=true docs list must be present: %q", out)
		}
		if strings.Contains(out, "idx:") {
			t.Fatalf("with TypedIndexes=false idx list must not appear: %q", out)
		}
		if strings.Contains(out, "searchkey(sk)") || strings.Contains(out, "searchindex(si)") || strings.Contains(out, "update(upd)") {
			t.Fatalf("with SearchIndex=false extended section must not list searchkey/searchindex/update: %q", out)
		}
	})
}

func TestEnterREPLBannerHitsCapabilities(t *testing.T) {
	wantCtxCaps := session.Capabilities{
		IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true,
	}
	prev := session.SetNewForTest(func(_ session.Options) (*session.Session, error) {
		s := &session.Session{}
		s.SetCapabilitiesForTest(wantCtxCaps)
		return s, nil
	})
	defer func() { session.SetNewForTest(prev) }()

	var buf bytes.Buffer
	app, sessPtr := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "9.9.9.9"
	ud.Opts.Port = 7381
	ud.Opts.AdminAuth = "X"
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
	if !strings.Contains(txt, "connected: admin 9.9.9.9:7381") {
		t.Fatalf("Banner(ctx) should pick host/port from AppData after BuildApp; got: %q", txt)
	}
	if strings.Contains(txt, "generic-redis") || strings.Contains(txt, "not a redisx server") {
		t.Fatalf("Banner(ctx) with session built via seam should NOT fall back to generic-redis — regression of 'BeforeRun empty-name no-op' bug: %q", txt)
	}
	if !strings.Contains(txt, "Admin Shell") {
		t.Fatalf("Banner(ctx) should read IsRedisx+AdminRole=true and print 'Admin Shell' head: %q", txt)
	}
	if ok, diag := bannerIsPerfectRect(txt); !ok {
		t.Fatalf("Banner(ctx) box is NOT a perfect rect (user-visible right-side zigzag): %s\nraw=%q", diag, txt)
	}
}

func TestBannerForBoxPerfectRect(t *testing.T) {
	cases := []struct {
		name string
		caps session.Capabilities
		host string
		port string
	}{
		{"PlainMin", session.Capabilities{}, "h", "1"},
		{"PlainLong", session.Capabilities{}, "very-long.hostname.with-many.subdomains.example.internal", "65535"},
		{"AdminFullShort", session.Capabilities{IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true}, "127.0.0.1", "7381"},
		{"AdminFullLong", session.Capabilities{IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true}, "a.very.long.subdomain.chain.for.admin.port.redisx.internal", "7381"},
		{"AppMode", session.Capabilities{IsRedisx: true, AdminRole: false, TypedDocs: true, TypedIndexes: true, SearchIndex: true}, "10.0.0.22", "7379"},
		{"PartialDocsOnly", session.Capabilities{IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: false, SearchIndex: false}, "::1", "7381"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			txt := BannerFor(c.caps, c.host, c.port)
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

func TestCommandsListMatchesAdminShellCaps(t *testing.T) {
	caps := session.Capabilities{
		IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true,
	}
	prev := session.SetNewForTest(func(_ session.Options) (*session.Session, error) {
		s := &session.Session{}
		s.SetCapabilitiesForTest(caps)
		return s, nil
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
		"Basic:", "Extended:", "Meta Management:",
		"regsch", "regidx", "searchkey",
		"admin 127.0.0.1:7381",
	}
	for _, m := range mustContain {
		if !strings.Contains(outPlain, m) {
			t.Fatalf("commands output missing %q — plain output:\n%s", m, outPlain)
		}
	}
	if strings.Contains(outPlain, "generic-redis") || strings.Contains(outPlain, "not a redisx server") {
		t.Fatalf("commands header should be admin shell but saw generic-redis fallback:\n%s", outPlain)
	}
}

func TestBannerVsCommandsHeaderForkRegression(t *testing.T) {
	var realSess *session.Session
	prev := session.SetNewForTest(func(_ session.Options) (*session.Session, error) {
		realSess = &session.Session{}
		realSess.SetCapabilitiesForTest(session.Capabilities{})
		return realSess, nil
	})
	defer func() { session.SetNewForTest(prev) }()
	adminCaps := session.Capabilities{
		IsRedisx: true, AdminRole: true, TypedDocs: true, TypedIndexes: true, SearchIndex: true,
	}
	app, _ := buildStubRoot(t)
	ud, _ := app.UserData().(*AppData)
	ud.Opts.Host = "127.0.0.1"
	ud.Opts.Port = 7381
	ud.Opts.AdminAuth = "X"
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
	realSess.SetCapabilitiesForTest(adminCaps)
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
	bodyAdmin := strings.Contains(bodyConnLine, "(Admin Shell") || strings.Contains(bodyConnLine, "Connected: admin ")
	if headGeneric && bodyAdmin {
		t.Fatalf(
			"BUG REPRODUCED — Banner head says Generic-redis mode, but commands Connected: header says Admin Shell\n\n%s",
			plain,
		)
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
