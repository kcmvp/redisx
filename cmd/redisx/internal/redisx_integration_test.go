//go:build testonly
// +build testonly

package internal

import (
	"fmt"
	"strings"
	"testing"

	h "github.com/kcmvp/redisx/cmd/_shared/harness"
	"github.com/kcmvp/redisx/cmd/_shared/session"
)

type dialRole int

const (
	dialAdmin dialRole = iota
	dialApp
)

type integCase struct {
	Name            string
	Harness         h.HarnessOpts
	DialRole        dialRole
	DialAuth        string
	WantNewShellSub string
	AfterDial       func(t *testing.T, sh *session.Session, hrn *h.Harness)
}

func runIntegCase(t *testing.T, c integCase) {
	t.Helper()
	hrn := h.NewHarness(t, c.Harness)
	opts := session.Options{TimeoutMs: 2500, AdminAuth: c.DialAuth}
	switch c.DialRole {
	case dialAdmin:
		opts.Host = hrn.AdminBind()
		opts.Port = hrn.AdminPort
	case dialApp:
		opts.Host = hrn.AppBind()
		opts.Port = hrn.AppPort
	}

	sh, err := session.New(opts)
	if c.WantNewShellSub != "" {
		if err == nil {
			_ = sh.Close()
			t.Fatalf("expected session.New error containing %q, got nil", c.WantNewShellSub)
		}
		if !strings.Contains(err.Error(), c.WantNewShellSub) {
			t.Fatalf("session.New error = %v; want substring %q", err, c.WantNewShellSub)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected session.New error: %v", err)
	}
	defer func() { _ = sh.Close() }()
	if c.AfterDial != nil {
		c.AfterDial(t, sh, hrn)
	}
}

var sharedAfterPingOK = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"PING"})
	s, err := r.Text()
	if err != nil {
		t.Fatalf("PING err: %v", err)
	}
	if s != "PONG" {
		t.Fatalf("PING=%q want PONG", s)
	}
}

var sharedAfterAdminSkeletonCmd = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"LSDOC"})
	s, err := r.Result()
	if err != nil {
		if !strings.Contains(err.Error(), "not implemented yet") {
			t.Fatalf("LSDOC err=%v want skeleton 'not implemented yet'", err)
		}
		return
	}
	if !strings.Contains(fmt.Sprintf("%s", s), "not implemented yet") {
		t.Fatalf("LSDOC=%q want skeleton marker", s)
	}
	r = sh.RawDo([]any{"REGDOC"})
	_, err = r.Result()
	if err == nil {
		t.Fatalf("REGDOC no-args should fail Gate3, got nil error")
	}
	if !strings.Contains(err.Error(), "expected >=") && !strings.Contains(err.Error(), "Usage") {
		t.Fatalf("REGDOC no-args Gate3 missing: %v", err)
	}
}

var sharedAfterSetGetOK = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"SET", "integ:k", "v1"})
	if _, err := r.Result(); err != nil {
		t.Fatalf("SET err: %v", err)
	}
	r = sh.RawDo([]any{"GET", "integ:k"})
	s, err := r.Text()
	if err != nil {
		t.Fatalf("GET err: %v", err)
	}
	if s != "v1" {
		t.Fatalf("GET=%q want v1", s)
	}
}

var sharedAfterAdminCmdOnAppPortBlocked = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"LSDOC"})
	_, err := r.Result()
	if err == nil {
		t.Fatalf("LSDOC on app-port must Gate1 reject, got nil error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "app port") &&
		!strings.Contains(msg, "admin-only commands are not available") {
		t.Fatalf("app-port LSDOC missing Gate1 hint in: %v", err)
	}
	if err := sh.RawDo([]any{"SET", "app:k", "2"}).Err(); err != nil {
		t.Fatalf("app-port SET failed: %v", err)
	}
	if v, err := sh.RawDo([]any{"GET", "app:k"}).Text(); err != nil || v != "2" {
		t.Fatalf("app-port GET = %q err=%v want 2", v, err)
	}
}

var sharedAfterAdminWrongAuthWrongPass = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"PING"})
	_, err := r.Result()
	if err == nil {
		t.Fatalf("PING with admin-port wrong AUTH should Gate0 WRONGPASS, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "WRONGPASS") {
		t.Fatalf("wrong-auth-on-admin PING err %q; want substring WRONGPASS (Gate0)", msg)
	}
}

func TestRdxmE2E(t *testing.T) {
	cases := []integCase{
		{
			Name:      "no_auth_admin_connect_ping",
			Harness:   h.HarnessOpts{},
			DialRole:  dialAdmin,
			AfterDial: sharedAfterPingOK,
		},
		{
			Name:            "admin_auth_set_user_omits_a_receives_noauth_hint",
			Harness:         h.HarnessOpts{AdminAuth: "secreta"},
			DialRole:        dialAdmin,
			DialAuth:        "",
			WantNewShellSub: "server admin-port requires AUTH. Pass the admin-auth key via",
		},
		{
			Name:      "admin_auth_set_user_passes_correct_key_connects_runs_lsdoc_skeleton",
			Harness:   h.HarnessOpts{AdminAuth: "abc123"},
			DialRole:  dialAdmin,
			DialAuth:  "abc123",
			AfterDial: sharedAfterAdminSkeletonCmd,
		},
		{
			Name:            "admin_auth_set_user_passes_wrong_key_receives_err_auth_failed",
			Harness:         h.HarnessOpts{AdminAuth: "rightkey"},
			DialRole:        dialAdmin,
			DialAuth:        "wrongkey",
			WantNewShellSub: "ERR authentication failed",
			AfterDial:       nil,
		},
		{
			Name:      "app_port_admin_only_cmd_gate1_blocked_while_setget_still_works",
			Harness:   h.HarnessOpts{AppAuth: "onlyapp", AdminAuth: "onlyadmin"},
			DialRole:  dialApp,
			DialAuth:  "onlyapp",
			AfterDial: sharedAfterAdminCmdOnAppPortBlocked,
		},
		{
			Name:      "app_port_correct_auth_runs_setget_ping",
			Harness:   h.HarnessOpts{AppAuth: "apk", AdminAuth: "adk"},
			DialRole:  dialApp,
			DialAuth:  "apk",
			AfterDial: sharedAfterSetGetOK,
		},
		{
			Name:            "app_port_user_passes_admin_auth_gets_gate0_wrongpass",
			Harness:         h.HarnessOpts{AppAuth: "a1", AdminAuth: "a2"},
			DialRole:        dialApp,
			DialAuth:        "a2",
			WantNewShellSub: "WRONGPASS",
		},
		{
			Name:            "admin_port_user_passes_app_auth_gets_gate0_wrongpass",
			Harness:         h.HarnessOpts{AppAuth: "a1", AdminAuth: "a2"},
			DialRole:        dialAdmin,
			DialAuth:        "a1",
			WantNewShellSub: "WRONGPASS",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			runIntegCase(t, c)
		})
	}
}
