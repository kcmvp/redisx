//go:build testonly
// +build testonly

package internal

import (
	"strings"
	"testing"

	h "github.com/kcmvp/redisx/cmd/_shared/harness"
	"github.com/kcmvp/redisx/cmd/_shared/session"
)

type dialRole int

const (
	dialCtrl dialRole = iota
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
	opts := session.Options{TimeoutMs: 2500, Auth: c.DialAuth}
	switch c.DialRole {
	case dialCtrl:
		opts.Host = hrn.CtrlBind()
		opts.Port = hrn.CtrlPort
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

var sharedAfterCtrlSkeletonCmd = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"REGSCH", `{"namespace":"skeletontest","mem":false,"key_attrs":["id"],"ttl_ns":0}`})
	_, err := r.Result()
	if err != nil {
		t.Fatalf("REGSCH expected OK (registry implemented), err=%v", err)
	}
	r = sh.RawDo([]any{"DROPSCH", "skeletontest"})
	_, err = r.Result()
	if err != nil {
		t.Fatalf("DROPSCH expected OK on just-registered doc, err=%v", err)
	}
	r = sh.RawDo([]any{"REGSCH"})
	_, err = r.Result()
	if err == nil {
		t.Fatalf("REGSCH no-args should fail Gate3, got nil error")
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "expected >=") &&
		!strings.Contains(msg, "usage") &&
		!strings.Contains(msg, "wrong number") {
		t.Fatalf("REGSCH no-args Gate3 missing: %v", err)
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

var sharedAfterCtrlCmdOnAppPortBlocked = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"REGSCH", `{"namespace":"test","mem":false,"key_attrs":["id"]}`})
	_, err := r.Result()
	if err == nil {
		t.Fatalf("REGSCH on app-port must Gate1 reject, got nil error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "app port") &&
		!strings.Contains(msg, "Meta Management") &&
		!strings.Contains(msg, "No Privilege") {
		t.Fatalf("app-port REGSCH missing Gate1 hint in: %v", err)
	}
	if err := sh.RawDo([]any{"SET", "app:k", "2"}).Err(); err != nil {
		t.Fatalf("app-port SET failed: %v", err)
	}
	if v, err := sh.RawDo([]any{"GET", "app:k"}).Text(); err != nil || v != "2" {
		t.Fatalf("app-port GET = %q err=%v want 2", v, err)
	}
}

var sharedAfterCtrlWrongAuthWrongPass = func(t *testing.T, sh *session.Session, _ *h.Harness) {
	t.Helper()
	r := sh.RawDo([]any{"PING"})
	_, err := r.Result()
	if err == nil {
		t.Fatalf("PING with ctrl-port wrong AUTH should Gate0 WRONGPASS, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "WRONGPASS") {
		t.Fatalf("wrong-auth-on-ctrl PING err %q; want substring WRONGPASS (Gate0)", msg)
	}
}

func TestRdxmE2E(t *testing.T) {
	cases := []integCase{
		{
			Name:      "no_auth_ctrl_connect_ping",
			Harness:   h.HarnessOpts{},
			DialRole:  dialCtrl,
			AfterDial: sharedAfterPingOK,
		},
		{
			Name:            "ctrl_auth_set_user_omits_a_receives_noauth",
			Harness:         h.HarnessOpts{CtrlAuth: "secreta"},
			DialRole:        dialCtrl,
			DialAuth:        "",
			WantNewShellSub: "NOAUTH",
		},
		{
			Name:      "ctrl_auth_set_user_passes_correct_key_connects_runs_registry_skeleton",
			Harness:   h.HarnessOpts{CtrlAuth: "abc123"},
			DialRole:  dialCtrl,
			DialAuth:  "abc123",
			AfterDial: sharedAfterCtrlSkeletonCmd,
		},
		{
			Name:            "ctrl_auth_set_user_passes_wrong_key_receives_err_auth_failed",
			Harness:         h.HarnessOpts{CtrlAuth: "rightkey"},
			DialRole:        dialCtrl,
			DialAuth:        "wrongkey",
			WantNewShellSub: "ERR authentication failed",
			AfterDial:       nil,
		},
		{
			Name:      "app_port_ctrl_only_cmd_gate1_blocked_while_setget_still_works",
			Harness:   h.HarnessOpts{AppAuth: "onlyapp", CtrlAuth: "onlyctrl"},
			DialRole:  dialApp,
			DialAuth:  "onlyapp",
			AfterDial: sharedAfterCtrlCmdOnAppPortBlocked,
		},
		{
			Name:      "app_port_correct_auth_runs_setget_ping",
			Harness:   h.HarnessOpts{AppAuth: "apk", CtrlAuth: "adk"},
			DialRole:  dialApp,
			DialAuth:  "apk",
			AfterDial: sharedAfterSetGetOK,
		},
		{
			Name:            "app_port_user_passes_ctrl_auth_gets_gate0_wrongpass",
			Harness:         h.HarnessOpts{AppAuth: "a1", CtrlAuth: "a2"},
			DialRole:        dialApp,
			DialAuth:        "a2",
			WantNewShellSub: "WRONGPASS",
		},
		{
			Name:            "ctrl_port_user_passes_app_auth_gets_gate0_wrongpass",
			Harness:         h.HarnessOpts{AppAuth: "a1", CtrlAuth: "a2"},
			DialRole:        dialCtrl,
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
