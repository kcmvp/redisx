package privateip

import (
	"net"
	"testing"
)

func TestFilterBasic(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: net.ParseIP("192.168.1.23"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("10.0.0.8"), Mask: net.CIDRMask(24, 32)},
		&net.IPAddr{IP: net.ParseIP("10.0.0.8")},
		&net.IPNet{IP: net.ParseIP("172.16.0.9"), Mask: net.CIDRMask(16, 32)},
		&net.IPNet{IP: net.ParseIP("8.8.8.8"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("fd00::1"), Mask: net.CIDRMask(64, 128)},
		&net.IPNet{IP: net.ParseIP("fe80::1"), Mask: net.CIDRMask(64, 128)},
	}

	got := Filter(addrs)
	want := []string{"192.168.1.23", "10.0.0.8", "172.16.0.9", "fd00::1"}

	if len(got) != len(want) {
		t.Fatalf("Filter() len = %d, want %d; got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Filter()[%d] = %q, want %q; got=%v", i, got[i], want[i], got)
		}
	}
}

func TestFilterPrefersLANOrder(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("172.19.0.1"), Mask: net.CIDRMask(16, 32)},
		&net.IPNet{IP: net.ParseIP("192.168.9.162"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("10.0.0.8"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("172.18.0.1"), Mask: net.CIDRMask(16, 32)},
		&net.IPNet{IP: net.ParseIP("fd00::1"), Mask: net.CIDRMask(64, 128)},
	}

	got := Filter(addrs)
	want := []string{
		"192.168.9.162",
		"10.0.0.8",
		"172.18.0.1",
		"172.19.0.1",
		"fd00::1",
	}

	if len(got) != len(want) {
		t.Fatalf("Filter() len = %d, want %d; got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Filter()[%d] = %q, want %q; got=%v", i, got[i], want[i], got)
		}
	}
}

func TestRank(t *testing.T) {
	cases := []struct {
		ip string
		r  int
	}{
		{"192.168.0.1", 0},
		{"10.2.3.4", 1},
		{"172.16.0.1", 2},
		{"172.31.255.1", 2},
		{"172.32.0.1", 3},
		{"8.8.8.8", 3},
		{"fd00::1", 3},
	}
	for i, c := range cases {
		got := Rank(c.ip)
		if got != c.r {
			t.Errorf("[%d] Rank(%q)=%d, want %d", i, c.ip, got, c.r)
		}
	}
}

func TestDetectAppBindNoPrivateAddrs(t *testing.T) {
	onlyLoopback := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: net.ParseIP("::1"), Mask: net.CIDRMask(128, 128)},
		&net.IPNet{IP: net.ParseIP("8.8.8.8"), Mask: net.CIDRMask(24, 32)},
	}
	filtered := Filter(onlyLoopback)
	if len(filtered) != 0 {
		t.Fatalf("expected 0 private addrs on loopback-only set, got %v", filtered)
	}

	// Directly simulate "no private IPs found" fallback by mocking via
	// construct DetectResult manually to verify fallback semantics — since
	// DetectAppBind uses the real system net.InterfaceAddrs() we assert only
	// invariants that hold regardless of host.
	res := DetectResult{
		BindIP:               FallbackLoopback(),
		UsedFallbackLoopback: true,
		Warning:              "no RFC1918 private interface addresses detected",
	}
	if res.BindIP != "127.0.0.1" {
		t.Fatalf("fallback bind IP = %q, want 127.0.0.1", res.BindIP)
	}
	if !res.UsedFallbackLoopback || res.Warning == "" {
		t.Fatalf("fallback result must flag loopback used and carry a warning: %+v", res)
	}
}

func TestDetectAppBindHostIntegrity(t *testing.T) {
	// NOTE: runs against the ACTUAL test host.  Both branches are fine:
	//   a) host has private IPs → res.BindIP is one of them, UsedFallback=false.
	//   b) host has no private IPs (e.g. sandboxed loopback-only CI) → falls back.
	res := DetectAppBind(nil)
	if res.BindIP == "" {
		t.Fatalf("DetectAppBind must always return a non-empty BindIP; got %+v", res)
	}
	parsed := net.ParseIP(res.BindIP)
	if parsed == nil {
		t.Fatalf("DetectAppBind returned unparseable bind IP %q", res.BindIP)
	}
	if res.UsedFallbackLoopback {
		if parsed.String() != FallbackLoopback() {
			t.Fatalf("UsedFallbackLoopback=true but bindIP=%q not loopback fallback %q", parsed, FallbackLoopback())
		}
		if res.Warning == "" {
			t.Fatalf("fallback case must carry Warning; got %+v", res)
		}
	} else {
		if !parsed.IsPrivate() {
			t.Fatalf("UsedFallbackLoopback=false but bindIP=%q is not private", parsed)
		}
	}
}
