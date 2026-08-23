package privateip

import (
	"net"
	"sort"
)

const fallbackLoopback = "127.0.0.1"

func All() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	return Filter(addrs), nil
}

func Filter(addrs []net.Addr) []string {
	seen := map[string]struct{}{}
	ips := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		default:
			continue
		}

		if ip == nil || ip.IsLoopback() || !ip.IsPrivate() {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
		}

		s := ip.String()
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		ips = append(ips, s)
	}

	sort.Slice(ips, func(i, j int) bool {
		li, ri := Rank(ips[i]), Rank(ips[j])
		if li != ri {
			return li < ri
		}
		return ips[i] < ips[j]
	})
	return ips
}

func Rank(s string) int {
	ip := net.ParseIP(s)
	if ip4 := ip.To4(); ip4 != nil {
		switch {
		case ip4[0] == 192 && ip4[1] == 168:
			return 0
		case ip4[0] == 10:
			return 1
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return 2
		}
	}
	return 3
}

type DetectResult struct {
	BindIP               string
	UsedFallbackLoopback bool
	PrivateCandidates    []string
	Warning              string
}

func DetectAppBind(customCIDRs []string) DetectResult {
	cands, err := All()
	_ = customCIDRs
	res := DetectResult{PrivateCandidates: append([]string(nil), cands...)}
	if err == nil && len(cands) > 0 {
		res.BindIP = cands[0]
		return res
	}
	res.BindIP = fallbackLoopback
	res.UsedFallbackLoopback = true
	if err != nil {
		res.Warning = "auto-detecting private IPs failed: " + err.Error()
	} else {
		res.Warning = "no RFC1918 private interface addresses detected on this host"
	}
	return res
}

func FallbackLoopback() string { return fallbackLoopback }
