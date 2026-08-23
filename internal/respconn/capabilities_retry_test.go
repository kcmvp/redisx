package respconn

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func readOneRESPCommand(br *bufio.Reader) (string, error) {
	ch, err := br.Peek(1)
	if err != nil {
		return "", err
	}
	if ch[0] == '*' {
		line, lerr := br.ReadString('\n')
		if lerr != nil && len(line) == 0 {
			return "", lerr
		}
		trimmed := strings.TrimSpace(strings.TrimPrefix(line, "*"))
		n, perr := strconv.Atoi(trimmed)
		if perr != nil {
			return line, nil
		}
		words := make([]string, 0, n)
		for i := 0; i < n; i++ {
			blLine, blerr := br.ReadString('\n')
			if blerr != nil {
				return strings.Join(words, " "), nil
			}
			if !strings.HasPrefix(blLine, "$") {
				continue
			}
			blenTrim := strings.TrimSpace(strings.TrimPrefix(blLine, "$"))
			blen, bperr := strconv.Atoi(blenTrim)
			if bperr != nil {
				continue
			}
			buf := make([]byte, blen+2)
			_, rrerr := io.ReadFull(br, buf)
			if rrerr != nil {
				return strings.Join(words, " "), nil
			}
			words = append(words, string(buf[:blen]))
		}
		return strings.Join(words, " "), nil
	}
	line, lerr := br.ReadString('\n')
	return strings.TrimSpace(line), lerr
}

func writeBulk(sb *strings.Builder, s string) {
	_, _ = fmt.Fprintf(sb, "$%d\r\n%s\r\n", len(s), s)
}
func writeIntSB(sb *strings.Builder, i int64) { _, _ = fmt.Fprintf(sb, ":%d\r\n", i) }
func writeMapH(sb *strings.Builder, n int)    { _, _ = fmt.Fprintf(sb, "%%%d\r\n", n) }

func redisxHello() []byte {
	var sb strings.Builder
	writeMapH(&sb, 7)
	writeBulk(&sb, "server")
	writeBulk(&sb, "redisx")
	writeBulk(&sb, "version")
	writeBulk(&sb, "1.0.0")
	writeBulk(&sb, "admin_role")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "dual_port")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "features")
	writeMapH(&sb, 6)
	writeBulk(&sb, "typed_docs")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "typed_indexes")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "live_rebuild")
	writeIntSB(&sb, 0)
	writeBulk(&sb, "write_hooks")
	writeIntSB(&sb, 0)
	writeBulk(&sb, "search_index")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "pubsub")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "stats")
	writeMapH(&sb, 2)
	writeBulk(&sb, "namespaces")
	writeIntSB(&sb, 3)
	writeBulk(&sb, "indexes")
	writeIntSB(&sb, 2)
	writeBulk(&sb, "storage")
	writeMapH(&sb, 1)
	writeBulk(&sb, "mode")
	writeBulk(&sb, "hybrid")
	return []byte(sb.String())
}

func genericHello() []byte {
	var sb strings.Builder
	writeMapH(&sb, 3)
	writeBulk(&sb, "server")
	writeBulk(&sb, "generic")
	writeBulk(&sb, "proto")
	writeIntSB(&sb, 3)
	writeBulk(&sb, "role")
	writeBulk(&sb, "master")
	return []byte(sb.String())
}

func writeArrH(sb *strings.Builder, n int) { _, _ = fmt.Fprintf(sb, "*%d\r\n", n) }

func redisxHelloResp2() []byte {
	var sb strings.Builder
	writeArrH(&sb, 12)
	writeBulk(&sb, "server")
	writeBulk(&sb, "redisx")
	writeBulk(&sb, "version")
	writeBulk(&sb, "1.0.0")
	writeBulk(&sb, "admin_role")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "dual_port")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "features")
	writeArrH(&sb, 12)
	writeBulk(&sb, "typed_docs")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "typed_indexes")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "live_rebuild")
	writeIntSB(&sb, 0)
	writeBulk(&sb, "write_hooks")
	writeIntSB(&sb, 0)
	writeBulk(&sb, "search_index")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "pubsub")
	writeIntSB(&sb, 1)
	writeBulk(&sb, "storage")
	writeArrH(&sb, 2)
	writeBulk(&sb, "mode")
	writeBulk(&sb, "hybrid")
	return []byte(sb.String())
}

func TestParseHello_Resp2FlatArrayRecognizesRedisx(t *testing.T) {
	addr := "127.0.0.1:18736"
	mp := startMock(t, addr, func(verb string, callNo int) []byte {
		switch verb {
		case "AUTH":
			return []byte("+OK\r\n")
		case "HELLO":
			return redisxHelloResp2()
		case "PING":
			return []byte("+PONG\r\n")
		}
		return nil
	})
	defer mp.Close()
	res, err := DialAndHandshake(Options{Host: "127.0.0.1", Port: 18736, Auth: "x", TimeoutMs: 2500, Protocol: 2})
	if err != nil {
		t.Fatalf("DialAndHandshake err: %v\nwire=%v", err, mp.Seen)
	}
	defer func() { _ = res.Client.Close() }()
	if !res.Capabilities.IsRedisx {
		t.Fatalf("BUG: RESP2 HELLO flat-array should still promote IsRedisx=true; caps=%+v\nwire=%v", res.Capabilities, mp.Seen)
	}
	if !res.Capabilities.AdminRole || !res.Capabilities.DualPort {
		t.Fatalf("RESP2 admin_role/dual_port not propagated: %+v", res.Capabilities)
	}
	if !res.Capabilities.TypedDocs || !res.Capabilities.SearchIndex || !res.Capabilities.PubSub {
		t.Fatalf("RESP2 features not parsed: %+v", res.Capabilities)
	}
	if res.Capabilities.StorageMode != "hybrid" || res.Capabilities.ServerVer != "1.0.0" {
		t.Fatalf("RESP2 storage/version not parsed: %+v", res.Capabilities)
	}
}

type mockPeer struct {
	ln   net.Listener
	Seen [][]string
	done chan struct{}
}

func startMock(t *testing.T, addr string, rules func(verb string, callNo int) []byte) *mockPeer {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	mp := &mockPeer{ln: ln, done: make(chan struct{})}
	go func() {
		defer close(mp.done)
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		br := bufio.NewReader(conn)
		total := 0
		for {
			line, rerr := readOneRESPCommand(br)
			if rerr != nil {
				return
			}
			line = strings.TrimSpace(line)
			if line == "" {
				return
			}
			words := strings.Fields(line)
			verb := strings.ToUpper(words[0])
			mp.Seen = append(mp.Seen, append([]string(nil), words...))
			switch verb {
			case "CLIENT", "COMMAND":
				_, _ = conn.Write([]byte("+OK\r\n"))
				continue
			case "QUIT":
				_, _ = conn.Write([]byte("+OK\r\n"))
				return
			}
			total++
			reply := rules(verb, total)
			if reply == nil {
				reply = []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", verb))
			}
			if _, werr := conn.Write(reply); werr != nil {
				return
			}
		}
	}()
	return mp
}

func (mp *mockPeer) Close() {
	if mp.ln != nil {
		_ = mp.ln.Close()
	}
}

// Scenario A: HELLO 1/2 generic, HELLO 3 redisx → retry must win.
func TestProbeRetry_RetryWins(t *testing.T) {
	addr := "127.0.0.1:18731"
	helloSeen := 0
	mp := startMock(t, addr, func(verb string, callNo int) []byte {
		if verb == "AUTH" {
			return []byte("+OK\r\n")
		}
		if verb == "HELLO" {
			helloSeen++
			if helloSeen <= 2 {
				return genericHello()
			}
			return redisxHello()
		}
		return nil
	})
	defer mp.Close()
	res, err := DialAndHandshake(Options{Host: "127.0.0.1", Port: 18731, Auth: "x", TimeoutMs: 3500})
	if err != nil {
		t.Fatalf("DialAndHandshake err: %v\nwire=%v", err, mp.Seen)
	}
	defer func() { _ = res.Client.Close() }()
	if !res.Capabilities.IsRedisx {
		t.Fatalf("BUG: retry should have elevated IsRedisx=true; caps=%+v\nwire=%v", res.Capabilities, mp.Seen)
	}
	if !res.Capabilities.AdminRole {
		t.Fatalf("admin_role not propagated: %+v", res.Capabilities)
	}
	if res.Capabilities.StatsNs != 3 || res.Capabilities.StorageMode != "hybrid" {
		t.Fatalf("stats/storage: %+v", res.Capabilities)
	}
	if !res.Capabilities.TypedDocs || !res.Capabilities.SearchIndex {
		t.Fatalf("features not parsed: %+v", res.Capabilities)
	}
}

// Scenario B: every HELLO redisx → immediate.
func TestProbeRetry_ImmediateRedisx(t *testing.T) {
	addr := "127.0.0.1:18732"
	mp := startMock(t, addr, func(verb string, callNo int) []byte {
		switch verb {
		case "AUTH":
			return []byte("+OK\r\n")
		case "HELLO":
			return redisxHello()
		}
		return nil
	})
	defer mp.Close()
	res, err := DialAndHandshake(Options{Host: "127.0.0.1", Port: 18732, Auth: "x", TimeoutMs: 2000})
	if err != nil {
		t.Fatalf("DialAndHandshake err: %v\nwire=%v", err, mp.Seen)
	}
	defer func() { _ = res.Client.Close() }()
	if !res.Capabilities.IsRedisx || !res.Capabilities.AdminRole {
		t.Fatalf("redisx caps wrong: %+v\nwire=%v", res.Capabilities, mp.Seen)
	}
}

// Scenario C: all HELLO generic → stay generic.
func TestProbeRetry_GenericStaysGeneric(t *testing.T) {
	addr := "127.0.0.1:18733"
	mp := startMock(t, addr, func(verb string, callNo int) []byte {
		switch verb {
		case "AUTH":
			return []byte("+OK\r\n")
		case "HELLO":
			return genericHello()
		}
		return nil
	})
	defer mp.Close()
	res, err := DialAndHandshake(Options{Host: "127.0.0.1", Port: 18733, Auth: "x", TimeoutMs: 2000})
	if err != nil {
		t.Fatalf("DialAndHandshake err: %v\nwire=%v", err, mp.Seen)
	}
	defer func() { _ = res.Client.Close() }()
	if res.Capabilities.IsRedisx {
		t.Fatalf("generic must not promote: %+v\nwire=%v", res.Capabilities, mp.Seen)
	}
}

// Scenario D: RefreshCapabilities (ProbeCapabilitiesWithRetry) on an existing client upgrades from generic to redisx.
func TestProbeRetry_RefreshUpgrades(t *testing.T) {
	addr := "127.0.0.1:18735"
	helloSeen := 0
	mp := startMock(t, addr, func(verb string, callNo int) []byte {
		if verb == "AUTH" {
			return []byte("+OK\r\n")
		}
		if verb == "HELLO" {
			helloSeen++
			if helloSeen <= 2 {
				return genericHello()
			}
			return redisxHello()
		}
		return nil
	})
	defer mp.Close()
	onConnect := func(ctx context.Context, cn *redis.Conn) error {
		return cn.Do(ctx, "AUTH", "x").Err()
	}
	cl := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           0,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  4 * time.Second,
		WriteTimeout: 2 * time.Second,
		OnConnect:    onConnect,
	})
	defer func() { _ = cl.Close() }()

	init := ProbeCapabilities(context.Background(), cl, 2*time.Second)
	if init.IsRedisx {
		t.Fatalf("first probe should be generic, got %+v\nwire=%v", init, mp.Seen)
	}
	final := ProbeCapabilitiesWithRetry(context.Background(), cl, 4*time.Second)
	if !final.IsRedisx {
		t.Fatalf("retry should have redisx caps=%+v\nwire=%v", final, mp.Seen)
	}
	if !final.AdminRole || final.StatsIdx != 2 {
		t.Fatalf("fields not populated: %+v", final)
	}
}
