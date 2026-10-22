package respconn

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDefaults(t *testing.T) {
	t.Run("fills zero values with package defaults", func(t *testing.T) {
		got := Defaults(Options{})
		if got.Host != "127.0.0.1" {
			t.Fatalf("Host = %q, want 127.0.0.1", got.Host)
		}
		if got.Port != 7381 {
			t.Fatalf("Port = %d, want 7381", got.Port)
		}
		if got.TimeoutMs != 3000 {
			t.Fatalf("TimeoutMs = %d, want 3000", got.TimeoutMs)
		}
	})

	t.Run("preserves explicit values", func(t *testing.T) {
		called := false
		onConnect := func(context.Context, *redis.Conn) error {
			called = true
			return nil
		}
		got := Defaults(Options{
			Host:         "10.0.0.9",
			Port:         6390,
			Auth:         "secret",
			TimeoutMs:    1250,
			AuthOptional: true,
			ReadTimeout:  2 * time.Second,
			OnConnect:    onConnect,
		})
		if got.Host != "10.0.0.9" || got.Port != 6390 || got.TimeoutMs != 1250 {
			t.Fatalf("Defaults() changed explicit network values: %+v", got)
		}
		if got.Auth != "secret" || !got.AuthOptional || got.ReadTimeout != 2*time.Second || got.OnConnect == nil {
			t.Fatalf("Defaults() changed explicit option fields: %+v", got)
		}
		if called {
			t.Fatal("Defaults() must not call OnConnect")
		}
	})
}

func TestDialAndHandshakeFallsBackToLegacyAuthOnHelloError(t *testing.T) {
	var sawOnConnect atomic.Bool
	cmdsCh := make(chan []string, 16)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		br := bufio.NewReader(conn)
		for i := 0; i < 256; i++ {
			words, rerr := readOneRESPWords(br)
			if rerr != nil {
				return
			}
			if len(words) == 0 {
				return
			}
			select {
			case cmdsCh <- words:
			default:
			}

			var reply string
			switch strings.ToUpper(words[0]) {
			case "HELLO":
				reply = "-ERR unknown command 'HELLO'\r\n"
			case "AUTH":
				reply = "+OK\r\n"
			case "PING":
				reply = "+PONG\r\n"
			case "COMMAND":
				reply = "*0\r\n"
			case "CLIENT":
				reply = "-ERR unknown command 'CLIENT'\r\n"
			default:
				reply = "-ERR unknown command '" + strings.ToUpper(words[0]) + "'\r\n"
			}
			if _, werr := conn.Write([]byte(reply)); werr != nil {
				return
			}
		}
	}()

	host, portText, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("Atoi(%q): %v", portText, err)
	}

	res, err := DialAndHandshake(Options{
		Host:      host,
		Port:      port,
		Auth:      "secret-key",
		TimeoutMs: 1800,
		OnConnect: func(context.Context, *redis.Conn) error {
			sawOnConnect.Store(true)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("DialAndHandshake() error = %v", err)
	}
	defer func() { _ = res.Client.Close() }()

	if res.Client == nil {
		t.Fatal("DialAndHandshake() returned nil client")
	}
	if res.Timeout != 1800*time.Millisecond {
		t.Fatalf("Timeout = %v, want %v", res.Timeout, 1800*time.Millisecond)
	}
	if !sawOnConnect.Load() {
		t.Fatal("expected OnConnect to run")
	}

	var sawHello2, sawAuth, sawPing bool
	var helloWords []string
	timeout := time.After(2 * time.Second)
collect:
	for {
		select {
		case words := <-cmdsCh:
			switch strings.ToUpper(words[0]) {
			case "HELLO":
				sawHello2 = len(words) >= 5 && words[1] == "2" && strings.EqualFold(words[2], "AUTH") && words[3] == "default" && words[4] == "secret-key"
				helloWords = words
			case "AUTH":
				sawAuth = len(words) == 2 && words[1] == "secret-key"
			case "PING":
				sawPing = true
			}
			if sawHello2 && sawAuth && sawPing {
				break collect
			}
		case <-timeout:
			break collect
		}
	}

	if !sawHello2 {
		t.Fatalf("want HELLO 2 AUTH default <pass>, saw %v", helloWords)
	}
	if !sawAuth {
		t.Fatal("want legacy AUTH <pass> fallback after HELLO error")
	}
	if !sawPing {
		t.Fatal("want Ping probe to run after handshake")
	}
}

func readOneRESPWords(br *bufio.Reader) ([]string, error) {
	ch, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	if ch[0] != '*' {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return strings.Fields(strings.TrimSpace(line)), nil
	}

	header, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "*")))
	if err != nil {
		return nil, fmt.Errorf("invalid multibulk header %q: %w", header, err)
	}

	words := make([]string, 0, n)
	for i := 0; i < n; i++ {
		bulkHeader, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(bulkHeader, "$") {
			return nil, fmt.Errorf("invalid bulk header %q", bulkHeader)
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(bulkHeader, "$")))
		if err != nil {
			return nil, fmt.Errorf("invalid bulk size %q: %w", bulkHeader, err)
		}
		buf := make([]byte, size+2)
		if _, err := br.Read(buf); err != nil {
			return nil, err
		}
		words = append(words, string(buf[:size]))
	}
	return words, nil
}
