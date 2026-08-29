package internal

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/cmd/_shared/tui/widgets"
)

const Version = "0.1.0"

type ConnOpts struct {
	Host      string
	Port      int
	Auth      string
	TimeoutMs int
}

type AppData struct {
	Opts    ConnOpts
	Session **session.Session
	Cache   *session.Cache
	Help    bool
	ShowVer bool
	// Endpoints are the connect shortcuts discovered in ./redisx.yaml.
	Endpoints []LocalEndpoint
	// InREPL is true while the interactive REPL loop runs (drives the
	// not-connected error instead of the one-shot lazy dial).
	InREPL bool
	// ConnHost/ConnPort record the endpoint the current session dialed.
	ConnHost string
	ConnPort int
}

func (a *AppData) HostPort() (string, string) {
	hs := a.Opts.Host
	if hs == "" {
		hs = "127.0.0.1"
	}
	ps := "7381"
	if a.Opts.Port != 0 {
		ps = fmt.Sprintf("%d", a.Opts.Port)
	}
	return hs, ps
}

func cBold(s string) string    { return tui.Bold(s) }
func cCyan(s string) string    { return tui.Cyan(s) }
func cYellow(s string) string  { return tui.Yellow(s) }
func cRed(s string) string     { return tui.Red(s) }
func cMagenta(s string) string { return tui.Magenta(s) }
func cDim(s string) string     { return tui.Dim(s) }

// sepSentinel marks the position of the mid-banner separator line; it is
// replaced with the rendered "- - " separator after the banner width is known.
const sepSentinel = "\x00sep"

func bannerLines(connected string, endpoints []LocalEndpoint) []string {
	headPure := "Redisx (Compatible with Redis Shell)"
	extPure := []string{"sk", "si", "upd", "regsch", "dropsch", "regidx", "dropidx"}
	hintPure := `Type "help <command>" for help — "quit" / Ctrl-C to exit.`
	body := []string{"connect: con [-h host] [-p port] [-a auth] | con <host> <port> [auth]"}
	body = append(body, "")
	body = append(body, "Extended: "+strings.Join(extPure, ", "))
	body = append(body, "")
	body = append(body, hintPure)
	if connected != "" {
		body = append(body, "")
		body = append(body, connected)
	}
	if len(endpoints) > 0 {
		body = append(body, sepSentinel)
		body = append(body, LocalConfigFile+" found — endpoint shortcuts:")
		for _, e := range endpoints {
			body = append(body, "  !"+e.Name+"  →  "+e.Addr())
		}
	}
	bodyMaxW := 0
	for _, l := range body {
		if rw := len([]rune(l)); rw > bodyMaxW {
			bodyMaxW = rw
		}
	}
	headW := len([]rune(headPure))
	totalW := bodyMaxW
	if totalW < headW {
		totalW = headW
	}
	if totalW < 80 {
		totalW = 80
	}
	var sepSb strings.Builder
	pattern := "- - "
	for r := 0; r < totalW; r++ {
		sepSb.WriteByte(pattern[r%len(pattern)])
	}
	sepPure := sepSb.String()
	pure := make([]string, 0, 3+len(body))
	pure = append(pure, headPure)
	pure = append(pure, "")
	pure = append(pure, sepPure)
	pure = append(pure, body...)
	out := make([]string, len(pure))
	for i, p := range pure {
		switch i {
		case 0:
			out[i] = cMagenta(p)
		case 1:
			out[i] = p
		default:
			line := p
			if line == sepSentinel {
				out[i] = sepPure
				continue
			}
			if line == hintPure {
				line = cDim(line)
			} else if strings.HasPrefix(line, "Extended:") {
				line = cBold("Extended") + line[len("Extended"):]
			} else if strings.HasPrefix(line, "connect:") {
				line = cBold("connect") + line[len("connect"):]
			} else if strings.HasPrefix(line, LocalConfigFile+" found") {
				line = cYellow(line)
			} else if strings.HasPrefix(line, "  !") {
				line = cYellow(line)
			}
			out[i] = line
		}
	}
	return out
}

func bannerLinesFor(host, port string) []string {
	return bannerLines("connected: "+host+":"+port+"  (raw RESP passthrough)", nil)
}

func bannerLinesDisconnected(endpoints []LocalEndpoint) []string {
	return bannerLines("", endpoints)
}

func stripANSIStrict(s string) string {
	var sb strings.Builder
	inEsc := false
	for _, r := range s {
		switch {
		case inEsc:
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
		case r == '\x1b':
			inEsc = true
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func BannerFor(host, port string) string {
	lines := bannerLinesFor(host, port)
	return renderBannerBox(lines)
}

// BannerDisconnectedFor renders the startup banner shown before the user has
// connected: it advertises the `con` command and, when ./redisx.yaml was
// found, the `!<name>` endpoint shortcuts derived from its top-level keys.
func BannerDisconnectedFor(endpoints []LocalEndpoint) string {
	lines := bannerLinesDisconnected(endpoints)
	return renderBannerBox(lines)
}

func renderBannerBox(lines []string) string {
	stripped := make([]string, len(lines))
	for i, l := range lines {
		stripped[i] = stripANSIStrict(l)
	}
	contentW := 0
	for _, l := range stripped {
		r := len([]rune(l))
		if r > contentW {
			contentW = r
		}
	}
	if contentW < 80 {
		contentW = 80
	}
	sidePad := 2
	borderRunes := contentW + sidePad*2
	borderLine := " " + strings.Repeat("─", borderRunes)
	var sb strings.Builder
	sb.WriteString(borderLine + "\n")
	for i, l := range lines {
		contentRunes := len([]rune(stripped[i]))
		pad := contentW - contentRunes
		if pad < 0 {
			pad = 0
		}
		var middle string
		if i == 0 {
			leftPad := pad / 2
			rightPad := pad - leftPad
			middle = strings.Repeat(" ", sidePad) + strings.Repeat(" ", leftPad) + l + strings.Repeat(" ", rightPad) + strings.Repeat(" ", sidePad)
			middleActual := sidePad + leftPad + contentRunes + rightPad + sidePad
			if middleActual < borderRunes {
				middle = middle + strings.Repeat(" ", borderRunes-middleActual)
			}
		} else {
			middle = strings.Repeat(" ", sidePad) + l + strings.Repeat(" ", pad+sidePad)
			middleActual := sidePad + contentRunes + pad + sidePad
			if middleActual < borderRunes {
				middle = middle + strings.Repeat(" ", borderRunes-middleActual)
			}
		}
		sb.WriteString("│" + middle + "│\n")
	}
	sb.WriteString(borderLine + "\n")
	return sb.String()
}

func printScreenClear() {
	if runtime.GOOS == "windows" {
		fmt.Print("\x1b[2J\x1b[H")
	} else {
		fmt.Print("\x1b[2J\x1b[H\x1b[3J")
	}
}

func HistoryFileInternal() string {
	return tui.HistoryFile(".redisx_history")
}

func HistoryFile() string {
	return HistoryFileInternal()
}

func TruncateMiddle(s string, n int) string {
	return tui.TruncateMiddle(s, n)
}

func PrintTabWriter(out io.Writer, headers []string, rows [][]string) {
	widgets.PrintTable(out, headers, rows)
}

func appendHistoryInternal(line string) {
	tui.AppendHistory(HistoryFileInternal(), line)
}

func shlexSplit(s string) ([]string, bool) {
	var out []string
	var sb strings.Builder
	inSingle, inDouble := false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s) && (inDouble || (!inSingle && !inDouble)):
			sb.WriteByte(s[i+1])
			i++
		case c == '\'' && !inDouble:
			inSingle = !inSingle
		case c == '"' && !inSingle:
			inDouble = !inDouble
		case (c == ' ' || c == '\t') && !inSingle && !inDouble:
			if sb.Len() > 0 {
				out = append(out, sb.String())
				sb.Reset()
			}
		default:
			sb.WriteByte(c)
		}
	}
	if sb.Len() > 0 {
		out = append(out, sb.String())
	}
	if inSingle || inDouble {
		fmt.Fprintln(os.Stderr, cRed("unterminated quoted string"))
		return nil, false
	}
	return out, true
}
