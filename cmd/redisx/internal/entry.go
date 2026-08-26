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
func cPurple(s string) string  { return tui.Magenta(s) }

func bannerLinesFor(host, port string) []string {
	headPure := "Redisx  RESP Shell  — Raw RESP passthrough; support & privilege enforcement handled server-side"
	basicPure := []string{"ping", "!version / !clear", "!help", "quit / exit (Ctrl-C)"}
	metaPure := []string{
		"docs:  regsch / dropsch",
		"idx:   regidx / dropidx",
	}
	extPure := []string{"searchkey(sk)  /  searchindex(si)  /  update(upd)"}
	body := []string{"Basic: " + strings.Join(basicPure, "  ·  ")}
	body = append(body, "Extended: "+strings.Join(extPure, "    "))
	body = append(body, "Meta:")
	for _, m := range metaPure {
		body = append(body, "  "+m)
	}
	body = append(body, "connected: "+host+":"+port+"  (raw RESP passthrough)")
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
	pure := make([]string, 0, 2+len(body))
	pure = append(pure, headPure)
	pure = append(pure, sepPure)
	pure = append(pure, body...)
	out := make([]string, len(pure))
	for i, p := range pure {
		switch i {
		case 0:
			idx := strings.Index(p, "—")
			if idx > 0 {
				out[i] = cBold(p[:idx] + "—" + cMagenta(p[idx+len("—"):]))
			} else {
				out[i] = cBold(cMagenta(p))
			}
		case 1:
			out[i] = p
		default:
			line := p
			if strings.HasPrefix(line, "Basic:") {
				line = cBold("Basic") + line[len("Basic"):]
			} else if strings.HasPrefix(line, "Extended:") {
				line = cBold("Extended") + line[len("Extended"):]
			} else if strings.HasPrefix(line, "Meta:") {
				line = cBold("Meta") + line[len("Meta"):]
			}
			out[i] = line
		}
	}
	return out
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
