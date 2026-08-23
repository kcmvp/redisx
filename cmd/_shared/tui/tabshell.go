package tui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type CmdEntry struct {
	Use   string
	Short string
	Run   func(args []string) (out string, err error)
}

type TabDef struct {
	Name    string
	Visible func() bool
	Entries []CmdEntry
}

type TabShell struct {
	tabs       []TabDef
	activeTab  int
	activeCmd  map[int]int
	output     map[int]map[int]string
	width      int
	height     int
	lastCmd    string
	lastOutput string
	cmdLine    string
	cursorPos  int
	scrollOff  int
	banner     string
}

func NewTabShell(tabs []TabDef) *TabShell {
	ts := &TabShell{tabs: tabs, activeCmd: map[int]int{}, output: map[int]map[int]string{}}
	for i, t := range tabs {
		if t.Visible != nil && !t.Visible() {
			continue
		}
		ts.activeTab = i
		break
	}
	return ts
}

func (m *TabShell) PrependBanner(s string) {
	m.banner = strings.TrimRight(s, "\n")
}

func (m *TabShell) Init() tea.Cmd { return nil }

func (m *TabShell) effectiveTabs() []TabDef {
	out := make([]TabDef, 0, len(m.tabs))
	for _, t := range m.tabs {
		if t.Visible == nil || t.Visible() {
			out = append(out, t)
		}
	}
	return out
}

func (m *TabShell) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch me := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = me.Width
		m.height = me.Height
	case tea.KeyMsg:
		switch me.Type {
		case tea.KeyCtrlC, tea.KeyEscape:
			return m, tea.Quit
		case tea.KeyTab:
			ts := m.effectiveTabs()
			if len(ts) > 0 {
				idx := 0
				for i, t := range ts {
					if t.Name == m.tabs[m.activeTab].Name {
						idx = i
						break
					}
				}
				idx = (idx + 1) % len(ts)
				for ai, t := range m.tabs {
					if t.Name == ts[idx].Name {
						m.activeTab = ai
						break
					}
				}
			}
		case tea.KeyShiftTab:
			ts := m.effectiveTabs()
			if len(ts) > 0 {
				idx := 0
				for i, t := range ts {
					if t.Name == m.tabs[m.activeTab].Name {
						idx = i
						break
					}
				}
				idx = (idx - 1 + len(ts)) % len(ts)
				for ai, t := range m.tabs {
					if t.Name == ts[idx].Name {
						m.activeTab = ai
						break
					}
				}
			}
		case tea.KeyUp, tea.KeyDown:
			es := m.tabs[m.activeTab].Entries
			if len(es) == 0 {
				return m, nil
			}
			cur := m.activeCmd[m.activeTab]
			if me.Type == tea.KeyUp {
				cur--
				if cur < 0 {
					cur = len(es) - 1
				}
			} else {
				cur++
				if cur >= len(es) {
					cur = 0
				}
			}
			m.activeCmd[m.activeTab] = cur
		case tea.KeyEnter:
			es := m.tabs[m.activeTab].Entries
			idx := m.activeCmd[m.activeTab]
			if len(es) != 0 && idx >= 0 && idx < len(es) {
				args, ok := shlexSplit(m.cmdLine)
				if !ok {
					m.cmdLine = strings.TrimRight(m.cmdLine, "\n")
					return m, nil
				}
				run := es[idx].Run
				if run != nil {
					out, err := run(args)
					if err != nil {
						if out != "" {
							out = out + "\n"
						}
						out = out + "(error) " + err.Error()
					}
					if m.output[m.activeTab] == nil {
						m.output[m.activeTab] = map[int]string{}
					}
					m.output[m.activeTab][idx] = out
					m.lastCmd = es[idx].Use
					if strings.TrimSpace(m.cmdLine) != "" {
						m.lastCmd += " " + strings.TrimSpace(m.cmdLine)
					}
					m.lastOutput = out
					m.cmdLine = ""
					m.cursorPos = 0
				}
			}
		case tea.KeyBackspace:
			r := []rune(m.cmdLine)
			if len(r) > 0 {
				if m.cursorPos <= 0 {
					m.cursorPos = len(r)
				}
				at := m.cursorPos - 1
				if at < 0 {
					at = 0
				}
				if at >= len(r) {
					at = len(r) - 1
				}
				r = append(r[:at], r[at+1:]...)
				m.cmdLine = string(r)
				m.cursorPos = at
			}
		case tea.KeyLeft:
			if m.cursorPos > 0 {
				m.cursorPos--
			}
		case tea.KeyRight:
			if m.cursorPos < len([]rune(m.cmdLine)) {
				m.cursorPos++
			}
		case tea.KeyHome:
			m.cursorPos = 0
		case tea.KeyEnd:
			m.cursorPos = len([]rune(m.cmdLine))
		case tea.KeyRunes:
			r := []rune(m.cmdLine)
			pos := m.cursorPos
			if pos < 0 || pos > len(r) {
				pos = len(r)
			}
			next := make([]rune, 0, len(r)+len(me.Runes))
			next = append(next, r[:pos]...)
			next = append(next, me.Runes...)
			next = append(next, r[pos:]...)
			m.cmdLine = string(next)
			m.cursorPos = pos + len(me.Runes)
		}
	}
	return m, nil
}

func shlexSplit(s string) ([]string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, true
	}
	out := make([]string, 0, 4)
	var cur strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inQuote != 0:
			if c == inQuote {
				inQuote = 0
				continue
			}
			if c == '\\' && i+1 < len(s) {
				n := s[i+1]
				switch n {
				case 'n':
					cur.WriteByte('\n')
				case 't':
					cur.WriteByte('\t')
				default:
					cur.WriteByte(n)
				}
				i++
				continue
			}
			cur.WriteByte(c)
		case c == '"' || c == '\'':
			inQuote = c
		case c == ' ' || c == '\t':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		case c == '\\' && i+1 < len(s):
			cur.WriteByte(s[i+1])
			i++
		default:
			cur.WriteByte(c)
		}
	}
	if inQuote != 0 {
		return nil, false
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out, true
}

func (m *TabShell) View() string {
	w := m.width
	if w <= 0 {
		w = 120
	}
	h := m.height
	if h <= 0 {
		h = 30
	}
	tabs := m.effectiveTabs()
	if len(tabs) == 0 {
		return "no tabs available — press Esc or Ctrl-C to quit.\n"
	}
	bar := renderTabBarSimple(tabs, m.tabs[m.activeTab].Name, w)
	innerW := w - 4
	tab := m.tabs[m.activeTab]
	avail := h - lipgloss.Height(bar) - 3
	if m.banner != "" {
		avail -= lipgloss.Height(m.banner) + 1
	}
	cmdH := max(8, avail/4*2)
	inpH := max(6, avail - cmdH - 2)
	_ = inpH
	availCmds := cmdH - 3
	listRows := renderCmdList(tab.Entries, m.activeCmd[m.activeTab], innerW-4, availCmds, &m.scrollOff)
	title := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("129")).Render(" Available commands")
	cmdInner := title + "\n" + strings.Repeat("─", innerW-4) + "\n" + strings.Join(listRows, "\n")
	cmdBoxStyle := lipgloss.NewStyle().Width(innerW).Height(cmdH).Border(lipgloss.RoundedBorder(), true).BorderForeground(lipgloss.Color("59")).Padding(0, 2).AlignVertical(lipgloss.Top)
	cmdBox := cmdBoxStyle.Render(cmdInner)

	inpTitle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("129")).Render(" Input Area / Workspace  —  last: ") + lipgloss.NewStyle().Foreground(lipgloss.Color("81")).Render(m.lastCmd)
	idx := m.activeCmd[m.activeTab]
	curOutput := m.lastOutput
	if len(tab.Entries) > 0 && idx >= 0 && idx < len(tab.Entries) && (strings.TrimSpace(curOutput) == "") {
		if m.output[m.activeTab] != nil && m.output[m.activeTab][idx] != "" {
			curOutput = m.output[m.activeTab][idx]
		} else {
			curOutput = "(" + tab.Entries[idx].Use + ") — not executed yet.\n  description: " + tab.Entries[idx].Short + "\n  usage: " + tab.Entries[idx].Use + "\n  ↓ type args below (if needed), then press Enter to run."
		}
	}
	outLines := splitLines(curOutput, innerW-4)
	availOut := inpH - 5
	if len(outLines) > availOut {
		outLines = outLines[len(outLines)-availOut:]
	}
	for len(outLines) < availOut {
		outLines = append(outLines, "")
	}
	outputBlock := strings.Join(outLines, "\n")

	cmdLineView := renderCmdLine(m.cmdLine, m.cursorPos, innerW-4)
	inpInner := inpTitle + "\n" + strings.Repeat("─", innerW-4) + "\n" + outputBlock + "\n" + strings.Repeat("─", innerW-4) + "\n  $ " + cmdLineView
	inpBoxStyle := lipgloss.NewStyle().Width(innerW).Height(inpH).Border(lipgloss.RoundedBorder(), true).BorderForeground(lipgloss.Color("59")).Padding(0, 2).AlignVertical(lipgloss.Top)
	inpBox := inpBoxStyle.Render(inpInner)

	hint := lipgloss.NewStyle().Foreground(lipgloss.Color("60")).Render("  Tab/Shift-Tab: switch tab   ↑/↓: pick command   Enter: run   any: edit args   ←/→/Home/End: move cursor   Backspace: erase   Esc/Ctrl-C: quit")

	body := bar + "\n" + cmdBox + "\n" + inpBox + "\n" + hint
	if m.banner != "" {
		return m.banner + "\n\n" + body
	}
	return body
}

func renderTabBarSimple(tabs []TabDef, active string, w int) string {
	accent := lipgloss.Color("99")
	muted := lipgloss.Color("244")
	bg := lipgloss.Color("234")
	var rendered []string
	for _, t := range tabs {
		isActive := t.Name == active
		st := lipgloss.NewStyle().Padding(0, 5).Border(lipgloss.RoundedBorder(), true)
		if isActive {
			st = st.BorderForeground(accent).Foreground(lipgloss.Color("255")).Background(bg).Bold(true)
		} else {
			st = st.BorderForeground(muted).Foreground(muted).Background(bg)
		}
		rendered = append(rendered, st.Render(t.Name))
	}
	joined := strings.Join(rendered, strings.Repeat(" ", max(2, w/40)))
	full := lipgloss.NewStyle().Padding(0, 3).Render(joined)
	return full
}

func renderCmdList(entries []CmdEntry, active int, w, rows int, off *int) []string {
	accent := lipgloss.Color("129")
	if *off < 0 {
		*off = 0
	}
	if *off > max(0, len(entries)-rows) {
		*off = max(0, len(entries)-rows)
	}
	if active >= (*off)+rows {
		*off = active - rows + 1
	}
	if active < *off {
		*off = active
	}
	out := make([]string, 0, rows)
	for i := *off; i < *off+rows && i < len(entries); i++ {
		e := entries[i]
		isActive := i == active
		var prefix string
		if isActive {
			prefix = "(●) "
		} else {
			prefix = "( ) "
		}
		useW := 22
		use := e.Use
		if len([]rune(use)) > useW {
			r := []rune(use)
			use = string(r[:useW-1]) + "…"
		}
		pad1 := strings.Repeat(" ", max(0, useW-len([]rune(use))))
		rest := max(10, w-len([]rune(prefix))-useW-3)
		short := e.Short
		if len([]rune(short)) > rest {
			r := []rune(short)
			short = string(r[:rest-1]) + "…"
		}
		pad2 := strings.Repeat(" ", max(0, rest-len([]rune(short))))
		line := prefix + use + pad1 + "  " + short + pad2
		if isActive {
			line = lipgloss.NewStyle().Foreground(accent).Bold(true).Render(line)
		} else {
			line = lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Render(line)
		}
		out = append(out, line)
	}
	for len(out) < rows {
		out = append(out, strings.Repeat(" ", w))
	}
	return out
}

func renderCmdLine(s string, pos int, w int) string {
	cursor := lipgloss.NewStyle().Background(lipgloss.Color("129")).Foreground(lipgloss.Color("232")).Render(" ")
	r := []rune(s)
	if pos < 0 || pos > len(r) {
		pos = len(r)
	}
	left := string(r[:pos])
	ch := ""
	right := ""
	if pos < len(r) {
		ch = string(r[pos])
		right = string(r[pos+1:])
	}
	if ch == "" {
		ch = " "
	} else {
		cursor = lipgloss.NewStyle().Background(lipgloss.Color("129")).Foreground(lipgloss.Color("232")).Render(ch)
		ch = ""
	}
	view := left + cursor + ch + right
	total := len([]rune(left)) + 1 + len([]rune(ch)) + len([]rune(right))
	if total > w {
		rr := []rune(view)
		if len(rr) > w {
			start := len(rr) - w
			if start < 0 {
				start = 0
			}
			view = string(rr[start:])
		}
	} else {
		view += strings.Repeat(" ", w-total)
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color("230")).Render(view)
}

func stripANSI(s string) string {
	var b strings.Builder
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
			b.WriteRune(r)
		}
	}
	return b.String()
}

func splitLines(s string, w int) []string {
	if w <= 0 {
		w = 80
	}
	out := make([]string, 0, strings.Count(s, "\n")+1)
	for _, raw := range strings.Split(s, "\n") {
		if raw == "" {
			out = append(out, "")
			continue
		}
		for len([]rune(raw)) > w {
			runes := []rune(raw)
			out = append(out, string(runes[:w]))
			raw = string(runes[w:])
		}
		if raw != "" || len(out) > 0 {
			out = append(out, raw)
		}
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
