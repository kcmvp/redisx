package tui

func Bold(s string) string    { return "\x1b[1m" + s + "\x1b[0m" }
func Cyan(s string) string    { return "\x1b[36m" + s + "\x1b[0m" }
func Yellow(s string) string  { return "\x1b[33m" + s + "\x1b[0m" }
func Green(s string) string   { return "\x1b[32m" + s + "\x1b[0m" }
func Red(s string) string     { return "\x1b[31m" + s + "\x1b[0m" }
func Magenta(s string) string { return "\x1b[35m" + s + "\x1b[0m" }
func Dim(s string) string     { return "\x1b[2m" + s + "\x1b[0m" }

func TruncateMiddle(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	head := (n - 3) / 2
	tail := (n - 3) - head
	return s[:head] + "..." + s[len(s)-tail:]
}
