package widgets

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/kcmvp/redisx/cmd/_shared/tui"
)

func PrintTable(out io.Writer, headers []string, rows [][]string) {
	tw := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	if len(headers) > 0 {
		for i, h := range headers {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			fmt.Fprint(tw, tui.Bold(h))
		}
		fmt.Fprintln(tw)
		sep := make([]string, len(headers))
		for i, h := range headers {
			sep[i] = strings.Repeat("─", len([]rune(h))+2)
		}
		fmt.Fprintln(tw, strings.Join(sep, "\t"))
	}
	for _, r := range rows {
		fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	_ = tw.Flush()
}
