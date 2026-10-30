/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package monitor

import (
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fatih/color"
	"golang.org/x/term"
)

func bold(s string) string   { return color.New(color.Bold).Sprint(s) }
func dim(s string) string    { return color.New(color.Faint).Sprint(s) }
func red(s string) string    { return color.New(color.FgRed).Sprint(s) }
func green(s string) string  { return color.New(color.FgGreen).Sprint(s) }
func yellow(s string) string { return color.New(color.FgYellow).Sprint(s) }
func cyan(s string) string   { return color.New(color.FgCyan).Sprint(s) }

type level int

const (
	levelOK level = iota
	levelWarn
	levelFail
)

func (l level) String() string {
	switch l {
	case levelOK:
		return "ok"
	case levelWarn:
		return "warn"
	default:
		return "fail"
	}
}

func (l level) badge() string {
	switch l {
	case levelOK:
		return green("✔ ok  ")
	case levelWarn:
		return yellow("▲ warn")
	default:
		return red("✘ fail")
	}
}

func (l level) MarshalText() ([]byte, error) { return []byte(l.String()), nil }

func termWidth() int {
	if w, _, err := term.GetSize(stdoutFd()); err == nil && w > 20 {
		return w
	}
	if w, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && w > 20 {
		return w
	}
	return 100
}

// table writes aligned columns. It measures cells without their ANSI
// escapes, which text/tabwriter would count as width.
type table struct {
	w    io.Writer
	rows [][]string
}

func newTable(w io.Writer, header ...string) *table {
	t := &table{w: w}
	if len(header) > 0 {
		t.row(dimAll(header)...)
	}
	return t
}

func dimAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = dim(s)
	}
	return out
}

func (t *table) row(cells ...string) { t.rows = append(t.rows, cells) }

func (t *table) flush() {
	var widths []int
	for _, r := range t.rows {
		for i, c := range r {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], visibleLen(c))
		}
	}
	for _, r := range t.rows {
		var sb strings.Builder
		for i, c := range r {
			sb.WriteString(c)
			if i < len(r)-1 {
				sb.WriteString(strings.Repeat(" ", widths[i]-visibleLen(c)+2))
			}
		}
		fmt.Fprintln(t.w, strings.TrimRight(sb.String(), " "))
	}
	t.rows = nil
}

func visibleLen(s string) int {
	n, inEsc := 0, false
	for _, r := range s {
		switch {
		case r == '\x1b':
			inEsc = true
		case inEsc:
			if r == 'm' {
				inEsc = false
			}
		default:
			n++
		}
	}
	return n
}

func section(w io.Writer, title string) {
	fmt.Fprintf(w, "\n%s\n", bold(cyan(title)))
}

func fmtCount(v float64) string {
	switch {
	case math.IsNaN(v):
		return "-"
	case v >= 1e9:
		return fmt.Sprintf("%.1fG", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case v >= 1e4:
		return fmt.Sprintf("%.1fk", v/1e3)
	case v == math.Trunc(v):
		return strconv.FormatFloat(v, 'f', 0, 64)
	default:
		return strconv.FormatFloat(v, 'f', 2, 64)
	}
}

func fmtRate(v float64) string {
	switch {
	case math.IsNaN(v):
		return "-"
	case v == 0:
		return "0"
	case v < 0.01:
		return "<0.01"
	case v < 10:
		return strconv.FormatFloat(v, 'f', 2, 64)
	default:
		return fmtCount(math.Round(v))
	}
}

func fmtMs(v float64) string {
	switch {
	case math.IsNaN(v):
		return "-"
	case v >= 1000:
		return strconv.FormatFloat(v/1000, 'f', 2, 64) + "s"
	case v >= 10:
		return strconv.FormatFloat(v, 'f', 0, 64) + "ms"
	default:
		return strconv.FormatFloat(v, 'f', 1, 64) + "ms"
	}
}

func fmtBytes(v float64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%.0fB", v)
	}
	div, exp := float64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", v/div, "KMGTPE"[exp])
}

func fmtPct(part, whole float64) string {
	if whole == 0 {
		return "-"
	}
	p := 100 * part / whole
	s := strconv.FormatFloat(p, 'f', 1, 64) + "%"
	switch {
	case p == 0:
		return s
	case p < 1:
		return yellow(s)
	default:
		return red(s)
	}
}

func fmtDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return d.Round(time.Second).String()
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n || n < 2 {
		return s
	}
	r := []rune(s)
	return string(r[:n-1]) + "…"
}

var sparkRunes = []rune("▁▂▃▄▅▆▇█")

// sparkline renders values as a single row of block characters scaled to
// the window's own max, padded on the left to width.
func sparkline(values []float64, width int) string {
	if len(values) > width {
		values = values[len(values)-width:]
	}
	maxV := 0.0
	for _, v := range values {
		if !math.IsNaN(v) && v > maxV {
			maxV = v
		}
	}
	var sb strings.Builder
	sb.WriteString(strings.Repeat(" ", width-len(values)))
	for _, v := range values {
		switch {
		case math.IsNaN(v):
			sb.WriteRune(' ')
		case maxV == 0:
			sb.WriteRune(sparkRunes[0])
		default:
			i := int(math.Round(v / maxV * float64(len(sparkRunes)-1)))
			sb.WriteRune(sparkRunes[i])
		}
	}
	return sb.String()
}

// chart renders a multi-row bar chart of values, newest on the right, with a
// y axis on the left. Partial blocks give sub-row resolution.
func chart(values []float64, width, height int, format func(float64) string) []string {
	if height < 2 {
		height = 2
	}
	maxV := 0.0
	for _, v := range values {
		if !math.IsNaN(v) && v > maxV {
			maxV = v
		}
	}
	if maxV == 0 {
		maxV = 1
	}

	labels := []string{format(maxV), format(maxV / 2), format(0)}
	axisW := 0
	for _, l := range labels {
		axisW = max(axisW, utf8.RuneCountInString(l))
	}
	plotW := max(width-axisW-2, 10)
	if len(values) > plotW {
		values = values[len(values)-plotW:]
	}
	pad := plotW - len(values)

	rows := make([]string, 0, height+1)
	for r := range height {
		rowTop := maxV * float64(height-r) / float64(height)
		rowBottom := maxV * float64(height-r-1) / float64(height)

		label := ""
		switch r {
		case 0:
			label = labels[0]
		case height / 2:
			label = labels[1]
		case height - 1:
			label = labels[2]
		}

		var sb strings.Builder
		sb.WriteString(strings.Repeat(" ", axisW-utf8.RuneCountInString(label)))
		sb.WriteString(dim(label))
		sb.WriteString(dim(" ┤"))
		sb.WriteString(strings.Repeat(" ", pad))
		for _, v := range values {
			switch {
			case math.IsNaN(v) || v <= rowBottom:
				if r == height-1 && !math.IsNaN(v) {
					sb.WriteRune('▁')
				} else {
					sb.WriteRune(' ')
				}
			case v >= rowTop:
				sb.WriteRune('█')
			default:
				frac := (v - rowBottom) / (rowTop - rowBottom)
				i := max(int(frac*float64(len(sparkRunes)))-1, 0)
				sb.WriteRune(sparkRunes[min(i, len(sparkRunes)-1)])
			}
		}
		rows = append(rows, cyan(sb.String()))
	}
	rows = append(rows, strings.Repeat(" ", axisW+1)+dim("└"+strings.Repeat("─", plotW)))
	return rows
}

// ring is a fixed-size history of samples for sparklines and charts.
type ring struct {
	values []float64
	size   int
}

func newRing(size int) *ring { return &ring{size: size} }

func (r *ring) push(v float64) {
	r.values = append(r.values, v)
	if len(r.values) > r.size {
		r.values = r.values[len(r.values)-r.size:]
	}
}

func (r *ring) last() float64 {
	if len(r.values) == 0 {
		return math.NaN()
	}
	return r.values[len(r.values)-1]
}

const (
	clearScreen = "\x1b[H\x1b[2J"
	hideCursor  = "\x1b[?25l"
	showCursor  = "\x1b[?25h"
)

var nan = math.NaN()

func termHeight() int {
	if _, h, err := term.GetSize(stdoutFd()); err == nil && h > 5 {
		return h
	}
	if h, err := strconv.Atoi(os.Getenv("LINES")); err == nil && h > 5 {
		return h
	}
	return 30
}

func stdoutFd() int {
	return int(os.Stdout.Fd()) //nolint:gosec // file descriptors fit in an int
}
