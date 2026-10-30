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
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// SignalHelp lists the named signals Graph accepts besides metric names.
func SignalHelp() string {
	var sb strings.Builder
	for _, s := range signals {
		fmt.Fprintf(&sb, "  %-11s %s\n", s.name, s.help)
	}
	return sb.String()
}

// GraphOptions configures Graph.
type GraphOptions struct {
	// Name is a signal from SignalHelp or a Prometheus metric name.
	Name     string
	Interval time.Duration
	// Height is the chart height in rows; 0 fits the terminal.
	Height int
	// Quantile is charted for histogram metrics.
	Quantile float64
	// Labels filters metric series by k=v or k!=v.
	Labels []string
}

// Graph redraws a live chart of one signal or metric per sidecar until ctx
// is done.
func Graph(ctx context.Context, w io.Writer, o Options, g GraphOptions) error {
	targets, err := resolveTargets(ctx, o)
	if err != nil {
		return err
	}
	sig, ok := signalByName(g.Name)
	if !ok {
		sig = metricSignal(g.Name, g.Quantile, labelFilter(g.Labels))
	}

	pollers := make([]*poller, len(targets))
	histories := make([]*ring, len(targets))
	for i, t := range targets {
		pollers[i] = newPoller(t, 1)
		histories[i] = newRing(1000)
	}

	return redraw(ctx, w, g.Interval, func(buf *bytes.Buffer) {
		parallel(ctx, targets, func(ctx context.Context, t target) struct{} {
			for i, p := range pollers {
				if p.t != t {
					continue
				}
				p.poll(ctx)
				switch {
				case p.err != nil:
					histories[i].push(nan)
				case p.window != nil:
					histories[i].push(sig.value(p.window, p.cur, p.windowDuration().Seconds()))
				}
			}
			return struct{}{}
		})

		topHeader(buf, "dapr metrics graph "+g.Name, g.Interval)
		fmt.Fprintln(buf, dim("  "+sig.help))

		rows := g.Height
		if rows == 0 {
			rows = max((termHeight()-2)/len(targets)-5, 4)
		}
		width := termWidth() - 2
		for i, p := range pollers {
			h := histories[i]
			cur := "warming up…"
			if len(h.values) > 0 {
				cur = sig.format(h.last())
			}
			if p.err != nil {
				cur = red(shortErr(p.err))
			}
			fmt.Fprintf(buf, "\n %s  %s\n", bold(p.t.Name), cur)
			for _, line := range chart(h.values, width, rows, sig.format) {
				fmt.Fprintln(buf, " "+line)
			}
			span := time.Duration(min(len(h.values), width)) * g.Interval
			fmt.Fprintln(buf, dim(fmt.Sprintf("%*s", width, "-"+span.String()+" … now")))
		}
	})
}

// metricSignal charts an arbitrary metric family, picking the natural
// representation for its type.
func metricSignal(name string, q float64, match matcher) indicator {
	return indicator{
		name: name,
		help: name,
		format: func(v float64) string {
			if strings.HasSuffix(name, "_bytes") {
				return fmtBytes(v)
			}
			return fmtRate(v)
		},
		value: func(d, cur *snapshot, w float64) float64 {
			f := cur.families[name]
			if f == nil {
				return nan
			}
			switch f.typ {
			case dto.MetricType_COUNTER:
				return d.total(name, match) / w
			case dto.MetricType_HISTOGRAM:
				var b []bucket
				for _, g := range d.sumBy(name, nil, match) {
					b = mergeBuckets(b, g.buckets)
				}
				return quantile(q, b)
			default:
				return cur.total(name, match)
			}
		},
	}
}

// Query prints every series whose metric name matches pattern, optionally
// filtered by k=v or k!=v label selectors.
func Query(ctx context.Context, w io.Writer, o Options, pattern string, labels []string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex: %w", err)
	}
	targets, err := resolveTargets(ctx, o)
	if err != nil {
		return err
	}
	match := labelFilter(labels)
	results := scrapeAll(ctx, targets)

	tb := newTable(w, "APP ID", "METRIC", "LABELS", "VALUE")
	rows := 0
	for i, t := range targets {
		r := results[i]
		if r.err != nil {
			tb.row(t.Name, red(shortErr(r.err)))
			continue
		}
		names := make([]string, 0, len(r.snap.families))
		for n := range r.snap.families {
			if re.MatchString(n) {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		for _, n := range names {
			f := r.snap.families[n]
			keys := make([]string, 0, len(f.series))
			for k := range f.series {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				s := f.series[k]
				if !match(s.labels) {
					continue
				}
				rows++
				tb.row(t.Name, n, dim(fmtLabels(s.labels)), fmtSeriesValue(f.typ, s))
			}
		}
	}
	tb.flush()
	if rows == 0 {
		fmt.Fprintln(w, dim("no series matched"))
	}
	return nil
}

func fmtLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		if k != "app_id" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return truncate(strings.Join(parts, " "), 100)
}

func fmtSeriesValue(typ dto.MetricType, s *series) string {
	if typ != dto.MetricType_HISTOGRAM {
		return fmtCount(s.value)
	}
	if s.value == 0 {
		return "count=0"
	}
	return fmt.Sprintf("count=%s avg=%s p50=%s p99=%s", fmtCount(s.value), fmtCount(s.sum/s.value),
		fmtCount(quantile(0.5, s.buckets)), fmtCount(quantile(0.99, s.buckets)))
}
