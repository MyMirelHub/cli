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
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// indicator is a derived per-window value shown in top and available to graph
// by name, so `graph rps` works without knowing Dapr's metric names.
type indicator struct {
	name   string
	help   string
	format func(float64) string
	// value computes the signal from the window delta and the current
	// totals. window is the delta's duration in seconds.
	value func(d, cur *snapshot, window float64) float64
}

func notNoise(l map[string]string) bool { return !isNoisePath(l) }

func inboundCount(s *snapshot, m matcher) float64 {
	return s.total("dapr_http_server_request_count", and(notNoise, m)) + s.total("dapr_grpc_io_server_completed_rpcs", m)
}

func inboundErrors(s *snapshot) float64 {
	return s.total("dapr_http_server_request_count", and(notNoise, isHTTPError)) +
		s.total("dapr_grpc_io_server_completed_rpcs", isGRPCError("grpc_server_status"))
}

func inboundLatency(s *snapshot) []bucket {
	var b []bucket
	for _, g := range s.sumBy("dapr_http_server_latency", nil, notNoise) {
		b = mergeBuckets(b, g.buckets)
	}
	for _, g := range s.sumBy("dapr_grpc_io_server_server_latency", nil, nil) {
		b = mergeBuckets(b, g.buckets)
	}
	return b
}

func ratio(part, whole float64) float64 {
	if whole == 0 {
		return math.NaN()
	}
	return 100 * part / whole
}

func fmtPercent(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", v)
}

var signals = []indicator{
	{"rps", "Dapr API requests per second (HTTP + gRPC, excluding healthz/metadata)", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return inboundCount(d, nil) / w
	}},
	{"errors", "Dapr API 5xx / gRPC server errors as a % of requests", fmtPercent, func(d, _ *snapshot, _ float64) float64 {
		return ratio(inboundErrors(d), inboundCount(d, nil))
	}},
	{"p50", "Dapr API median latency", fmtMs, func(d, _ *snapshot, _ float64) float64 { return quantile(0.5, inboundLatency(d)) }},
	{"p99", "Dapr API p99 latency", fmtMs, func(d, _ *snapshot, _ float64) float64 { return quantile(0.99, inboundLatency(d)) }},
	{"app-rps", "sidecar → app calls per second (invocations, deliveries, actor calls)", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return (d.total("dapr_http_client_completed_count", nil) + d.total("dapr_grpc_io_client_completed_rpcs", nil)) / w
	}},
	{"app-errors", "sidecar → app 5xx / gRPC errors as a % of calls", fmtPercent, func(d, _ *snapshot, _ float64) float64 {
		calls := d.total("dapr_http_client_completed_count", nil) + d.total("dapr_grpc_io_client_completed_rpcs", nil)
		errs := d.total("dapr_http_client_completed_count", isHTTPError) + d.total("dapr_grpc_io_client_completed_rpcs", isGRPCError("grpc_client_status"))
		return ratio(errs, calls)
	}},
	{"pub", "messages published per second", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return d.total("dapr_component_pubsub_egress_count", nil) / w
	}},
	{"sub", "messages delivered to the app per second", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return d.total("dapr_component_pubsub_ingress_count", nil) / w
	}},
	{"state", "state store operations per second", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return d.total("dapr_component_state_count", nil) / w
	}},
	{"workflows", "workflow executions per second", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return d.total("dapr_runtime_workflow_execution_count", nil) / w
	}},
	{"pending", "pending actor calls", fmtCount, func(_, c *snapshot, _ float64) float64 {
		return c.total("dapr_runtime_actor_pending_actor_calls", nil)
	}},
	{"goroutines", "sidecar goroutines", fmtCount, func(_, c *snapshot, _ float64) float64 { return c.total("go_goroutines", nil) }},
	{"heap", "sidecar heap in use", fmtBytes, func(_, c *snapshot, _ float64) float64 { return c.total("go_memstats_heap_inuse_bytes", nil) }},
	{"cpu", "sidecar CPU, % of one core", fmtPercent, func(d, _ *snapshot, w float64) float64 {
		return 100 * d.total("process_cpu_seconds_total", nil) / w
	}},
}

func allIndicators() []indicator {
	return append(append([]indicator{}, signals...), controlPlaneIndicators...)
}

func signalByName(name string) (indicator, bool) {
	for _, s := range allIndicators() {
		if s.name == name {
			return s, true
		}
	}
	return indicator{}, false
}

// poller scrapes a target on an interval and keeps a history per signal.
type poller struct {
	t       target
	prev    *snapshot
	cur     *snapshot
	window  *snapshot
	healthy healthResult
	err     error
	history map[string]*ring
}

func newPoller(t target, size int) *poller {
	p := &poller{t: t, history: map[string]*ring{}}
	for _, s := range allIndicators() {
		p.history[s.name] = newRing(size)
	}
	return p
}

func (p *poller) poll(ctx context.Context) {
	switch {
	case p.t.isSidecar():
		p.healthy = checkHealth(ctx, p.t.HTTPAddr, "/v1.0/healthz")
	case p.t.HealthAddr != "":
		p.healthy = checkHealth(ctx, p.t.HealthAddr, "/healthz")
	}
	if p.t.MetricsURL == "" {
		p.err = errors.New("metrics disabled")
		return
	}
	snap, err := scrape(ctx, p.t.MetricsURL)
	if err != nil {
		p.err = err
		for _, r := range p.history {
			r.push(math.NaN())
		}
		return
	}
	p.err = nil
	p.prev, p.cur = p.cur, snap
	if p.prev == nil {
		return
	}
	p.window = delta(p.cur, p.prev)
	w := p.cur.at.Sub(p.prev.at).Seconds()
	for _, s := range allIndicators() {
		p.history[s.name].push(s.value(p.window, p.cur, w))
	}
}

func (p *poller) windowDuration() time.Duration {
	if p.prev == nil {
		return 0
	}
	return p.cur.at.Sub(p.prev.at)
}

// TopOptions configures Top.
type TopOptions struct {
	Interval time.Duration
	// Detail shows the full dashboard even with several sidecars.
	Detail bool
	// Limit caps the rows per table in the detailed view.
	Limit int
}

// Top redraws a live per-second view of the sidecars until ctx is done.
func Top(ctx context.Context, w io.Writer, o Options, to TopOptions) error {
	targets, err := resolveTargets(ctx, o)
	if err != nil {
		return err
	}
	detail := to.Detail || len(targets) == 1
	so := SummaryOptions{Limit: to.Limit}

	pollers := make([]*poller, len(targets))
	for i, t := range targets {
		pollers[i] = newPoller(t, 240)
	}

	return redraw(ctx, w, to.Interval, func(buf *bytes.Buffer) {
		parallel(ctx, targets, func(ctx context.Context, t target) struct{} {
			for _, p := range pollers {
				if p.t == t {
					p.poll(ctx)
				}
			}
			return struct{}{}
		})
		if detail {
			for _, p := range pollers {
				renderDetail(buf, p, to.Interval, so)
			}
		} else {
			renderOverview(buf, pollers, to.Interval)
		}
	})
}

// redraw calls frame every interval and replaces the screen with its output.
func redraw(ctx context.Context, w io.Writer, interval time.Duration, frame func(*bytes.Buffer)) error {
	fmt.Fprint(w, hideCursor)
	defer fmt.Fprint(w, showCursor)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		var buf bytes.Buffer
		frame(&buf)
		fmt.Fprint(w, clearScreen)
		if _, err := w.Write(buf.Bytes()); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			fmt.Fprintln(w)
			return nil
		case <-ticker.C:
		}
	}
}

func topHeader(w io.Writer, title string, interval time.Duration) {
	right := dim(fmt.Sprintf("every %s · %s · ctrl-c to quit", interval, time.Now().Format("15:04:05")))
	fmt.Fprintf(w, "%s  %s\n", bold(title), right)
}

func healthCell(p *poller) string {
	switch {
	case !p.t.isSidecar() && p.t.HealthAddr == "":
		if p.err != nil {
			return red("✘ down")
		}
		return green("✔ up")
	case p.healthy.ok():
		return green("✔ ok")
	case p.healthy.Err != "":
		return red("✘ down")
	default:
		return yellow(fmt.Sprintf("▲ %d", p.healthy.Code))
	}
}

func renderOverview(w io.Writer, pollers []*poller, interval time.Duration) {
	topHeader(w, "dapr top", interval)
	fmt.Fprintln(w)
	var sidecars, controlPlane []*poller
	for _, p := range pollers {
		if p.t.isSidecar() {
			sidecars = append(sidecars, p)
		} else {
			controlPlane = append(controlPlane, p)
		}
	}
	if len(sidecars) > 0 {
		renderSidecarOverview(w, sidecars)
	}
	if len(controlPlane) > 0 {
		if len(sidecars) > 0 {
			fmt.Fprintln(w)
		}
		renderControlPlaneOverview(w, controlPlane)
	}
	fmt.Fprintln(w, dim("\n  dapr top -a <app-id> for a dashboard · dapr metrics graph <signal> -a <app-id> to chart one"))
}

func renderSidecarOverview(w io.Writer, pollers []*poller) {
	trendW := max(termWidth()-110, 10)
	tb := newTable(w, "APP ID", "HEALTH", "REQ/S", "ERR", "P99", "APP/S", "PUB/S", "SUB/S", "CPU", "HEAP", "GOROUTINES", "REQ/S TREND")
	for _, p := range pollers {
		if p.window == nil {
			msg := dim("warming up…")
			if p.err != nil {
				msg = red(shortErr(p.err))
			}
			tb.row(p.t.Name, healthCell(p), msg)
			continue
		}
		h := func(name string) float64 { return p.history[name].last() }
		tb.row(
			p.t.Name, healthCell(p),
			fmtRate(h("rps")), errCell(h("errors")), latencyCell(h("p99")),
			fmtRate(h("app-rps")), fmtRate(h("pub")), fmtRate(h("sub")),
			fmtPercent(h("cpu")), fmtBytes(h("heap")), fmtCount(h("goroutines")),
			cyan(sparkline(p.history["rps"].values, trendW)),
		)
	}
	tb.flush()
}

func errCell(pct float64) string {
	s := fmtPercent(pct)
	switch {
	case math.IsNaN(pct) || pct == 0:
		return s
	case pct < 1:
		return yellow(s)
	default:
		return red(s)
	}
}

var detailSignals = []string{"rps", "errors", "p99", "app-rps", "app-errors", "pub", "sub", "state", "pending", "cpu", "heap", "goroutines"}

func renderDetail(w io.Writer, p *poller, interval time.Duration, so SummaryOptions) {
	topHeader(w, p.t.Name+"  "+healthCell(p), interval)
	if p.err != nil {
		fmt.Fprintln(w, red("  "+shortErr(p.err)))
	}
	if p.window == nil {
		fmt.Fprintln(w, dim("  warming up…"))
		return
	}

	sparkW := max(termWidth()-32, 10)
	fmt.Fprintln(w)
	names := detailSignals
	if !p.t.isSidecar() {
		names = controlPlaneDetailSignals
	}
	for _, name := range names {
		s, _ := signalByName(name)
		r := p.history[name]
		if allZeroOrNaN(r.values) && name != "rps" {
			continue
		}
		fmt.Fprintf(w, "  %-13s %10s  %s\n", dim(name), s.format(r.last()), cyan(sparkline(r.values, sparkW)))
	}

	so.window = p.windowDuration()
	var buf bytes.Buffer
	printSummary(&buf, p.window, so)
	// The summary ends with the process line, which top already shows.
	out := strings.TrimRight(buf.String(), "\n")
	if i := strings.LastIndex(out, "\n\n"); i >= 0 && strings.Contains(out[i:], "process") {
		out = out[:i]
	}
	fmt.Fprintln(w, out)
	fmt.Fprintln(w)
}

func allZeroOrNaN(vs []float64) bool {
	for _, v := range vs {
		if !math.IsNaN(v) && v != 0 {
			return false
		}
	}
	return true
}
