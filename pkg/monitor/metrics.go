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
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// view is one table of the summary: a counter grouped by labels, with an
// optional latency histogram that shares the same labels and a matcher for
// the series that count as errors.
type view struct {
	title   string
	count   string
	latency string
	by      []string
	headers []string
	errors  matcher
	noise   matcher
}

var views = []view{
	{
		title:   "Dapr API · HTTP (app → sidecar)",
		count:   "dapr_http_server_request_count",
		latency: "dapr_http_server_latency",
		by:      []string{"method", "path"},
		headers: []string{"METHOD", "PATH"},
		errors:  isHTTPError,
		noise:   isNoisePath,
	},
	{
		title:   "Dapr API · gRPC (app → sidecar)",
		count:   "dapr_grpc_io_server_completed_rpcs",
		latency: "dapr_grpc_io_server_server_latency",
		by:      []string{"grpc_server_method"},
		headers: []string{"METHOD"},
		errors:  isGRPCError("grpc_server_status"),
	},
	{
		title:   "App channel · HTTP (sidecar → app)",
		count:   "dapr_http_client_completed_count",
		latency: "dapr_http_client_roundtrip_latency",
		by:      []string{"method", "path"},
		headers: []string{"METHOD", "PATH"},
		errors:  isHTTPError,
	},
	{
		title:   "App channel · gRPC (sidecar → app)",
		count:   "dapr_grpc_io_client_completed_rpcs",
		latency: "dapr_grpc_io_client_roundtrip_latency",
		by:      []string{"grpc_client_method"},
		headers: []string{"METHOD"},
		errors:  isGRPCError("grpc_client_status"),
	},
	{
		title:   "Service invocation · outbound",
		count:   "dapr_runtime_service_invocation_res_recv_total",
		latency: "dapr_runtime_service_invocation_res_recv_latency_ms",
		by:      []string{"src_app_id"},
		headers: []string{"TARGET APP"},
		errors:  isHTTPOrGRPCStatusError,
	},
	{
		title:   "Pub/sub · published",
		count:   "dapr_component_pubsub_egress_count",
		latency: "dapr_component_pubsub_egress_latencies",
		by:      []string{"component", "topic"},
		headers: []string{"PUBSUB", "TOPIC"},
		errors:  labelEq("success", "false"),
	},
	{
		title:   "Pub/sub · delivered to app",
		count:   "dapr_component_pubsub_ingress_count",
		latency: "dapr_component_pubsub_ingress_latencies",
		by:      []string{"component", "topic"},
		headers: []string{"PUBSUB", "TOPIC"},
		errors:  labelNot("process_status", "success"),
	},
	{
		title:   "State",
		count:   "dapr_component_state_count",
		latency: "dapr_component_state_latencies",
		by:      []string{"component", "operation"},
		headers: []string{"STORE", "OPERATION"},
		errors:  labelEq("success", "false"),
	},
	{
		title:   "Bindings · output",
		count:   "dapr_component_output_binding_count",
		latency: "dapr_component_output_binding_latencies",
		by:      []string{"component", "operation"},
		headers: []string{"BINDING", "OPERATION"},
		errors:  labelEq("success", "false"),
	},
	{
		title:   "Bindings · input",
		count:   "dapr_component_input_binding_count",
		latency: "dapr_component_input_binding_latencies",
		by:      []string{"component"},
		headers: []string{"BINDING"},
		errors:  labelEq("success", "false"),
	},
	{
		title:   "Secrets",
		count:   "dapr_component_secret_count",
		latency: "dapr_component_secret_latencies",
		by:      []string{"component", "operation"},
		headers: []string{"STORE", "OPERATION"},
		errors:  labelEq("success", "false"),
	},
	{
		title:   "Configuration",
		count:   "dapr_component_configuration_count",
		latency: "dapr_component_configuration_latencies",
		by:      []string{"component", "operation"},
		headers: []string{"STORE", "OPERATION"},
		errors:  labelEq("success", "false"),
	},
	{
		title:   "Conversation",
		count:   "dapr_component_conversation_count",
		latency: "dapr_component_conversation_latencies",
		by:      []string{"component"},
		headers: []string{"COMPONENT"},
		errors:  labelEq("success", "false"),
	},
	{
		title:   "Workflows",
		count:   "dapr_runtime_workflow_execution_count",
		latency: "dapr_runtime_workflow_execution_latency",
		by:      []string{"workflow_name"},
		headers: []string{"WORKFLOW"},
		errors:  labelEq("status", "failed"),
	},
	{
		title:   "Workflow activities",
		count:   "dapr_runtime_workflow_activity_execution_count",
		latency: "dapr_runtime_workflow_activity_execution_latency",
		by:      []string{"activity_name"},
		headers: []string{"ACTIVITY"},
		errors:  labelEq("status", "failed"),
	},
	{
		title:   "Jobs",
		count:   "dapr_component_job_success_count",
		latency: "dapr_component_job_latencies",
		by:      []string{"operation"},
		headers: []string{"OPERATION"},
	},
}

func isHTTPOrGRPCStatusError(l map[string]string) bool {
	if statusClass(l["status"]) == '5' {
		return true
	}
	return isGRPCError("status")(l)
}

// SummaryOptions configures Metrics.
type SummaryOptions struct {
	// All includes the Dapr CLI's own metadata and healthz calls.
	All bool
	// Limit caps the rows per table; 0 means no limit.
	Limit int

	window time.Duration // zero for totals since start
}

// Metrics prints a summary of every sidecar's metrics since it started.
func Metrics(ctx context.Context, w io.Writer, o Options, so SummaryOptions) error {
	targets, err := resolveTargets(ctx, o)
	if err != nil {
		return err
	}
	results := scrapeAll(ctx, targets)
	for i, t := range targets {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, bold("━━ "+t.Name+" "+strings.Repeat("━", max(termWidth()-len(t.Name)-5, 3))))
		if err := results[i].err; err != nil {
			fmt.Fprintln(w, red("  "+shortErr(err)))
			continue
		}
		printSummary(w, results[i].snap, so)
	}
	return nil
}

func printSummary(w io.Writer, s *snapshot, o SummaryOptions) {
	printed := printControlPlane(w, s, o)
	countHeader := "COUNT"
	if o.window > 0 {
		countHeader = "REQ/S"
	}
	for _, v := range views {
		match := matchAll
		if v.noise != nil && !o.All {
			noise := v.noise
			match = func(l map[string]string) bool { return !noise(l) }
		}
		groups := s.sumBy(v.count, v.by, match)
		if o.window > 0 {
			groups = nonZero(groups)
		}
		if len(groups) == 0 {
			continue
		}
		printed++

		section(w, v.title)
		header := append(append([]string{}, v.headers...), countHeader, "ERR", "P50", "P95", "P99")
		tb := newTable(w, append([]string{" "}, header...)...)

		latency := map[string]*group{}
		for _, g := range s.sumBy(v.latency, v.by, match) {
			latency[labelKey(g.labels)] = g
		}
		var errs map[string]float64
		if v.errors != nil {
			errs = map[string]float64{}
			for _, g := range s.sumBy(v.count, v.by, and(match, v.errors)) {
				errs[labelKey(g.labels)] = g.value
			}
		}

		hidden := 0
		if o.Limit > 0 && len(groups) > o.Limit {
			hidden = len(groups) - o.Limit
			groups = groups[:o.Limit]
		}
		for _, g := range groups {
			key := labelKey(g.labels)
			cells := []string{" "}
			for j, l := range v.by {
				width := 28
				if j == len(v.by)-1 {
					width = 60
				}
				cells = append(cells, truncate(dash(g.labels[l]), width))
			}
			cells = append(cells, rateOrCount(g.value, o.window))
			if errs != nil {
				cells = append(cells, fmtPct(errs[key], g.value))
			} else {
				cells = append(cells, "-")
			}
			var b []bucket
			if lg := latency[key]; lg != nil {
				b = lg.buckets
			}
			cells = append(cells, fmtMs(quantile(0.5, b)), fmtMs(quantile(0.95, b)), latencyCell(quantile(0.99, b)))
			tb.row(cells...)
		}
		tb.flush()
		if hidden > 0 {
			fmt.Fprintln(w, dim(fmt.Sprintf("    … %d more, use -n 0", hidden)))
		}
	}

	printed += printActors(w, s)
	printed += printResiliency(w, s, o)
	printed += printErrorCodes(w, s, o)

	if printed == 0 {
		if o.window > 0 {
			fmt.Fprintln(w, dim("\n  no Dapr traffic in this window"))
		} else {
			fmt.Fprintln(w, dim("\n  no Dapr traffic yet, only process metrics"))
		}
	}
	printProcess(w, s)
}

func nonZero(groups []*group) []*group {
	out := groups[:0:0]
	for _, g := range groups {
		if g.value > 0 {
			out = append(out, g)
		}
	}
	return out
}

func rateOrCount(v float64, window time.Duration) string {
	if window > 0 {
		return fmtRate(v / window.Seconds())
	}
	return fmtCount(v)
}

func latencyCell(ms float64) string {
	s := fmtMs(ms)
	switch {
	case math.IsNaN(ms):
		return s
	case ms >= 1000:
		return red(s)
	case ms >= 250:
		return yellow(s)
	default:
		return s
	}
}

func printActors(w io.Writer, s *snapshot) int {
	types := map[string]bool{}
	for _, name := range []string{"dapr_runtime_actor_pending_actor_calls", "dapr_runtime_actor_timers", "dapr_runtime_actor_reminders", "dapr_runtime_actor_reminders_fired_total"} {
		for _, g := range s.sumBy(name, []string{"actor_type"}, nil) {
			if g.value > 0 {
				types[g.labels["actor_type"]] = true
			}
		}
	}
	if len(types) == 0 {
		return 0
	}
	section(w, "Actors")
	tb := newTable(w, " ", "ACTOR TYPE", "PENDING CALLS", "TIMERS", "REMINDERS", "REMINDERS FIRED", "DEACTIVATED")
	for _, g := range s.sumBy("dapr_runtime_actor_timers", []string{"actor_type"}, nil) {
		at := g.labels["actor_type"]
		if !types[at] {
			continue
		}
		delete(types, at)
		tb.row(actorRow(s, at)...)
	}
	for at := range types {
		tb.row(actorRow(s, at)...)
	}
	tb.flush()
	return 1
}

func actorRow(s *snapshot, at string) []string {
	m := labelEq("actor_type", at)
	pending := s.total("dapr_runtime_actor_pending_actor_calls", m)
	pendingCell := fmtCount(pending)
	if pending > 0 {
		pendingCell = yellow(pendingCell)
	}
	return []string{
		" ", at, pendingCell,
		fmtCount(s.total("dapr_runtime_actor_timers", m)),
		fmtCount(s.total("dapr_runtime_actor_reminders", m)),
		fmtCount(s.total("dapr_runtime_actor_reminders_fired_total", m)),
		fmtCount(s.total("dapr_runtime_actor_deactivated_total", m)),
	}
}

func printResiliency(w io.Writer, s *snapshot, o SummaryOptions) int {
	groups := s.sumBy("dapr_resiliency_activations_total", []string{"name", "policy", "target", "status"}, nil)
	if o.window > 0 {
		groups = nonZero(groups)
	}
	if len(groups) == 0 {
		return 0
	}
	section(w, "Resiliency activations")
	tb := newTable(w, " ", "POLICY", "TYPE", "TARGET", "STATUS", "COUNT")
	for _, g := range groups {
		tb.row(" ", g.labels["name"], g.labels["policy"], truncate(g.labels["target"], 40), dash(g.labels["status"]), rateOrCount(g.value, o.window))
	}
	tb.flush()
	return 1
}

func printErrorCodes(w io.Writer, s *snapshot, o SummaryOptions) int {
	groups := s.sumBy("dapr_error_code_total", []string{"error_code", "category"}, nil)
	if o.window > 0 {
		groups = nonZero(groups)
	}
	if len(groups) == 0 {
		return 0
	}
	section(w, "Dapr error codes")
	tb := newTable(w, " ", "CODE", "CATEGORY", "COUNT")
	for _, g := range groups {
		tb.row(" ", red(g.labels["error_code"]), g.labels["category"], rateOrCount(g.value, o.window))
	}
	tb.flush()
	return 1
}

func printProcess(w io.Writer, s *snapshot) {
	parts := []string{}
	if start := s.total("process_start_time_seconds", nil); start > 0 {
		parts = append(parts, "up "+fmtDuration(time.Since(time.Unix(int64(start), 0))))
	}
	if s.has("go_goroutines") {
		parts = append(parts, fmtCount(s.total("go_goroutines", nil))+" goroutines")
	}
	if s.has("go_memstats_heap_inuse_bytes") {
		parts = append(parts, fmtBytes(s.total("go_memstats_heap_inuse_bytes", nil))+" heap")
	}
	if s.has("process_resident_memory_bytes") {
		parts = append(parts, fmtBytes(s.total("process_resident_memory_bytes", nil))+" rss")
	}
	if s.has("process_open_fds") {
		parts = append(parts, fmtCount(s.total("process_open_fds", nil))+" fds")
	}
	if len(parts) > 0 {
		fmt.Fprintln(w, "\n"+dim("  process  "+strings.Join(parts, " · ")))
	}
}
