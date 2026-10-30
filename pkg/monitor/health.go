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
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
)

// ErrUnhealthy is returned by Health when a sidecar is failing.
var ErrUnhealthy = errors.New("one or more Dapr instances are unhealthy")

type check struct {
	Name   string `json:"name"`
	Level  level  `json:"status"`
	Detail string `json:"detail"`
}

type componentHealth struct {
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Level    level   `json:"status"`
	Ops      float64 `json:"operations"`
	Failures float64 `json:"failures"`
	Detail   string  `json:"detail,omitempty"`
}

type report struct {
	Name       string            `json:"name"`
	Version    string            `json:"runtimeVersion,omitempty"`
	Level      level             `json:"status"`
	Checks     []check           `json:"checks"`
	Components []componentHealth `json:"components,omitempty"`
}

func (r *report) addf(name string, l level, format string, a ...any) {
	r.Checks = append(r.Checks, check{Name: name, Level: l, Detail: fmt.Sprintf(format, a...)})
	r.Level = max(r.Level, l)
}

// HealthOptions configures Health.
type HealthOptions struct {
	// Output is "table" (default) or "json".
	Output string
	// Strict makes warnings count as unhealthy.
	Strict bool
}

// Health prints a health report for each sidecar and returns ErrUnhealthy
// when any of them is failing.
func Health(ctx context.Context, w io.Writer, o Options, ho HealthOptions) error {
	if ho.Output == "json" {
		color.NoColor = true
	}
	targets, err := resolveTargets(ctx, o)
	if err != nil {
		return err
	}

	reports := parallel(ctx, targets, healthOf)

	if ho.Output == "json" {
		if err := printJSON(w, reports); err != nil {
			return err
		}
	} else {
		for i, r := range reports {
			if i > 0 {
				fmt.Fprintln(w)
			}
			printReport(w, r)
		}
	}

	threshold := levelFail
	if ho.Strict {
		threshold = levelWarn
	}
	for _, r := range reports {
		if r.Level >= threshold {
			return ErrUnhealthy
		}
	}
	return nil
}

func healthOf(ctx context.Context, t target) report {
	r := report{Name: t.Name}

	var md *metadata
	if t.isSidecar() {
		h := checkHealth(ctx, t.HTTPAddr, "/v1.0/healthz")
		switch {
		case h.ok():
			r.addf("sidecar", levelOK, "healthz %d on %s", h.Code, t.HTTPAddr)
		case h.Err != "":
			r.addf("sidecar", levelFail, "%s on %s", h.Err, t.HTTPAddr)
		default:
			r.addf("sidecar", levelFail, "healthz %d on %s, still initialising or a component failed", h.Code, t.HTTPAddr)
		}

		if h.Err == "" {
			o := checkHealth(ctx, t.HTTPAddr, "/v1.0/healthz/outbound")
			if o.ok() {
				r.addf("outbound", levelOK, "ready for app calls")
			} else {
				r.addf("outbound", levelFail, "healthz/outbound %d, the app cannot call Dapr yet", o.Code)
			}

			var err error
			md, err = fetchMetadata(ctx, t.HTTPAddr)
			if err != nil {
				r.addf("metadata", levelWarn, "%s", shortErr(err))
			} else {
				r.Version = md.RuntimeVersion
				metadataChecks(&r, md)
			}
		}
	}

	var snap *snapshot
	if t.MetricsURL != "" {
		var err error
		snap, err = scrape(ctx, t.MetricsURL)
		switch {
		case err != nil && !t.isSidecar():
			// A metrics-only target has nothing else to go on.
			r.addf("metrics", levelFail, "%s on %s", shortErr(err), t.MetricsURL)
		case err != nil:
			r.addf("metrics", levelWarn, "%s", shortErr(err))
		default:
			metricChecks(&r, snap, t.isSidecar())
		}
	} else if t.isSidecar() {
		r.addf("metrics", levelWarn, "disabled (--enable-metrics=false)")
	}

	if md != nil {
		r.Components = componentHealthOf(md, snap)
		for _, c := range r.Components {
			r.Level = max(r.Level, c.Level)
		}
	}
	return r
}

func metadataChecks(r *report, md *metadata) {
	if ac := md.AppConnection; ac != nil && ac.Port != 0 {
		detail := fmt.Sprintf("%s on port %d", ac.Protocol, ac.Port)
		if ac.Health == nil {
			detail += dim(", no app health check configured")
		}
		r.addf("app channel", levelOK, "%s", detail)
	} else {
		r.addf("app channel", levelOK, "%s", dim("no app port, sidecar only"))
	}

	if ar := md.ActorRuntime; ar != nil && ar.RuntimeStatus != "" {
		switch {
		case ar.Placement == "" || strings.HasSuffix(ar.Placement, "connected") && !strings.HasSuffix(ar.Placement, "disconnected"):
			detail := ar.Placement
			if n := len(ar.ActiveActors); n > 0 {
				detail += fmt.Sprintf(", %d actor types hosted", n)
			}
			r.addf("placement", levelOK, "%s", dash(detail))
		case !ar.HostReady && len(ar.ActiveActors) == 0:
			r.addf("placement", levelWarn, "%s; actors and workflows will not work until it connects (is placement running? 'dapr init')", ar.Placement)
		default:
			r.addf("placement", levelFail, "%s with %s registered; they cannot be activated", ar.Placement, actorKinds(ar.ActiveActors))
		}
	}

	if s := md.Scheduler; s != nil {
		if len(s.Connected) > 0 {
			r.addf("scheduler", levelOK, "connected to %s", strings.Join(s.Connected, ", "))
		} else {
			r.addf("scheduler", levelWarn, "not connected; jobs, reminders and workflow timers will stall")
		}
	}

	if n := len(md.Subscriptions); n > 0 {
		topics := make([]string, 0, n)
		for _, s := range md.Subscriptions {
			topics = append(topics, s.PubsubName+"/"+s.Topic)
		}
		r.addf("subscriptions", levelOK, "%d: %s", n, truncate(strings.Join(topics, ", "), 80))
	}
}

// highGoroutines is far above what an idle or lightly loaded local sidecar
// runs with, so crossing it usually means something is leaking.
const highGoroutines = 10000

func metricChecks(r *report, snap *snapshot, sidecar bool) {
	if start := snap.total("process_start_time_seconds", nil); start > 0 {
		up := time.Since(time.Unix(int64(start), 0))
		goroutines := snap.total("go_goroutines", nil)
		l, note := levelOK, ""
		if goroutines > highGoroutines {
			l, note = levelWarn, ", unusually high goroutine count"
		}
		r.addf("process", l, "up %s, %s goroutines, %s heap%s", fmtDuration(up),
			fmtCount(goroutines), fmtBytes(snap.total("go_memstats_heap_inuse_bytes", nil)), note)
	}
	if !sidecar {
		return
	}

	if fails := snap.sumBy("dapr_runtime_component_init_fail_total", []string{"component", "reason"}, nil); len(fails) > 0 {
		var parts []string
		for _, f := range fails {
			parts = append(parts, fmt.Sprintf("%s (%s) x%s", f.labels["component"], f.labels["reason"], fmtCount(f.value)))
		}
		r.addf("component init", levelFail, "%s", strings.Join(parts, "; "))
	}

	if fails := snap.total("dapr_runtime_mtls_init_fail_total", nil) + snap.total("dapr_runtime_mtls_workload_cert_rotated_fail_total", nil); fails > 0 {
		r.addf("mtls", levelFail, "%s certificate failures", fmtCount(fails))
	}

	var open []string
	for _, g := range snap.sumBy("dapr_resiliency_cb_state", []string{"name", "policy", "target", "status"}, nil) {
		if g.value > 0 && g.labels["status"] != "closed" {
			open = append(open, fmt.Sprintf("%s %s on %s", g.labels["name"], g.labels["status"], g.labels["target"]))
		}
	}
	if len(open) > 0 {
		sort.Strings(open)
		r.addf("circuit breakers", levelWarn, "%s", strings.Join(open, "; "))
	}

	appCalls := snap.total("dapr_http_client_completed_count", nil) + snap.total("dapr_grpc_io_client_completed_rpcs", nil)
	appErrs := snap.total("dapr_http_client_completed_count", isHTTPError) + snap.total("dapr_grpc_io_client_completed_rpcs", isGRPCError("grpc_client_status"))
	if appErrs > 0 {
		r.addf("app responses", levelWarn, "%s of %s calls from the sidecar to the app failed (%s)", fmtCount(appErrs), fmtCount(appCalls), fmtPct(appErrs, appCalls))
	}

	if dropped := snap.total("dapr_component_pubsub_ingress_count", labelEq("process_status", "drop")); dropped > 0 {
		r.addf("pub/sub", levelWarn, "%s messages dropped by the app", fmtCount(dropped))
	}
}

// componentFamilies maps a component type prefix to the operation counter
// that tells whether it is working.
var componentFamilies = map[string]string{ //nolint:gosec // metric names, not credentials
	"state":         "dapr_component_state_count",
	"secretstores":  "dapr_component_secret_count",
	"configuration": "dapr_component_configuration_count",
	"bindings":      "dapr_component_output_binding_count",
	"crypto":        "dapr_component_crypto_count",
	"conversation":  "dapr_component_conversation_count",
}

// componentHealthOf combines the loaded components from metadata with their
// operation results from metrics. Dapr has no component health endpoint, so a
// component is healthy if it loaded and its recent calls succeed.
func componentHealthOf(md *metadata, snap *snapshot) []componentHealth {
	out := make([]componentHealth, 0, len(md.Components))
	for _, c := range md.Components {
		ch := componentHealth{Name: c.Name, Type: c.Type + "/" + c.Version}
		kind, _, _ := strings.Cut(c.Type, ".")
		_, measured := componentFamilies[kind]
		measured = measured || kind == "pubsub"
		if snap != nil && measured {
			byName := labelEq("component", c.Name)
			switch kind {
			case "pubsub":
				ch.Ops = snap.total("dapr_component_pubsub_egress_count", byName) + snap.total("dapr_component_pubsub_ingress_count", byName)
				ch.Failures = snap.total("dapr_component_pubsub_egress_count", and(byName, labelEq("success", "false"))) +
					snap.total("dapr_component_pubsub_ingress_count", and(byName, labelEq("process_status", "retry")))
			default:
				if fam, ok := componentFamilies[kind]; ok {
					ch.Ops = snap.total(fam, byName)
					ch.Failures = snap.total(fam, and(byName, labelEq("success", "false")))
				}
				if kind == "bindings" {
					ch.Ops += snap.total("dapr_component_input_binding_count", byName)
					ch.Failures += snap.total("dapr_component_input_binding_count", and(byName, labelEq("success", "false")))
				}
			}
		}
		switch {
		case !measured:
			ch.Detail = dim("loaded")
		case ch.Ops == 0:
			ch.Detail = dim("loaded, no calls yet")
		case ch.Failures == 0:
			ch.Detail = fmtCount(ch.Ops) + " calls, all succeeded"
		default:
			ch.Level = levelWarn
			if ch.Failures == ch.Ops {
				ch.Level = levelFail
			}
			ch.Detail = fmt.Sprintf("%s of %s calls failed (%s)", fmtCount(ch.Failures), fmtCount(ch.Ops), fmtPct(ch.Failures, ch.Ops))
		}
		out = append(out, ch)
	}
	return out
}

func actorKinds(actors []activeActor) string {
	workflows, user := false, 0
	for _, a := range actors {
		if strings.HasPrefix(a.Type, "dapr.internal.") {
			workflows = true
		} else {
			user++
		}
	}
	switch {
	case workflows && user > 0:
		return fmt.Sprintf("workflows and %d actor types", user)
	case workflows:
		return "workflows"
	default:
		return fmt.Sprintf("%d actor types", user)
	}
}

func printReport(w io.Writer, r report) {
	title := bold(r.Name)
	if r.Version != "" {
		title += dim("  dapr " + r.Version)
	}
	fmt.Fprintf(w, "%s %s\n", r.Level.badge(), title)

	tb := newTable(w)
	for _, c := range r.Checks {
		tb.row("  "+c.Level.badge(), c.Name, c.Detail)
	}
	tb.flush()

	if len(r.Components) > 0 {
		fmt.Fprintln(w, dim("  components"))
		tb := newTable(w)
		for _, c := range r.Components {
			tb.row("  "+c.Level.badge(), c.Name, dim(c.Type), c.Detail)
		}
		tb.flush()
	}
}
