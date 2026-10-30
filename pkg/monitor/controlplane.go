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
	"strconv"
	"strings"
	"time"

	"github.com/dapr/cli/pkg/standalone"
)

const (
	schedulerName = "scheduler"
	placementName = "placement"

	// realQuota separates a configured etcd size quota from the effectively
	// unlimited default a local scheduler reports.
	realQuota = 1 << 50
	// slowFsyncMs is well past etcd's own guidance of a 10ms p99 WAL fsync.
	slowFsyncMs = 100
	// minFsyncs keeps a handful of startup fsyncs from raising a warning.
	minFsyncs = 50
)

// controlPlaneTargets returns the scheduler and placement services that
// `dapr init` started, if they answer on their default host ports.
func controlPlaneTargets(ctx context.Context) []target {
	candidates := []target{
		{
			Name:       schedulerName,
			MetricsURL: "http://127.0.0.1:" + strconv.Itoa(standalone.SchedulerMetricsHostPort) + "/",
			HealthAddr: "127.0.0.1:" + strconv.Itoa(standalone.SchedulerHealthHostPort),
		},
		{
			Name:       placementName,
			MetricsURL: "http://127.0.0.1:" + strconv.Itoa(standalone.PlacementMetricsHostPort) + "/",
			HealthAddr: "127.0.0.1:" + strconv.Itoa(standalone.PlacementHealthHostPort),
		},
	}
	found := parallel(ctx, candidates, func(ctx context.Context, t target) bool {
		ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		return checkHealth(ctx, t.HealthAddr, "/healthz").Err == ""
	})
	var out []target
	for i, ok := range found {
		if ok {
			out = append(out, candidates[i])
		}
	}
	return out
}

func isScheduler(s *snapshot) bool {
	return s.has("etcd_server_has_leader") || s.hasPrefix("dapr_scheduler_")
}

func isPlacement(s *snapshot) bool {
	return s.has("dapr_placement_leader_status") || s.has("dapr_placement_runtimes_total")
}

func (s *snapshot) hasPrefix(prefix string) bool {
	for name, f := range s.families {
		if strings.HasPrefix(name, prefix) && len(f.series) > 0 {
			return true
		}
	}
	return false
}

// histogramQuantile merges every series of a histogram family and returns
// its q-th quantile, scaled by unit (1000 turns etcd's seconds into ms).
func (s *snapshot) histogramQuantile(name string, q, unit float64) float64 {
	var b []bucket
	for _, g := range s.sumBy(name, nil, nil) {
		b = mergeBuckets(b, g.buckets)
	}
	return quantile(q, b) * unit
}

func controlPlaneChecks(r *report, s *snapshot) {
	if isScheduler(s) {
		etcdChecks(r, s)
		schedulerChecks(r, s)
	}
	if isPlacement(s) {
		placementChecks(r, s)
	}
}

func etcdChecks(r *report, s *snapshot) {
	if !s.has("etcd_server_has_leader") {
		return
	}
	changes := fmtCount(s.total("etcd_server_leader_changes_seen_total", nil))
	if s.total("etcd_server_has_leader", nil) == 1 {
		r.addf("etcd leader", levelOK, "elected, %s leader changes seen", changes)
	} else {
		r.addf("etcd leader", levelFail, "no leader; the scheduler cannot store or trigger jobs")
	}

	size := s.total("etcd_mvcc_db_total_size_in_bytes", nil)
	detail := fmt.Sprintf("%s on disk, %s in use, %s keys", fmtBytes(size),
		fmtBytes(s.total("etcd_mvcc_db_total_size_in_use_in_bytes", nil)), fmtCount(s.total("etcd_debugging_mvcc_keys_total", nil)))
	l := levelOK
	if quota := s.total("etcd_server_quota_backend_bytes", nil); quota > 0 && quota < realQuota {
		pct := 100 * size / quota
		detail += fmt.Sprintf(", %.0f%% of the %s quota", pct, fmtBytes(quota))
		switch {
		case pct >= 95:
			l = levelFail
		case pct >= 80:
			l = levelWarn
		}
	}
	r.addf("etcd storage", l, "%s", detail)

	fsync := s.histogramQuantile("etcd_disk_wal_fsync_duration_seconds", 0.99, 1000)
	failed := s.total("etcd_server_proposals_failed_total", nil)
	switch {
	case failed > 0:
		r.addf("etcd writes", levelWarn, "%s failed proposals, WAL fsync p99 %s", fmtCount(failed), fmtMs(fsync))
	case fsync > slowFsyncMs && s.total("etcd_disk_wal_fsync_duration_seconds", nil) >= minFsyncs:
		r.addf("etcd writes", levelWarn, "WAL fsync p99 %s, the disk is too slow for etcd", fmtMs(fsync))
	default:
		r.addf("etcd writes", levelOK, "WAL fsync p99 %s, %s pending proposals", fmtMs(fsync),
			fmtCount(s.total("etcd_server_proposals_pending", nil)))
	}
}

func schedulerChecks(r *report, s *snapshot) {
	// The scheduler's own series only appear once something has happened.
	if !s.hasPrefix("dapr_scheduler_") {
		r.addf("sidecars", levelOK, "%s", dim("no sidecar has connected yet"))
		return
	}
	// A sidecar holds several streams, so this counts connections, not apps.
	r.addf("sidecars", levelOK, "%s sidecar connections", fmtCount(s.total("dapr_scheduler_sidecars_connected", nil)))

	triggered := s.total("dapr_scheduler_jobs_triggered_total", nil)
	failed := s.total("dapr_scheduler_jobs_failed_total", nil)
	undelivered := s.total("dapr_scheduler_jobs_undelivered_total", nil)
	createFailed := s.total("dapr_scheduler_jobs_created_failed_total", nil)
	detail := fmt.Sprintf("%s created, %s delivered, trigger p99 %s", fmtCount(s.total("dapr_scheduler_jobs_created_total", nil)),
		fmtCount(triggered), fmtMs(s.histogramQuantile("dapr_scheduler_trigger_latency", 0.99, 1)))
	var problems []string
	if failed > 0 {
		problems = append(problems, fmtCount(failed)+" triggers failed")
	}
	if undelivered > 0 {
		problems = append(problems, fmtCount(undelivered)+" undelivered, no sidecar of the app was connected")
	}
	if createFailed > 0 {
		problems = append(problems, fmtCount(createFailed)+" failed to create")
	}
	if len(problems) > 0 {
		r.addf("jobs", levelWarn, "%s; %s", detail, strings.Join(problems, ", "))
	} else {
		r.addf("jobs", levelOK, "%s", detail)
	}

	if s.has("dapr_scheduler_placement_leader") {
		streams := fmtCount(s.total("dapr_scheduler_placement_streams_connected", nil))
		switch {
		case s.total("dapr_scheduler_placement_leader", nil) != 1:
			r.addf("placement", levelWarn, "not the placement leader, %s streams", streams)
		case s.total("dapr_scheduler_placement_incapable_sidecars", nil) > 0:
			r.addf("placement", levelWarn, "leader, %s streams, %s sidecars cannot use scheduler placement", streams,
				fmtCount(s.total("dapr_scheduler_placement_incapable_sidecars", nil)))
		default:
			r.addf("placement", levelOK, "leader, %s streams, dissemination p99 %s", streams,
				fmtMs(s.histogramQuantile("dapr_scheduler_placement_dissemination_latency", 0.99, 1)))
		}
	}
}

func placementChecks(r *report, s *snapshot) {
	hosts := fmt.Sprintf("%s sidecars, %s hosting actors", fmtCount(s.total("dapr_placement_runtimes_total", nil)),
		fmtCount(s.total("dapr_placement_actor_runtimes_total", nil)))
	if s.total("dapr_placement_leader_status", nil) == 1 && s.total("dapr_placement_raft_leader_status", nil) == 1 {
		r.addf("placement", levelOK, "leader, %s", hosts)
	} else {
		r.addf("placement", levelFail, "not the leader; actors and workflows cannot be placed (%s)", hosts)
	}
}

// printControlPlane adds the scheduler, etcd and placement sections of the
// summary. It returns how many sections it printed.
func printControlPlane(w io.Writer, s *snapshot, o SummaryOptions) int {
	printed := 0
	if g := s.sumBy("dapr_scheduler_jobs_triggered_total", []string{"type"}, nil); len(g) > 0 || s.has("dapr_scheduler_jobs_created_total") {
		printed++
		printSchedulerJobs(w, s, o)
	}
	if s.has("etcd_server_has_leader") {
		printed++
		printEtcd(w, s, o)
	}
	if s.has("dapr_scheduler_placement_streams_connected") || isPlacement(s) {
		printed++
		printPlacement(w, s)
	}
	return printed
}

func printSchedulerJobs(w io.Writer, s *snapshot, o SummaryOptions) {
	types := map[string]bool{}
	for _, name := range []string{"dapr_scheduler_jobs_created_total", "dapr_scheduler_jobs_triggered_total", "dapr_scheduler_jobs_failed_total", "dapr_scheduler_jobs_undelivered_total"} {
		for _, g := range s.sumBy(name, []string{"type"}, nil) {
			types[g.labels["type"]] = true
		}
	}
	section(w, "Scheduler jobs")
	header := "CREATED"
	if o.window > 0 {
		header = "CREATED/S"
	}
	tb := newTable(w, " ", "TYPE", header, "DELIVERED", "FAILED", "UNDELIVERED", "TRIGGER P50", "P99")
	for _, typ := range []string{"job", "actor", "unknown"} {
		if !types[typ] {
			continue
		}
		m := labelEq("type", typ)
		var b []bucket
		for _, g := range s.sumBy("dapr_scheduler_trigger_latency", nil, m) {
			b = mergeBuckets(b, g.buckets)
		}
		tb.row(" ", typ,
			rateOrCount(s.total("dapr_scheduler_jobs_created_total", m), o.window),
			rateOrCount(s.total("dapr_scheduler_jobs_triggered_total", m), o.window),
			problemCell(s.total("dapr_scheduler_jobs_failed_total", m), o.window),
			problemCell(s.total("dapr_scheduler_jobs_undelivered_total", m), o.window),
			fmtMs(quantile(0.5, b)), latencyCell(quantile(0.99, b)))
	}
	tb.flush()
	if n := s.total("dapr_scheduler_sidecars_connected", nil); s.has("dapr_scheduler_sidecars_connected") {
		fmt.Fprintln(w, dim(fmt.Sprintf("    %s sidecar connections", fmtCount(n))))
	}
}

func problemCell(v float64, window time.Duration) string {
	s := rateOrCount(v, window)
	if v > 0 {
		return red(s)
	}
	return s
}

func printEtcd(w io.Writer, s *snapshot, o SummaryOptions) {
	section(w, "Scheduler etcd")
	leader := green("yes")
	if s.total("etcd_server_has_leader", nil) != 1 {
		leader = red("no")
	}
	quota := "unlimited"
	if q := s.total("etcd_server_quota_backend_bytes", nil); q > 0 && q < realQuota {
		quota = fmtBytes(q)
	}
	opsHeader, ops := "PROPOSALS APPLIED", s.total("etcd_server_proposals_applied_total", nil)
	if o.window > 0 {
		opsHeader = "PROPOSALS/S"
	}
	tb := newTable(w, " ", "LEADER", "DB SIZE", "IN USE", "QUOTA", "KEYS", opsHeader, "FAILED", "FSYNC P99", "COMMIT P99")
	fsync := s.histogramQuantile("etcd_disk_wal_fsync_duration_seconds", 0.99, 1000)
	fsyncCell := fmtMs(fsync)
	if fsync > slowFsyncMs {
		fsyncCell = red(fsyncCell)
	}
	tb.row(" ", leader,
		fmtBytes(s.total("etcd_mvcc_db_total_size_in_bytes", nil)),
		fmtBytes(s.total("etcd_mvcc_db_total_size_in_use_in_bytes", nil)),
		quota,
		fmtCount(s.total("etcd_debugging_mvcc_keys_total", nil)),
		rateOrCount(ops, o.window),
		problemCell(s.total("etcd_server_proposals_failed_total", nil), o.window),
		fsyncCell,
		fmtMs(s.histogramQuantile("etcd_disk_backend_commit_duration_seconds", 0.99, 1000)))
	tb.flush()
}

func printPlacement(w io.Writer, s *snapshot) {
	section(w, "Placement")
	if isPlacement(s) {
		leader := green("yes")
		if s.total("dapr_placement_leader_status", nil) != 1 || s.total("dapr_placement_raft_leader_status", nil) != 1 {
			leader = red("no")
		}
		tb := newTable(w, " ", "NAMESPACE", "LEADER", "SIDECARS", "HOSTING ACTORS")
		groups := s.sumBy("dapr_placement_runtimes_total", []string{"namespace"}, nil)
		if len(groups) == 0 {
			tb.row(" ", "-", leader, "0", "0")
		}
		for _, g := range groups {
			ns := g.labels["namespace"]
			tb.row(" ", dash(ns), leader, fmtCount(g.value), fmtCount(s.total("dapr_placement_actor_runtimes_total", labelEq("namespace", ns))))
		}
		tb.flush()
		return
	}
	tb := newTable(w, " ", "NAMESPACE", "STREAMS", "DISSEMINATIONS", "DISSEMINATION P99")
	for _, g := range s.sumBy("dapr_scheduler_placement_streams_connected", []string{"namespace"}, nil) {
		ns := g.labels["namespace"]
		var b []bucket
		for _, h := range s.sumBy("dapr_scheduler_placement_dissemination_latency", nil, labelEq("namespace", ns)) {
			b = mergeBuckets(b, h.buckets)
		}
		tb.row(" ", dash(ns), fmtCount(g.value), fmtCount(s.total("dapr_scheduler_placement_disseminations_total", labelEq("namespace", ns))), latencyCell(quantile(0.99, b)))
	}
	tb.flush()
}

// controlPlaneIndicators are the top and graph signals for the scheduler,
// its etcd and placement.
var controlPlaneIndicators = []indicator{
	{"jobs", "scheduler jobs and actor reminders delivered per second", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return d.total("dapr_scheduler_jobs_triggered_total", nil) / w
	}},
	{"job-failures", "scheduler triggers per second that failed or had no sidecar to deliver to", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return (d.total("dapr_scheduler_jobs_failed_total", nil) + d.total("dapr_scheduler_jobs_undelivered_total", nil)) / w
	}},
	{"trigger-p99", "scheduler trigger p99 latency", fmtMs, func(d, _ *snapshot, _ float64) float64 {
		return d.histogramQuantile("dapr_scheduler_trigger_latency", 0.99, 1)
	}},
	{"sidecars", "sidecar connections to the scheduler, or sidecars registered with placement", fmtCount, func(_, c *snapshot, _ float64) float64 {
		if c.has("dapr_placement_runtimes_total") {
			return c.total("dapr_placement_runtimes_total", nil)
		}
		return c.total("dapr_scheduler_sidecars_connected", nil)
	}},
	{"etcd-db", "scheduler etcd database size on disk", fmtBytes, func(_, c *snapshot, _ float64) float64 {
		return c.total("etcd_mvcc_db_total_size_in_bytes", nil)
	}},
	{"etcd-keys", "keys in the scheduler etcd", fmtCount, func(_, c *snapshot, _ float64) float64 {
		return c.total("etcd_debugging_mvcc_keys_total", nil)
	}},
	{"etcd-writes", "etcd proposals applied per second", fmtRate, func(d, _ *snapshot, w float64) float64 {
		return d.total("etcd_server_proposals_applied_total", nil) / w
	}},
	{"fsync-p99", "etcd WAL fsync p99 latency", fmtMs, func(d, _ *snapshot, _ float64) float64 {
		return d.histogramQuantile("etcd_disk_wal_fsync_duration_seconds", 0.99, 1000)
	}},
}

var controlPlaneDetailSignals = []string{"jobs", "job-failures", "trigger-p99", "sidecars", "etcd-db", "etcd-keys", "etcd-writes", "fsync-p99", "cpu", "heap", "goroutines"}

func leaderCell(s *snapshot) string {
	switch {
	case s == nil:
		return "-"
	case s.has("etcd_server_has_leader"):
		if s.total("etcd_server_has_leader", nil) == 1 {
			return green("yes")
		}
		return red("no")
	case isPlacement(s):
		if s.total("dapr_placement_leader_status", nil) == 1 {
			return green("yes")
		}
		return red("no")
	default:
		return "-"
	}
}

func renderControlPlaneOverview(w io.Writer, pollers []*poller) {
	trendW := max(termWidth()-112, 10)
	tb := newTable(w, "CONTROL PLANE", "HEALTH", "LEADER", "SIDECARS", "JOBS/S", "FAILED/S", "TRIGGER P99", "ETCD DB", "KEYS", "FSYNC P99", "HEAP", "JOBS/S TREND")
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
		etcdDB, keys, fsync := "-", "-", "-"
		if p.cur.has("etcd_mvcc_db_total_size_in_bytes") {
			etcdDB, keys, fsync = fmtBytes(h("etcd-db")), fmtCount(h("etcd-keys")), fmtMs(h("fsync-p99"))
		}
		jobs, failed, trigger := "-", "-", "-"
		if p.cur.hasPrefix("dapr_scheduler_") || p.cur.has("etcd_server_has_leader") {
			jobs, failed, trigger = fmtRate(h("jobs")), fmtRate(h("job-failures")), latencyCell(h("trigger-p99"))
			if v := h("job-failures"); v > 0 && !math.IsNaN(v) {
				failed = red(failed)
			}
		}
		tb.row(p.t.Name, healthCell(p), leaderCell(p.cur), fmtCount(h("sidecars")), jobs, failed,
			trigger, etcdDB, keys, fsync, fmtBytes(h("heap")),
			cyan(sparkline(p.history["jobs"].values, trendW)))
	}
	tb.flush()
}
