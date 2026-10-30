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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

var httpClient = &http.Client{Timeout: 3 * time.Second}

type bucket struct {
	le    float64
	count float64
}

type series struct {
	labels  map[string]string
	value   float64 // counter or gauge value, histogram sample count
	sum     float64 // histogram sum
	buckets []bucket
}

type family struct {
	name   string
	typ    dto.MetricType
	series map[string]*series
}

// snapshot is one scrape of a metrics endpoint. A snapshot produced by delta
// holds increases over a window instead of totals since process start.
type snapshot struct {
	at       time.Time
	families map[string]*family
}

func scrape(ctx context.Context, url string) (*snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s returned %s", url, resp.Status)
	}
	return parseSnapshot(resp.Body, time.Now())
}

func parseSnapshot(r io.Reader, at time.Time) (*snapshot, error) {
	var parser expfmt.TextParser
	mfs, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return nil, fmt.Errorf("parsing metrics: %w", err)
	}
	snap := &snapshot{at: at, families: make(map[string]*family, len(mfs))}
	for name, mf := range mfs {
		f := &family{name: name, typ: mf.GetType(), series: make(map[string]*series, len(mf.GetMetric()))}
		for _, m := range mf.GetMetric() {
			s := &series{labels: make(map[string]string, len(m.GetLabel()))}
			for _, l := range m.GetLabel() {
				s.labels[l.GetName()] = l.GetValue()
			}
			switch f.typ {
			case dto.MetricType_COUNTER:
				s.value = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				s.value = m.GetGauge().GetValue()
			case dto.MetricType_UNTYPED:
				s.value = m.GetUntyped().GetValue()
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				s.value = float64(h.GetSampleCount())
				s.sum = h.GetSampleSum()
				for _, b := range h.GetBucket() {
					s.buckets = append(s.buckets, bucket{le: b.GetUpperBound(), count: float64(b.GetCumulativeCount())})
				}
				if n := len(s.buckets); n == 0 || !math.IsInf(s.buckets[n-1].le, 1) {
					s.buckets = append(s.buckets, bucket{le: math.Inf(1), count: s.value})
				}
			case dto.MetricType_SUMMARY:
				s.value = float64(m.GetSummary().GetSampleCount())
				s.sum = m.GetSummary().GetSampleSum()
			}
			f.series[labelKey(s.labels)] = s
		}
		snap.families[name] = f
	}
	return snap, nil
}

func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
		sb.WriteByte(',')
	}
	return sb.String()
}

// delta returns what changed between prev and cur. Counters and histograms
// become increases over the window; gauges keep their current value. A
// counter that went backwards means the process restarted, so the current
// value is the increase.
func delta(cur, prev *snapshot) *snapshot {
	if prev == nil {
		return cur
	}
	out := &snapshot{at: cur.at, families: make(map[string]*family, len(cur.families))}
	for name, f := range cur.families {
		pf := prev.families[name]
		nf := &family{name: name, typ: f.typ, series: make(map[string]*series, len(f.series))}
		for key, s := range f.series {
			var ps *series
			if pf != nil {
				ps = pf.series[key]
			}
			nf.series[key] = deltaSeries(f.typ, s, ps)
		}
		out.families[name] = nf
	}
	return out
}

func deltaSeries(typ dto.MetricType, s, ps *series) *series {
	if typ == dto.MetricType_GAUGE || typ == dto.MetricType_UNTYPED || ps == nil || s.value < ps.value {
		return s
	}
	ns := &series{labels: s.labels, value: s.value - ps.value, sum: s.sum - ps.sum}
	if len(s.buckets) > 0 {
		ns.buckets = make([]bucket, len(s.buckets))
		for i, b := range s.buckets {
			ns.buckets[i] = b
			if i < len(ps.buckets) && ps.buckets[i].le == b.le {
				ns.buckets[i].count = b.count - ps.buckets[i].count
			}
		}
	}
	return ns
}

type healthResult struct {
	Code int    `json:"code"`
	Err  string `json:"error,omitempty"`
}

func (h healthResult) ok() bool { return h.Code == http.StatusNoContent || h.Code == http.StatusOK }

func checkHealth(ctx context.Context, addr, path string) healthResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return healthResult{Err: err.Error()}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return healthResult{Err: shortErr(err)}
	}
	resp.Body.Close()
	return healthResult{Code: resp.StatusCode}
}

// metadata is the subset of the /v1.0/metadata response this tool uses.
type metadata struct {
	ID             string   `json:"id"`
	RuntimeVersion string   `json:"runtimeVersion"`
	Features       []string `json:"enabledFeatures"`
	Components     []struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Version string `json:"version"`
	} `json:"components"`
	Subscriptions []struct {
		PubsubName string `json:"pubsubname"`
		Topic      string `json:"topic"`
	} `json:"subscriptions"`
	AppConnection *struct {
		Port     int    `json:"port"`
		Protocol string `json:"protocol"`
		Health   *struct {
			Probe string `json:"healthProbeInterval"`
		} `json:"health"`
	} `json:"appConnectionProperties"`
	ActorRuntime *struct {
		RuntimeStatus string        `json:"runtimeStatus"`
		HostReady     bool          `json:"hostReady"`
		Placement     string        `json:"placement"`
		ActiveActors  []activeActor `json:"activeActors"`
	} `json:"actorRuntime"`
	Scheduler *struct {
		Connected []string `json:"connected_addresses"`
	} `json:"scheduler"`
}

type activeActor struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

func fetchMetadata(ctx context.Context, addr string) (*metadata, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/v1.0/metadata", nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metadata returned %s", resp.Status)
	}
	var md metadata
	if err := json.NewDecoder(resp.Body).Decode(&md); err != nil {
		return nil, fmt.Errorf("decoding metadata: %w", err)
	}
	return &md, nil
}

func shortErr(err error) string {
	s := err.Error()
	if strings.Contains(s, "connection refused") {
		return "connection refused"
	}
	if strings.Contains(s, "deadline exceeded") || strings.Contains(s, "Timeout") {
		return "timeout"
	}
	return s
}
