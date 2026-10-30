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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/cli/pkg/standalone"
)

const sample = `# TYPE dapr_http_server_request_count counter
dapr_http_server_request_count{app_id="a",method="GET",path="/v1.0/state/s/k",status="200"} 8
dapr_http_server_request_count{app_id="a",method="GET",path="/v1.0/state/s/k",status="500"} 2
dapr_http_server_request_count{app_id="a",method="GET",path="/v1.0/metadata",status="200"} 50
# TYPE dapr_http_server_latency histogram
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="200",le="1"} 0
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="200",le="10"} 4
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="200",le="100"} 8
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="200",le="+Inf"} 8
dapr_http_server_latency_sum{app_id="a",method="GET",path="/v1.0/state/s/k",status="200"} 300
dapr_http_server_latency_count{app_id="a",method="GET",path="/v1.0/state/s/k",status="200"} 8
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="500",le="1"} 0
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="500",le="10"} 0
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="500",le="100"} 2
dapr_http_server_latency_bucket{app_id="a",method="GET",path="/v1.0/state/s/k",status="500",le="+Inf"} 2
dapr_http_server_latency_sum{app_id="a",method="GET",path="/v1.0/state/s/k",status="500"} 150
dapr_http_server_latency_count{app_id="a",method="GET",path="/v1.0/state/s/k",status="500"} 2
# TYPE dapr_component_state_count counter
dapr_component_state_count{app_id="a",component="s",operation="get",success="true"} 7
dapr_component_state_count{app_id="a",component="s",operation="get",success="false"} 3
# TYPE go_goroutines gauge
go_goroutines 42
`

func mustParse(t *testing.T, text string, at time.Time) *snapshot {
	t.Helper()
	s, err := parseSnapshot(strings.NewReader(text), at)
	require.NoError(t, err)
	return s
}

func TestSumBy(t *testing.T) {
	s := mustParse(t, sample, time.Now())

	groups := s.sumBy("dapr_http_server_request_count", []string{"path"}, nil)
	require.Len(t, groups, 2)
	assert.Equal(t, "/v1.0/metadata", groups[0].labels["path"])
	assert.InDelta(t, 50.0, groups[0].value, 0)
	assert.InDelta(t, 10.0, groups[1].value, 0)

	assert.InDelta(t, 2.0, s.total("dapr_http_server_request_count", isHTTPError), 0)
	assert.InDelta(t, 10.0, s.total("dapr_http_server_request_count", notNoise), 0)
	assert.InDelta(t, 3.0, s.total("dapr_component_state_count", labelFilter([]string{"success=false"})), 0)
	assert.InDelta(t, 7.0, s.total("dapr_component_state_count", labelFilter([]string{"success!=false"})), 0)
	assert.InDelta(t, 0.0, s.total("does_not_exist", nil), 0)
}

func TestHistogramMergeAndQuantile(t *testing.T) {
	s := mustParse(t, sample, time.Now())
	groups := s.sumBy("dapr_http_server_latency", []string{"path"}, nil)
	require.Len(t, groups, 1)
	b := groups[0].buckets
	require.Len(t, b, 4)
	assert.InDelta(t, 10.0, b[3].count, 0)

	// 10 observations: 4 in (1,10], 6 in (10,100].
	assert.InDelta(t, 10.0, quantile(0.4, b), 1e-9)
	assert.InDelta(t, 55.0, quantile(0.7, b), 1e-9)
	assert.InDelta(t, 100.0, quantile(1, b), 1e-9)
	assert.True(t, math.IsNaN(quantile(0.5, nil)))
}

func TestQuantileAboveLastBucket(t *testing.T) {
	b := []bucket{{le: 10, count: 1}, {le: math.Inf(1), count: 10}}
	assert.InDelta(t, 10.0, quantile(0.99, b), 0)
}

func TestDelta(t *testing.T) {
	t0 := time.Now()
	prev := mustParse(t, sample, t0)
	cur := mustParse(t, strings.NewReplacer(
		`status="200"} 8`+"\n", `status="200"} 18`+"\n",
		"go_goroutines 42", "go_goroutines 40",
	).Replace(sample), t0.Add(2*time.Second))

	d := delta(cur, prev)
	assert.InDelta(t, 10.0, d.total("dapr_http_server_request_count", labelEq("status", "200")), 0)
	assert.InDelta(t, 0.0, d.total("dapr_http_server_request_count", labelEq("status", "500")), 0)
	assert.InDelta(t, 40.0, d.total("go_goroutines", nil), 0, "gauges keep their current value")

	// A counter that went backwards means the sidecar restarted.
	restarted := delta(prev, cur)
	assert.InDelta(t, 8.0, restarted.total("dapr_http_server_request_count", labelEq("status", "200")), 0)
}

func TestTargetsFromList(t *testing.T) {
	targets := targetsFromList([]standalone.ListOutput{
		{AppID: "orders", DaprdPID: 101, HTTPPort: 3501, MetricsEnabled: true, MetricsPort: 9091},
		{AppID: "checkout", DaprdPID: 103, HTTPPort: 3500},
	})
	assert.Equal(t, []target{
		{Name: "orders", PID: 101, HTTPAddr: "127.0.0.1:3501", MetricsURL: "http://127.0.0.1:9091/"},
		{Name: "checkout", PID: 103, HTTPAddr: "127.0.0.1:3500"},
	}, targets)
}

func TestFilterTargets(t *testing.T) {
	all := []target{{Name: "a"}, {Name: "b"}, {Name: "c"}}

	got, err := filterTargets(all, nil)
	require.NoError(t, err)
	assert.Equal(t, all, got)

	got, err = filterTargets(all, []string{"c", "a"})
	require.NoError(t, err)
	assert.Equal(t, []target{{Name: "a"}, {Name: "c"}}, got)

	_, err = filterTargets(all, []string{"a", "nope"})
	require.ErrorContains(t, err, "nope")
}

func TestParseURLTarget(t *testing.T) {
	assert.Equal(t, target{Name: "scheduler", MetricsURL: "http://localhost:9091/"}, parseURLTarget("scheduler=localhost:9091"))
	assert.Equal(t, target{Name: "localhost:9090", MetricsURL: "http://localhost:9090/"}, parseURLTarget("localhost:9090"))
	assert.Equal(t, "https://x:1/metrics", parseURLTarget("x=https://x:1/metrics").MetricsURL)
}

func TestRender(t *testing.T) {
	assert.Equal(t, 4, visibleLen("\x1b[31mfail\x1b[0m"))
	assert.Equal(t, "  ▁█", sparkline([]float64{0, 1}, 4))
	assert.Equal(t, "▁▁", sparkline([]float64{0, 0}, 2))
	assert.Equal(t, " █", sparkline([]float64{math.NaN(), 3}, 2))
	assert.Equal(t, "ab…", truncate("abcdef", 3))

	rows := chart([]float64{0, 5, 10}, 30, 4, fmtCount)
	require.Len(t, rows, 5)
	for _, r := range rows[:4] {
		assert.Equal(t, visibleLen(rows[0]), visibleLen(r))
	}
	assert.True(t, strings.HasSuffix(rows[0], "█"))

	assert.Equal(t, "1500", fmtCount(1500))
	assert.Equal(t, "12.0k", fmtCount(12000))
	assert.Equal(t, "1.50s", fmtMs(1500))
	assert.Equal(t, "1.0KiB", fmtBytes(1024))
	assert.Equal(t, "<0.01", fmtRate(0.001))
}
