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
	"slices"
	"sort"
	"strings"
)

// group is an aggregate of every series that shares the same values for a
// set of labels, i.e. `sum by (labels) (metric)`.
type group struct {
	labels  map[string]string
	value   float64
	sum     float64
	buckets []bucket
}

type matcher func(map[string]string) bool

func matchAll(map[string]string) bool { return true }

// sumBy aggregates a family by the given labels. It returns groups sorted by
// descending value so the busiest rows come first.
func (s *snapshot) sumBy(name string, by []string, match matcher) []*group {
	f := s.families[name]
	if f == nil {
		return nil
	}
	if match == nil {
		match = matchAll
	}
	groups := map[string]*group{}
	for _, sr := range f.series {
		if !match(sr.labels) {
			continue
		}
		key := make(map[string]string, len(by))
		for _, l := range by {
			key[l] = sr.labels[l]
		}
		k := labelKey(key)
		g := groups[k]
		if g == nil {
			g = &group{labels: key}
			groups[k] = g
		}
		g.value += sr.value
		g.sum += sr.sum
		g.buckets = mergeBuckets(g.buckets, sr.buckets)
	}
	out := make([]*group, 0, len(groups))
	for _, g := range groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].value != out[j].value {
			return out[i].value > out[j].value
		}
		return labelKey(out[i].labels) < labelKey(out[j].labels)
	})
	return out
}

// total sums every matching series of a family.
func (s *snapshot) total(name string, match matcher) float64 {
	var t float64
	for _, g := range s.sumBy(name, nil, match) {
		t += g.value
	}
	return t
}

func (s *snapshot) has(name string) bool {
	f := s.families[name]
	return f != nil && len(f.series) > 0
}

func mergeBuckets(acc, add []bucket) []bucket {
	if len(add) == 0 {
		return acc
	}
	if len(acc) == 0 {
		out := make([]bucket, len(add))
		copy(out, add)
		return out
	}
	// All series of a family share bucket boundaries, so merge by position
	// when they line up and fall back to boundary lookup otherwise.
	if len(acc) == len(add) {
		for i := range acc {
			if acc[i].le == add[i].le {
				acc[i].count += add[i].count
			}
		}
		return acc
	}
	for _, b := range add {
		for i := range acc {
			if acc[i].le == b.le {
				acc[i].count += b.count
				break
			}
		}
	}
	return acc
}

// quantile estimates the q-th quantile from cumulative buckets with linear
// interpolation inside the bucket, the same way PromQL histogram_quantile
// does. It returns NaN when there are no observations.
func quantile(q float64, buckets []bucket) float64 {
	if len(buckets) == 0 {
		return math.NaN()
	}
	total := buckets[len(buckets)-1].count
	if total <= 0 {
		return math.NaN()
	}
	rank := q * total
	prevLE, prevCount := 0.0, 0.0
	for _, b := range buckets {
		if b.count >= rank {
			if math.IsInf(b.le, 1) {
				// Above the highest finite bucket; report its bound.
				return prevLE
			}
			inBucket := b.count - prevCount
			if inBucket <= 0 {
				return b.le
			}
			return prevLE + (b.le-prevLE)*(rank-prevCount)/inBucket
		}
		prevLE, prevCount = b.le, b.count
	}
	return prevLE
}

func labelEq(k, v string) matcher {
	return func(l map[string]string) bool { return l[k] == v }
}

func labelNot(k string, vs ...string) matcher {
	return func(l map[string]string) bool {
		return !slices.Contains(vs, l[k])
	}
}

func and(ms ...matcher) matcher {
	return func(l map[string]string) bool {
		for _, m := range ms {
			if m != nil && !m(l) {
				return false
			}
		}
		return true
	}
}

func statusClass(status string) byte {
	if len(status) == 3 {
		return status[0]
	}
	return 0
}

func isHTTPError(l map[string]string) bool { return statusClass(l["status"]) == '5' }

func isGRPCError(key string) matcher {
	return func(l map[string]string) bool {
		// Server-side failures only, the gRPC equivalent of a 5xx.
		switch l[key] {
		case "UNKNOWN", "INTERNAL", "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "UNIMPLEMENTED", "DATA_LOSS", "ABORTED":
			return true
		}
		return false
	}
}

// noisePaths are Dapr API calls made by the Dapr CLI and this tool itself.
// They dominate a quiet local sidecar and hide the traffic you care about.
func isNoisePath(l map[string]string) bool {
	p := l["path"]
	return strings.HasPrefix(p, "/v1.0/metadata") || strings.HasPrefix(p, "/v1.0/healthz")
}

// labelFilter parses `k=v` and `k!=v` selectors from the command line.
func labelFilter(selectors []string) matcher {
	var ms []matcher
	for _, sel := range selectors {
		if k, v, ok := strings.Cut(sel, "!="); ok {
			ms = append(ms, labelNot(k, v))
			continue
		}
		if k, v, ok := strings.Cut(sel, "="); ok {
			ms = append(ms, labelEq(k, v))
		}
	}
	return and(ms...)
}
