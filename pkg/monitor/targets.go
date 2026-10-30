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

// Package monitor implements quick health and metrics views of locally
// running Dapr sidecars, scraped directly without Prometheus or Grafana.
package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dapr/cli/pkg/standalone"
)

// Options selects the sidecars and endpoints to inspect.
type Options struct {
	// AppIDs narrows discovered sidecars (and --url names) to these app IDs.
	AppIDs []string
	// URLs are extra metrics-only endpoints as [name=]host:port.
	URLs []string
	// HTTPPort and MetricsPort point at one sidecar instead of discovering
	// local daprd processes.
	HTTPPort    int
	MetricsPort int
}

// target is one metrics endpoint to inspect. A sidecar has both a Dapr HTTP
// API and a metrics endpoint; a control plane service only has metrics.
type target struct {
	Name       string `json:"name"`
	PID        int    `json:"pid,omitempty"`
	HTTPAddr   string `json:"httpAddr,omitempty"`
	MetricsURL string `json:"metricsURL,omitempty"`
}

func (t target) isSidecar() bool { return t.HTTPAddr != "" }

func targetsFromList(list []standalone.ListOutput) []target {
	targets := make([]target, 0, len(list))
	for _, l := range list {
		t := target{
			Name:     l.AppID,
			PID:      l.DaprdPID,
			HTTPAddr: "127.0.0.1:" + strconv.Itoa(l.HTTPPort),
		}
		if l.MetricsEnabled {
			t.MetricsURL = "http://127.0.0.1:" + strconv.Itoa(l.MetricsPort) + "/"
		}
		targets = append(targets, t)
	}
	return targets
}

// parseURLTarget turns `name=host:port` or `host:port` into a metrics-only
// target, for control plane services or port-forwarded pods.
func parseURLTarget(spec string) target {
	name, addr, ok := strings.Cut(spec, "=")
	if !ok {
		addr, name = spec, spec
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	if !strings.HasSuffix(addr, "/") && strings.Count(addr, "/") == 2 {
		addr += "/"
	}
	return target{Name: name, MetricsURL: addr}
}

func resolveTargets(ctx context.Context, o Options) ([]target, error) {
	var targets []target
	switch {
	case o.HTTPPort != 0:
		t := target{Name: "sidecar", HTTPAddr: "127.0.0.1:" + strconv.Itoa(o.HTTPPort)}
		if o.MetricsPort != 0 {
			t.MetricsURL = "http://127.0.0.1:" + strconv.Itoa(o.MetricsPort) + "/"
		}
		if md, err := fetchMetadata(ctx, t.HTTPAddr); err == nil && md.ID != "" {
			t.Name = md.ID
		}
		targets = append(targets, t)
	case len(o.URLs) == 0:
		list, err := standalone.List()
		if err != nil {
			return nil, fmt.Errorf("listing Dapr instances: %w", err)
		}
		targets = append(targets, targetsFromList(list)...)
	}
	for _, u := range o.URLs {
		targets = append(targets, parseURLTarget(u))
	}

	targets, err := filterTargets(targets, o.AppIDs)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, errors.New("no Dapr instances found; start one with 'dapr run', or pass --http-port/--metrics-port or --url")
	}
	return targets, nil
}

func filterTargets(targets []target, appIDs []string) ([]target, error) {
	if len(appIDs) == 0 {
		return targets, nil
	}
	want := map[string]bool{}
	for _, id := range appIDs {
		want[id] = true
	}
	var kept []target
	for _, t := range targets {
		if want[t.Name] {
			kept = append(kept, t)
			delete(want, t.Name)
		}
	}
	if len(want) > 0 {
		missing := make([]string, 0, len(want))
		for k := range want {
			missing = append(missing, k)
		}
		return nil, fmt.Errorf("no Dapr instance found for app ID %s (see 'dapr list')", strings.Join(missing, ", "))
	}
	return kept, nil
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// parallel runs fn for every target concurrently and returns the results in
// target order.
func parallel[T any](ctx context.Context, targets []target, fn func(context.Context, target) T) []T {
	out := make([]T, len(targets))
	done := make(chan struct{}, len(targets))
	for i, t := range targets {
		go func() {
			out[i] = fn(ctx, t)
			done <- struct{}{}
		}()
	}
	for range targets {
		<-done
	}
	return out
}

type scrapeResult struct {
	snap *snapshot
	err  error
}

func scrapeAll(ctx context.Context, targets []target) []scrapeResult {
	return parallel(ctx, targets, func(ctx context.Context, t target) scrapeResult {
		if t.MetricsURL == "" {
			return scrapeResult{err: errors.New("metrics are disabled (--enable-metrics=false)")}
		}
		s, err := scrape(ctx, t.MetricsURL)
		return scrapeResult{s, err}
	})
}
