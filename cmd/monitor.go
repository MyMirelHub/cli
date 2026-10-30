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

package cmd

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/dapr/cli/pkg/monitor"
	"github.com/dapr/cli/pkg/print"
)

var (
	monitorOpts monitor.Options

	healthOpts  monitor.HealthOptions
	metricsOpts monitor.SummaryOptions
	topOpts     monitor.TopOptions
	graphOpts   monitor.GraphOptions

	queryLabels []string
)

var HealthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check the health of local Dapr instances and their components. Supported platforms: Self-hosted",
	Long: `Check the health of local Dapr instances and their components.

For each sidecar this checks healthz and outbound healthz, the app channel,
placement and scheduler connections, subscriptions, mTLS, circuit breakers and
the app's error rate. Components are reported as failing when their calls
fail, even while healthz still returns 204.

Exits with code 1 when any instance is failing.`,
	Example: `
# Check every Dapr instance running on this machine
dapr health

# Check one app, and fail on warnings too
dapr health --app-id orders --strict

# Check a sidecar that is not a local process, e.g. in Docker or port-forwarded
dapr health --http-port 3500 --metrics-port 9090
`,
	Run: func(cmd *cobra.Command, args []string) {
		err := monitor.Health(cmd.Context(), os.Stdout, monitorOpts, healthOpts)
		if errors.Is(err, monitor.ErrUnhealthy) {
			os.Exit(1)
		}
		exitOnError(err)
	},
}

var MetricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Summarise the metrics of local Dapr instances. Supported platforms: Self-hosted",
	Long: `Summarise the metrics of local Dapr instances since they started.

Metrics are scraped straight from each sidecar's metrics endpoint, so no
Prometheus is needed. Tables cover the Dapr API, the app channel, service
invocation, pub/sub, state, bindings, secrets, configuration, workflows,
actors, resiliency and error codes, with count, error rate and p50/p95/p99.`,
	Example: `
# Summarise every Dapr instance running on this machine
dapr metrics

# Summarise one app, including the Dapr CLI's own metadata and healthz calls
dapr metrics --app-id orders --all

# Print raw series, like a Prometheus query
dapr metrics query pubsub --app-id orders -l topic=orders

# Chart a signal or metric live in the terminal
dapr metrics graph p99 --app-id orders
`,
	Run: func(cmd *cobra.Command, args []string) {
		exitOnError(monitor.Metrics(cmd.Context(), os.Stdout, monitorOpts, metricsOpts))
	},
}

var metricsQueryCmd = &cobra.Command{
	Use:   "query <metric-regex>",
	Short: "Print the raw series whose metric name matches a regex",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		exitOnError(monitor.Query(cmd.Context(), os.Stdout, monitorOpts, args[0], queryLabels))
	},
}

var metricsGraphCmd = &cobra.Command{
	Use:   "graph <signal|metric>",
	Short: "Chart a signal or metric live in the terminal",
	Long: `Chart a signal or metric live in the terminal, one chart per Dapr instance.

Signals:
` + monitor.SignalHelp() + `
Any other argument is a Prometheus metric name: counters are charted as a
per-second rate, gauges as their value and histograms as a quantile.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		graphOpts.Name = args[0]
		ctx, cancel := interruptContext(cmd.Context())
		defer cancel()
		exitOnError(monitor.Graph(ctx, os.Stdout, monitorOpts, graphOpts))
	},
}

var TopCmd = &cobra.Command{
	Use:   "top",
	Short: "Show a live view of local Dapr instances. Supported platforms: Self-hosted",
	Long: `Show a live, per-second view of local Dapr instances.

With several instances there is one row per app with a request rate trend.
With a single instance, or --detail, there is a dashboard of sparklines and
the busiest routes and components over the last interval.`,
	Example: `
# Watch every Dapr instance running on this machine
dapr top

# Dashboard for one app, refreshed every second
dapr top --app-id orders -i 1s
`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := interruptContext(cmd.Context())
		defer cancel()
		exitOnError(monitor.Top(ctx, os.Stdout, monitorOpts, topOpts))
	},
}

func addMonitorTargetFlags(c *cobra.Command) {
	f := c.PersistentFlags()
	f.StringSliceVarP(&monitorOpts.AppIDs, "app-id", "a", nil, "Only these application ids (repeatable or comma separated)")
	f.StringArrayVar(&monitorOpts.URLs, "url", nil, "Extra metrics endpoint as [name=]host:port, e.g. a control plane service (repeatable)")
	f.IntVar(&monitorOpts.HTTPPort, "http-port", 0, "Dapr HTTP port of a sidecar to inspect instead of local Dapr instances")
	f.IntVar(&monitorOpts.MetricsPort, "metrics-port", 0, "Metrics port of the sidecar given by --http-port")
}

func init() {
	for _, c := range []*cobra.Command{HealthCmd, MetricsCmd, TopCmd} {
		addMonitorTargetFlags(c)
		c.Flags().BoolP("help", "h", false, "Print this help message")
		RootCmd.AddCommand(c)
	}

	HealthCmd.Flags().StringVarP(&healthOpts.Output, "output", "o", "table", "The output format. Valid values are: json or table (default)")
	HealthCmd.Flags().BoolVar(&healthOpts.Strict, "strict", false, "Exit with code 1 on warnings as well as failures")

	MetricsCmd.Flags().BoolVar(&metricsOpts.All, "all", false, "Include the Dapr CLI's own metadata and healthz calls")
	MetricsCmd.Flags().IntVarP(&metricsOpts.Limit, "limit", "n", 10, "Maximum rows per table, 0 for no limit")

	metricsQueryCmd.Flags().StringArrayVarP(&queryLabels, "label", "l", nil, "Only series with label k=v or k!=v (repeatable)")

	metricsGraphCmd.Flags().DurationVarP(&graphOpts.Interval, "interval", "i", time.Second, "Sample interval")
	metricsGraphCmd.Flags().IntVar(&graphOpts.Height, "height", 0, "Chart height in rows (default fits the terminal)")
	metricsGraphCmd.Flags().Float64VarP(&graphOpts.Quantile, "quantile", "q", 0.99, "Quantile to chart for histogram metrics")
	metricsGraphCmd.Flags().StringArrayVarP(&graphOpts.Labels, "label", "l", nil, "Only series with label k=v or k!=v, for metric names (repeatable)")

	MetricsCmd.AddCommand(metricsQueryCmd, metricsGraphCmd)

	TopCmd.Flags().DurationVarP(&topOpts.Interval, "interval", "i", 2*time.Second, "Refresh interval")
	TopCmd.Flags().BoolVarP(&topOpts.Detail, "detail", "d", false, "Show the detailed dashboard even with several instances")
	TopCmd.Flags().IntVarP(&topOpts.Limit, "limit", "n", 5, "Maximum rows per table in the detailed view")
}

func interruptContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

func exitOnError(err error) {
	if err != nil {
		print.FailureStatusEvent(os.Stderr, "%s", err)
		os.Exit(1)
	}
}
