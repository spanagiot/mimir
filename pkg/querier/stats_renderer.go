// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	promql_stats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func StatsRenderer(ctx context.Context, s *promql_stats.Statistics, param string) promql_stats.QueryStats {
	mimirStats := stats.FromContext(ctx)
	if mimirStats != nil && s != nil {
		mimirStats.AddTotalSamples(uint64(s.Samples.TotalSamples))
	}
	// The default implementation of the StatsRenderer is not public, so copying here until that's changed.
	if param != "" {
		return promql_stats.NewQueryStats(s)
	}
	return nil
}
