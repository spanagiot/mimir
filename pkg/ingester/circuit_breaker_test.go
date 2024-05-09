package ingester

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestIngester_Push_CircuitBreaker(t *testing.T) {
	tests := map[string]struct {
		expectedErrorWhenCircuitBreakerClosed error
		ctx                                   func(context.Context) context.Context
		limits                                InstanceLimits
	}{
		"deadline exceeded": {
			expectedErrorWhenCircuitBreakerClosed: nil,
			limits:                                InstanceLimits{MaxInMemoryTenants: 3},
			ctx: func(ctx context.Context) context.Context {
				ctx, _ = context.WithTimeout(ctx, time.Millisecond)
				time.Sleep(2 * time.Millisecond)
				return ctx
			},
		},
		"instance limit hit": {
			expectedErrorWhenCircuitBreakerClosed: instanceLimitReachedError{},
			limits:                                InstanceLimits{MaxInMemoryTenants: 1},
		},
	}

	for initialDelayEnabled, initialDelayStatus := range map[bool]string{false: "disabled", true: "enabled"} {
		for testName, testCase := range tests {
			t.Run(fmt.Sprintf("%s with initial delay %s", testName, initialDelayStatus), func(t *testing.T) {
				metricLabelAdapters := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test"}}}
				metricNames := []string{
					"cortex_ingester_circuit_breaker_results_total",
					"cortex_ingester_circuit_breaker_transitions_total",
				}

				registry := prometheus.NewRegistry()

				// Create a mocked ingester
				cfg := defaultIngesterTestConfig(t)
				cfg.ActiveSeriesMetrics.IdleTimeout = 100 * time.Millisecond
				cfg.InstanceLimitsFn = func() *InstanceLimits {
					return &testCase.limits
				}
				failureThreshold := 2
				var initialDelay time.Duration
				if initialDelayEnabled {
					initialDelay = 200 * time.Millisecond
				}
				cfg.CircuitBreakerConfig = CircuitBreakerConfig{
					Enabled:          true,
					FailureThreshold: uint(failureThreshold),
					CooldownPeriod:   10 * time.Second,
					InitialDelay:     initialDelay,
					testModeEnabled:  true,
				}

				i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
				require.NoError(t, err)
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
				defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

				// Wait until the ingester is healthy
				test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
					return i.lifecycler.HealthyInstancesCount()
				})

				// the first request is successful
				ctx := user.InjectOrgID(context.Background(), "test-0")
				req := mimirpb.ToWriteRequest(
					metricLabelAdapters,
					[]mimirpb.Sample{{Value: 1, TimestampMs: 8}},
					nil,
					nil,
					mimirpb.API,
				)
				_, err = i.Push(ctx, req)
				require.NoError(t, err)

				count := 0

				// Push timeseries for each user
				for _, userID := range []string{"test-1", "test-2"} {
					reqs := []*mimirpb.WriteRequest{
						mimirpb.ToWriteRequest(
							metricLabelAdapters,
							[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
							nil,
							nil,
							mimirpb.API,
						),
						mimirpb.ToWriteRequest(
							metricLabelAdapters,
							[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
							nil,
							nil,
							mimirpb.API,
						),
					}

					for _, req := range reqs {
						ctx := user.InjectOrgID(context.Background(), userID)
						count++
						if testCase.ctx != nil {
							ctx = testCase.ctx(ctx)
						}
						_, err = i.Push(ctx, req)
						if initialDelayEnabled {
							if testCase.expectedErrorWhenCircuitBreakerClosed != nil {
								require.ErrorAs(t, err, &testCase.expectedErrorWhenCircuitBreakerClosed)
							} else {
								require.NoError(t, err)
							}
						} else {
							if count <= failureThreshold {
								if testCase.expectedErrorWhenCircuitBreakerClosed != nil {
									require.ErrorAs(t, err, &testCase.expectedErrorWhenCircuitBreakerClosed)
								}
							} else {
								var cbOpenErr circuitBreakerOpenError
								require.ErrorAs(t, err, &cbOpenErr)
							}
						}
					}
				}

				// Check tracked Prometheus metrics
				var expectedMetrics string
				if initialDelayEnabled {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 0
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number times the circuit breaker has entered a state
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 0
    				`
				} else {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="circuit_breaker_open"} 2
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 2
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="success"} 1
						# HELP cortex_ingester_circuit_breaker_transitions_total Number times the circuit breaker has entered a state
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 1
    				`
				}
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
			})
		}
	}
}

func TestIngester_QueryStream_CircuitBreaker(t *testing.T) {
	tests := map[string]struct {
		expectedErrorWhenCircuitBreakerClosed error
		ctx                                   func(context.Context) context.Context
	}{
		"deadline exceeded": {
			expectedErrorWhenCircuitBreakerClosed: nil,
			ctx: func(ctx context.Context) context.Context {
				ctx, _ = context.WithTimeout(ctx, time.Millisecond)
				time.Sleep(2 * time.Millisecond)
				return ctx
			},
		},
	}

	for initialDelayEnabled, initialDelayStatus := range map[bool]string{false: "disabled", true: "enabled"} {
		for testName, testCase := range tests {
			t.Run(fmt.Sprintf("%s with initial delay %s", testName, initialDelayStatus), func(t *testing.T) {
				metricNames := []string{
					"cortex_ingester_circuit_breaker_results_total",
					"cortex_ingester_circuit_breaker_transitions_total",
				}

				registry := prometheus.NewRegistry()

				// Create a mocked ingester
				cfg := defaultIngesterTestConfig(t)
				cfg.ActiveSeriesMetrics.IdleTimeout = 100 * time.Millisecond

				failureThreshold := 2
				var initialDelay time.Duration
				if initialDelayEnabled {
					initialDelay = 200 * time.Millisecond
				}
				cfg.CircuitBreakerConfig = CircuitBreakerConfig{
					Enabled:          true,
					FailureThreshold: uint(failureThreshold),
					CooldownPeriod:   10 * time.Second,
					InitialDelay:     initialDelay,
					testModeEnabled:  true,
				}

				i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
				require.NoError(t, err)
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
				defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

				// Wait until the ingester is healthy
				test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
					return i.lifecycler.HealthyInstancesCount()
				})

				userID := "test-1"
				ctx := user.InjectOrgID(context.Background(), userID)

				for k := 0; k < 10; k++ {
					req := &client.QueryRequest{
						StartTimestampMs: math.MinInt64,
						EndTimestampMs:   math.MaxInt64,
						Matchers: []*client.LabelMatcher{
							{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "foo"},
						},
					}
					c := ctx
					if testCase.ctx != nil {
						c = testCase.ctx(c)
					}
					s := stream{ctx: c}
					err = i.QueryStream(req, &s)
					if initialDelayEnabled {
						if testCase.expectedErrorWhenCircuitBreakerClosed != nil {
							require.ErrorAs(t, err, &testCase.expectedErrorWhenCircuitBreakerClosed)
						} else {
							require.NoError(t, err)
						}
					} else {
						if k <= failureThreshold {
							if testCase.expectedErrorWhenCircuitBreakerClosed != nil {
								require.ErrorAs(t, err, &testCase.expectedErrorWhenCircuitBreakerClosed)
							}
						} else {
							var cbOpenErr circuitBreakerOpenError
							require.ErrorAs(t, err, &cbOpenErr)
						}
					}
				}

				// Check tracked Prometheus metrics

				var expectedMetrics string
				if initialDelayEnabled {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 0
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number times the circuit breaker has entered a state
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 0
    				`
				} else {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="circuit_breaker_open"} 8
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 2
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number times the circuit breaker has entered a state
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 1
    				`
				}
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
			})
		}
	}
}
