package ingester

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/ingester/client"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestIngester_Push_CircuitBreaker(t *testing.T) {
	pushTimeout := 100 * time.Millisecond
	tests := map[string]struct {
		expectedErrorWhenCircuitBreakerClosed error
		ctx                                   func(context.Context) context.Context
		limits                                InstanceLimits
	}{
		"deadline exceeded": {
			expectedErrorWhenCircuitBreakerClosed: nil,
			limits:                                InstanceLimits{MaxInMemoryTenants: 3},
			ctx: func(ctx context.Context) context.Context {
				return context.WithValue(ctx, testDelayKey, (2 * pushTimeout).String())
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
					PushTimeout:      pushTimeout,
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
								checkCircuitBreakerOpenErr(t, ctx, err)
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
	queryStreamTimeout := 100 * time.Millisecond
	tests := map[string]struct {
		expectedErrorWhenCircuitBreakerClosed error
		ctx                                   func(context.Context) context.Context
	}{
		"deadline exceeded": {
			expectedErrorWhenCircuitBreakerClosed: nil,
			ctx: func(ctx context.Context) context.Context {
				return context.WithValue(ctx, testDelayKey, (2 * queryStreamTimeout).String())
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
					Enabled:            true,
					FailureThreshold:   uint(failureThreshold),
					CooldownPeriod:     10 * time.Second,
					InitialDelay:       initialDelay,
					QueryStreamTimeout: queryStreamTimeout,
					testModeEnabled:    true,
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
							checkCircuitBreakerOpenErr(t, ctx, err)
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

func TestIngester_InflightPushRequestsBytes_CircuitBreaker(t *testing.T) {
	for _, grpcLimitEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("gRPC limit enabled: %t", grpcLimitEnabled), func(t *testing.T) {
			var limitsMx sync.Mutex
			limits := InstanceLimits{MaxInflightPushRequestsBytes: 0}

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LimitInflightRequestsUsingGrpcMethodLimiter = grpcLimitEnabled
			cfg.InstanceLimitsFn = func() *InstanceLimits {
				limitsMx.Lock()
				defer limitsMx.Unlock()

				// Make a copy
				il := limits
				return &il
			}
			cfg.CircuitBreakerConfig = CircuitBreakerConfig{
				Enabled:          true,
				FailureThreshold: uint(2),
				CooldownPeriod:   10 * time.Second,
				PushTimeout:      10 * time.Second,
				testModeEnabled:  true,
			}

			reg := prometheus.NewPedanticRegistry()

			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			ctx := user.InjectOrgID(context.Background(), "test")

			startCh := make(chan int)

			const targetRequestDuration = 1 * time.Second
			var requestSize int

			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				req := prepareRequestForTargetRequestDuration(ctx, t, i, targetRequestDuration)

				// Update instance limits. Set limit to EXACTLY the request size.
				limitsMx.Lock()
				limits.MaxInflightPushRequestsBytes = int64(req.Size())
				limitsMx.Unlock()

				// Signal that we're going to do the real push now.
				startCh <- req.Size()
				close(startCh)

				var err error
				if grpcLimitEnabled {
					_, err = pushWithSimulatedGRPCHandler(ctx, i, req)
				} else {
					_, err = i.Push(ctx, req)
				}
				return err
			})

			g.Go(func() error {
				req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, "testcase1"), 1, 1024)

				select {
				case <-ctx.Done():
				// failed to setup
				case requestSize = <-startCh:
					// we can start the test.
				}

				test.Poll(t, targetRequestDuration/3, int64(1), func() interface{} {
					return i.inflightPushRequests.Load()
				})

				expectedMetrics := fmt.Sprintf(`
								# HELP cortex_ingester_inflight_push_requests_bytes Total sum of inflight push request sizes in ingester in bytes.
								# TYPE cortex_ingester_inflight_push_requests_bytes gauge
								cortex_ingester_inflight_push_requests_bytes %d
								# HELP cortex_ingester_instance_rejected_requests_total Requests rejected for hitting per-instance limits
								# TYPE cortex_ingester_instance_rejected_requests_total counter
								cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests"} 0
								cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests_bytes"} 0
								cortex_ingester_instance_rejected_requests_total{reason="ingester_max_ingestion_rate"} 0
								cortex_ingester_instance_rejected_requests_total{reason="ingester_max_series"} 0
								cortex_ingester_instance_rejected_requests_total{reason="ingester_max_tenants"} 0
								# HELP cortex_ingester_circuit_breaker_transitions_total Number times the circuit breaker has entered a state
								# TYPE cortex_ingester_circuit_breaker_transitions_total counter
								cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
								cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
								cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 0
				`, requestSize)

				metricNames := []string{
					"cortex_ingester_circuit_breaker_transitions_total",
					"cortex_ingester_inflight_push_requests_bytes",
					"cortex_ingester_instance_rejected_requests_total",
				}

				require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))

				// Starting push request fails
				_, err = i.StartPushRequest(ctx, 100)
				require.ErrorIs(t, err, errMaxInflightRequestsBytesReached)

				// Starting push request with unknown size fails
				_, err = i.StartPushRequest(ctx, 0)
				require.ErrorIs(t, err, errMaxInflightRequestsBytesReached)

				// Sending push request fails
				if grpcLimitEnabled {
					_, err := pushWithSimulatedGRPCHandler(ctx, i, req)
					checkCircuitBreakerOpenErr(t, ctx, err)
				} else {
					_, err := i.Push(ctx, req)
					checkCircuitBreakerOpenErr(t, ctx, err)
				}

				return nil
			})

			require.NoError(t, g.Wait())

			expectedMetrics := `
				# HELP cortex_ingester_instance_rejected_requests_total Requests rejected for hitting per-instance limits
				# TYPE cortex_ingester_instance_rejected_requests_total counter
				cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests"} 0
				cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests_bytes"} 2
				cortex_ingester_instance_rejected_requests_total{reason="ingester_max_ingestion_rate"} 0
				cortex_ingester_instance_rejected_requests_total{reason="ingester_max_series"} 0
				cortex_ingester_instance_rejected_requests_total{reason="ingester_max_tenants"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number times the circuit breaker has entered a state
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 1
			`

			// Ensure the rejected request has been tracked in a metric.
			metricNames := []string{
				"cortex_ingester_instance_rejected_requests_total",
				"cortex_ingester_circuit_breaker_transitions_total",
			}
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
		})
	}
}

func checkCircuitBreakerOpenErr(t *testing.T, ctx context.Context, err error) {
	var cbOpenErr circuitBreakerOpenError
	require.ErrorAs(t, err, &cbOpenErr)

	var optional middleware.OptionalLogging
	require.ErrorAs(t, err, &optional)

	shouldLog, _ := optional.ShouldLog(ctx)
	require.False(t, shouldLog, "expected not to log via .ShouldLog()")

	s, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected to be able to convert to gRPC status")
	require.Equal(t, codes.Unavailable, s.Code())
}
