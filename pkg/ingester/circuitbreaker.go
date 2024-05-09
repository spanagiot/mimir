package ingester

import (
	"context"
	"flag"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/spanlogger"

	"github.com/grafana/mimir/pkg/ingester/client"

	"github.com/grafana/mimir/pkg/mimirpb"

	"google.golang.org/grpc/codes"
)

const (
	resultSuccess = "success"
	resultError   = "error"
	resultOpen    = "circuit_breaker_open"
)

type CircuitBreakerConfig struct {
	Enabled                   bool          `yaml:"enabled" category:"experimental"`
	FailureThreshold          uint          `yaml:"failure_threshold" category:"experimental"`
	FailureExecutionThreshold uint          `yaml:"failure_execution_threshold" category:"experimental"`
	ThresholdingPeriod        time.Duration `yaml:"thresholding_period" category:"experimental"`
	CooldownPeriod            time.Duration `yaml:"cooldown_period" category:"experimental"`
	InitialDelay              time.Duration `yaml:"initial_delay" category:"experimental"`
	testModeEnabled           bool          `yaml:"-"`
}

func (cfg *CircuitBreakerConfig) RegisterFlags(f *flag.FlagSet) {
	prefix := "ingester.circuit-breaker."
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable circuit breaking when making requests to ingesters")
	f.UintVar(&cfg.FailureThreshold, prefix+"failure-threshold", 10, "Max percentage of requests that can fail over period before the circuit breaker opens")
	f.UintVar(&cfg.FailureExecutionThreshold, prefix+"failure-execution-threshold", 100, "How many requests must have been executed in period for the circuit breaker to be eligible to open for the rate of failures")
	f.DurationVar(&cfg.ThresholdingPeriod, prefix+"thresholding-period", time.Minute, "Moving window of time that the percentage of failed requests is computed over")
	f.DurationVar(&cfg.CooldownPeriod, prefix+"cooldown-period", 10*time.Second, "How long the circuit breaker will stay in the open state before allowing some requests")
	f.DurationVar(&cfg.InitialDelay, prefix+"initial-delay", 0, "How long the circuit breaker should wait between creation and starting up. During that time both failures and successes will not be counted.")
}

func (cfg *CircuitBreakerConfig) Validate() error {
	return nil
}

type circuitBreaker struct {
	circuitbreaker.CircuitBreaker[any]
	ingester  *Ingester
	executor  failsafe.Executor[any]
	startTime time.Time
}

func newCircuitBreaker(ingester *Ingester) *circuitBreaker {
	ingesterID := ingester.cfg.IngesterRing.InstanceID
	cfg := ingester.cfg.CircuitBreakerConfig
	metrics := ingester.metrics
	// Initialize each of the known labels for circuit breaker metrics for this particular ingester
	transitionOpen := metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.OpenState.String())
	transitionHalfOpen := metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.HalfOpenState.String())
	transitionClosed := metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.ClosedState.String())
	countSuccess := metrics.circuitBreakerResults.WithLabelValues(ingesterID, resultSuccess)
	countError := metrics.circuitBreakerResults.WithLabelValues(ingesterID, resultError)

	cbBuilder := circuitbreaker.Builder[any]().
		WithFailureThreshold(cfg.FailureThreshold).
		WithDelay(cfg.CooldownPeriod).
		OnFailure(func(failsafe.ExecutionEvent[any]) {
			countError.Inc()
		}).
		OnSuccess(func(failsafe.ExecutionEvent[any]) {
			countSuccess.Inc()
		}).
		OnClose(func(event circuitbreaker.StateChangedEvent) {
			transitionClosed.Inc()
			level.Info(ingester.logger).Log("msg", "circuit breaker is closed", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionOpen.Inc()
			level.Info(ingester.logger).Log("msg", "circuit breaker is open", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnHalfOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionHalfOpen.Inc()
			level.Info(ingester.logger).Log("msg", "circuit breaker is half-open", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		HandleIf(func(_ any, err error) bool { return isFailure(err) })

	if cfg.testModeEnabled {
		cbBuilder = cbBuilder.WithFailureThreshold(cfg.FailureThreshold)
	} else {
		cbBuilder = cbBuilder.WithFailureRateThreshold(cfg.FailureThreshold, cfg.FailureExecutionThreshold, cfg.ThresholdingPeriod)
	}

	cb := cbBuilder.Build()
	return &circuitBreaker{
		CircuitBreaker: cb,
		ingester:       ingester,
		executor:       failsafe.NewExecutor[any](cb),
		startTime:      time.Now().Add(cfg.InitialDelay),
	}
}

func isFailure(err error) bool {
	if err == nil {
		return false
	}

	// We only consider timeouts or ingester hitting a per-instance limit
	// to be errors worthy of tripping the circuit breaker since these
	// are specific to a particular ingester, not a user or request.

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var ingesterErr ingesterError
	if errors.As(err, &ingesterErr) {
		return ingesterErr.errorCause() == mimirpb.INSTANCE_LIMIT
	}

	return false
}

func (cb *circuitBreaker) ingesterID() string {
	return cb.ingester.cfg.IngesterRing.InstanceID
}

func (cb *circuitBreaker) logger() log.Logger {
	return cb.ingester.logger
}

func (cb *circuitBreaker) metrics() *ingesterMetrics {
	return cb.ingester.metrics
}

func (cb *circuitBreaker) run(ctx context.Context, f func() error) error {
	err := cb.executor.Run(f)
	if err != nil && errors.Is(err, circuitbreaker.ErrOpen) {
		cb.metrics().circuitBreakerResults.WithLabelValues(cb.ingesterID(), resultOpen).Inc()
		return newErrorWithStatus(newCircuitBreakerOpenError(cb.RemainingDelay()), codes.Unavailable)
	}
	return cb.processError(ctx, err)
}

func (cb *circuitBreaker) get(ctx context.Context, f func() (any, error)) (any, error) {
	res, err := cb.executor.Get(f)
	if err != nil && errors.Is(err, circuitbreaker.ErrOpen) {
		cb.metrics().circuitBreakerResults.WithLabelValues(cb.ingesterID(), resultOpen).Inc()
		return res, newErrorWithStatus(newCircuitBreakerOpenError(cb.RemainingDelay()), codes.Unavailable)
	}
	return res, cb.processError(ctx, err)
}

func (cb *circuitBreaker) processError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	requestID := getRequestID(ctx)
	if err == ctx.Err() {
		level.Error(cb.logger()).Log("msg", "Run's callback completed with an error found in the context", "ingester", cb.ingesterID(), "ctxErr", ctx.Err(), "ingesterState", cb.ingester.State().String(), "requestID", requestID, "ingesterState", cb.ingester.State().String())
		// ctx.Err() was registered with the circuit breaker's executor, but we don't propagate it
		return nil
	}

	level.Error(cb.logger()).Log("msg", "Run's callback completed with an error", "ingester", cb.ingesterID(), "err", err, "ingesterState", cb.ingester.State().String(), "requestID", requestID)
	return err
}

func nonCancelableContext(parent context.Context) context.Context {
	ctx := parent
	if deadline, ok := parent.Deadline(); ok {
		ctx, _ = context.WithDeadline(context.WithoutCancel(parent), deadline)
		if requestID := getRequestID(ctx); requestID == "unknown" {
			ctx = context.WithValue(ctx, "request-id", getRequestID(parent))
		}
	}
	return ctx
}

func (cb *circuitBreaker) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	ctx = nonCancelableContext(ctx)

	callbackResult, callbackErr := cb.get(ctx, func() (any, error) {
		callbackResult, callbackErr := cb.ingester.push(ctx, req)
		if callbackErr != nil {
			return callbackResult, callbackErr
		}
		// We return ctx.Err() in order to register it with the circuit breaker's executor
		return callbackResult, ctx.Err()
	})

	if callbackResult == nil {
		return nil, callbackErr
	}
	return callbackResult.(*mimirpb.WriteResponse), callbackErr
}

func (cb *circuitBreaker) QueryStream(ctx context.Context, req *client.QueryRequest, stream client.Ingester_QueryStreamServer, spanlog *spanlogger.SpanLogger) error {
	ctx = nonCancelableContext(ctx)

	return cb.run(ctx, func() error {
		err := cb.ingester.queryStream(ctx, req, stream, spanlog)
		if err != nil {
			return err
		}
		// We return ctx.Err() in order to register it with the circuit breaker's executor
		return ctx.Err()
	})
}
