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
	testModeEnabled           bool          `yaml:"-"`
}

func (cfg *CircuitBreakerConfig) RegisterFlags(f *flag.FlagSet) {
	prefix := "ingester.circuit-breaker."
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable circuit breaking when making requests to ingesters")
	f.UintVar(&cfg.FailureThreshold, prefix+"failure-threshold", 10, "Max percentage of requests that can fail over period before the circuit breaker opens")
	f.UintVar(&cfg.FailureExecutionThreshold, prefix+"failure-execution-threshold", 100, "How many requests must have been executed in period for the circuit breaker to be eligible to open for the rate of failures")
	f.DurationVar(&cfg.ThresholdingPeriod, prefix+"thresholding-period", time.Minute, "Moving window of time that the percentage of failed requests is computed over")
	f.DurationVar(&cfg.CooldownPeriod, prefix+"cooldown-period", 10*time.Second, "How long the circuit breaker will stay in the open state before allowing some requests")
}

func (cfg *CircuitBreakerConfig) Validate() error {
	return nil
}

type circuitBreaker struct {
	circuitbreaker.CircuitBreaker[any]
	ingesterID string
	metrics    *ingesterMetrics
	executor   failsafe.Executor[any]
	logger     log.Logger
}

func newCircuitBreaker(ingesterID string, cfg CircuitBreakerConfig, metrics *ingesterMetrics, logger log.Logger) *circuitBreaker {
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
			level.Info(logger).Log("msg", "circuit breaker is closed", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionOpen.Inc()
			level.Info(logger).Log("msg", "circuit breaker is open", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnHalfOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionHalfOpen.Inc()
			level.Info(logger).Log("msg", "circuit breaker is half-open", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
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
		ingesterID:     ingesterID,
		metrics:        metrics,
		executor:       failsafe.NewExecutor[any](cb),
		logger:         logger,
	}
}

func (cb *circuitBreaker) Run(f func() error) error {
	err := cb.executor.Run(f)

	if err != nil && errors.Is(err, circuitbreaker.ErrOpen) {
		cb.metrics.circuitBreakerResults.WithLabelValues(cb.ingesterID, resultOpen).Inc()
		return newErrorWithStatus(newCircuitBreakerOpenError(cb.RemainingDelay()), codes.Unavailable)
	}
	return err
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

func RunWithResult[R any](ctx context.Context, cb *circuitBreaker, callback func(ctx context.Context) (*R, error)) (*R, error) {
	if cb == nil {
		return callback(ctx)
	}
	var callbackResult *R
	requestID := getRequestID(ctx)
	err := cb.Run(func() error {
		var callbackErr error
		callbackResult, callbackErr = callback(ctx)
		if callbackErr != nil {
			return callbackErr
		}
		return ctx.Err()
	})

	if ctx.Err() != nil {
		level.Error(cb.logger).Log("msg", "RunWithResult's callback ended without any error, but the error was found in the context", "ingester", cb.ingesterID, "ctxErr", ctx.Err(), "requestID", requestID)
		return callbackResult, nil
	}

	if err != nil {
		level.Error(cb.logger).Log("msg", "RunWithResult's callback ended with an error", "err", err, "requestID", requestID)
	}

	return callbackResult, err
}

func Run(ctx context.Context, cb *circuitBreaker, callback func(ctx context.Context) error) error {
	if cb == nil {
		return callback(ctx)
	}
	var callbackErr error
	requestID := getRequestID(ctx)
	err := cb.Run(func() error {
		callbackErr = callback(ctx)
		if callbackErr != nil {
			return callbackErr
		}
		return ctx.Err()
	})

	if ctx.Err() != nil {
		level.Error(cb.logger).Log("msg", "Run's callback ended without any error, but the error was found in the context", "ingester", cb.ingesterID, "ctxErr", ctx.Err(), requestID, requestID)
		return nil
	}

	if err != nil {
		level.Error(cb.logger).Log("msg", "Run's callback ended with an error", "err", err, "requestID", requestID)
	}

	return err
}
