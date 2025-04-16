package fluxus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// CircuitBreakerState represents the state of a circuit breaker.
type CircuitBreakerState int

const (
	// CircuitClosed is the normal state where requests are allowed through.
	CircuitClosed CircuitBreakerState = iota
	// CircuitOpen is the state where requests are blocked.
	CircuitOpen
	// CircuitHalfOpen is the state where a limited number of requests are allowed through to test the system.
	CircuitHalfOpen
)

// ErrCircuitOpen is returned when the circuit is open and requests are blocked.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreaker represents a circuit breaker that can prevent requests to a failing service.
type CircuitBreaker[I, O any] struct {
	stage            Stage[I, O]
	failureThreshold int
	resetTimeout     time.Duration
	halfOpenMax      int

	state                CircuitBreakerState
	failures             int
	lastError            error
	openTime             time.Time
	halfOpenCount        int
	consecutiveSuccesses int
	successThreshold     int

	mu sync.RWMutex
}

// CircuitBreakerOption is a function that configures a CircuitBreaker.
type CircuitBreakerOption[I, O any] func(*CircuitBreaker[I, O])

// WithSuccessThreshold sets the number of consecutive successes needed to close the circuit.
func WithSuccessThreshold[I, O any](threshold int) CircuitBreakerOption[I, O] {
	return func(cb *CircuitBreaker[I, O]) {
		cb.successThreshold = threshold
	}
}

// WithHalfOpenMaxRequests sets the maximum number of requests allowed when half-open.
func WithHalfOpenMaxRequests[I, O any](maxReq int) CircuitBreakerOption[I, O] {
	return func(cb *CircuitBreaker[I, O]) {
		cb.halfOpenMax = maxReq
	}
}

// NewCircuitBreaker creates a new circuit breaker with the given stage.
func NewCircuitBreaker[I, O any](
	stage Stage[I, O],
	failureThreshold int,
	resetTimeout time.Duration,
	options ...CircuitBreakerOption[I, O],
) *CircuitBreaker[I, O] {
	cb := &CircuitBreaker[I, O]{
		stage:            stage,
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		state:            CircuitClosed,
		halfOpenMax:      1,
		successThreshold: 1,
	}

	// Apply options
	for _, option := range options {
		option(cb)
	}

	return cb
}

// Process implements the Stage interface for CircuitBreaker.
func (cb *CircuitBreaker[I, O]) Process(ctx context.Context, input I) (O, error) {
	var zero O

	// Check if the circuit allows the request
	if !cb.allowRequest() {
		// If not allowed, return ErrCircuitOpen immediately
		// We need to acquire the RLock briefly to get the lastError safely if needed,
		// but ErrCircuitOpen is the primary error here.
		return zero, ErrCircuitOpen
	}

	// Track starting time for metrics
	startTime := time.Now()

	// Process the request
	output, err := cb.stage.Process(ctx, input)

	// Update circuit state based on the result
	cb.recordResult(err, time.Since(startTime))

	return output, err
}

// allowRequest checks if a request should be allowed based on the current state.
// It delegates state-specific logic to helper methods.
func (cb *CircuitBreaker[I, O]) allowRequest() bool {
	cb.mu.Lock() // Acquire write lock for potential state changes

	switch cb.state {
	case CircuitClosed:
		cb.mu.Unlock() // Unlock before returning
		return true    // Always allow when closed
	case CircuitOpen:
		// Delegate to helper, passing true as we hold the lock
		allowed := cb.checkOpenState(true)
		cb.mu.Unlock() // Unlock after helper returns
		return allowed
	case CircuitHalfOpen:
		// Delegate to helper, passing true as we hold the lock
		allowed := cb.checkHalfOpenState(true)
		cb.mu.Unlock() // Unlock after helper returns
		return allowed
	default:
		// Should not happen
		cb.mu.Unlock()
		return false
	}
}

// checkOpenState handles logic when the circuit is Open.
// Assumes the write lock is held if locked is true.
// Returns true if the request should be allowed (transitioning to HalfOpen).
func (cb *CircuitBreaker[I, O]) checkOpenState(locked bool) bool {
	// If the lock isn't held (e.g., called from State()), acquire read lock
	if !locked {
		cb.mu.RLock()
		defer cb.mu.RUnlock()
		// Cannot transition state with only read lock, so just check time
		return cb.state == CircuitOpen && time.Since(cb.openTime) > cb.resetTimeout
	}

	// --- Write lock is held ---
	// Check if the reset timeout has passed
	if time.Since(cb.openTime) > cb.resetTimeout {
		// Timeout passed, transition to HalfOpen
		cb.state = CircuitHalfOpen
		cb.halfOpenCount = 0 // Reset counter for the new half-open window
		cb.consecutiveSuccesses = 0

		// Allow the first request(s) in HalfOpen state and increment count
		if cb.halfOpenCount < cb.halfOpenMax {
			cb.halfOpenCount++ // Increment count for the request being allowed
			return true
		}
		// If halfOpenMax is 0 or somehow already reached (shouldn't happen on transition), deny
		return false
	}
	// Still open, timeout not passed
	return false
}

// checkHalfOpenState handles logic when the circuit is HalfOpen.
// Assumes the write lock is held if locked is true.
// Returns true if the request should be allowed.
func (cb *CircuitBreaker[I, O]) checkHalfOpenState(locked bool) bool {
	// If the lock isn't held, acquire read lock
	if !locked {
		cb.mu.RLock()
		defer cb.mu.RUnlock()
		// Just check if we are below the limit
		return cb.state == CircuitHalfOpen && cb.halfOpenCount < cb.halfOpenMax
	}

	// --- Write lock is held ---
	// Allow if count is below max, and increment count
	if cb.halfOpenCount < cb.halfOpenMax {
		cb.halfOpenCount++ // Increment count for the request being allowed
		return true
	}
	// Max requests reached for the current half-open window
	return false
}

// recordResult updates the circuit state based on the result of a request.
func (cb *CircuitBreaker[I, O]) recordResult(err error, _ time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// The state could have changed between allowRequest unlocking and this lock acquisition,
	// but we primarily care about the outcome based on the state *during* the request.
	// allowRequest already determined if the request should run based on the state *then*.

	if err != nil {
		// --- Handle Failure ---
		//nolint:exhaustive // Exhaustive check is not needed here, as we only care about Closed/HalfOpen states
		switch cb.state {
		case CircuitHalfOpen:
			// Failure in half-open state immediately re-opens the circuit
			cb.tripOpen() // tripOpen resets necessary counters
		case CircuitClosed:
			cb.failures++
			cb.lastError = err
			if cb.failures >= cb.failureThreshold {
				cb.tripOpen() // tripOpen resets necessary counters
			}
		}
		// If state is Open, a failure doesn't change the state or counters.
	} else {
		// --- Handle Success ---
		//nolint:exhaustive // Exhaustive check is not needed here, as we only care about Closed/HalfOpen states
		switch cb.state {
		case CircuitHalfOpen:
			cb.consecutiveSuccesses++
			// Check if enough successes to close the circuit
			if cb.consecutiveSuccesses >= cb.successThreshold {
				cb.closeCircuit() // closeCircuit resets necessary counters
			}
			// Note: halfOpenCount was incremented in allowRequest
		case CircuitClosed:
			// Reset failure count on success when closed
			cb.failures = 0
			cb.lastError = nil
			// consecutiveSuccesses is not tracked/relevant when closed
		}
		// If state is Open, a success shouldn't normally happen, but if it did, ignore it.
	}
}

// tripOpen transitions the circuit to the open state.
func (cb *CircuitBreaker[I, O]) tripOpen() {
	// Assumes lock is already held by caller (recordResult)
	cb.state = CircuitOpen
	cb.openTime = time.Now()
	cb.halfOpenCount = 0        // Reset for the next potential HalfOpen window
	cb.consecutiveSuccesses = 0 // Reset successes
	// failures count is implicitly reset when moving back to Closed later
}

// closeCircuit transitions the circuit to the closed state.
func (cb *CircuitBreaker[I, O]) closeCircuit() {
	// Assumes lock is already held by caller (recordResult)
	cb.state = CircuitClosed
	cb.failures = 0             // Reset failures
	cb.lastError = nil          // Clear last error
	cb.halfOpenCount = 0        // Reset just in case
	cb.consecutiveSuccesses = 0 // Reset successes
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker[I, O]) State() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// LastError returns the last error that caused the circuit to open.
func (cb *CircuitBreaker[I, O]) LastError() error {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.lastError
}

// Reset forces the circuit breaker back to the closed state.
func (cb *CircuitBreaker[I, O]) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.failures = 0
	cb.halfOpenCount = 0
	cb.consecutiveSuccesses = 0
	cb.lastError = nil
}

// Retry is a stage that retries the given stage a specified number of times
// if it fails, with an optional backoff strategy.
type Retry[I, O any] struct {
	stage       Stage[I, O]
	maxAttempts int
	shouldRetry func(error) bool
	backoff     func(attempt int) int // Returns delay in milliseconds
	errHandler  func(error) error
}

// NewRetry creates a new retry stage.
func NewRetry[I, O any](stage Stage[I, O], maxAttempts int) *Retry[I, O] {
	return &Retry[I, O]{
		stage:       stage,
		maxAttempts: maxAttempts,
		shouldRetry: func(_ error) bool { return true }, // Retry all errors by default
		backoff:     func(_ int) int { return 0 },       // No backoff by default
		errHandler:  func(err error) error { return err },
	}
}

// WithShouldRetry adds a predicate to determine if an error should be retried.
func (r *Retry[I, O]) WithShouldRetry(shouldRetry func(error) bool) *Retry[I, O] {
	r.shouldRetry = shouldRetry
	return r
}

// WithBackoff adds a backoff strategy to the retry stage.
func (r *Retry[I, O]) WithBackoff(backoff func(attempt int) int) *Retry[I, O] {
	r.backoff = backoff
	return r
}

// WithErrorHandler adds a custom error handler to the retry stage.
func (r *Retry[I, O]) WithErrorHandler(handler func(error) error) *Retry[I, O] {
	r.errHandler = handler
	return r
}

// Process implements the Stage interface for Retry.
func (r *Retry[I, O]) Process(ctx context.Context, input I) (O, error) {
	var result O
	var lastErr error

	for attempt := 0; attempt < r.maxAttempts; attempt++ {
		// Check for context cancellation
		if ctx.Err() != nil {
			if lastErr != nil {
				return result, r.errHandler(fmt.Errorf("retry failed after %d attempts: %w (context cancelled: %w)",
					attempt, lastErr, ctx.Err()))
			}
			return result, r.errHandler(ctx.Err())
		}

		// Try to process
		var err error
		result, err = r.stage.Process(ctx, input)
		if err == nil {
			return result, nil // Success
		}

		// Handle error
		lastErr = err
		if !r.shouldRetry(err) {
			return result, r.errHandler(fmt.Errorf("retry giving up after %d attempts: %w",
				attempt+1, err))
		}

		// Apply backoff if this is not the last attempt
		if attempt < r.maxAttempts-1 {
			backoffMs := r.backoff(attempt)
			if backoffMs > 0 {
				select {
				case <-ctx.Done():
					return result, r.errHandler(fmt.Errorf("retry interrupted during backoff: %w", ctx.Err()))
				case <-time.After(time.Duration(backoffMs) * time.Millisecond):
					// Continue after backoff
				}
			}
		}
	}

	return result, r.errHandler(fmt.Errorf("retry exhausted %d attempts: %w", r.maxAttempts, lastErr))
}

// Timeout adds a timeout to a stage.
type Timeout[I, O any] struct {
	stage      Stage[I, O]
	timeout    time.Duration
	errHandler func(error) error
}

// NewTimeout creates a new timeout stage.
func NewTimeout[I, O any](stage Stage[I, O], timeout time.Duration) *Timeout[I, O] {
	return &Timeout[I, O]{
		stage:      stage,
		timeout:    timeout,
		errHandler: func(err error) error { return err },
	}
}

// WithErrorHandler adds a custom error handler to the timeout stage.
func (t *Timeout[I, O]) WithErrorHandler(handler func(error) error) *Timeout[I, O] {
	t.errHandler = handler
	return t
}

// Process implements the Stage interface for Timeout.
func (t *Timeout[I, O]) Process(ctx context.Context, input I) (O, error) {
	var result O

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	// Process with timeout
	var err error
	result, err = t.stage.Process(ctx, input)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return result, t.errHandler(fmt.Errorf("stage timed out after %v: %w", t.timeout, err))
		}
		return result, t.errHandler(err)
	}

	return result, nil
}

// DLQHandler defines the interface for handling items sent to a Dead Letter Queue.
type DLQHandler[I any] interface {
	// Handle processes an item that failed processing along with the error that caused it.
	// Implementations should be robust and potentially handle their own errors during DLQ processing.
	// The returned error typically indicates a failure in the DLQ handling itself,
	// which the DeadLetterQueue stage will log by default.
	Handle(ctx context.Context, item I, processingError error) error
}

// DLQHandlerFunc is an adapter to allow the use of ordinary functions as DLQHandlers.
// If f is a func(context.Context, I, error) error, DLQHandlerFunc[I](f) is a DLQHandler[I].
type DLQHandlerFunc[I any] func(ctx context.Context, item I, processingError error) error

// Handle calls f(ctx, item, processingError).
func (f DLQHandlerFunc[I]) Handle(ctx context.Context, item I, processingError error) error {
	return f(ctx, item, processingError)
}

// DeadLetterQueue is a stage decorator that sends items failing processing to a Dead Letter Queue.
// It wraps an existing stage and intercepts errors. If an error occurs and meets the
// criteria defined by `shouldDLQ`, the input item and the error are passed to the `dlqHandler`.
// The original error from the wrapped stage is always returned.
type DeadLetterQueue[I, O any] struct {
	stage       Stage[I, O]
	dlqHandler  DLQHandler[I]
	shouldDLQ   func(error) bool // Predicate to decide if an error triggers DLQ
	logDLQError func(error)      // Function to log errors from the DLQ handler itself
}

// DeadLetterQueueOption configures a DeadLetterQueue.
type DeadLetterQueueOption[I, O any] func(*DeadLetterQueue[I, O])

// WithDLQHandler sets the handler for dead-lettered items. This option is mandatory
// when creating a DeadLetterQueue.
func WithDLQHandler[I, O any](handler DLQHandler[I]) DeadLetterQueueOption[I, O] {
	return func(dlq *DeadLetterQueue[I, O]) {
		if handler == nil {
			// Panic early if a nil handler is explicitly passed via the option.
			panic("fluxus.WithDLQHandler: provided DLQHandler cannot be nil")
		}
		dlq.dlqHandler = handler
	}
}

// WithShouldDLQ sets a custom predicate to determine which errors trigger sending
// an item to the DLQ. By default, context cancellation and deadline errors are excluded.
// The predicate receives the error returned by the wrapped stage.
func WithShouldDLQ[I, O any](predicate func(error) bool) DeadLetterQueueOption[I, O] {
	return func(dlq *DeadLetterQueue[I, O]) {
		if predicate == nil {
			// Reset to default if nil is passed
			dlq.shouldDLQ = defaultShouldDLQ
		} else {
			dlq.shouldDLQ = predicate
		}
	}
}

// WithDLQErrorLogger sets a custom logger for errors that occur within the DLQ handler itself.
func WithDLQErrorLogger[I, O any](logger func(error)) DeadLetterQueueOption[I, O] {
	return func(dlq *DeadLetterQueue[I, O]) {
		if logger == nil {
			// Reset to default if nil is passed
			dlq.logDLQError = defaultLogDLQError
		} else {
			dlq.logDLQError = logger
		}
	}
}

// defaultShouldDLQ is the default predicate for WithShouldDLQ.
// It excludes context cancellation and deadline exceeded errors from triggering the DLQ,
// as these usually indicate external issues rather than item-specific processing failures.
func defaultShouldDLQ(err error) bool {
	// Also exclude ErrCircuitOpen as it's an expected flow control error, not an item processing error.
	// Also exclude ErrItemFiltered and ErrNoRouteMatched as these are expected filtering/routing outcomes.
	return !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, ErrCircuitOpen) &&
		!errors.Is(err, ErrItemFiltered) &&
		!errors.Is(err, ErrNoRouteMatched)
}

// defaultLogDLQError logs errors from the DLQ handler using the standard logger.
func defaultLogDLQError(err error) {
	log.Printf("fluxus: DLQ handler error: %v", err)
}

// NewDeadLetterQueue creates a new DeadLetterQueue stage decorator.
// It wraps the provided `stage` and sends items that cause errors (matching the `shouldDLQ`
// predicate) to the specified `DLQHandler`.
//
// A non-nil `DLQHandler` must be provided using the `WithDLQHandler` option, otherwise
// this function will panic, as the stage is non-functional without a destination.
func NewDeadLetterQueue[I, O any](stage Stage[I, O], options ...DeadLetterQueueOption[I, O]) *DeadLetterQueue[I, O] {
	dlq := &DeadLetterQueue[I, O]{
		stage:       stage,
		shouldDLQ:   defaultShouldDLQ,
		logDLQError: defaultLogDLQError,
		// dlqHandler is initially nil, must be set by option
	}

	for _, option := range options {
		option(dlq)
	}

	// Ensure a handler was provided via options. Panic if not, as it's a configuration error.
	if dlq.dlqHandler == nil {
		panic("fluxus.NewDeadLetterQueue: DLQHandler must be provided using the WithDLQHandler option")
	}

	return dlq
}

// Process implements the Stage interface for DeadLetterQueue.
// It executes the wrapped stage. If the stage returns an error that satisfies the
// `shouldDLQ` predicate, it attempts to send the input item and the error to the
// configured `DLQHandler`. It then returns the original result and error from the
// wrapped stage, regardless of the DLQ outcome. Errors from the DLQ handler itself are logged.
func (dlq *DeadLetterQueue[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Process the underlying stage
	output, err := dlq.stage.Process(ctx, input)

	// If an error occurred AND the predicate determines it should be sent to DLQ...
	if err != nil && dlq.shouldDLQ(err) {
		// Attempt to handle the item in the DLQ.
		// We pass the original context, allowing the DLQ handler to respect cancellation if needed.
		if dlqErr := dlq.dlqHandler.Handle(ctx, input, err); dlqErr != nil {
			// Log the error that occurred within the DLQ handler itself.
			dlq.logDLQError(fmt.Errorf("failed to handle item in DLQ: %w (original processing error: %w)", dlqErr, err))
			// Note: We log the DLQ handler error but still return the original processing error below.
		}
	}

	// Always return the original result and error from the underlying stage.
	// The DLQ mechanism is a side effect for handling failures, not for altering the primary result/error flow.
	return output, err
}

// Ensure DeadLetterQueue implements the Stage interface.
var _ Stage[string, string] = (*DeadLetterQueue[string, string])(nil)
