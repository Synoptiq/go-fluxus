package fluxus

import (
	"context"
	"errors"
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

	// Check if the circuit is open
	if !cb.allowRequest() {
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

// allowRequest checks if a request should be allowed through the circuit breaker.
func (cb *CircuitBreaker[I, O]) allowRequest() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitClosed:
		// When closed, allow all requests
		return true
	case CircuitOpen:
		// When open, check if enough time has passed to try again
		if time.Since(cb.openTime) > cb.resetTimeout {
			// Need to transition to half-open, but require a write lock
			cb.mu.RUnlock()
			cb.transitionToHalfOpen()
			cb.mu.RLock()
			return cb.halfOpenCount < cb.halfOpenMax
		}
		return false
	case CircuitHalfOpen:
		// When half-open, allow a limited number of requests
		return cb.halfOpenCount < cb.halfOpenMax
	default:
		return false
	}
}

// transitionToHalfOpen transitions the circuit from open to half-open.
func (cb *CircuitBreaker[I, O]) transitionToHalfOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Only transition if we're still open and enough time has passed
	if cb.state == CircuitOpen && time.Since(cb.openTime) > cb.resetTimeout {
		cb.state = CircuitHalfOpen
		cb.halfOpenCount = 0
		cb.consecutiveSuccesses = 0
	}
}

// recordResult updates the circuit state based on the result of a request.
func (cb *CircuitBreaker[I, O]) recordResult(err error, _ time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		// Record failure
		switch cb.state {
		case CircuitClosed:
			cb.failures++
			cb.lastError = err
			if cb.failures >= cb.failureThreshold {
				cb.tripOpen()
			}
		case CircuitHalfOpen:
			cb.tripOpen()
		case CircuitOpen:
			// No state change needed
		}
		cb.consecutiveSuccesses = 0
	} else {
		// Record success
		switch cb.state {
		case CircuitClosed:
			cb.failures = 0
		case CircuitHalfOpen:
			cb.consecutiveSuccesses++
			cb.halfOpenCount++
			if cb.consecutiveSuccesses >= cb.successThreshold {
				cb.closeCircuit()
			}
		case CircuitOpen:
			// No state change needed
		}
	}
}

// tripOpen transitions the circuit to the open state.
func (cb *CircuitBreaker[I, O]) tripOpen() {
	cb.state = CircuitOpen
	cb.openTime = time.Now()
	cb.halfOpenCount = 0
	cb.consecutiveSuccesses = 0
}

// closeCircuit transitions the circuit to the closed state.
func (cb *CircuitBreaker[I, O]) closeCircuit() {
	cb.state = CircuitClosed
	cb.failures = 0
	cb.halfOpenCount = 0
	cb.consecutiveSuccesses = 0
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
