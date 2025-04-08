package fluxus

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter is a stage decorator that limits the rate of requests to a stage.
type RateLimiter[I, O any] struct {
	stage   Stage[I, O]
	limiter *rate.Limiter
	timeout time.Duration
	mu      sync.RWMutex
}

// RateLimiterOption is a function that configures a RateLimiter.
type RateLimiterOption[I, O any] func(*RateLimiter[I, O])

// WithLimiterTimeout sets a timeout for waiting for the limiter.
func WithLimiterTimeout[I, O any](timeout time.Duration) RateLimiterOption[I, O] {
	return func(rl *RateLimiter[I, O]) {
		rl.timeout = timeout
	}
}

// NewRateLimiter creates a new rate limiter with the given stage.
// r is the rate limit (e.g., 10 means 10 requests per second)
// b is the maximum burst size (maximum number of tokens that can be consumed in a single burst)
func NewRateLimiter[I, O any](
	stage Stage[I, O],
	r rate.Limit,
	b int,
	options ...RateLimiterOption[I, O],
) *RateLimiter[I, O] {
	rl := &RateLimiter[I, O]{
		stage:   stage,
		limiter: rate.NewLimiter(r, b),
		timeout: time.Second, // Default timeout
	}

	// Apply options
	for _, option := range options {
		option(rl)
	}

	return rl
}

// Process implements the Stage interface for RateLimiter.
func (rl *RateLimiter[I, O]) Process(ctx context.Context, input I) (O, error) {
	var zero O

	// Wait for rate limiter or timeout
	limiterCtx := ctx
	if rl.timeout > 0 {
		var cancel context.CancelFunc
		limiterCtx, cancel = context.WithTimeout(ctx, rl.timeout)
		defer cancel()
	}

	// Try to acquire a token
	if err := rl.limiter.Wait(limiterCtx); err != nil {
		return zero, err
	}

	// Forward the request to the underlying stage
	output, err := rl.stage.Process(ctx, input)

	return output, err
}

// SetLimit updates the rate limit.
func (rl *RateLimiter[I, O]) SetLimit(r rate.Limit) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limiter.SetLimit(r)
}

// SetBurst updates the burst limit.
func (rl *RateLimiter[I, O]) SetBurst(b int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limiter.SetBurst(b)
}

// Allow checks if a request can be processed without blocking.
func (rl *RateLimiter[I, O]) Allow() bool {
	return rl.limiter.Allow()
}
