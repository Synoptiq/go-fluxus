package fluxus_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/synoptiq/go-fluxus"
)

// --- Mock Service (Keep as is) ---
type mockExternalService struct {
	failureCount    int
	maxFailures     int
	mu              sync.Mutex
	processingDelay time.Duration // Added delay for concurrency tests
	failOnDemand    bool          // Added flag to force failure
	forceFailErr    error         // Specific error for forced failures
	callCount       atomic.Int32  // Track total calls for retry tests
}

// Process simulates an external service call that can fail
func (s *mockExternalService) Process(_ context.Context, input string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callCount.Add(1) // Increment call count

	if s.processingDelay > 0 {
		time.Sleep(s.processingDelay)
	}

	if s.failOnDemand {
		return "", s.forceFailErr // Use the specific error
	}

	s.failureCount++
	if s.failureCount <= s.maxFailures {
		return "", fmt.Errorf("service failure %d", s.failureCount)
	}

	return fmt.Sprintf("processed: %s", input), nil
}

func (s *mockExternalService) Reset() {
	s.mu.Lock()
	s.failureCount = 0
	s.failOnDemand = false
	s.mu.Unlock()
}

func (s *mockExternalService) ForceFailure(fail bool, err error) {
	s.mu.Lock()
	s.failOnDemand = fail
	s.forceFailErr = err
	s.mu.Unlock()
}

func (s *mockExternalService) CallCount() int32 {
	return s.callCount.Load()
}

// --- Mock Stage Function for Retry ---
// Simpler mock just focusing on attempt-based failure
func newRetryMockStage(
	attemptsToSucceed int,
	failWithError error,
	delay time.Duration,
) fluxus.StageFunc[string, string] {
	var callCount atomic.Int32
	return func(ctx context.Context, input string) (string, error) {
		count := callCount.Add(1)

		if delay > 0 {
			select {
			case <-time.After(delay):
				// continue processing
			case <-ctx.Done():
				return "", ctx.Err() // Respect context cancellation during delay
			}
		}

		if attemptsToSucceed > 0 && count >= int32(attemptsToSucceed) {
			return fmt.Sprintf("success on attempt %d: %s", count, input), nil
		}

		err := failWithError
		if err == nil {
			err = fmt.Errorf("mock failure on attempt %d", count)
		}
		return "", err
	}
}

func newDelayedMockStage(delay time.Duration, succeed bool, returnErr error) fluxus.StageFunc[string, string] {
	return func(ctx context.Context, input string) (string, error) {
		select {
		case <-time.After(delay):
			// Delay completed
			if succeed {
				return fmt.Sprintf("processed after delay: %s", input), nil
			}
			if returnErr == nil {
				return "", errors.New("mock stage failed after delay")
			}
			return "", returnErr
		case <-ctx.Done():
			// Context cancelled during delay
			return "", ctx.Err()
		}
	}
}

// TestCircuitBreakerPipeline demonstrates circuit breaker in a pipeline context
func TestCircuitBreakerPipeline(t *testing.T) {
	scenarios := []struct {
		name               string
		maxFailures        int // How many times the mock service should fail before succeeding
		failureThreshold   int // CB threshold
		resetTimeout       time.Duration
		callsToMake        int // How many times to call Process
		expectedErrors     int // How many errors are expected from the CB stage
		expectedResults    int // How many successful results are expected
		expectedFinalState fluxus.CircuitBreakerState
	}{
		{
			name:               "StaysClosed_UnderThreshold",
			maxFailures:        2,
			failureThreshold:   3,
			resetTimeout:       100 * time.Millisecond,
			callsToMake:        5, // 2 failures, 3 successes
			expectedErrors:     2,
			expectedResults:    3,
			expectedFinalState: fluxus.CircuitClosed,
		},
		{
			name:               "Opens_AtThreshold",
			maxFailures:        5, // Will always fail in this test run
			failureThreshold:   3,
			resetTimeout:       100 * time.Millisecond,
			callsToMake:        5, // 3 real failures, 2 CB errors
			expectedErrors:     5, // 3 service errors + 2 CB errors
			expectedResults:    0,
			expectedFinalState: fluxus.CircuitOpen,
		},
		{
			name:               "Opens_ThresholdOne",
			maxFailures:        5,
			failureThreshold:   1,
			resetTimeout:       100 * time.Millisecond,
			callsToMake:        3, // 1 real failure, 2 CB errors
			expectedErrors:     3,
			expectedResults:    0,
			expectedFinalState: fluxus.CircuitOpen,
		},
	}

	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			service := &mockExternalService{maxFailures: tc.maxFailures}
			serviceStage := fluxus.StageFunc[string, string](service.Process)
			circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, tc.failureThreshold, tc.resetTimeout)
			pipeline := fluxus.NewPipeline(circuitBreaker)
			ctx := context.Background()

			// --- FIX: Start the pipeline ---
			err := pipeline.Start(ctx)
			require.NoError(t, err, "Pipeline should start without error")
			// --- FIX: Ensure pipeline is stopped ---
			defer func() {
				stopErr := pipeline.Stop(ctx)
				assert.NoError(t, stopErr, "Pipeline should stop without error")
			}()

			var results []string
			var errs []error

			for i := 0; i < tc.callsToMake; i++ {
				result, pipelineProcessErr := pipeline.Process(ctx, fmt.Sprintf("input-%d", i))
				if pipelineProcessErr != nil {
					errs = append(errs, pipelineProcessErr)
				} else {
					results = append(results, result)
				}
				// Optional: Short sleep to prevent tight loop issues if needed
				// time.Sleep(5 * time.Millisecond)
			}

			assert.Len(t, errs, tc.expectedErrors, "Number of errors mismatch")
			assert.Len(t, results, tc.expectedResults, "Number of results mismatch")
			assert.Equal(t, tc.expectedFinalState, circuitBreaker.State(), "Final circuit breaker state mismatch")

			// Check if CB error is returned when open
			if tc.expectedFinalState == fluxus.CircuitOpen {
				require.NotEmpty(t, errs, "Should have errors when circuit is open")
				lastErr := errs[len(errs)-1]
				require.ErrorIs(t, lastErr, fluxus.ErrCircuitOpen, "Expected ErrCircuitOpen when state is Open")
			}
		})
	}
}

// TestCircuitBreakerRecovery tests the recovery mechanism (Open -> HalfOpen -> Closed)
func TestCircuitBreakerRecovery(t *testing.T) {
	service := &mockExternalService{maxFailures: 3} // Fails 3 times, then succeeds
	serviceStage := fluxus.StageFunc[string, string](service.Process)
	resetTimeout := 50 * time.Millisecond
	// Use options to require more than one success/attempt to clearly see HalfOpen state
	circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, 3, resetTimeout,
		fluxus.WithSuccessThreshold[string, string](2),    // Require 2 successes to close
		fluxus.WithHalfOpenMaxRequests[string, string](5), // Allow multiple attempts
	)
	ctx := context.Background()

	// 1. Trigger failures to open the circuit
	for i := 0; i < 3; i++ {
		_, err := circuitBreaker.Process(ctx, fmt.Sprintf("fail-%d", i))
		require.Error(t, err, "Expected error on iteration %d", i)
	}
	require.Equal(t, fluxus.CircuitOpen, circuitBreaker.State(), "Circuit should be open after 3 failures")

	// 2. Verify it returns ErrCircuitOpen immediately
	_, err := circuitBreaker.Process(ctx, "fail-immediate")
	require.ErrorIs(t, err, fluxus.ErrCircuitOpen, "Expected ErrCircuitOpen while open")

	// 3. Wait for reset timeout
	time.Sleep(resetTimeout + 10*time.Millisecond) // Add buffer

	// 4. Make the first request attempt AFTER the timeout.
	//    This call to Process should trigger the transition Open -> HalfOpen internally.
	//    The request itself should succeed as the service recovers.
	result1, err1 := circuitBreaker.Process(ctx, "half-open-attempt-1")
	require.NoError(t, err1, "Expected first half-open attempt to succeed")
	assert.Equal(t, "processed: half-open-attempt-1", result1)

	// 5. NOW check the state. It should be HalfOpen because successThreshold is 2.
	require.Equal(
		t,
		fluxus.CircuitHalfOpen,
		circuitBreaker.State(),
		"Circuit should be half-open after first successful attempt",
	)

	// 6. Make the second successful request in HalfOpen state.
	result2, err2 := circuitBreaker.Process(ctx, "half-open-attempt-2")
	require.NoError(t, err2, "Expected second half-open attempt to succeed")
	assert.Equal(t, "processed: half-open-attempt-2", result2)

	// 7. Verify circuit is now closed because successThreshold (2) is met.
	require.Equal(
		t,
		fluxus.CircuitClosed,
		circuitBreaker.State(),
		"Circuit should close after meeting success threshold",
	)

	// 8. Subsequent requests should also succeed in Closed state.
	result3, err3 := circuitBreaker.Process(ctx, "post-recovery-input")
	require.NoError(t, err3, "Expected successful request after recovery")
	assert.Equal(t, "processed: post-recovery-input", result3)
}

// TestCircuitBreakerHalfOpenFailure tests Open -> HalfOpen -> Open transition
func TestCircuitBreakerHalfOpenFailure(t *testing.T) {
	service := &mockExternalService{maxFailures: 3} // Fails 3 times initially
	serviceStage := fluxus.StageFunc[string, string](service.Process)
	resetTimeout := 50 * time.Millisecond
	circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, 3, resetTimeout) // Defaults are fine here
	ctx := context.Background()

	// 1. Trigger failures to open the circuit
	for i := 0; i < 3; i++ {
		_, err := circuitBreaker.Process(ctx, fmt.Sprintf("fail-%d", i))
		require.Error(t, err, "Expected error on iteration %d", i)
	}
	require.Equal(t, fluxus.CircuitOpen, circuitBreaker.State(), "Circuit should be open after 3 failures")

	// 2. Wait for reset timeout
	time.Sleep(resetTimeout + 10*time.Millisecond)

	// 3. Force the service to fail again for the half-open check
	forcedErr := errors.New("forced failure in half-open")
	service.ForceFailure(true, forcedErr)

	// 4. Make the first request attempt AFTER the timeout.
	//    This call to Process should trigger the transition Open -> HalfOpen internally.
	//    The request itself should FAIL because we forced the service to fail.
	_, err := circuitBreaker.Process(ctx, "half-open-fail-input")
	require.Error(t, err, "Expected error on failing half-open attempt")
	require.ErrorIs(t, err, forcedErr, "Expected the specific forced error") // Check it's the underlying error

	// 5. Verify circuit is now Open again because the half-open attempt failed.
	require.Equal(
		t,
		fluxus.CircuitOpen,
		circuitBreaker.State(),
		"Circuit should re-open after failed half-open request",
	)

	// 6. Verify it returns ErrCircuitOpen immediately again
	_, err = circuitBreaker.Process(ctx, "fail-immediate-again")
	require.ErrorIs(t, err, fluxus.ErrCircuitOpen, "Expected ErrCircuitOpen after re-opening")
}

// TestCircuitBreakerConcurrency tests thread safety and state transitions under load
func TestCircuitBreakerConcurrency(t *testing.T) {
	concurrencyLevel := 50
	requestsPerGoroutine := 10
	failureThreshold := 10
	resetTimeout := 50 * time.Millisecond

	// Service will fail failureThreshold times, then succeed. Add slight delay.
	service := &mockExternalService{
		maxFailures:     failureThreshold,
		processingDelay: 1 * time.Millisecond,
	}
	serviceStage := fluxus.StageFunc[string, string](service.Process)
	circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, failureThreshold, resetTimeout)
	ctx := context.Background()

	var wg sync.WaitGroup
	var totalErrors atomic.Int64
	var circuitOpenErrors atomic.Int64

	wg.Add(concurrencyLevel)

	for i := 0; i < concurrencyLevel; i++ {
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				_, err := circuitBreaker.Process(ctx, fmt.Sprintf("gr%d-req%d", gID, j))
				if err != nil {
					totalErrors.Add(1)
					if errors.Is(err, fluxus.ErrCircuitOpen) {
						circuitOpenErrors.Add(1)
					}
				}
				// Small sleep to allow state changes to propagate, avoid pure CPU spinning
				time.Sleep(time.Duration(gID%5+1) * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Total Errors: %d, Circuit Open Errors: %d", totalErrors.Load(), circuitOpenErrors.Load())

	// Assertions: These are harder to predict exactly due to concurrency races,
	// but we can check for reasonable outcomes.
	assert.GreaterOrEqual(t, totalErrors.Load(), int64(failureThreshold), "Should have at least threshold errors")
	assert.LessOrEqual(
		t,
		totalErrors.Load(),
		int64(concurrencyLevel*requestsPerGoroutine),
		"Errors cannot exceed total requests",
	)

	// Check final state - it might be Closed if recovery happened, or Open/HalfOpen if still failing/recovering
	finalState := circuitBreaker.State()
	assert.Contains(
		t,
		[]fluxus.CircuitBreakerState{fluxus.CircuitClosed, fluxus.CircuitOpen, fluxus.CircuitHalfOpen},
		finalState,
		"Final state is unexpected",
	)

	// If many errors occurred, a good portion should be ErrCircuitOpen
	if totalErrors.Load() > int64(failureThreshold*2) { // Heuristic: if significantly more errors than threshold
		assert.Positive(
			t,
			circuitOpenErrors.Load(),
			"Expected some ErrCircuitOpen errors if many total errors occurred",
		)
	}
}

// TestCircuitBreakerContextCancellation tests behavior when context is cancelled
func TestCircuitBreakerContextCancellation(t *testing.T) {
	t.Run("CancellationDuringProcessing_ClosedState", func(t *testing.T) {
		serviceStage := fluxus.StageFunc[string, string](func(ctx context.Context, _ string) (string, error) {
			select {
			case <-time.After(100 * time.Millisecond):
				return "done", nil
			case <-ctx.Done():
				return "", ctx.Err() // Propagate context error
			}
		})
		circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, 3, 1*time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) // Cancel before stage finishes
		defer cancel()

		_, err := circuitBreaker.Process(ctx, "input")

		require.Error(t, err, "Expected an error due to context cancellation")
		require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded error")
		assert.Equal(
			t,
			fluxus.CircuitClosed,
			circuitBreaker.State(),
			"Circuit should remain closed on context cancellation",
		)
		// Verify failure count didn't increase (assuming context errors don't count)
		// This requires access to internal state or specific error handling in CB implementation
		// For now, we just check the state.
	})

	t.Run("CancellationWhileOpen", func(t *testing.T) {
		service := &mockExternalService{maxFailures: 1}
		serviceStage := fluxus.StageFunc[string, string](service.Process)
		circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, 1, 1*time.Second)
		ctx := context.Background()

		// Open the circuit
		_, err := circuitBreaker.Process(ctx, "fail-input")
		require.Error(t, err)
		require.Equal(t, fluxus.CircuitOpen, circuitBreaker.State())

		// Try processing with a cancelled context
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err = circuitBreaker.Process(cancelCtx, "input")
		require.Error(t, err, "Expected an error")
		// IMPORTANT: Expect ErrCircuitOpen first, as it checks state before context usually.
		require.ErrorIs(t, err, fluxus.ErrCircuitOpen, "Expected ErrCircuitOpen even if context is cancelled")
	})
}

// BenchmarkCircuitBreaker benchmarks the overhead in different states
func BenchmarkCircuitBreaker(b *testing.B) {
	failureThreshold := 3
	resetTimeout := 50 * time.Millisecond

	// --- Scenario 1: Always Closed (Successful Service) ---
	b.Run("StateClosed", func(b *testing.B) {
		service := &mockExternalService{maxFailures: 0} // Always succeeds
		serviceStage := fluxus.StageFunc[string, string](service.Process)
		circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, failureThreshold, resetTimeout)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = circuitBreaker.Process(ctx, "bench-input")
		}
	})

	// --- Scenario 2: Always Open (Service always fails, CB trips) ---
	b.Run("StateOpen", func(b *testing.B) {
		service := &mockExternalService{maxFailures: b.N + failureThreshold + 1} // Always fails
		serviceStage := fluxus.StageFunc[string, string](service.Process)
		circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, failureThreshold, resetTimeout)
		ctx := context.Background()

		// Trip the breaker first
		for i := 0; i < failureThreshold; i++ {
			_, _ = circuitBreaker.Process(ctx, "trip")
		}
		require.Equal(b, fluxus.CircuitOpen, circuitBreaker.State())

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = circuitBreaker.Process(ctx, "bench-input") // Should return ErrCircuitOpen quickly
		}
	})

	// --- Scenario 3: Transitioning (Service fails then recovers) ---
	// This is similar to the original benchmark but more explicit
	b.Run("StateTransitioning", func(b *testing.B) {
		// Reset service and CB for each benchmark iteration for consistent state
		b.StopTimer()
		service := &mockExternalService{maxFailures: failureThreshold}
		serviceStage := fluxus.StageFunc[string, string](service.Process)
		circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, failureThreshold, resetTimeout)
		ctx := context.Background()
		service.Reset() // Ensure service starts fresh

		b.StartTimer()
		for i := 0; i < b.N; i++ {
			// Reset service failure count periodically to simulate recovery cycles
			// This is tricky in benchmarks; the original approach might be sufficient
			// to measure average overhead during potential transitions.
			// Let's stick to the original approach for simplicity here:
			_, _ = circuitBreaker.Process(ctx, "bench-input")

			// Alternative: Reset state explicitly if needed, but complicates timing
			// if (i+1)% (failureThreshold + 5) == 0 { // Reset every few cycles
			// 	b.StopTimer()
			// 	service.Reset()
			// 	// Force CB closed? Requires internal access or waiting for recovery.
			// 	// This makes the benchmark less reliable.
			// 	b.StartTimer()
			// }
		}
	})
}

// BenchmarkCircuitBreakerConcurrency benchmarks performance under concurrent load
func BenchmarkCircuitBreakerConcurrency(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50, 100} // Test different levels
	failureThreshold := 10
	resetTimeout := 50 * time.Millisecond

	for _, level := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency%d", level), func(b *testing.B) {
			// Setup service and circuit breaker *inside* the sub-benchmark
			// to ensure a fresh state for each run.
			service := &mockExternalService{
				maxFailures:     failureThreshold, // Fails first few times
				processingDelay: 0,                // Just benchmark the implementation
			}
			serviceStage := fluxus.StageFunc[string, string](service.Process)
			circuitBreaker := fluxus.NewCircuitBreaker(serviceStage, failureThreshold, resetTimeout)
			ctx := context.Background()

			b.ResetTimer() // Reset timer right before starting parallel execution

			// Set the desired level of parallelism
			b.SetParallelism(level)

			// Run b.N operations in parallel using 'level' goroutines.
			// RunParallel distributes the b.N iterations across the goroutines.
			b.RunParallel(func(pb *testing.PB) {
				// Each goroutine loops as long as pb.Next() returns true
				for pb.Next() {
					// The code inside this loop is executed b.N times in total,
					// distributed across the parallel goroutines.
					_, _ = circuitBreaker.Process(ctx, "bench-conc")
				}
			})

			// No need for explicit wg.Wait() or b.StopTimer() with RunParallel
		})
	}
}

// TestRetrySuccessFirstTry verifies that the retry stage returns successfully
// without any retries if the underlying stage succeeds on the first attempt.
func TestRetrySuccessFirstTry(t *testing.T) {
	mockStage := newRetryMockStage(1, nil, 0) // Succeeds on first attempt
	retryStage := fluxus.NewRetry(mockStage, 3)
	ctx := context.Background()

	result, err := retryStage.Process(ctx, "input-1")

	require.NoError(t, err)
	assert.Equal(t, "success on attempt 1: input-1", result)
}

// TestRetrySuccessAfterRetries verifies that the retry stage successfully returns
// after the underlying stage fails a few times but eventually succeeds within the max attempts.
func TestRetrySuccessAfterRetries(t *testing.T) {
	attemptsNeeded := 3
	mockStage := newRetryMockStage(attemptsNeeded, errors.New("transient error"), 0)
	retryStage := fluxus.NewRetry(mockStage, 5) // Allow up to 5 attempts
	ctx := context.Background()

	result, err := retryStage.Process(ctx, "input-retry")

	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("success on attempt %d: input-retry", attemptsNeeded), result)
}

// TestRetryFailureMaxAttempts verifies that the retry stage returns an error
// after the underlying stage fails for the maximum number of allowed attempts.
func TestRetryFailureMaxAttempts(t *testing.T) {
	maxAttempts := 3
	mockStage := newRetryMockStage(0, errors.New("persistent error"), 0) // Always fails
	retryStage := fluxus.NewRetry(mockStage, maxAttempts)
	ctx := context.Background()

	_, err := retryStage.Process(ctx, "input-fail")

	require.Error(t, err)
	// Check if the error message indicates exhaustion and includes the last error
	assert.Contains(t, err.Error(), fmt.Sprintf("retry exhausted %d attempts", maxAttempts))
	assert.ErrorContains(t, err, "persistent error") // Check underlying error is wrapped
}

// TestRetryShouldRetryPredicate verifies that the WithShouldRetry option correctly
// prevents retries for errors that don't match the predicate.
func TestRetryShouldRetryPredicate(t *testing.T) {
	transientErr := errors.New("transient")
	permanentErr := errors.New("permanent")
	var callCount atomic.Int32

	mockStage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		count := callCount.Add(1)
		if count == 1 {
			return "", transientErr
		}
		return "", permanentErr // Fail with permanent error on second attempt
	})

	retryStage := fluxus.NewRetry(mockStage, 3).
		WithShouldRetry(func(err error) bool {
			return errors.Is(err, transientErr) // Only retry transient errors
		})

	ctx := context.Background()
	_, err := retryStage.Process(ctx, "input-predicate")

	require.Error(t, err)
	require.ErrorIs(t, err, permanentErr)
	// Check if the error message indicates giving up
	assert.Contains(t, err.Error(), "retry giving up after 2 attempts")
	assert.Equal(t, int32(2), callCount.Load(), "Should have been called twice")
}

// TestRetryBackoff verifies that the WithBackoff option introduces delays
// between retry attempts.
func TestRetryBackoff(t *testing.T) {
	attemptsNeeded := 3
	backoffDelay := 20 * time.Millisecond
	mockStage := newRetryMockStage(attemptsNeeded, errors.New("backoff error"), 0)

	retryStage := fluxus.NewRetry(mockStage, 5).
		WithBackoff(func(attempt int) int {
			// Exponential backoff (simple version)
			return int(backoffDelay.Milliseconds()) * (attempt + 1) // attempt is 0-based
		})

	ctx := context.Background()
	startTime := time.Now()
	result, err := retryStage.Process(ctx, "input-backoff")
	duration := time.Since(startTime)

	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("success on attempt %d: input-backoff", attemptsNeeded), result)

	// Expected minimum duration: delay for attempt 0 + delay for attempt 1
	// attempt 0 -> delay = 20ms * 1 = 20ms
	// attempt 1 -> delay = 20ms * 2 = 40ms
	// Total expected backoff = 60ms
	expectedMinDuration := time.Duration(1+2) * backoffDelay
	// Add a small buffer for execution time
	assert.GreaterOrEqual(t, duration, expectedMinDuration, "Execution time should be at least the sum of backoffs")
	// Add a reasonable upper bound check too
	assert.Less(t, duration, expectedMinDuration*3, "Execution time seems too long")

	t.Logf("Backoff test duration: %v (expected min backoff: %v)", duration, expectedMinDuration)
}

// TestRetryContextCancellationDuringBackoff verifies that the retry stage respects context
// cancellation that occurs while waiting during a backoff period.
func TestRetryContextCancellationDuringBackoff(t *testing.T) {
	cancelTime := 30 * time.Millisecond
	backoffFunc := func(_ int) int { return 50 }                         // 50ms backoff
	mockStage := newRetryMockStage(0, errors.New("persistent error"), 0) // Always fails
	retryStage := fluxus.NewRetry(mockStage, 3).WithBackoff(backoffFunc)

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	startTime := time.Now()
	_, err := retryStage.Process(ctx, "input-cancel-backoff")
	duration := time.Since(startTime)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded error")
	assert.Contains(t, err.Error(), "retry interrupted during backoff")
	// Should fail after the first attempt + backoff attempt starts
	assert.Less(
		t,
		duration,
		time.Duration(backoffFunc(0))*time.Millisecond+15*time.Millisecond,
	) // Should cancel during first backoff
}

// TestRetryContextCancellationBeforeFirstAttempt verifies that the retry stage respects context
// cancellation that occurs even before the first attempt is made.
func TestRetryContextCancellationBeforeFirstAttempt(t *testing.T) {
	mockStage := newRetryMockStage(1, nil, 0) // Would succeed if called
	retryStage := fluxus.NewRetry(mockStage, 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := retryStage.Process(ctx, "input-cancel-immediate")

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled, "Expected context canceled error")
	// Ensure the underlying stage was never called (mock call count would be 0)
	// This requires the mock stage to be accessible or track calls globally/passed in.
	// For simplicity, we rely on the error type here.
}

// TestRetryContextCancellationDuringProcessing verifies that the retry stage respects context
// cancellation that occurs while the underlying stage is processing its logic.
func TestRetryContextCancellationDuringProcessing(t *testing.T) {
	processingDelay := 100 * time.Millisecond
	cancelTime := 50 * time.Millisecond
	mockStage := newRetryMockStage(0, errors.New("some error"), processingDelay) // Always fails, takes time
	retryStage := fluxus.NewRetry(mockStage, 3)

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	_, err := retryStage.Process(ctx, "input-cancel-processing")

	require.Error(t, err)
	// The mock stage returns ctx.Err() when cancelled during its delay
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded from underlying stage")
	// The retry stage might wrap this error
	assert.Contains(t, err.Error(), "context cancelled: context deadline exceeded") // Check wrapping
}

// TestRetryErrorHandler verifies that a custom error handler provided via WithErrorHandler
// is called when the retry stage ultimately fails, allowing error wrapping or transformation.
func TestRetryErrorHandler(t *testing.T) {
	maxAttempts := 2
	originalErr := errors.New("original persistent error")
	mockStage := newRetryMockStage(0, originalErr, 0) // Always fails
	customErr := errors.New("custom wrapped error")

	retryStage := fluxus.NewRetry(mockStage, maxAttempts).
		WithErrorHandler(func(err error) error {
			return fmt.Errorf("%w: %w", customErr, err) // Wrap with custom error
		})

	ctx := context.Background()
	_, err := retryStage.Process(ctx, "input-errorhandler")

	require.Error(t, err)
	require.ErrorIs(t, err, customErr, "Expected custom error wrapper")
	require.ErrorIs(t, err, originalErr, "Expected original error to be nested")
	assert.Contains(t, err.Error(), fmt.Sprintf("retry exhausted %d attempts", maxAttempts))
}

// TestRetryZeroAttempts verifies the behavior when maxAttempts is set to 0.
// It should fail immediately without calling the underlying stage.
func TestRetryZeroAttempts(t *testing.T) {
	mockStage := newRetryMockStage(1, nil, 0) // Would succeed if called
	retryStage := fluxus.NewRetry(mockStage, 0)
	ctx := context.Background()

	_, err := retryStage.Process(ctx, "input-zero")

	// Expecting an error indicating 0 attempts were made or exhausted immediately
	require.Error(t, err)
	assert.Contains(t, err.Error(), "retry exhausted 0 attempts")
	// The underlying error in this case is nil because the stage was never called.
	// Depending on implementation, it might return a generic error or nil wrapped.
	// Let's assume it wraps nil, resulting in just the exhaustion message.
}

// TestRetryOneAttempt verifies the behavior when maxAttempts is set to 1.
// It should call the underlying stage exactly once.
func TestRetryOneAttempt(t *testing.T) {
	// TestRetryOneAttemptSucceeds tests the case where the single attempt succeeds.
	t.Run("Succeeds", func(t *testing.T) {
		mockStage := newRetryMockStage(1, nil, 0) // Succeeds on first try
		retryStage := fluxus.NewRetry(mockStage, 1)
		ctx := context.Background()
		result, err := retryStage.Process(ctx, "input-one-succeed")
		require.NoError(t, err)
		assert.Equal(t, "success on attempt 1: input-one-succeed", result)
	})

	// TestRetryOneAttemptFails tests the case where the single attempt fails.
	t.Run("Fails", func(t *testing.T) {
		failErr := errors.New("fail one")
		mockStage := newRetryMockStage(0, failErr, 0) // Always fails
		retryStage := fluxus.NewRetry(mockStage, 1)
		ctx := context.Background()
		_, err := retryStage.Process(ctx, "input-one-fail")
		require.Error(t, err)
		require.ErrorIs(t, err, failErr)
		assert.Contains(t, err.Error(), "retry exhausted 1 attempts")
	})
}

// BenchmarkRetry benchmarks the overhead of the retry stage under different scenarios:
// no retries needed, always retrying max attempts (with and without backoff).
func BenchmarkRetry(b *testing.B) {
	ctx := context.Background()
	input := "bench-input"

	// BenchmarkNoRetries measures overhead when the underlying stage always succeeds.
	b.Run("NoRetries", func(b *testing.B) {
		// Use a mock that succeeds immediately and has minimal overhead
		successStage := fluxus.StageFunc[string, string](func(_ context.Context, s string) (string, error) {
			return s, nil
		})
		retryStage := fluxus.NewRetry(successStage, 5) // Max attempts don't matter here

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = retryStage.Process(ctx, input)
		}
	})

	// BenchmarkMaxRetriesNoBackoff measures overhead when always hitting max attempts without backoff delays.
	b.Run("MaxRetriesNoBackoff", func(b *testing.B) {
		maxAttempts := 3
		failErr := errors.New("fail")
		// Use a mock that always fails quickly
		failStage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
			return "", failErr
		})
		retryStage := fluxus.NewRetry(failStage, maxAttempts)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = retryStage.Process(ctx, input) // Error is expected and ignored in benchmark
		}
	})

	// BenchmarkMaxRetriesWithBackoff measures overhead including backoff delays.
	// Note: This measures wall-clock time including sleep, not just CPU overhead.
	b.Run("MaxRetriesWithBackoff", func(b *testing.B) {
		maxAttempts := 3
		failErr := errors.New("fail")
		failStage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
			return "", failErr
		})
		// Simple constant backoff for predictability
		retryStage := fluxus.NewRetry(failStage, maxAttempts).
			WithBackoff(func(_ int) int { return 1 }) // 1ms backoff

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = retryStage.Process(ctx, input)
		}
	})
}

// BenchmarkRetryConcurrency benchmarks the retry stage performance under various levels
// of concurrent load, simulating a mix of successes and failures.
func BenchmarkRetryConcurrency(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50, 100}
	maxAttempts := 3
	ctx := context.Background()

	// BenchmarkConcurrencyMixed simulates a mix of success and failure under load.
	for _, level := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency%dMixed", level), func(b *testing.B) {
			// Setup service and retry stage *inside* the sub-benchmark
			// to ensure a fresh state for each run.
			service := &mockExternalService{
				maxFailures:     maxAttempts - 1, // Fail maxAttempts-1 times, succeed on the last retry attempt
				processingDelay: 0,
			}
			serviceStage := fluxus.StageFunc[string, string](service.Process)
			retryStage := fluxus.NewRetry(serviceStage, maxAttempts)

			b.ResetTimer()
			b.SetParallelism(level)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					// Reset service periodically within each goroutine to keep the mix active
					// This is tricky; RunParallel makes it hard to coordinate resets globally.
					// A simpler approach is to let the state evolve naturally,
					// measuring the average performance across different states.
					// Or, use a stateless mock that fails probabilistically.

					// Let's stick to the simpler approach: use the stateful mock without resets inside RunParallel.
					// The benchmark will measure average perf as the mock transitions from failing to succeeding.
					_, _ = retryStage.Process(ctx, "bench-conc-retry")

					// Alternative: Stateless probabilistic failure
					// probabilisticFailStage := fluxus.StageFunc[string, string](func(_ context.Context, s string) (string, error) {
					// 	if rand.Intn(maxAttempts) != 0 { // Fail most of the time
					// 		return "", failErr
					// 	}
					// 	return s, nil
					// })
					// retryStageProb := fluxus.NewRetry[string, string](probabilisticFailStage, maxAttempts)
					// _, _ = retryStageProb.Process(ctx, "bench-conc-retry")
				}
			})
		})
	}
}

// TestTimeoutSuccess verifies that the timeout stage passes the result through
// when the underlying stage completes within the specified timeout.
func TestTimeoutSuccess(t *testing.T) {
	stageDelay := 20 * time.Millisecond
	timeoutDuration := 50 * time.Millisecond
	mockStage := newDelayedMockStage(stageDelay, true, nil)
	timeoutStage := fluxus.NewTimeout(mockStage, timeoutDuration)
	ctx := context.Background()

	result, err := timeoutStage.Process(ctx, "input-fast")

	require.NoError(t, err)
	assert.Equal(t, "processed after delay: input-fast", result)
}

// TestTimeoutExpired verifies that the timeout stage returns a context.DeadlineExceeded
// error when the underlying stage takes longer than the specified timeout.
func TestTimeoutExpired(t *testing.T) {
	stageDelay := 100 * time.Millisecond
	timeoutDuration := 30 * time.Millisecond
	mockStage := newDelayedMockStage(stageDelay, true, nil) // Would succeed if not timed out
	timeoutStage := fluxus.NewTimeout(mockStage, timeoutDuration)
	ctx := context.Background()

	startTime := time.Now()
	_, err := timeoutStage.Process(ctx, "input-slow")
	duration := time.Since(startTime)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded error")
	assert.Contains(t, err.Error(), fmt.Sprintf("stage timed out after %v", timeoutDuration))
	// Check that the error occurred roughly around the timeout duration
	assert.GreaterOrEqual(t, duration, timeoutDuration, "Duration should be at least the timeout")
	assert.Less(
		t,
		duration,
		timeoutDuration+stageDelay,
		"Duration should be less than stage delay",
	) // Ensure it didn't wait for the stage
}

// TestTimeoutStageError verifies that the timeout stage propagates errors returned
// by the underlying stage if they occur before the timeout expires.
func TestTimeoutStageError(t *testing.T) {
	stageDelay := 20 * time.Millisecond
	timeoutDuration := 50 * time.Millisecond
	stageErr := errors.New("underlying stage error")
	mockStage := newDelayedMockStage(stageDelay, false, stageErr) // Fails quickly
	timeoutStage := fluxus.NewTimeout(mockStage, timeoutDuration)
	ctx := context.Background()

	_, err := timeoutStage.Process(ctx, "input-stage-fail")

	require.Error(t, err)
	require.ErrorIs(t, err, stageErr, "Expected the underlying stage error")
	assert.NotErrorIs(t, err, context.DeadlineExceeded, "Should not be a timeout error")
}

// TestTimeoutParentContextCancelled verifies that the timeout stage respects cancellation
// from the parent context, even if its own timeout hasn't expired.
func TestTimeoutParentContextCancelled(t *testing.T) {
	stageDelay := 100 * time.Millisecond
	timeoutDuration := 200 * time.Millisecond // Timeout longer than parent cancellation
	parentCancelDelay := 50 * time.Millisecond

	mockStage := newDelayedMockStage(stageDelay, true, nil)
	timeoutStage := fluxus.NewTimeout(mockStage, timeoutDuration)

	parentCtx, cancel := context.WithTimeout(context.Background(), parentCancelDelay)
	defer cancel()

	startTime := time.Now()
	_, err := timeoutStage.Process(parentCtx, "input-parent-cancel")
	duration := time.Since(startTime)

	require.Error(t, err)
	// The error comes from the mock stage respecting the context passed to it by Timeout stage
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded from parent context")
	// Check that the error occurred roughly around the parent cancellation time
	assert.GreaterOrEqual(t, duration, parentCancelDelay, "Duration should be at least the parent cancel delay")
	assert.Less(t, duration, timeoutDuration, "Duration should be less than the stage's own timeout")
}

// TestTimeoutErrorHandler verifies that a custom error handler provided via WithErrorHandler
// is called when the timeout expires.
func TestTimeoutErrorHandler(t *testing.T) {
	stageDelay := 100 * time.Millisecond
	timeoutDuration := 30 * time.Millisecond
	customErr := errors.New("custom timeout wrapper")

	mockStage := newDelayedMockStage(stageDelay, true, nil)
	timeoutStage := fluxus.NewTimeout(mockStage, timeoutDuration).
		WithErrorHandler(func(err error) error {
			// Only wrap timeout errors
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: %w", customErr, err)
			}
			return err
		})
	ctx := context.Background()

	_, err := timeoutStage.Process(ctx, "input-timeout-handler")

	require.Error(t, err)
	require.ErrorIs(t, err, customErr, "Expected custom error wrapper")
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected original deadline exceeded to be nested")
	assert.Contains(t, err.Error(), fmt.Sprintf("stage timed out after %v", timeoutDuration))
}

// BenchmarkTimeout benchmarks the overhead of the timeout stage.
func BenchmarkTimeout(b *testing.B) {
	ctx := context.Background()
	input := "bench-timeout-input"
	timeoutDuration := 50 * time.Millisecond

	// BenchmarkSuccessWithinTimeout measures overhead when the stage always completes quickly.
	b.Run("SuccessWithinTimeout", func(b *testing.B) {
		// Use a mock that completes almost instantly
		fastStage := fluxus.StageFunc[string, string](func(_ context.Context, s string) (string, error) {
			return s, nil
		})
		timeoutStage := fluxus.NewTimeout(fastStage, timeoutDuration)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = timeoutStage.Process(ctx, input)
		}
	})

	// BenchmarkAlwaysTimesOut measures overhead when the stage consistently times out.
	// Note: This measures wall-clock time including the timeout delay.
	b.Run("AlwaysTimesOut", func(b *testing.B) {
		// Use a mock that takes longer than the timeout
		slowStage := newDelayedMockStage(timeoutDuration*2, true, nil)
		timeoutStage := fluxus.NewTimeout(slowStage, timeoutDuration)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = timeoutStage.Process(ctx, input) // Error is expected and ignored
		}
	})
}

// BenchmarkTimeoutConcurrency benchmarks the timeout stage performance under concurrent load.
func BenchmarkTimeoutConcurrency(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50, 100}
	timeoutDuration := 20 * time.Millisecond // Relatively short timeout for benchmark
	ctx := context.Background()

	// BenchmarkConcurrencyMixed simulates load where some requests might timeout.
	for _, level := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency%dMixed", level), func(b *testing.B) {
			// Use a mock stage with a small, fixed delay. Some might finish, some might timeout
			// depending on scheduling and overhead.
			// Alternatively, use a probabilistic delay. Let's use fixed for simplicity.
			stageDelay := 10 * time.Millisecond // Shorter than timeout, but concurrency might push some over
			mockStage := newDelayedMockStage(stageDelay, true, nil)
			timeoutStage := fluxus.NewTimeout(mockStage, timeoutDuration)

			b.ResetTimer()
			b.SetParallelism(level)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _ = timeoutStage.Process(ctx, "bench-conc-timeout")
				}
			})
		})
	}
}

// mockDLQHandler tracks handled items and can simulate errors
type mockDLQHandler[I any] struct {
	mu             sync.Mutex
	handledItems   []I
	handledErrors  []error
	handleError    error // If set, Handle will return this error
	handleCallback func(ctx context.Context, item I, processingError error) error
}

func (m *mockDLQHandler[I]) Handle(ctx context.Context, item I, processingError error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.handledItems = append(m.handledItems, item)
	m.handledErrors = append(m.handledErrors, processingError)

	if m.handleCallback != nil {
		return m.handleCallback(ctx, item, processingError)
	}

	return m.handleError // Return the pre-configured error (nil if not set)
}

func (m *mockDLQHandler[I]) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handledItems = nil
	m.handledErrors = nil
	m.handleError = nil
	m.handleCallback = nil
}

func (m *mockDLQHandler[I]) HandledCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.handledItems)
}

func (m *mockDLQHandler[I]) LastHandledItem() (I, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.handledItems) == 0 {
		var zero I
		return zero, false
	}
	return m.handledItems[len(m.handledItems)-1], true
}

func (m *mockDLQHandler[I]) LastHandledError() (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.handledErrors) == 0 {
		return false, nil
	}
	return true, m.handledErrors[len(m.handledErrors)-1]
}

// TestDeadLetterQueueSuccess verifies DLQ is not called on success.
func TestDeadLetterQueueSuccess(t *testing.T) {
	mockStage := newRetryMockStage(1, nil, 0) // Succeeds immediately
	mockDLQ := &mockDLQHandler[string]{}
	dlqStage := fluxus.NewDeadLetterQueue(
		mockStage,
		fluxus.WithDLQHandler[string, string](mockDLQ),
	)
	ctx := context.Background()

	result, err := dlqStage.Process(ctx, "input-ok")

	require.NoError(t, err)
	assert.Equal(t, "success on attempt 1: input-ok", result)
	assert.Equal(t, 0, mockDLQ.HandledCount(), "DLQ handler should not be called on success")
}

// TestDeadLetterQueueTriggered verifies DLQ is called on eligible failure.
func TestDeadLetterQueueTriggered(t *testing.T) {
	processingErr := errors.New("processing failed")
	mockStage := newRetryMockStage(0, processingErr, 0) // Always fails
	mockDLQ := &mockDLQHandler[string]{}
	dlqStage := fluxus.NewDeadLetterQueue(
		mockStage,
		fluxus.WithDLQHandler[string, string](mockDLQ),
	)
	ctx := context.Background()
	input := "input-fail"

	_, err := dlqStage.Process(ctx, input)

	require.Error(t, err, "Expected original processing error")
	require.ErrorIs(t, err, processingErr, "Expected original processing error to be returned")

	// Verify DLQ handler was called
	assert.Equal(t, 1, mockDLQ.HandledCount(), "DLQ handler should be called once")

	// Verify item and error passed to DLQ handler
	handledItem, okItem := mockDLQ.LastHandledItem()
	require.True(t, okItem, "Should have handled item")
	assert.Equal(t, input, handledItem)

	okErr, handledErr := mockDLQ.LastHandledError()
	require.True(t, okErr, "Should have handled error")
	require.ErrorIs(t, handledErr, processingErr, "DLQ handler should receive the original error")
}

// TestDeadLetterQueueNotTriggeredContext verifies DLQ is not called for context errors by default.
func TestDeadLetterQueueNotTriggeredContext(t *testing.T) {
	mockStage := newDelayedMockStage(100*time.Millisecond, false, nil) // Fails after delay
	mockDLQ := &mockDLQHandler[string]{}
	dlqStage := fluxus.NewDeadLetterQueue(
		mockStage,
		fluxus.WithDLQHandler[string, string](mockDLQ),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond) // Cancel before stage finishes
	defer cancel()

	_, err := dlqStage.Process(ctx, "input-cancel")

	require.Error(t, err, "Expected context error")
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded")
	assert.Equal(t, 0, mockDLQ.HandledCount(), "DLQ handler should not be called for context errors by default")
}

// TestDeadLetterQueueNotTriggeredFiltered verifies DLQ is not called for ErrItemFiltered by default.
func TestDeadLetterQueueNotTriggeredFiltered(t *testing.T) {
	mockStage := fluxus.StageFunc[string, string](func(_ context.Context, s string) (string, error) {
		return s, fluxus.ErrItemFiltered // Simulate a filter dropping the item
	})
	mockDLQ := &mockDLQHandler[string]{}
	dlqStage := fluxus.NewDeadLetterQueue(
		mockStage,
		fluxus.WithDLQHandler[string, string](mockDLQ),
	)
	ctx := context.Background()

	_, err := dlqStage.Process(ctx, "input-filtered")

	require.Error(t, err, "Expected filter error")
	require.ErrorIs(t, err, fluxus.ErrItemFiltered, "Expected ErrItemFiltered")
	assert.Equal(t, 0, mockDLQ.HandledCount(), "DLQ handler should not be called for ErrItemFiltered by default")
}

// TestDeadLetterQueueHandlerError verifies logging of DLQ handler errors.
func TestDeadLetterQueueHandlerError(t *testing.T) {
	processingErr := errors.New("processing failed")
	dlqHandlerErr := errors.New("dlq handler failed")
	mockStage := newRetryMockStage(0, processingErr, 0) // Always fails
	mockDLQ := &mockDLQHandler[string]{
		handleError: dlqHandlerErr, // Make the handler itself fail
	}

	// Capture log output
	var logBuf bytes.Buffer
	originalOutput := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(originalOutput) // Restore original logger

	dlqStage := fluxus.NewDeadLetterQueue(
		mockStage,
		fluxus.WithDLQHandler[string, string](mockDLQ),
		// No custom logger, use default
	)
	ctx := context.Background()

	_, err := dlqStage.Process(ctx, "input-dlq-fail")

	// Still expect the original processing error
	require.Error(t, err, "Expected original processing error")
	require.ErrorIs(t, err, processingErr, "Expected original processing error to be returned")

	// Verify DLQ handler was called
	assert.Equal(t, 1, mockDLQ.HandledCount(), "DLQ handler should have been called")

	// Verify the DLQ handler's error was logged
	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "fluxus: DLQ handler error", "Log should indicate DLQ handler error")
	assert.Contains(t, logOutput, dlqHandlerErr.Error(), "Log should contain the DLQ handler's specific error")
	assert.Contains(t, logOutput, processingErr.Error(), "Log should contain the original processing error for context")
}

// TestDeadLetterQueueCustomShouldDLQ verifies custom predicate usage.
func TestDeadLetterQueueCustomShouldDLQ(t *testing.T) {
	specificErr := errors.New("only this goes to DLQ")
	otherErr := errors.New("this does not go to DLQ")

	mockStage := fluxus.StageFunc[string, string](func(_ context.Context, s string) (string, error) {
		if s == "dlq-this" {
			return "", specificErr
		}
		return "", otherErr
	})
	mockDLQ := &mockDLQHandler[string]{}

	// Custom predicate: only send specificErr to DLQ
	shouldDLQ := func(err error) bool {
		return errors.Is(err, specificErr)
	}

	dlqStage := fluxus.NewDeadLetterQueue(
		mockStage,
		fluxus.WithDLQHandler[string, string](mockDLQ),
		fluxus.WithShouldDLQ[string, string](shouldDLQ),
	)
	ctx := context.Background()

	// --- Case 1: Error should trigger DLQ ---
	_, err1 := dlqStage.Process(ctx, "dlq-this")
	require.ErrorIs(t, err1, specificErr)
	assert.Equal(t, 1, mockDLQ.HandledCount(), "DLQ handler should be called for specificErr")
	mockDLQ.Reset() // Reset for next case

	// --- Case 2: Error should NOT trigger DLQ ---
	_, err2 := dlqStage.Process(ctx, "ignore-this")
	require.ErrorIs(t, err2, otherErr)
	assert.Equal(t, 0, mockDLQ.HandledCount(), "DLQ handler should NOT be called for otherErr")
}

// TestDeadLetterQueueCustomLogger verifies custom DLQ error logger usage.
func TestDeadLetterQueueCustomLogger(t *testing.T) {
	processingErr := errors.New("processing failed")
	dlqHandlerErr := errors.New("dlq handler failed")
	mockStage := newRetryMockStage(0, processingErr, 0) // Always fails
	mockDLQ := &mockDLQHandler[string]{
		handleError: dlqHandlerErr, // Make the handler itself fail
	}

	var loggedErr error
	var loggerCalled atomic.Bool
	customLogger := func(err error) {
		loggedErr = err
		loggerCalled.Store(true)
	}

	dlqStage := fluxus.NewDeadLetterQueue(
		mockStage,
		fluxus.WithDLQHandler[string, string](mockDLQ),
		fluxus.WithDLQErrorLogger[string, string](customLogger),
	)
	ctx := context.Background()

	_, err := dlqStage.Process(ctx, "input-custom-log")

	// Still expect the original processing error
	require.ErrorIs(t, err, processingErr)

	// Verify custom logger was called
	require.True(t, loggerCalled.Load(), "Custom logger should have been called")
	require.Error(t, loggedErr, "Logged error should not be nil")
	require.ErrorIs(t, loggedErr, dlqHandlerErr, "Logged error should wrap the DLQ handler error")
	require.ErrorContains(
		t,
		loggedErr,
		processingErr.Error(),
		"Logged error should contain original processing error context",
	)
}

// TestDeadLetterQueuePanicNilHandler tests panic if handler is not provided.
func TestDeadLetterQueuePanicNilHandler(t *testing.T) {
	mockStage := newRetryMockStage(1, nil, 0)

	// Test panic when no WithDLQHandler is used
	assert.PanicsWithValue(t,
		"fluxus.NewDeadLetterQueue: DLQHandler must be provided using the WithDLQHandler option",
		func() {
			_ = fluxus.NewDeadLetterQueue(mockStage)
		},
		"Should panic if WithDLQHandler is missing",
	)

	// Test panic when nil is explicitly passed to WithDLQHandler
	assert.PanicsWithValue(t,
		"fluxus.WithDLQHandler: provided DLQHandler cannot be nil",
		func() {
			_ = fluxus.NewDeadLetterQueue(mockStage, fluxus.WithDLQHandler[string, string](nil))
		},
		"Should panic if nil is passed to WithDLQHandler",
	)
}

// BenchmarkDeadLetterQueue benchmarks the overhead of the DLQ stage.
func BenchmarkDeadLetterQueue(b *testing.B) {
	ctx := context.Background()
	input := "bench-input"
	processingErr := errors.New("bench error")

	// --- Base Stage (Success) ---
	successStage := fluxus.StageFunc[string, string](func(_ context.Context, s string) (string, error) {
		// Minimal work
		return s + "-ok", nil
	})

	// --- Base Stage (Failure) ---
	failStage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", processingErr
	})

	// --- DLQ Handler (No-op) ---
	noopDLQ := fluxus.DLQHandlerFunc[string](func(_ context.Context, _ string, _ error) error {
		return nil // Does nothing
	})

	// --- Benchmark Scenarios ---

	// Baseline: Raw stage succeeding
	b.Run("Baseline_Success", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = successStage.Process(ctx, input)
		}
	})

	// DLQ Wrapper around succeeding stage
	b.Run("DLQ_SuccessPath", func(b *testing.B) {
		dlqStage := fluxus.NewDeadLetterQueue(successStage, fluxus.WithDLQHandler[string, string](noopDLQ))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dlqStage.Process(ctx, input)
		}
	})

	// Baseline: Raw stage failing
	b.Run("Baseline_Failure", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = failStage.Process(ctx, input) // Ignore error in benchmark
		}
	})

	// DLQ Wrapper around failing stage (triggering DLQ handler)
	b.Run("DLQ_FailurePath", func(b *testing.B) {
		dlqStage := fluxus.NewDeadLetterQueue(failStage, fluxus.WithDLQHandler[string, string](noopDLQ))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dlqStage.Process(ctx, input) // Ignore error in benchmark
		}
	})
}
