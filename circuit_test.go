package fluxus_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// mockExternalService simulates an unreliable service
type mockExternalService struct {
	failureCount int
	maxFailures  int
	mu           sync.Mutex
}

// Process simulates an external service call that can fail
func (s *mockExternalService) Process(_ context.Context, input string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failureCount++
	if s.failureCount <= s.maxFailures {
		return "", fmt.Errorf("service failure %d", s.failureCount)
	}

	return fmt.Sprintf("processed: %s", input), nil
}

// TestCircuitBreakerPipeline demonstrates circuit breaker in a pipeline context
func TestCircuitBreakerPipeline(t *testing.T) {
	scenarios := []struct {
		name             string
		maxFailures      int
		failureThreshold int
		resetTimeout     time.Duration
		expectedErrors   int
		expectedResults  int
		expectedState    fluxus.CircuitBreakerState
	}{
		{
			name:             "Fails before threshold",
			maxFailures:      2,
			failureThreshold: 3,
			resetTimeout:     100 * time.Millisecond,
			expectedErrors:   2,
			expectedResults:  0,
			expectedState:    fluxus.CircuitClosed,
		},
		{
			name:             "Opens after threshold",
			maxFailures:      5,
			failureThreshold: 3,
			resetTimeout:     100 * time.Millisecond,
			expectedErrors:   3,
			expectedResults:  0,
			expectedState:    fluxus.CircuitOpen,
		},
	}

	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock external service
			service := &mockExternalService{
				maxFailures: tc.maxFailures,
			}

			// Create a stage from the service
			serviceStage := fluxus.StageFunc[string, string](service.Process)

			// Wrap the stage with a circuit breaker
			circuitBreaker := fluxus.NewCircuitBreaker(
				serviceStage,
				tc.failureThreshold,
				tc.resetTimeout,
			)

			// Create a pipeline with the circuit breaker
			pipeline := fluxus.NewPipeline(circuitBreaker)

			ctx := context.Background()
			results := make([]string, 0)
			errors := make([]error, 0)

			// Try to process multiple times
			for i := 0; i < 10; i++ {
				result, err := pipeline.Process(ctx, fmt.Sprintf("input-%d", i))

				if err != nil {
					errors = append(errors, err)

					// For "Fails before threshold" scenario
					if tc.name == "Fails before threshold" && len(errors) == tc.maxFailures {
						break
					}

					// For "Opens after threshold" scenario
					if tc.name == "Opens after threshold" && len(errors) == tc.failureThreshold {
						break
					}
				} else {
					results = append(results, result)
				}
			}

			// Verify errors
			if len(errors) != tc.expectedErrors {
				t.Errorf("Expected %d errors, got %d (errors: %v)",
					tc.expectedErrors, len(errors), errors)
			}

			// Verify results
			if len(results) != tc.expectedResults {
				t.Errorf("Expected %d results, got %d (results: %v)",
					tc.expectedResults, len(results), results)
			}

			// Check circuit breaker state
			currentState := circuitBreaker.State()
			if currentState != tc.expectedState {
				t.Errorf("Expected circuit to be %v, got %v", tc.expectedState, currentState)
			}
		})
	}
}

// TestCircuitBreakerRecovery tests the recovery mechanism
func TestCircuitBreakerRecovery(t *testing.T) {
	service := &mockExternalService{
		maxFailures: 3,
	}

	serviceStage := fluxus.StageFunc[string, string](service.Process)

	circuitBreaker := fluxus.NewCircuitBreaker(
		serviceStage,
		3,
		50*time.Millisecond,
	)

	ctx := context.Background()

	// Trigger failures
	for i := 0; i < 3; i++ {
		_, err := circuitBreaker.Process(ctx, "input")
		if err == nil {
			t.Fatalf("Expected error on iteration %d", i)
		}
	}

	// Verify circuit is open
	if circuitBreaker.State() != fluxus.CircuitOpen {
		t.Errorf("Expected circuit to be open, got %v", circuitBreaker.State())
	}

	// Wait for reset timeout
	time.Sleep(100 * time.Millisecond)

	// First request should be allowed in half-open state
	result, err := circuitBreaker.Process(ctx, "recovery-input")
	if err != nil {
		t.Errorf("Expected successful request in half-open state, got %v", err)
	}

	if result != "processed: recovery-input" {
		t.Errorf("Unexpected result: %s", result)
	}

	// Verify circuit is now closed
	if circuitBreaker.State() != fluxus.CircuitClosed {
		t.Errorf("Expected circuit to close after successful request, got %v", circuitBreaker.State())
	}
}

// Benchmark circuit breaker performance
func BenchmarkCircuitBreaker(b *testing.B) {
	service := &mockExternalService{
		maxFailures: 5,
	}

	serviceStage := fluxus.StageFunc[string, string](service.Process)

	circuitBreaker := fluxus.NewCircuitBreaker(
		serviceStage,
		3,
		50*time.Millisecond,
	)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = circuitBreaker.Process(ctx, "benchmark-input")
	}
}
