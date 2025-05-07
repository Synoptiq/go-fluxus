package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/synoptiq/go-fluxus"
)

// mockStage is a configurable stage for testing the rate limiter
type mockStage struct {
	delay       time.Duration
	shouldError bool
	callCount   int
	mu          sync.Mutex
}

func (s *mockStage) Process(ctx context.Context, input string) (string, error) {
	s.mu.Lock()
	s.callCount++
	s.mu.Unlock()

	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
			// Continue after delay
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	if s.shouldError {
		return "", errors.New("mock stage error")
	}

	return strings.ToUpper(input), nil
}

func TestRateLimiter_Process(t *testing.T) {
	tests := []struct {
		name          string
		rate          rate.Limit
		burst         int
		timeout       time.Duration
		stageDelay    time.Duration
		stageError    bool
		input         string
		expectError   bool
		expectedCalls int
	}{
		{
			name:          "Basic success",
			rate:          10,
			burst:         1,
			timeout:       time.Second,
			stageDelay:    0,
			stageError:    false,
			input:         "test",
			expectError:   false,
			expectedCalls: 1,
		},
		{
			name:          "Stage error",
			rate:          10,
			burst:         1,
			timeout:       time.Second,
			stageDelay:    0,
			stageError:    true,
			input:         "test",
			expectError:   true,
			expectedCalls: 1,
		},
		{
			name:          "Timeout error",
			rate:          1,                     // 1 request per second
			burst:         1,                     // No bursting
			timeout:       10 * time.Millisecond, // Very short timeout
			stageDelay:    0,
			stageError:    false,
			input:         "test",
			expectError:   false, // First call should succeed
			expectedCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock stage
			stage := &mockStage{
				delay:       tt.stageDelay,
				shouldError: tt.stageError,
			}

			// Create the rate limiter
			limiter := fluxus.NewRateLimiter(
				fluxus.StageFunc[string, string](stage.Process),
				tt.rate,
				tt.burst,
				fluxus.WithLimiterTimeout[string, string](tt.timeout),
			)

			// Process the request
			result, err := limiter.Process(context.Background(), tt.input)

			// Check error expectation
			if tt.expectError && err == nil {
				t.Error("Expected an error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Did not expect an error but got: %v", err)
			}

			// For successful cases, check the result
			if !tt.expectError && err == nil {
				expected := strings.ToUpper(tt.input)
				if result != expected {
					t.Errorf("Expected result %q, got %q", expected, result)
				}
			}

			// Check call count
			if stage.callCount != tt.expectedCalls {
				t.Errorf("Expected %d calls to the stage, got %d", tt.expectedCalls, stage.callCount)
			}
		})
	}
}

func TestRateLimiter_RateLimiting(t *testing.T) {
	// This test verifies that rate limiting actually works

	// Create a mock stage that responds immediately
	stage := &mockStage{}

	// Create a rate limiter with a very low rate (1 per second) and no bursting
	limiter := fluxus.NewRateLimiter(
		fluxus.StageFunc[string, string](stage.Process),
		rate.Limit(1), // 1 request per second
		1,             // Burst of 1
	)

	// Process the first request - should succeed immediately
	_, err := limiter.Process(context.Background(), "first")
	if err != nil {
		t.Fatalf("First request failed unexpectedly: %v", err)
	}

	// Try to process a second request immediately
	// With a very small timeout, this should fail due to rate limiting
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = limiter.Process(ctx, "second")
	if err == nil {
		t.Error("Second request should have failed due to rate limiting")
	}

	if !strings.Contains(err.Error(), "would exceed context deadline") {
		t.Errorf("Expected error about exceeding context deadline, got: %v", err)
	}

	// Now wait more than one second and try again - should succeed
	time.Sleep(1100 * time.Millisecond)

	_, err = limiter.Process(context.Background(), "third")
	if err != nil {
		t.Fatalf("Third request failed unexpectedly: %v", err)
	}

	// Check that we made exactly 2 successful calls
	if stage.callCount != 2 {
		t.Errorf("Expected 2 successful calls, got %d", stage.callCount)
	}
}

func TestRateLimiter_SetLimit(t *testing.T) {
	// Create a mock stage
	stage := &mockStage{}

	// Create a rate limiter with an initial low rate
	limiter := fluxus.NewRateLimiter(
		fluxus.StageFunc[string, string](stage.Process),
		rate.Limit(1), // 1 request per second
		1,             // Burst of 1
	)

	// Process the first request - should succeed immediately
	_, err := limiter.Process(context.Background(), "first")
	if err != nil {
		t.Fatalf("First request failed unexpectedly: %v", err)
	}

	// Increase the rate limit
	limiter.SetLimit(rate.Limit(100)) // 100 requests per second

	// Try to process a second request immediately
	// This should now succeed because we increased the rate limit
	_, err = limiter.Process(context.Background(), "second")
	if err != nil {
		t.Errorf("Second request failed after increasing rate limit: %v", err)
	}

	// Check that we made 2 successful calls
	if stage.callCount != 2 {
		t.Errorf("Expected 2 successful calls, got %d", stage.callCount)
	}
}

func TestRateLimiter_SetBurst(t *testing.T) {
	// Create a mock stage
	stage := &mockStage{}

	// Create a rate limiter with a high rate but initially low burst
	// We need a high rate to ensure tokens replenish quickly during our test
	limiter := fluxus.NewRateLimiter(
		fluxus.StageFunc[string, string](stage.Process),
		rate.Limit(100), // 100 requests per second - tokens replenish quickly
		1,               // Start with burst of 1
	)

	// Use the initial burst capacity
	_, err := limiter.Process(context.Background(), "first")
	if err != nil {
		t.Fatalf("First request failed unexpectedly: %v", err)
	}

	// Second request should be limited initially
	// We use a very short timeout to make the test fail fast if it doesn't work
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err = limiter.Process(ctx, "should-be-limited")
	if err == nil {
		t.Error("Second request should have been rate limited before increasing burst")
	}

	// Now increase the burst limit
	limiter.SetBurst(5)

	// Wait a tiny bit for a token to replenish (since our rate is high)
	time.Sleep(20 * time.Millisecond)

	// Now we should be able to make more requests
	for i := 0; i < 3; i++ {
		_, err = limiter.Process(context.Background(), fmt.Sprintf("after-burst-increase-%d", i))
		if err != nil {
			t.Errorf("Request %d after increasing burst failed: %v", i+1, err)
		}
	}

	// Verify the correct number of successful calls
	expectedCalls := 4 // 1 initial + 3 after burst increase
	if stage.callCount != expectedCalls {
		t.Errorf("Expected %d successful calls, got %d", expectedCalls, stage.callCount)
	}
}

func TestRateLimiter_Allow(t *testing.T) {
	// Create a mock stage
	stage := &mockStage{}

	// Create a rate limiter
	limiter := fluxus.NewRateLimiter(
		fluxus.StageFunc[string, string](stage.Process),
		rate.Limit(1), // 1 request per second
		1,             // Burst of 1
	)

	// First Allow should return true
	if !limiter.Allow() {
		t.Error("First call to Allow should return true")
	}

	// Second Allow should return false
	if limiter.Allow() {
		t.Error("Second call to Allow should return false")
	}

	// Wait for rate limit to reset
	time.Sleep(1100 * time.Millisecond)

	// Should be allowed again
	if !limiter.Allow() {
		t.Error("Call to Allow after waiting should return true")
	}
}

func TestRateLimiter_Concurrent(t *testing.T) {
	// Create a mock stage with a slight delay
	stage := &mockStage{
		delay: 10 * time.Millisecond,
	}

	// Create a rate limiter that allows 10 RPS with burst of 5
	limiter := fluxus.NewRateLimiter(
		fluxus.StageFunc[string, string](stage.Process),
		rate.Limit(10),
		5,
	)

	// Run 10 concurrent requests
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	successCount := atomic.Int32{}
	errorCount := atomic.Int32{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := limiter.Process(ctx, fmt.Sprintf("concurrent-%d", i))
			if err != nil {
				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// We should have had some successful requests and some failures
	// due to rate limiting
	t.Logf("Concurrent test: %d successes, %d errors", successCount.Load(), errorCount.Load())

	// Verify that some requests succeeded
	if successCount.Load() == 0 {
		t.Error("Expected some successful requests")
	}

	// With a rate limit of 10 RPS and burst of 5, we should have had around 5-6 successes
	// within the 1 second timeout, but this can vary based on timing
	if successCount.Load() < 5 {
		t.Errorf("Expected at least 5 successful requests, got %d", successCount.Load())
	}

	// Total attempts should equal 10
	if successCount.Load()+errorCount.Load() != 10 {
		t.Errorf("Expected 10 total attempts, got %d", successCount.Load()+errorCount.Load())
	}
}

// BenchmarkRateLimiter benchmarks the performance overhead of the rate limiter without waiting
func BenchmarkRateLimiter(b *testing.B) {
	// Create a fast-responding mock stage
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// We'll test different configurations
	benchmarks := []struct {
		name  string
		rate  rate.Limit
		burst int
	}{
		{"NoLimiter", 0, 0},                 // No rate limiter - baseline
		{"RateInf_Burst1", rate.Inf, 1},     // Unlimited rate with min burst
		{"RateInf_Burst100", rate.Inf, 100}, // Unlimited rate with large burst
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var testStage fluxus.Stage[string, string]

			if bm.name == "NoLimiter" {
				// Use the stage directly as baseline
				testStage = stage
			} else {
				// Create a rate limiter
				testStage = fluxus.NewRateLimiter(
					stage,
					bm.rate,
					bm.burst,
				)
			}

			// Create a context
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = testStage.Process(ctx, "benchmark")
			}
		})
	}
}

// BenchmarkRateLimiterAllowCheck benchmarks just the Allow check
func BenchmarkRateLimiterAllowCheck(b *testing.B) {
	benchmarks := []struct {
		name  string
		rate  rate.Limit
		burst int
	}{
		{"RateInf_Burst1", rate.Inf, 1},     // Unlimited rate with min burst
		{"RateInf_Burst100", rate.Inf, 100}, // Unlimited rate with large burst
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Create a rate limiter (without attaching a stage)
			limiter := fluxus.NewRateLimiter(
				fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
					return input, nil
				}),
				bm.rate,
				bm.burst,
			)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = limiter.Allow()
			}
		})
	}
}

// This benchmark measures the real-world overhead when waiting is NOT required
func BenchmarkRateLimiterRealWorld(b *testing.B) {
	// We'll benchmark a simple pipeline with some stages
	makeBasicPipeline := func() fluxus.Stage[string, string] {
		return fluxus.Chain(
			// First stage - convert to lowercase
			fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
				return strings.ToLower(input), nil
			}),
			// Second stage - reverse the string
			fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
				runes := []rune(input)
				for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
					runes[i], runes[j] = runes[j], runes[i]
				}
				return string(runes), nil
			}),
		)
	}

	// Create the basic pipeline
	pipeline := makeBasicPipeline()

	// Create a rate-limited version with an unlimited rate (to measure just overhead)
	rateLimit := fluxus.NewRateLimiter(
		pipeline,
		rate.Inf, // Unlimited rate
		10,       // Reasonable burst
	)

	// Benchmark with different inputs to ensure reasonable workload
	inputs := []string{
		"hello", // Short input
		"The quick brown fox jumps over the lazy dog", // Medium input
		strings.Repeat("abcdefghij", 10),              // Longer input
	}

	for _, input := range inputs {
		size := "short"
		if len(input) > 10 && len(input) <= 50 {
			size = "medium"
		} else if len(input) > 50 {
			size = "long"
		}

		// Benchmark without rate limiter
		b.Run("NormalPipeline_"+size, func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = pipeline.Process(ctx, input)
			}
		})

		// Benchmark with rate limiter
		b.Run("RateLimitedPipeline_"+size, func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = rateLimit.Process(ctx, input)
			}
		})
	}
}

// This benchmark compares different components in the pipeline architecture
func BenchmarkComponentComparison(b *testing.B) {
	// Create a simple stage for baseline
	baseStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Create a metrics collector
	collector := &noopMetricsCollector{}

	// Create various wrapped versions
	metricStage := fluxus.NewMetricatedStage(
		baseStage,
		fluxus.WithMetricsCollector[string, string](collector),
	)

	rateLimitStage := fluxus.NewRateLimiter(
		baseStage,
		rate.Inf, // Use unlimited rate to measure just the overhead
		10,
	)

	// Create a pipeline with just the stage
	simplePipeline := fluxus.NewPipeline(baseStage)

	// Create a combined version (metrics + rate limiting)
	combinedStage := fluxus.NewRateLimiter(
		metricStage,
		rate.Inf,
		10,
	)

	benchmarks := []struct {
		name  string
		stage fluxus.Stage[string, string]
	}{
		{"BaseStage", baseStage},
		{"MetricStage", metricStage},
		{"RateLimitStage", rateLimitStage},
		{"SimplePipeline", simplePipeline},
		{"Combined_MetricsAndRateLimit", combinedStage},
	}

	ctx := context.Background()
	input := "benchmark test input"

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = bm.stage.Process(ctx, input)
			}
		})
	}
}

// Simple no-op metrics collector for benchmarks
type noopMetricsCollector struct{}

func (*noopMetricsCollector) StageStarted(_ context.Context, _ string)                            {}
func (*noopMetricsCollector) StageCompleted(_ context.Context, _ string, _ time.Duration)         {}
func (*noopMetricsCollector) StageError(_ context.Context, _ string, _ error)                     {}
func (*noopMetricsCollector) RetryAttempt(_ context.Context, _ string, _ int, _ error)            {}
func (*noopMetricsCollector) BufferBatchProcessed(_ context.Context, _ int, _ time.Duration)      {}
func (*noopMetricsCollector) FanOutStarted(_ context.Context, _ string, _ int)                    {}
func (*noopMetricsCollector) FanOutCompleted(_ context.Context, _ string, _ int, _ time.Duration) {}
func (*noopMetricsCollector) FanInStarted(_ context.Context, _ string, _ int)                     {}
func (*noopMetricsCollector) FanInCompleted(_ context.Context, _ string, _ int, _ time.Duration)  {}
func (*noopMetricsCollector) PipelineStarted(_ context.Context, _ string)                         {}
func (*noopMetricsCollector) PipelineCompleted(_ context.Context, _ string, _ time.Duration, _ error) {
}
func (*noopMetricsCollector) StageWorkerConcurrency(_ context.Context, _ string, _ int)             {}
func (*noopMetricsCollector) StageWorkerItemProcessed(_ context.Context, _ string, _ time.Duration) {}
func (*noopMetricsCollector) StageWorkerItemSkipped(_ context.Context, _ string, _ error)           {}
func (*noopMetricsCollector) StageWorkerErrorSent(_ context.Context, _ string, _ error)             {}
func (*noopMetricsCollector) WindowEmitted(_ context.Context, _ string, _ int)                      {}
