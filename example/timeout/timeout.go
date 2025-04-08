package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// SlowService represents a service with unpredictable response times
type SlowService struct {
	name            string
	minLatencyMs    int
	maxLatencyMs    int
	failureRate     float64
	callCount       int
	timeoutCount    int
	successCount    int
	naturalFailures int
}

// NewSlowService creates a new slow service simulator
func NewSlowService(name string, minLatencyMs, maxLatencyMs int, failureRate float64) *SlowService {
	return &SlowService{
		name:         name,
		minLatencyMs: minLatencyMs,
		maxLatencyMs: maxLatencyMs,
		failureRate:  failureRate,
	}
}

// Call simulates a service call with variable response time
func (s *SlowService) Call(ctx context.Context, request string) (string, error) {
	s.callCount++

	// First check if this call will fail naturally
	if rand.Float64() < s.failureRate {
		s.naturalFailures++
		return "", fmt.Errorf("%s failed to process request: %s (natural failure)", s.name, request)
	}

	// Determine processing time
	processingTime := s.minLatencyMs + rand.Intn(s.maxLatencyMs-s.minLatencyMs+1)

	// Create a timer for the expected processing time
	timer := time.NewTimer(time.Duration(processingTime) * time.Millisecond)
	defer timer.Stop()

	// Simulate processing
	select {
	case <-timer.C:
		// Processing completed successfully
		s.successCount++
		return fmt.Sprintf("[%s] Successfully processed %s in %d ms",
			s.name, request, processingTime), nil

	case <-ctx.Done():
		// Context was cancelled (likely timeout)
		s.timeoutCount++
		return "", ctx.Err()
	}
}

// GetStats returns service statistics
func (s *SlowService) GetStats() map[string]int {
	return map[string]int{
		"calls":            s.callCount,
		"successes":        s.successCount,
		"timeouts":         s.timeoutCount,
		"natural_failures": s.naturalFailures,
	}
}

// TimeoutProtectedClient wraps a service with timeout protection
type TimeoutProtectedClient struct {
	service *SlowService
	timeout *fluxus.Timeout[string, string]
}

// NewTimeoutProtectedClient creates a new client with timeout protection
func NewTimeoutProtectedClient(service *SlowService, timeoutDuration time.Duration) *TimeoutProtectedClient {
	// Create a stage for the service
	serviceStage := fluxus.StageFunc[string, string](service.Call)

	// Wrap with timeout
	timeoutStage := fluxus.NewTimeout(serviceStage, timeoutDuration)

	return &TimeoutProtectedClient{
		service: service,
		timeout: timeoutStage,
	}
}

// Call makes a call to the service with timeout protection
func (c *TimeoutProtectedClient) Call(ctx context.Context, request string) (string, error) {
	return c.timeout.Process(ctx, request)
}

// RunComparisonDemo demonstrates the effect of different timeout settings
func RunComparisonDemo(service *SlowService, requestCount int, timeouts []time.Duration) {
	fmt.Printf("\n🕒 Running timeout comparison for %d requests\n", requestCount)
	fmt.Printf("Service: %s (latency %d-%d ms, failure rate %.1f%%)\n\n",
		service.name, service.minLatencyMs, service.maxLatencyMs, service.failureRate*100)

	// Create base context
	baseCtx := context.Background()

	// Run tests with different timeouts
	for _, timeout := range timeouts {
		fmt.Printf("Testing with timeout: %v\n", timeout)

		// Create a client with this timeout
		client := NewTimeoutProtectedClient(service, timeout)

		// Track results
		successCount := 0
		timeoutCount := 0
		naturalFailureCount := 0
		totalSuccessLatency := 0

		// Process requests
		for i := 1; i <= requestCount; i++ {
			requestID := fmt.Sprintf("req-%d", i)

			startTime := time.Now()
			result, err := client.Call(baseCtx, requestID)
			duration := time.Since(startTime)

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					timeoutCount++
					fmt.Printf("  ⏱️  Request %2d timed out after %7.1f ms\n",
						i, float64(duration.Microseconds())/1000)
				} else {
					naturalFailureCount++
					fmt.Printf("  ❌ Request %2d failed after %7.1f ms: %v\n",
						i, float64(duration.Microseconds())/1000, err)
				}
			} else {
				successCount++
				totalSuccessLatency += int(duration.Milliseconds())
				fmt.Printf("  ✅ Request %2d completed in %7.1f ms: %s\n",
					i, float64(duration.Microseconds())/1000, result)
			}
		}

		// Print summary for this timeout
		fmt.Printf("\n  Summary (timeout=%v):\n", timeout)
		fmt.Printf("    Successful: %d (%.1f%%)\n",
			successCount, float64(successCount)*100/float64(requestCount))
		fmt.Printf("    Timeouts: %d (%.1f%%)\n",
			timeoutCount, float64(timeoutCount)*100/float64(requestCount))
		fmt.Printf("    Natural failures: %d (%.1f%%)\n",
			naturalFailureCount, float64(naturalFailureCount)*100/float64(requestCount))

		if successCount > 0 {
			fmt.Printf("    Avg. latency for successes: %.1f ms\n",
				float64(totalSuccessLatency)/float64(successCount))
		}

		fmt.Println()
	}

	// Verify service stats
	stats := service.GetStats()
	fmt.Printf("Service stats after all tests:\n")
	fmt.Printf("  Total calls: %d\n", stats["calls"])
	fmt.Printf("  Successful responses: %d\n", stats["successes"])
	fmt.Printf("  Timeouts: %d\n", stats["timeouts"])
	fmt.Printf("  Natural failures: %d\n", stats["natural_failures"])
}

// RunChainedTimeoutsDemo demonstrates timeouts in a chained pipeline
func RunChainedTimeoutsDemo() {
	fmt.Println("\n🔄 Running chained timeouts demo")

	// Create multiple services with different characteristics
	authService := NewSlowService("AuthService", 50, 150, 0.05)
	databaseService := NewSlowService("DatabaseService", 100, 400, 0.1)
	processingService := NewSlowService("ProcessingService", 200, 700, 0.05)

	// Create timeout stages for each service
	authStage := fluxus.NewTimeout(
		fluxus.StageFunc[string, string](authService.Call),
		200*time.Millisecond,
	)

	dbStage := fluxus.NewTimeout(
		fluxus.StageFunc[string, string](databaseService.Call),
		500*time.Millisecond,
	)

	processingStage := fluxus.NewTimeout(
		fluxus.StageFunc[string, string](processingService.Call),
		800*time.Millisecond,
	)

	// Chain stages together with data transformations
	stage1 := fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		// Authentication stage
		authResult, err := authStage.Process(ctx, fmt.Sprintf("auth:%s", input))
		if err != nil {
			return "", fmt.Errorf("auth failed: %w", err)
		}
		return authResult, nil
	})

	stage2 := fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		// Database lookup stage
		dbResult, err := dbStage.Process(ctx, fmt.Sprintf("db:%s", input))
		if err != nil {
			return "", fmt.Errorf("database lookup failed: %w", err)
		}
		return dbResult, nil
	})

	stage3 := fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		// Processing stage
		processResult, err := processingStage.Process(ctx, fmt.Sprintf("process:%s", input))
		if err != nil {
			return "", fmt.Errorf("processing failed: %w", err)
		}
		return processResult, nil
	})

	// Chain all stages
	chainedStage := fluxus.Chain(
		stage1,
		fluxus.Chain(stage2, stage3),
	)

	// Create a pipeline with global timeout
	pipeline := fluxus.NewPipeline(
		fluxus.NewTimeout(chainedStage, 2*time.Second),
	)

	// Process some requests
	for i := 1; i <= 5; i++ {
		fmt.Printf("\nRequest %d:\n", i)

		startTime := time.Now()
		requestID := fmt.Sprintf("request-%d", i)

		ctx := context.Background()
		result, err := pipeline.Process(ctx, requestID)

		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("❌ Pipeline failed after %.1f ms: %v\n",
				float64(duration.Microseconds())/1000, err)
		} else {
			fmt.Printf("✅ Pipeline completed in %.1f ms: %s\n",
				float64(duration.Microseconds())/1000, result)
		}
	}

	// Print service stats
	fmt.Println("\nService statistics:")
	fmt.Printf("  Auth Service: %d calls, %d successes, %d timeouts, %d natural failures\n",
		authService.callCount, authService.successCount,
		authService.timeoutCount, authService.naturalFailures)

	fmt.Printf("  Database Service: %d calls, %d successes, %d timeouts, %d natural failures\n",
		databaseService.callCount, databaseService.successCount,
		databaseService.timeoutCount, databaseService.naturalFailures)

	fmt.Printf("  Processing Service: %d calls, %d successes, %d timeouts, %d natural failures\n",
		processingService.callCount, processingService.successCount,
		processingService.timeoutCount, processingService.naturalFailures)
}

func main() {
	// Set random seed
	rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Println("Fluxus Timeout Mechanism Demonstration")
	fmt.Println("======================================")
	fmt.Println("This example demonstrates timeout handling to protect against")
	fmt.Println("slow or unresponsive services, allowing graceful failure when")
	fmt.Println("services don't respond within expected timeframes.")

	// Create a service with variable latency
	// Latency between 200-800ms with 5% natural failure rate
	service := NewSlowService("VariableLatencyAPI", 200, 800, 0.05)

	// Test different timeout settings
	timeouts := []time.Duration{
		300 * time.Millisecond,  // Very aggressive
		500 * time.Millisecond,  // Moderate
		1000 * time.Millisecond, // Lenient
	}

	// Run comparison demo
	RunComparisonDemo(service, 10, timeouts)

	// Run chained timeouts demo
	RunChainedTimeoutsDemo()

	fmt.Println("\nDemo Complete!")
}
