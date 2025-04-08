package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// TransientError represents a temporary failure that can be retried
type TransientError struct {
	message string
}

func (e TransientError) Error() string {
	return fmt.Sprintf("transient error: %s", e.message)
}

// PermanentError represents a permanent failure that should not be retried
type PermanentError struct {
	message string
}

func (e PermanentError) Error() string {
	return fmt.Sprintf("permanent error: %s", e.message)
}

// FlakyService simulates a service that fails intermittently
type FlakyService struct {
	name                string
	transientErrorRate  float64
	permanentErrorRate  float64
	successfulAttempts  int
	failedAttempts      int
	recoveringThreshold int // After this many calls, the service starts behaving better
}

// NewFlakyService creates a new flaky service simulator
func NewFlakyService(name string, transientRate, permanentRate float64, recoveringThreshold int) *FlakyService {
	return &FlakyService{
		name:                name,
		transientErrorRate:  transientRate,
		permanentErrorRate:  permanentRate,
		recoveringThreshold: recoveringThreshold,
	}
}

// Call simulates calling the flaky service
func (s *FlakyService) Call(ctx context.Context, request string) (string, error) {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
		// Continue with the call
	}

	// Simulate some processing time
	processingTime := 100 + rand.Intn(200)
	time.Sleep(time.Duration(processingTime) * time.Millisecond)

	// Determine if the service should start recovering
	totalAttempts := s.successfulAttempts + s.failedAttempts
	if totalAttempts >= s.recoveringThreshold {
		// Reduce error rates once we've had enough attempts
		s.transientErrorRate /= 2
		s.permanentErrorRate /= 2
	}

	// Determine outcome
	r := rand.Float64()

	if r < s.permanentErrorRate {
		// Permanent failure
		s.failedAttempts++
		errorMsg := fmt.Sprintf("%s failed with permanent error (attempt #%d for %s)",
			s.name, totalAttempts+1, request)
		return "", PermanentError{message: errorMsg}
	} else if r < s.permanentErrorRate+s.transientErrorRate {
		// Transient failure
		s.failedAttempts++
		errorMsg := fmt.Sprintf("%s failed with transient error (attempt #%d for %s)",
			s.name, totalAttempts+1, request)
		return "", TransientError{message: errorMsg}
	}

	// Success
	s.successfulAttempts++
	return fmt.Sprintf("%s successfully processed %s (attempt #%d)",
		s.name, request, totalAttempts+1), nil
}

// RetryingServiceClient wraps a service with retry capabilities
type RetryingServiceClient struct {
	service *FlakyService
	retry   *fluxus.Retry[string, string]
}

// NewRetryingServiceClient creates a new service client with retry logic
func NewRetryingServiceClient(service *FlakyService, maxAttempts int) *RetryingServiceClient {
	// Create a stage that calls the service
	serviceStage := fluxus.StageFunc[string, string](service.Call)

	// Wrap with retry logic
	retry := fluxus.NewRetry(serviceStage, maxAttempts)

	// Only retry on transient errors
	retry.WithShouldRetry(func(err error) bool {
		var transientErr TransientError
		return errors.As(err, &transientErr)
	})

	// Use exponential backoff
	retry.WithBackoff(func(attempt int) int {
		// Base delay is 100ms, doubles each attempt with some jitter
		baseDelay := 100
		maxJitter := baseDelay / 2
		delay := baseDelay * (1 << attempt) // 100, 200, 400, 800, ...
		jitter := rand.Intn(maxJitter)

		return delay + jitter
	})

	return &RetryingServiceClient{
		service: service,
		retry:   retry,
	}
}

// Call sends a request to the service with retries
func (c *RetryingServiceClient) Call(ctx context.Context, request string) (string, error) {
	return c.retry.Process(ctx, request)
}

// runDemo demonstrates the retry mechanism with a series of requests
func runDemo(service *FlakyService, client *RetryingServiceClient, numRequests int) {
	fmt.Printf("\nRunning %d requests with retries to %s\n", numRequests, service.name)
	fmt.Printf("----------------------------------------------\n")

	successes := 0
	transientFailures := 0
	permanentFailures := 0

	for i := 1; i <= numRequests; i++ {
		requestID := fmt.Sprintf("request-%d", i)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		// Call service with retries
		fmt.Printf("\nSending %s... ", requestID)
		result, err := client.Call(ctx, requestID)

		if err != nil {
			var transientErr TransientError
			var permanentErr PermanentError

			if errors.As(err, &transientErr) {
				transientFailures++
				fmt.Printf("FAILED after retries (transient): %v\n", err)
			} else if errors.As(err, &permanentErr) {
				permanentFailures++
				fmt.Printf("❌ FAILED permanently: %v\n", err)
			} else if errors.Is(err, context.DeadlineExceeded) {
				fmt.Printf("⌛ TIMEOUT: %v\n", err)
				permanentFailures++
			} else {
				fmt.Printf("❌ ERROR: %v\n", err)
				permanentFailures++
			}
		} else {
			successes++
			fmt.Printf("✅ SUCCESS: %s\n", result)
		}

		cancel() // Clean up the context

		// Slight pause between requests
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Total requests: %d\n", numRequests)
	fmt.Printf("  Successes: %d (%.1f%%)\n", successes, float64(successes)/float64(numRequests)*100)
	fmt.Printf("  Permanent failures: %d (%.1f%%)\n", permanentFailures, float64(permanentFailures)/float64(numRequests)*100)
	fmt.Printf("  Transient failures: %d (%.1f%%)\n", transientFailures, float64(transientFailures)/float64(numRequests)*100)
	fmt.Printf("  Service stats: %d successful attempts, %d failed attempts\n",
		service.successfulAttempts, service.failedAttempts)
}

func main() {
	// Set random seed for deterministic results
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Configure logging
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	fmt.Println("Fluxus Retry Mechanism Demonstration")
	fmt.Println("====================================")
	fmt.Println("This example demonstrates the retry mechanism with exponential backoff")
	fmt.Println("and intelligent retry policies. The simulated service begins with high")
	fmt.Println("failure rates but improves over time as more requests are processed.")
	fmt.Println("")
	fmt.Println("- Green checkmarks indicate success")
	fmt.Println("- Yellow warnings indicate transient errors (retried)")
	fmt.Println("- Red X's indicate permanent errors (not retried)")

	// Create a flaky service
	// Initially 50% transient errors, 20% permanent errors, improves after 10 requests
	service := NewFlakyService("ExampleAPI", 0.5, 0.2, 10)

	// Create a client with retry logic
	client := NewRetryingServiceClient(service, 5) // Max 5 attempts

	// Run the demo with 20 requests
	runDemo(service, client, 20)

	fmt.Println("\nDemo Complete!")
}
