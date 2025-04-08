package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/synoptiq/go-fluxus"
	"golang.org/x/time/rate"
)

// APIService represents a service that makes API calls
type APIService struct {
	name             string
	processingTimeMs int
	callCount        int
	mu               sync.Mutex
}

// NewAPIService creates a new API service simulator
func NewAPIService(name string, processingTimeMs int) *APIService {
	return &APIService{
		name:             name,
		processingTimeMs: processingTimeMs,
	}
}

// Call simulates making an API call with some processing time
func (s *APIService) Call(ctx context.Context, request string) (string, error) {
	// Increment call count
	s.mu.Lock()
	s.callCount++
	currentCount := s.callCount
	s.mu.Unlock()

	// Simulate processing delay
	select {
	case <-time.After(time.Duration(s.processingTimeMs) * time.Millisecond):
		// Continue after delay
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// Return successful response
	return fmt.Sprintf("[%s] Response #%d for request: %s", s.name, currentCount, request), nil
}

// RateLimitedAPIClient wraps a service with rate limiting
type RateLimitedAPIClient struct {
	service     *APIService
	rateLimiter *fluxus.RateLimiter[string, string]
	limit       rate.Limit
	burst       int
}

// NewRateLimitedAPIClient creates a new API client with rate limiting
func NewRateLimitedAPIClient(service *APIService, rps float64, burst int) *RateLimitedAPIClient {
	// Create a stage for the service
	serviceStage := fluxus.StageFunc[string, string](service.Call)

	// Wrap with rate limiter
	rateLimiter := fluxus.NewRateLimiter(
		serviceStage,
		rate.Limit(rps), // Requests per second
		burst,           // Burst capacity
		fluxus.WithLimiterTimeout[string, string](2*time.Second), // Wait up to 2 seconds for a token
	)

	return &RateLimitedAPIClient{
		service:     service,
		rateLimiter: rateLimiter,
		limit:       rate.Limit(rps),
		burst:       burst,
	}
}

// Call makes a rate-limited call to the service
func (c *RateLimitedAPIClient) Call(ctx context.Context, request string) (string, error) {
	return c.rateLimiter.Process(ctx, request)
}

// UpdateLimit updates the rate limit for this client
func (c *RateLimitedAPIClient) UpdateLimit(newRPS float64) {
	c.limit = rate.Limit(newRPS)
	c.rateLimiter.SetLimit(c.limit)
	fmt.Printf("🔄 Rate limit updated to %.1f RPS\n", newRPS)
}

// UpdateBurst updates the burst limit for this client
func (c *RateLimitedAPIClient) UpdateBurst(newBurst int) {
	c.burst = newBurst
	c.rateLimiter.SetBurst(newBurst)
	fmt.Printf("🔄 Burst limit updated to %d\n", newBurst)
}

// DisplayStatus shows the current rate limit settings
func (c *RateLimitedAPIClient) DisplayStatus() {
	fmt.Printf("📊 Client Status: %.1f RPS, Burst: %d, Service: %s\n",
		float64(c.limit), c.burst, c.service.name)
}

// RunBurstDemo demonstrates a burst of requests
func RunBurstDemo(client *RateLimitedAPIClient, requestCount int) {
	fmt.Printf("\n🚀 Starting burst demo with %d requests...\n", requestCount)

	startTime := time.Now()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send requests sequentially
	successCount := 0
	failureCount := 0

	for i := 1; i <= requestCount; i++ {
		requestID := fmt.Sprintf("burst-req-%d", i)

		// Measure this specific request
		requestStart := time.Now()

		result, err := client.Call(ctx, requestID)

		elapsed := time.Since(requestStart)

		if err != nil {
			fmt.Printf("❌ Request %2d failed after %7.1fms: %v\n",
				i, float64(elapsed.Microseconds())/1000, err)
			failureCount++
		} else {
			fmt.Printf("✅ Request %2d completed in %7.1fms: %s\n",
				i, float64(elapsed.Microseconds())/1000, result)
			successCount++
		}
	}

	totalTime := time.Since(startTime)

	fmt.Printf("\n📋 Burst Demo Results:\n")
	fmt.Printf("   Total time: %.2f seconds\n", totalTime.Seconds())
	fmt.Printf("   Successful requests: %d\n", successCount)
	fmt.Printf("   Failed requests: %d\n", failureCount)
	fmt.Printf("   Effective rate: %.2f RPS\n", float64(successCount)/totalTime.Seconds())
}

// RunConcurrentDemo demonstrates concurrent requests with rate limiting
func RunConcurrentDemo(client *RateLimitedAPIClient, concurrency, requestsPerWorker int) {
	totalRequests := concurrency * requestsPerWorker
	fmt.Printf("\n🔄 Starting concurrent demo with %d workers, %d requests each (%d total)...\n",
		concurrency, requestsPerWorker, totalRequests)

	startTime := time.Now()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create wait group for workers
	var wg sync.WaitGroup
	wg.Add(concurrency)

	// Tracking results
	var (
		successCount   int64
		failureCount   int64
		totalLatencyMs int64
	)

	// Mutex for thread-safe updates
	var mu sync.Mutex

	// Launch workers
	for w := 1; w <= concurrency; w++ {
		go func(workerID int) {
			defer wg.Done()

			for i := 1; i <= requestsPerWorker; i++ {
				requestID := fmt.Sprintf("worker-%d-req-%d", workerID, i)

				requestStart := time.Now()
				_, err := client.Call(ctx, requestID)
				elapsed := time.Since(requestStart)

				mu.Lock()
				if err != nil {
					failureCount++
					fmt.Printf("❌ Worker %2d: Request %2d failed after %7.1fms: %v\n",
						workerID, i, float64(elapsed.Microseconds())/1000, err)
				} else {
					successCount++
					totalLatencyMs += elapsed.Milliseconds()
					fmt.Printf("✅ Worker %2d: Request %2d completed in %7.1fms\n",
						workerID, i, float64(elapsed.Microseconds())/1000)
				}
				mu.Unlock()

				// Small sleep to make output more readable
				time.Sleep(10 * time.Millisecond)
			}
		}(w)
	}

	// Wait for all workers to finish
	wg.Wait()

	totalTime := time.Since(startTime)

	fmt.Printf("\n📋 Concurrent Demo Results:\n")
	fmt.Printf("   Total time: %.2f seconds\n", totalTime.Seconds())
	fmt.Printf("   Successful requests: %d\n", successCount)
	fmt.Printf("   Failed requests: %d\n", failureCount)
	fmt.Printf("   Effective rate: %.2f RPS\n", float64(successCount)/totalTime.Seconds())

	if successCount > 0 {
		fmt.Printf("   Average latency: %.2f ms\n", float64(totalLatencyMs)/float64(successCount))
	}
}

// RunDynamicRateDemo demonstrates changing rate limits on the fly
func RunDynamicRateDemo(client *RateLimitedAPIClient) {
	fmt.Printf("\n🔄 Starting dynamic rate limiting demo...\n")

	// Initial settings
	client.DisplayStatus()

	// Run with initial rate
	fmt.Println("\n1️⃣ Initial rate limit:")
	RunBurstDemo(client, 5)

	// Decrease rate limit
	client.UpdateLimit(1.0) // 1 request per second
	client.UpdateBurst(1)   // No bursting
	fmt.Println("\n2️⃣ Decreased rate limit:")
	RunBurstDemo(client, 5)

	// Increase rate limit
	client.UpdateLimit(10.0) // 10 requests per second
	client.UpdateBurst(5)    // Burst of 5
	fmt.Println("\n3️⃣ Increased rate limit:")
	RunBurstDemo(client, 10)
}

func main() {
	fmt.Println("Fluxus Rate Limiter Demonstration")
	fmt.Println("=================================")
	fmt.Println("This example demonstrates rate limiting to control the flow of requests")
	fmt.Println("to a service, preventing it from being overwhelmed while allowing bursts")
	fmt.Println("of traffic when capacity is available.")

	// Create an API service with 100ms processing time
	service := NewAPIService("ExampleAPI", 100)

	// Create a rate-limited client
	// Start with 3 RPS and burst capacity of 2
	client := NewRateLimitedAPIClient(service, 3.0, 2)

	// Part 1: Simple burst demo
	RunBurstDemo(client, 8)

	// Part 2: Concurrent requests demo
	RunConcurrentDemo(client, 3, 4)

	// Part 3: Dynamic rate limiting demo
	RunDynamicRateDemo(client)

	fmt.Println("\nDemo Complete!")
}
