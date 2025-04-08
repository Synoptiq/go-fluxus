package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// UnreliableService simulates an external service that occasionally fails
type UnreliableService struct {
	failureRate     float64
	responseTimeMs  int
	requestCount    int
	recoveryTimeMs  int
	isInFailureMode bool
	failureModeTime time.Time
}

// NewUnreliableService creates a new service simulator
func NewUnreliableService(failureRate float64, responseTimeMs, recoveryTimeMs int) *UnreliableService {
	return &UnreliableService{
		failureRate:    failureRate,
		responseTimeMs: responseTimeMs,
		recoveryTimeMs: recoveryTimeMs,
	}
}

// Call simulates calling the external service, which may fail
func (s *UnreliableService) Call(ctx context.Context, request string) (string, error) {
	s.requestCount++

	// Check if the service was in failure mode but has recovered
	if s.isInFailureMode && time.Since(s.failureModeTime) > time.Duration(s.recoveryTimeMs)*time.Millisecond {
		s.isInFailureMode = false
		log.Printf("Service recovered after %d ms", s.recoveryTimeMs)
	}

	// Simulate delay
	time.Sleep(time.Duration(s.responseTimeMs) * time.Millisecond)

	// Either simulate failure or success
	if s.isInFailureMode || rand.Float64() < s.failureRate {
		if !s.isInFailureMode {
			s.isInFailureMode = true
			s.failureModeTime = time.Now()
			log.Printf("Service entered failure mode! Will recover in %d ms", s.recoveryTimeMs)
		}
		return "", fmt.Errorf("service unavailable (request: %s)", request)
	}

	// Service worked correctly
	return fmt.Sprintf("Response for: %s", request), nil
}

// ServiceClient represents a client that uses the circuit breaker pattern
type ServiceClient struct {
	service        *UnreliableService
	circuitBreaker *fluxus.CircuitBreaker[string, string]
}

// NewServiceClient creates a new client with circuit breaker protection
func NewServiceClient(service *UnreliableService) *ServiceClient {
	// Create a stage that calls the service
	serviceStage := fluxus.StageFunc[string, string](service.Call)

	// Wrap the service stage with a circuit breaker
	circuitBreaker := fluxus.NewCircuitBreaker(
		serviceStage,
		3,             // Failures threshold: open after 3 failures
		5*time.Second, // Reset timeout: try again after 5 seconds
		fluxus.WithSuccessThreshold[string, string](2),    // Require 2 successes to close again
		fluxus.WithHalfOpenMaxRequests[string, string](3), // Allow 3 test requests when half-open
	)

	return &ServiceClient{
		service:        service,
		circuitBreaker: circuitBreaker,
	}
}

// CallService calls the service with circuit breaker protection
func (c *ServiceClient) CallService(ctx context.Context, request string) (string, error) {
	return c.circuitBreaker.Process(ctx, request)
}

// GetState returns the current state of the circuit breaker
func (c *ServiceClient) GetState() fluxus.CircuitBreakerState {
	return c.circuitBreaker.State()
}

// GetLastError returns the last error that caused the circuit to open
func (c *ServiceClient) GetLastError() error {
	return c.circuitBreaker.LastError()
}

// HTTP handler for the example
func serviceHandler(client *ServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract request ID from query params or use default
		requestID := r.URL.Query().Get("id")
		if requestID == "" {
			requestID = fmt.Sprintf("req-%d", rand.Intn(1000))
		}

		// Call service with circuit breaker protection
		response, err := client.CallService(r.Context(), requestID)

		// Get circuit state for display
		circuitState := client.GetState()
		var stateStr string

		switch circuitState {
		case fluxus.CircuitOpen:
			stateStr = "OPEN"
		case fluxus.CircuitHalfOpen:
			stateStr = "HALF-OPEN"
		case fluxus.CircuitClosed:
			stateStr = "CLOSED"
		}

		// Respond based on result
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "Error: %v\nCircuit State: %s\n", err, stateStr)
			if errors.Is(err, fluxus.ErrCircuitOpen) {
				lastErr := client.GetLastError()
				fmt.Fprintf(w, "Circuit is open due to: %v\n", lastErr)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Success: %s\nCircuit State: %s\n", response, stateStr)
	}
}

func resetHandler(client *ServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client.circuitBreaker.Reset()
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Circuit breaker has been reset\n")
	}
}

func main() {
	// Seed random for consistent demo
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create an unreliable service (40% failures, 200ms response time, recovers after 8 seconds)
	service := NewUnreliableService(0.4, 200, 8000)

	// Create a client with circuit breaker
	client := NewServiceClient(service)

	// Setup HTTP server
	http.HandleFunc("/call", serviceHandler(client))
	http.HandleFunc("/reset", resetHandler(client))

	// Instructions
	port := 8080
	fmt.Printf("Circuit Breaker Demo Server\n")
	fmt.Printf("---------------------------\n")
	fmt.Printf("The server simulates an unreliable service with a 40%% failure rate.\n")
	fmt.Printf("The circuit breaker will open after 3 failures and try again after 5 seconds.\n\n")
	fmt.Printf("Endpoints:\n")
	fmt.Printf("  http://localhost:%d/call?id=your-request-id - Call the service\n", port)
	fmt.Printf("  http://localhost:%d/reset - Reset the circuit breaker\n\n", port)
	fmt.Printf("Try making several calls rapidly to see the circuit breaker in action!\n\n")

	// Start server
	fmt.Printf("Server started on port %d\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}
