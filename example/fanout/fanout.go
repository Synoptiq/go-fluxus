package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// --- Stages for Demonstration ---

// stageToUpper converts input string to uppercase.
var stageToUpper = fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
	fmt.Printf("  [ToUpper] Processing '%s'...\n", input)
	time.Sleep(50 * time.Millisecond) // Simulate work
	return strings.ToUpper(input), nil
})

// stageToLower converts input string to lowercase.
var stageToLower = fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
	fmt.Printf("  [ToLower] Processing '%s'...\n", input)
	time.Sleep(70 * time.Millisecond) // Simulate work
	return strings.ToLower(input), nil
})

// stageReverse reverses the input string.
var stageReverse = fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
	fmt.Printf("  [Reverse] Processing '%s'...\n", input)
	time.Sleep(60 * time.Millisecond) // Simulate work
	runes := []rune(input)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
})

// stageWithError sometimes returns an error.
var stageWithError = fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
	fmt.Printf("  [WithError] Processing '%s'...\n", input)
	time.Sleep(40 * time.Millisecond) // Simulate work
	if rand.Intn(2) == 0 {            // 50% chance of error
		return "", errors.New("simulated error from WithError stage")
	}
	return fmt.Sprintf("NoError: %s", input), nil
})

// stageWithDelay simulates a longer running stage.
func stageWithDelay(id int, delay time.Duration) fluxus.Stage[string, string] {
	return fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		fmt.Printf("  [Delay %d] Processing '%s', sleeping for %v...\n", id, input, delay)
		select {
		case <-time.After(delay):
			fmt.Printf("  [Delay %d] Finished processing '%s'\n", id, input)
			return fmt.Sprintf("Delayed-%d: %s", id, input), nil
		case <-ctx.Done():
			fmt.Printf("  [Delay %d] Cancelled processing '%s'\n", id, input)
			return "", ctx.Err()
		}
	})
}

// --- Demonstration Functions ---

// runBasicFanOutDemo demonstrates basic fan-out functionality.
func runBasicFanOutDemo() {
	fmt.Println("\n🔄 Running Basic FanOut Demo")
	fmt.Println("============================")

	// Create a FanOut stage with multiple processing functions
	fanOut := fluxus.NewFanOut(
		stageToUpper,
		stageToLower,
		stageReverse,
	)

	input := "Hello Fluxus"
	fmt.Printf("Processing input '%s' with %d stages concurrently...\n", input, 3)

	startTime := time.Now()
	results, err := fanOut.Process(context.Background(), input)
	duration := time.Since(startTime)

	if err != nil {
		log.Fatalf("Basic FanOut failed: %v", err)
	}

	fmt.Printf("\n📊 Basic FanOut Results (took %v):\n", duration)
	for i, result := range results {
		fmt.Printf("  Result from stage %d: %s\n", i, result)
	}
}

// runConcurrencyLimitedFanOutDemo demonstrates limiting concurrency.
func runConcurrencyLimitedFanOutDemo() {
	fmt.Println("\n🚦 Running Concurrency Limited FanOut Demo")
	fmt.Println("==========================================")

	numStages := 8
	stages := make([]fluxus.Stage[string, string], numStages)
	for i := 0; i < numStages; i++ {
		stages[i] = stageWithDelay(i, time.Duration(50+rand.Intn(100))*time.Millisecond)
	}

	input := "Concurrency Test"

	// --- Run with unlimited concurrency ---
	fmt.Printf("\nProcessing '%s' with %d stages (unlimited concurrency)...\n", input, numStages)
	fanOutUnlimited := fluxus.NewFanOut(stages...)
	startTimeUnlimited := time.Now()
	resultsUnlimited, err := fanOutUnlimited.Process(context.Background(), input)
	durationUnlimited := time.Since(startTimeUnlimited)

	if err != nil {
		log.Fatalf("Unlimited FanOut failed: %v", err)
	}
	fmt.Printf("✅ Unlimited concurrency finished in %v. Results: %d\n", durationUnlimited, len(resultsUnlimited))

	// --- Run with limited concurrency ---
	concurrencyLimit := runtime.NumCPU() // Limit to number of CPU cores
	if concurrencyLimit > numStages {
		concurrencyLimit = numStages
	}
	if concurrencyLimit < 1 {
		concurrencyLimit = 1
	}

	fmt.Printf("\nProcessing '%s' with %d stages (concurrency limited to %d)...\n", input, numStages, concurrencyLimit)
	fanOutLimited := fluxus.NewFanOut(stages...).WithConcurrency(concurrencyLimit)
	startTimeLimited := time.Now()
	resultsLimited, err := fanOutLimited.Process(context.Background(), input)
	durationLimited := time.Since(startTimeLimited)

	if err != nil {
		log.Fatalf("Limited FanOut failed: %v", err)
	}
	fmt.Printf("✅ Limited concurrency (%d) finished in %v. Results: %d\n", concurrencyLimit, durationLimited, len(resultsLimited))

	fmt.Printf("\n📊 Concurrency Comparison:\n")
	fmt.Printf("  Unlimited: %v\n", durationUnlimited)
	fmt.Printf("  Limited (%d): %v\n", concurrencyLimit, durationLimited)
	// Note: Limited concurrency might be slower if stages are short and CPU-bound,
	// but can be faster or use fewer resources if stages involve I/O or contention.
}

// runErrorHandlingFanOutDemo demonstrates error handling.
func runErrorHandlingFanOutDemo() {
	fmt.Println("\n⚠️ Running Error Handling FanOut Demo")
	fmt.Println("=====================================")

	// Create a FanOut stage including one that might error
	fanOut := fluxus.NewFanOut(
		stageToUpper,
		stageWithError, // This one might fail
		stageReverse,
	)

	input := "Error Test"
	fmt.Printf("Processing input '%s' with stages (one might error)...\n", input)

	// --- Default error handling ---
	fmt.Println("\nAttempt 1 (Default Error Handling):")
	_, err := fanOut.Process(context.Background(), input)

	if err != nil {
		fmt.Printf("❌ Received expected error: %v\n", err)
		// Check if the error message indicates which stage failed
		if strings.Contains(err.Error(), "fan-out stage 1") {
			fmt.Println("✅ Error message correctly identifies the failing stage index.")
		} else {
			fmt.Println("❌ Error message does not clearly identify the failing stage index.")
		}
	} else {
		fmt.Println("❓ Expected an error but got none (stageWithError might have succeeded). Run again?")
	}

	// --- Custom error handling ---
	fmt.Println("\nAttempt 2 (Custom Error Handling):")
	customFanOut := fanOut.WithErrorHandler(func(err error) error {
		// Wrap the original error
		return fmt.Errorf("custom fan-out handler: %w", err)
	})

	_, err = customFanOut.Process(context.Background(), input)

	if err != nil {
		fmt.Printf("❌ Received expected error: %v\n", err)
		// Check if the error is wrapped
		if strings.Contains(err.Error(), "custom fan-out handler:") {
			fmt.Println("✅ Error was properly wrapped by the custom handler.")
			// You can use errors.Unwrap to get the original error
			originalErr := errors.Unwrap(err)
			fmt.Printf("  Original error: %v\n", originalErr)
		} else {
			fmt.Println("❌ Error was not wrapped by the custom handler.")
		}
	} else {
		fmt.Println("❓ Expected an error but got none (stageWithError might have succeeded). Run again?")
	}
}

func main() {
	// Set seed for reproducible error in stageWithError (optional)
	// rand.New(rand.NewSource(1)) // Use a fixed seed for consistent error behavior
	rand.New(rand.NewSource(time.Now().UnixNano())) // Use random seed

	fmt.Println("Fluxus FanOut Stage Demonstration")
	fmt.Println("=================================")
	fmt.Println("This example demonstrates processing a single input")
	fmt.Println("through multiple stages concurrently using FanOut.")

	// Run demos
	runBasicFanOutDemo()
	runConcurrencyLimitedFanOutDemo()
	runErrorHandlingFanOutDemo()

	fmt.Println("\nDemo Complete!")
}
