package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// TestChain verifies the Chain functionality
func TestChain(t *testing.T) {
	// Create two stages with different input/output types
	stage1 := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
		return len(input), nil
	})

	stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Length: %d", input), nil
	})

	// Chain the stages
	chainedStage := fluxus.Chain(stage1, stage2)

	// Process with the chained stage
	result, err := chainedStage.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Chain failed: %v", err)
	}

	// Verify result
	expected := "Length: 5"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestBasicPipeline verifies a simple linear pipeline works correctly
func TestBasicPipeline(t *testing.T) {
	// Create two stages with different input/output types
	stage1 := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
		return len(input), nil
	})

	stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Length: %d", input), nil
	})

	// Chain the stages and create a pipeline
	chainedStage := fluxus.Chain(stage1, stage2)
	p := fluxus.NewPipeline(chainedStage)

	// Process the pipeline
	result, err := p.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	expected := "Length: 5"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestChainError verifies error handling in a chain
func TestChainError(t *testing.T) {
	// Create two stages, one of which returns an error
	expectedErr := errors.New("test error")
	stage1 := fluxus.StageFunc[string, int](func(_ context.Context, _ string) (int, error) {
		return 0, expectedErr
	})

	stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Length: %d", input), nil
	})

	// Chain the stages
	chainedStage := fluxus.Chain(stage1, stage2)

	// Process with the chained stage
	_, err := chainedStage.Process(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Verify the error is correctly propagated
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to be %v, got %v", expectedErr, err)
	}
}

// TestPipelineWithError verifies error handling in a pipeline
func TestPipelineWithError(t *testing.T) {
	// Create a pipeline with a stage that returns an error
	expectedErr := errors.New("test error")
	p := fluxus.NewPipeline(
		fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
			return "", expectedErr
		}),
	)

	// Process the pipeline
	_, err := p.Process(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Verify the error is correctly wrapped
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to be %v, got %v", expectedErr, err)
	}
}

// TestFanOut verifies the fan-out functionality
func TestFanOut(t *testing.T) {
	// Create stages that modify the input in different ways
	stage1 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	stage2 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ReplaceAll(input, "l", "L"), nil
	})

	// Create a fan-out with both stages
	fanOut := fluxus.NewFanOut(stage1, stage2)

	// Process the fan-out
	results, err := fanOut.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}

	// Verify results
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	if results[0] != "HELLO" {
		t.Errorf("Expected stage1 result to be %q, got %q", "HELLO", results[0])
	}

	if results[1] != "heLLo" {
		t.Errorf("Expected stage2 result to be %q, got %q", "heLLo", results[1])
	}
}

// TestFanOutWithError verifies error handling in fan-out
func TestFanOutWithError(t *testing.T) {
	// Create stages, one of which returns an error
	stage1 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	expectedErr := errors.New("stage2 error")
	stage2 := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", expectedErr
	})

	// Create a fan-out with both stages
	fanOut := fluxus.NewFanOut(stage1, stage2)

	// Process the fan-out
	_, err := fanOut.Process(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Verify the error contains the stage index and the original error
	if !strings.Contains(err.Error(), "fan-out stage 1") || !errors.Is(err, expectedErr) {
		t.Errorf("Error message doesn't properly identify the failed stage or original error: %v", err)
	}
}

// TestFanIn verifies the fan-in functionality
func TestFanIn(t *testing.T) {
	// Create a fan-in that sums integers
	fanIn := fluxus.NewFanIn(func(inputs []int) (int, error) {
		sum := 0
		for _, input := range inputs {
			sum += input
		}
		return sum, nil
	})

	// Process the fan-in
	result, err := fanIn.Process(context.Background(), []int{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("FanIn failed: %v", err)
	}

	// Verify result
	expected := 15
	if result != expected {
		t.Errorf("Expected result to be %d, got %d", expected, result)
	}
}

// TestParallel verifies the complete parallel processing flow
func TestParallel(t *testing.T) {
	// Create stages for parallel processing
	doubleStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		return input * 2, nil
	})

	tripleStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		return input * 3, nil
	})

	// Create a parallel stage
	parallelStage := fluxus.Parallel(
		[]fluxus.Stage[int, int]{doubleStage, tripleStage},
		func(results []int) (int, error) {
			sum := 0
			for _, result := range results {
				sum += result
			}
			return sum, nil
		},
	)

	// Process the parallel stage
	result, err := parallelStage.Process(context.Background(), 5)
	if err != nil {
		t.Fatalf("Parallel stage failed: %v", err)
	}

	// Verify result (5*2 + 5*3 = 25)
	expected := 25
	if result != expected {
		t.Errorf("Expected result to be %d, got %d", expected, result)
	}
}

// TestMultipleChaining verifies chaining multiple stages with different types
func TestMultipleChaining(t *testing.T) {
	// Create stages with different input/output types
	stage1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, " "), nil
	})

	stage2 := fluxus.StageFunc[[]string, int](func(_ context.Context, input []string) (int, error) {
		return len(input), nil
	})

	stage3 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Word count: %d", input), nil
	})

	// Chain all three stages
	chainedStage := fluxus.Chain(stage1, fluxus.Chain(stage2, stage3))

	// Process with the chained stage
	result, err := chainedStage.Process(context.Background(), "hello world pipeline test")
	if err != nil {
		t.Fatalf("Multiple chain failed: %v", err)
	}

	// Verify result
	expected := "Word count: 4"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestBuffer verifies the buffer functionality
func TestBuffer(t *testing.T) {
	// Create a buffer that processes items in batches of 2
	buffer := fluxus.NewBuffer(2, func(_ context.Context, batch []int) ([]int, error) {
		// Double each item in the batch
		results := make([]int, len(batch))
		for i, item := range batch {
			results[i] = item * 2
		}
		return results, nil
	})

	// Process the buffer with 5 items
	results, err := buffer.Process(context.Background(), []int{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("Buffer failed: %v", err)
	}

	// Verify results
	expected := []int{2, 4, 6, 8, 10}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, result := range results {
		if result != expected[i] {
			t.Errorf("At index %d: expected %d, got %d", i, expected[i], result)
		}
	}
}

// TestRetry verifies the retry functionality
func TestRetry(t *testing.T) {
	attemptCount := 0
	maxAttempts := 3

	// Create a stage that fails the first two times
	failingStage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		attemptCount++
		if attemptCount < maxAttempts {
			return "", fmt.Errorf("attempt %d failed", attemptCount)
		}
		return fmt.Sprintf("Success on attempt %d", attemptCount), nil
	})

	// Create a retry stage
	retry := fluxus.NewRetry(failingStage, maxAttempts)

	// Process with retry
	result, err := retry.Process(context.Background(), "test")
	if err != nil {
		t.Fatalf("Retry failed: %v", err)
	}

	// Verify result
	expected := "Success on attempt 3"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}

	// Verify attempt count
	if attemptCount != maxAttempts {
		t.Errorf("Expected %d attempts, got %d", maxAttempts, attemptCount)
	}
}

// TestRetryExhausted verifies behavior when retries are exhausted
func TestRetryExhausted(t *testing.T) {
	// Create a stage that always fails
	failingStage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", errors.New("always fails")
	})

	// Create a retry stage with 3 attempts
	retry := fluxus.NewRetry(failingStage, 3)

	// Process with retry
	_, err := retry.Process(context.Background(), "test")
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Verify error message contains attempt count
	if !strings.Contains(err.Error(), "exhausted 3 attempts") {
		t.Errorf("Error message doesn't mention exhausted attempts: %v", err)
	}
}

// TestTimeout verifies the timeout functionality
func TestTimeout(t *testing.T) {
	// Create a stage that takes longer than the timeout
	slowStage := fluxus.StageFunc[string, string](func(ctx context.Context, _ string) (string, error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return "Done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	// Create a timeout stage with a short timeout
	timeout := fluxus.NewTimeout(slowStage, 100*time.Millisecond)

	// Process with timeout
	_, err := timeout.Process(context.Background(), "test")
	if err == nil {
		t.Fatal("Expected a timeout error, got nil")
	}

	// Verify error message mentions timeout
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("Error message doesn't mention timeout: %v", err)
	}
}

// TestCancellation verifies that pipeline respects context cancellation
func TestCancellation(t *testing.T) {
	// Create a stage that checks for cancellation
	stage := fluxus.StageFunc[string, string](func(ctx context.Context, _ string) (string, error) {
		select {
		case <-time.After(500 * time.Millisecond):
			return "Done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	// Create a pipeline with the stage
	p := fluxus.NewPipeline(stage)

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Start processing in a goroutine
	resultCh := make(chan string)
	errCh := make(chan error)
	go func() {
		result, err := p.Process(ctx, "test")
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	// Cancel the context after a short delay
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Wait for result or error
	select {
	case <-resultCh:
		t.Fatal("Expected an error due to cancellation, got success")
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Test timed out")
	}
}

// TestComplexPipeline tests a more complex pipeline with multiple stages and error handling
func TestComplexPipeline(t *testing.T) {
	// Create a complex pipeline that matches the diagram in the request:
	//
	//                       (parallel processing)
	//                      /-> func2 --\
	// input-> func1 -<                  -> func4 -> output
	//                      \-> func3 --/

	// Define the stages
	func1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, " "), nil
	})

	func2 := fluxus.StageFunc[[]string, string](func(_ context.Context, words []string) (string, error) {
		var result []string
		for _, word := range words {
			result = append(result, strings.ToUpper(word))
		}
		return strings.Join(result, "+"), nil
	})

	func3 := fluxus.StageFunc[[]string, string](func(_ context.Context, words []string) (string, error) {
		return fmt.Sprintf("Word count: %d", len(words)), nil
	})

	func4 := fluxus.StageFunc[[]string, string](func(_ context.Context, inputs []string) (string, error) {
		return strings.Join(inputs, " | "), nil
	})

	// Create the parallel part
	parallelStage := fluxus.Parallel[[]string, string, []string](
		[]fluxus.Stage[[]string, string]{func2, func3},
		func(results []string) ([]string, error) {
			return results, nil
		},
	)

	// Create the complete pipeline using Chain
	chainedStage := fluxus.Chain(func1,
		fluxus.Chain(parallelStage, func4))

	p := fluxus.NewPipeline(chainedStage)

	// Process with the pipeline
	input := "hello world pipeline"
	result, err := p.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Complex pipeline failed: %v", err)
	}

	// Verify the result contains outputs from both parallel branches
	if !strings.Contains(result, "HELLO+WORLD+PIPELINE") || !strings.Contains(result, "Word count: 3") {
		t.Errorf("Expected result to contain both parallel outputs, got: %s", result)
	}
}

// TestMapBasic tests the basic functionality of the Map stage.
func TestMapBasic(t *testing.T) {
	// Stage that converts int to string
	intToString := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return strconv.Itoa(input), nil
	})

	// Create Map stage
	mapStage := fluxus.NewMap(intToString).WithConcurrency(2)

	// Process
	inputs := []int{1, 2, 3, 4, 5}
	results, err := mapStage.Process(context.Background(), inputs)
	if err != nil {
		t.Fatalf("Map stage failed: %v", err)
	}

	// Verify results
	expected := []string{"1", "2", "3", "4", "5"}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, res := range results {
		if res != expected[i] {
			t.Errorf("Expected result %q at index %d, got %q", expected[i], i, res)
		}
	}
}

// TestMapEmptyInput tests the Map stage with an empty input slice.
func TestMapEmptyInput(t *testing.T) {
	intToString := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return strconv.Itoa(input), nil
	})
	mapStage := fluxus.NewMap(intToString)

	results, err := mapStage.Process(context.Background(), []int{})
	if err != nil {
		t.Fatalf("Map stage failed with empty input: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty input, got %d", len(results))
	}
}

// TestMapFailFast tests the default error handling (fail on first error).
func TestMapFailFast(t *testing.T) {
	expectedErr := errors.New("processing error")
	// Stage that fails for even numbers
	failOnEven := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		if input%2 == 0 {
			return "", expectedErr
		}
		return strconv.Itoa(input), nil
	})

	mapStage := fluxus.NewMap(failOnEven).WithConcurrency(2)

	inputs := []int{1, 3, 2, 4, 5} // Error should occur at input '2' (index 2)
	_, err := mapStage.Process(context.Background(), inputs)

	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Check if the error is the expected one, potentially wrapped
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to wrap %v, got %v", expectedErr, err)
	}

	// Check if the error message indicates the item index
	if !strings.Contains(err.Error(), "map stage item") {
		t.Errorf("Expected error message to contain 'map stage item', got: %s", err.Error())
	}
}

// TestMapCollectErrors tests error collection mode.
func TestMapCollectErrors(t *testing.T) {
	expectedErr1 := errors.New("error on 2")
	expectedErr2 := errors.New("error on 4")

	// Stage that fails for even numbers
	failOnEven := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		time.Sleep(10 * time.Millisecond) // Add slight delay
		if input == 2 {
			return "", expectedErr1
		}
		if input == 4 {
			return "", expectedErr2
		}
		return strconv.Itoa(input), nil
	})

	mapStage := fluxus.NewMap(failOnEven).
		WithConcurrency(4).
		WithCollectErrors(true)

	inputs := []int{1, 2, 3, 4, 5}
	results, err := mapStage.Process(context.Background(), inputs)

	if err == nil {
		t.Fatal("Expected a MultiError, got nil")
	}

	// Check if it's a MultiError
	var multiErr *fluxus.MultiError
	if !errors.As(err, &multiErr) {
		t.Fatalf("Expected error to be a *fluxus.MultiError, got %T", err)
	}

	// Check the number of collected errors
	if len(multiErr.Errors) != 2 {
		t.Errorf("Expected 2 errors in MultiError, got %d", len(multiErr.Errors))
	}

	// Check if the specific errors are present (order might vary due to concurrency)
	foundErr1 := false
	foundErr2 := false
	for _, itemErr := range multiErr.Errors {
		if errors.Is(itemErr, expectedErr1) {
			foundErr1 = true
		}
		if errors.Is(itemErr, expectedErr2) {
			foundErr2 = true
		}
	}
	if !foundErr1 || !foundErr2 {
		t.Errorf("Expected MultiError to contain both specific errors, got: %v", multiErr.Errors)
	}

	// Check the results slice (should contain zero values for failed items)
	expectedResults := []string{"1", "", "3", "", "5"} // "" is the zero value for string
	if len(results) != len(expectedResults) {
		t.Fatalf("Expected %d results, got %d", len(expectedResults), len(results))
	}
	for i, res := range results {
		if res != expectedResults[i] {
			t.Errorf("Expected result %q at index %d, got %q", expectedResults[i], i, res)
		}
	}
}

// TestMapConcurrencyLimit tests if the concurrency limit is respected.
func TestMapConcurrencyLimit(t *testing.T) {
	concurrencyLimit := 2
	var currentConcurrent atomic.Int32
	var maxConcurrent atomic.Int32

	// Stage that tracks concurrency
	trackingStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		concurrent := currentConcurrent.Add(1)
		// Track maximum observed concurrency
		for {
			currentMax := maxConcurrent.Load()
			if concurrent > currentMax {
				if maxConcurrent.CompareAndSwap(currentMax, concurrent) {
					break // Successfully updated max
				}
				// Contention, retry load and compare
				continue
			}
			break // Current is not greater than max
		}

		time.Sleep(50 * time.Millisecond) // Simulate work
		currentConcurrent.Add(-1)
		return input * 2, nil
	})

	mapStage := fluxus.NewMap(trackingStage).WithConcurrency(concurrencyLimit)

	inputs := make([]int, 10) // Process 10 items
	_, err := mapStage.Process(context.Background(), inputs)
	if err != nil {
		t.Fatalf("Map stage failed: %v", err)
	}

	// Check max concurrency
	observedMax := maxConcurrent.Load()
	if observedMax > int32(concurrencyLimit) {
		t.Errorf("Exceeded concurrency limit: expected max %d, observed %d", concurrencyLimit, observedMax)
	}
	if observedMax == 0 && len(inputs) > 0 {
		t.Error("Max concurrency was 0, indicating stage might not have run")
	}
	t.Logf("Observed max concurrency: %d (Limit: %d)", observedMax, concurrencyLimit)
}

// TestMapContextCancellation tests cancellation during processing.
func TestMapContextCancellation(t *testing.T) {
	cancelTime := 50 * time.Millisecond
	stageDuration := 100 * time.Millisecond

	// Stage that sleeps longer than the cancellation time
	slowStage := fluxus.StageFunc[int, int](func(ctx context.Context, input int) (int, error) {
		select {
		case <-time.After(stageDuration):
			return input, nil
		case <-ctx.Done():
			return 0, ctx.Err() // Propagate cancellation error
		}
	})

	mapStage := fluxus.NewMap(slowStage).WithConcurrency(4)

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	inputs := make([]int, 10)
	_, err := mapStage.Process(ctx, inputs)

	if err == nil {
		t.Fatal("Expected a cancellation error, got nil")
	}

	// Check if the error is context.DeadlineExceeded or context.Canceled
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.DeadlineExceeded or context.Canceled, got %v", err)
	}
}

// TestMapContextCancellationWaiting tests cancellation while waiting for semaphore.
func TestMapContextCancellationWaiting(t *testing.T) {
	concurrencyLimit := 1
	cancelTime := 20 * time.Millisecond
	stageDuration := 100 * time.Millisecond

	// Stage that sleeps
	slowStage := fluxus.StageFunc[int, int](func(ctx context.Context, input int) (int, error) {
		select {
		case <-time.After(stageDuration):
			return input, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	mapStage := fluxus.NewMap(slowStage).WithConcurrency(concurrencyLimit)

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	// Process more items than concurrency limit to force waiting
	inputs := make([]int, 5)
	_, err := mapStage.Process(ctx, inputs)

	if err == nil {
		t.Fatal("Expected a cancellation error, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.DeadlineExceeded or context.Canceled, got %v", err)
	}
}

// BenchmarkSimplePipeline benchmarks a simple pipeline with a single stage
func BenchmarkSimplePipeline(b *testing.B) {
	// Create a simple stage
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Create a pipeline
	p := fluxus.NewPipeline(stage)

	// Prepare input
	input := "hello world"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkChainedPipeline benchmarks a pipeline with multiple chained stages
func BenchmarkChainedPipeline(b *testing.B) {
	// Create stages
	stage1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, " "), nil
	})

	stage2 := fluxus.StageFunc[[]string, int](func(_ context.Context, input []string) (int, error) {
		return len(input), nil
	})

	stage3 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Count: %d", input), nil
	})

	// Chain the stages
	chainedStage := fluxus.Chain(stage1, fluxus.Chain(stage2, stage3))

	// Create a pipeline
	p := fluxus.NewPipeline(chainedStage)

	// Prepare input
	input := "hello world benchmark test"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkParallelProcessing benchmarks a pipeline with parallel processing
func BenchmarkParallelProcessing(b *testing.B) {
	// Create stages for parallel processing
	stages := []fluxus.Stage[int, int]{
		fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input * 2, nil
		}),
		fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input * 3, nil
		}),
		fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input * 4, nil
		}),
	}

	// Create a parallel stage
	parallelStage := fluxus.Parallel(
		stages,
		func(results []int) (int, error) {
			sum := 0
			for _, result := range results {
				sum += result
			}
			return sum, nil
		},
	)

	// Create a pipeline
	p := fluxus.NewPipeline(parallelStage)

	// Prepare input
	input := 42
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkFanOut benchmarks the fan-out operation
func BenchmarkFanOut(b *testing.B) {
	// Create stages for fan-out
	numStages := 10
	stages := make([]fluxus.Stage[string, string], numStages)
	for i := 0; i < numStages; i++ {
		// Capture loop variable
		stages[i] = fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
			return fmt.Sprintf("%s-%d", input, i), nil
		})
	}

	// Create a fan-out
	fanOut := fluxus.NewFanOut(stages...)

	// Prepare input
	input := "test"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fanOut.Process(ctx, input)
	}
}

// BenchmarkFanOutConcurrencyLimited benchmarks fan-out with limited concurrency
func BenchmarkFanOutConcurrencyLimited(b *testing.B) {
	// Create stages for fan-out
	numStages := 100
	stages := make([]fluxus.Stage[string, string], numStages)
	for i := 0; i < numStages; i++ {
		// Capture loop variable
		stages[i] = fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
			// Minimal CPU-bound work with limited allocations
			x := i
			for j := 0; j < 1000; j++ {
				x = (x * 31) % 997 // Simple hash computation, no allocations
			}
			return fmt.Sprintf("%s-%d", input, x), nil // Single allocation
		})
	}

	// Create a fan-out with limited concurrency
	fanOut := fluxus.NewFanOut(stages...).WithConcurrency(4)

	// Prepare input
	input := "test"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fanOut.Process(ctx, input)
	}
}

// BenchmarkRetry benchmarks the retry functionality
// BenchmarkRetry benchmarks the retry functionality
func BenchmarkRetry(b *testing.B) {
	// Prepare input
	input := "test"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset counter for each benchmark iteration
		attemptCounter := 0
		maxFailures := 2

		// Create a stage that fails deterministically based on the counter
		stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
			attemptCounter++ // Counter persists across calls within one retry.Process
			if attemptCounter <= maxFailures {
				return "", errors.New("simulated error")
			}
			return input + "-success", nil
		})

		// Create a retry stage for this iteration
		retry := fluxus.NewRetry(stage, maxFailures+1) // Max attempts = failures + 1 success

		// Process
		_, _ = retry.Process(ctx, input)
	}
}

// BenchmarkBuffer benchmarks the buffer functionality
func BenchmarkBuffer(b *testing.B) {
	// Create input data
	const batchSize = 100
	const numItems = 1000
	inputs := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		inputs[i] = i
	}

	// Create a buffer
	buffer := fluxus.NewBuffer(batchSize, func(_ context.Context, batch []int) ([]int, error) {
		results := make([]int, len(batch))
		for i, item := range batch {
			results[i] = item * 2
		}
		return results, nil
	})

	// Prepare context
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = buffer.Process(ctx, inputs)
	}
}

// BenchmarkComplexPipeline benchmarks a complex pipeline with multiple stages
func BenchmarkComplexPipeline(b *testing.B) {
	// Create stages for a more complex pipeline
	preprocessStage := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, ","), nil
	})

	// Create parallel stages
	stage1 := fluxus.StageFunc[[]string, []string](func(_ context.Context, input []string) ([]string, error) {
		result := make([]string, len(input))
		for i, s := range input {
			result[i] = strings.ToUpper(s)
		}
		return result, nil
	})

	stage2 := fluxus.StageFunc[[]string, int](func(_ context.Context, input []string) (int, error) {
		totalLen := 0
		for _, s := range input {
			totalLen += len(s)
		}
		return totalLen, nil
	})

	// Create parallel processing
	parallelStages := []fluxus.Stage[[]string, interface{}]{
		// We need to adapt the stages to have a common output type
		fluxus.StageFunc[[]string, interface{}](func(ctx context.Context, input []string) (interface{}, error) {
			return stage1.Process(ctx, input)
		}),
		fluxus.StageFunc[[]string, interface{}](func(ctx context.Context, input []string) (interface{}, error) {
			return stage2.Process(ctx, input)
		}),
	}

	// Create a parallel stage
	parallelStage := fluxus.StageFunc[[]string, []interface{}](
		func(ctx context.Context, input []string) ([]interface{}, error) {
			results := make([]interface{}, len(parallelStages))
			for i, stage := range parallelStages {
				result, err := stage.Process(ctx, input)
				if err != nil {
					return nil, err
				}
				results[i] = result
			}
			return results, nil
		},
	)

	// Final stage to combine results
	finalStage := fluxus.StageFunc[[]interface{}, string](func(_ context.Context, input []interface{}) (string, error) {
		if len(input) != 2 {
			return "", fmt.Errorf("expected 2 inputs, got %d", len(input))
		}

		// Type assertions
		upperStrings, ok1 := input[0].([]string)
		totalLen, ok2 := input[1].(int)

		if !ok1 || !ok2 {
			return "", errors.New("type assertion failed")
		}

		return fmt.Sprintf("Uppercase: %s, Total length: %d", strings.Join(upperStrings, "+"), totalLen), nil
	})

	// Chain all stages
	chainedStage := fluxus.Chain(preprocessStage, fluxus.Chain(parallelStage, finalStage))

	// Create a pipeline
	p := fluxus.NewPipeline(chainedStage)

	// Prepare input
	input := "hello,world,pipeline,benchmark"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkChainVsDirectCalls compares chained stages vs. direct function calls
func BenchmarkChainVsDirectCalls(b *testing.B) {
	// Define the functions we'll use
	lenFunc := func(s string) int {
		return len(s)
	}

	formatFunc := func(n int) string {
		return fmt.Sprintf("Length: %d", n)
	}

	// Direct function calls
	b.Run("DirectCalls", func(b *testing.B) {
		input := "hello world benchmark"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			length := lenFunc(input)
			_ = formatFunc(length)
		}
	})

	// Using Chain
	b.Run("ChainedStages", func(b *testing.B) {
		// Create stages
		stage1 := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
			return lenFunc(input), nil
		})

		stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
			return formatFunc(input), nil
		})

		// Chain the stages
		chainedStage := fluxus.Chain(stage1, stage2)

		// Prepare input
		input := "hello world benchmark"
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = chainedStage.Process(ctx, input)
		}
	})
}

// BenchmarkConcurrencyScaling tests how the system scales with different concurrency levels
func BenchmarkConcurrencyScaling(b *testing.B) {
	// Create a set of stages that do some work
	createStages := func(numStages int) []fluxus.Stage[int, int] {
		stages := make([]fluxus.Stage[int, int], numStages)
		for i := 0; i < numStages; i++ {
			stages[i] = fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
				// Simulate CPU-bound work
				result := input
				for j := 0; j < 1000; j++ {
					result = (result * 17) % 987
				}
				return result, nil
			})
		}
		return stages
	}

	// Number of stages
	const numStages = 100

	// Test different concurrency levels
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32, 0} // 0 means unlimited

	for _, concurrency := range concurrencyLevels {
		name := "Concurrency-"
		if concurrency == 0 {
			name += "Unlimited"
		} else {
			name += strconv.Itoa(concurrency)
		}

		b.Run(name, func(b *testing.B) {
			stages := createStages(numStages)
			fanOut := fluxus.NewFanOut(stages...).WithConcurrency(concurrency)

			// Prepare input
			input := 42
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = fanOut.Process(ctx, input)
			}
		})
	}
}

// BenchmarkChainDepth tests how performance scales with chain depth
func BenchmarkChainDepth(b *testing.B) {
	// Test different chain depths
	depths := []int{1, 5, 10, 20, 50}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("ChainDepth-%d", depth), func(b *testing.B) {
			// Create a simple stage that we'll chain multiple times
			simpleStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
				return input + 1, nil
			})

			// Chain the stage to the specified depth
			var chainedStage fluxus.Stage[int, int] = simpleStage
			for i := 1; i < depth; i++ {
				chainedStage = fluxus.Chain(chainedStage, simpleStage)
			}

			// Create a pipeline
			p := fluxus.NewPipeline(chainedStage)

			// Prepare input
			input := 0
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = p.Process(ctx, input)
			}
		})
	}
}

// BenchmarkMap benchmarks the Map stage with varying concurrency and input size.
func BenchmarkMap(b *testing.B) {
	// Simple stage for benchmarking
	processItem := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		// Simulate some CPU work without heavy allocation
		result := input
		for i := 0; i < 50; i++ {
			result = (result*17 + 13) % 1000000
		}
		return strconv.Itoa(result), nil
	})

	inputSizes := []int{10, 100, 1000}
	concurrencyLevels := []int{1, runtime.NumCPU(), runtime.NumCPU() * 4, 0} // 0 means default (NumCPU)

	for _, size := range inputSizes {
		inputs := make([]int, size)
		for i := 0; i < size; i++ {
			inputs[i] = i
		}

		for _, conc := range concurrencyLevels {
			concurrencyLabel := strconv.Itoa(conc)
			if conc == 0 {
				concurrencyLabel = "DefaultCPU"
			}

			mapStage := fluxus.NewMap(processItem)
			if conc > 0 {
				mapStage = mapStage.WithConcurrency(conc)
			}

			b.Run(fmt.Sprintf("Size%d_Conc%s", size, concurrencyLabel), func(b *testing.B) {
				ctx := context.Background()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = mapStage.Process(ctx, inputs)
				}
			})
		}
	}
}

// BenchmarkMapVsManual compares Map stage to manual goroutine implementation.
func BenchmarkMapVsManual(b *testing.B) {
	// Simple stage for benchmarking
	processItem := func(input int) string {
		result := input
		for i := 0; i < 50; i++ {
			result = (result*17 + 13) % 1000000
		}
		return strconv.Itoa(result)
	}
	stageFunc := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return processItem(input), nil
	})

	inputSize := 1000
	inputs := make([]int, inputSize)
	for i := 0; i < inputSize; i++ {
		inputs[i] = i
	}

	concurrency := runtime.NumCPU()

	// Benchmark Map stage
	b.Run("MapStage", func(b *testing.B) {
		mapStage := fluxus.NewMap(stageFunc).WithConcurrency(concurrency)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = mapStage.Process(ctx, inputs)
		}
	})

	// Benchmark Manual Goroutines with Semaphore
	b.Run("ManualGoroutines", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// --- Manual Implementation ---
			results := make([]string, inputSize)
			sem := make(chan struct{}, concurrency)
			var wg sync.WaitGroup

			for j := 0; j < inputSize; j++ {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					// Handle cancellation if needed, skipped for benchmark simplicity.
					// The break only exits the select, which is acceptable here as
					// full cancellation handling isn't the focus of this benchmark.
					//nolint:staticcheck // SA4011: Simple break is intentional for benchmark simplicity.
					break
				}

				wg.Add(1)
				go func(index int, item int) {
					defer func() {
						<-sem
						wg.Done()
					}()
					results[index] = processItem(item)
				}(j, inputs[j])
			}
			wg.Wait()
			// --- End Manual Implementation ---
			_ = results // Use results to prevent optimization
		}
	})
}

// BenchmarkMapVsBuffer compares Map stage to Buffer stage with internal concurrency.
func BenchmarkMapVsBuffer(b *testing.B) {
	// Simple stage for benchmarking
	processItem := func(input int) string {
		result := input
		for i := 0; i < 50; i++ {
			result = (result*17 + 13) % 1000000
		}
		return strconv.Itoa(result)
	}
	stageFunc := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return processItem(input), nil
	})

	inputSize := 1000
	inputs := make([]int, inputSize)
	for i := 0; i < inputSize; i++ {
		inputs[i] = i
	}

	concurrency := runtime.NumCPU()

	// Benchmark Map stage
	b.Run("MapStage", func(b *testing.B) {
		mapStage := fluxus.NewMap(stageFunc).WithConcurrency(concurrency)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = mapStage.Process(ctx, inputs)
		}
	})

	// Benchmark Buffer stage with internal concurrency
	b.Run("BufferWithInternalConc", func(b *testing.B) {
		bufferStage := fluxus.NewBuffer(inputSize, // Process all items in one batch
			func(ctx context.Context, batch []int) ([]string, error) {
				// --- Concurrency logic inside buffer processor ---
				batchLen := len(batch)
				results := make([]string, batchLen)
				sem := make(chan struct{}, concurrency)
				var wg sync.WaitGroup

				for j := 0; j < batchLen; j++ {
					select {
					case sem <- struct{}{}:
					case <-ctx.Done():
						return nil, ctx.Err()
					}

					wg.Add(1)
					go func(index int, item int) {
						defer func() {
							<-sem
							wg.Done()
						}()
						results[index] = processItem(item)
					}(j, batch[j])
				}
				wg.Wait()
				// --- End Concurrency logic ---
				return results, nil
			})

		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = bufferStage.Process(ctx, inputs)
		}
	})
}

func FuzzChain(f *testing.F) {
	// Add some seeds
	f.Add("hello", 5)
	f.Add("", 0)
	f.Add("abcdefghijklmnopqrstuvwxyz", 100)
	f.Add("!@#$%^&*()", -10)

	// Fuzz test
	f.Fuzz(func(t *testing.T, input string, _ int) {
		// Define our stages
		stage1 := fluxus.StageFunc[string, int](func(_ context.Context, s string) (int, error) {
			if len(s) > 1000 {
				// Avoid excessive computation/memory usage
				return 0, errors.New("input too large")
			}
			return len(s), nil
		})

		stage2 := fluxus.StageFunc[int, string](func(_ context.Context, n int) (string, error) {
			// Use the original 'n' (derived from input length), not the capped 'repeat'
			if n < 0 {
				// Handle potential negative length if stage1 logic changes
				return "", nil // Or handle appropriately
			}
			// Cap the repetition based on length to avoid huge strings
			maxRepeat := 100
			if n > maxRepeat {
				n = maxRepeat
			}
			return strings.Repeat("a", n), nil
		})

		// Chain the stages
		chainedStage := fluxus.Chain(stage1, stage2)

		// Process with the chained stage
		ctx := context.Background()
		result, err := chainedStage.Process(ctx, input)

		// Verify results
		if err != nil {
			// Some errors are expected for certain inputs
			t.Logf("Got expected error: %v", err)
			return
		}

		// Verify that the length of the result is correct (within cap)
		expectedLen := len(input)
		if expectedLen > 100 { // Apply the cap used in stage2
			expectedLen = 100
		}
		if len(result) != expectedLen {
			t.Errorf("Input: %q, Expected result length %d, got %d", input, expectedLen, len(result))
		}

		// Verify that the result contains only 'a's
		for i, c := range result {
			if c != 'a' {
				t.Errorf("Input: %q, Expected 'a' at position %d, got %c", input, i, c)
			}
		}
	})
}

// FuzzFanOut tests the fan-out functionality with fuzzed inputs
func FuzzFanOut(f *testing.F) {
	// Add some seeds
	f.Add("hello", 3)
	f.Add("", 0)
	f.Add("test", 10)

	// Fuzz test
	f.Fuzz(func(t *testing.T, input string, numStages int) {
		// Cap numStages to avoid excessive resource usage
		if numStages > 20 {
			numStages = 20
		} else if numStages < 0 {
			numStages = 0
		}

		// Create stages
		stages := make([]fluxus.Stage[string, string], numStages)
		for i := 0; i < numStages; i++ {
			stageIndex := i // Capture loop variable
			stages[i] = fluxus.StageFunc[string, string](func(_ context.Context, s string) (string, error) {
				if len(s) > 1000 {
					// Avoid excessive computation
					return "", errors.New("input too large")
				}
				return fmt.Sprintf("%s-%d", s, stageIndex), nil
			})
		}

		// Create a fan-out
		fanOut := fluxus.NewFanOut(stages...)

		// Process with fan-out
		ctx := context.Background()
		results, err := fanOut.Process(ctx, input)

		// Check results
		if err != nil {
			t.Logf("Got error: %v", err)
			return
		}

		// Verify number of results
		if len(results) != numStages {
			t.Errorf("Expected %d results, got %d", numStages, len(results))
		}

		// Verify each result
		for i, result := range results {
			expected := fmt.Sprintf("%s-%d", input, i)
			if result != expected {
				t.Errorf("Expected %q at index %d, got %q", expected, i, result)
			}
		}
	})
}

// FuzzChainMany tests the ChainMany function with fuzzed inputs
func FuzzChainMany(f *testing.F) {
	// Add some seeds
	f.Add("12,34,56", 3)
	f.Add("", 0)
	f.Add("99", 1)

	// Fuzz test
	f.Fuzz(func(t *testing.T, input string, numStages int) {
		// Cap numStages to avoid excessive resource usage
		if numStages > 10 {
			numStages = 10
		} else if numStages < 0 {
			numStages = 0
		}

		// Skip invalid cases
		if numStages == 0 {
			return
		}

		// Create a pipeline that:
		// 1. Splits a comma-separated string into parts
		// 2. Converts each part to an integer
		// 3. Performs some operations on the integers
		// 4. Joins the results back into a string

		// Stage 1: Split the string
		stage1 := fluxus.StageFunc[string, []string](func(_ context.Context, s string) ([]string, error) {
			if len(s) > 1000 {
				// Avoid excessive computation
				return nil, errors.New("input too large")
			}
			if s == "" {
				return []string{}, nil
			}
			return strings.Split(s, ","), nil
		})

		// Stage 2: Convert to integers
		stage2 := fluxus.StageFunc[[]string, []int](func(_ context.Context, parts []string) ([]int, error) {
			if len(parts) > 100 {
				// Avoid excessive computation
				return nil, errors.New("too many parts")
			}

			result := make([]int, 0, len(parts))
			for _, part := range parts {
				n, err := strconv.Atoi(part)
				if err != nil {
					// Just skip invalid parts
					continue
				}
				result = append(result, n)
			}
			return result, nil
		})

		// Create additional stages based on numStages
		stages := make([]interface{}, numStages)
		stages[0] = stage1

		if numStages >= 2 {
			stages[1] = stage2
		}

		for i := 2; i < numStages; i++ {
			if i%2 == 0 {
				// A stage that doubles integers
				stages[i] = fluxus.StageFunc[[]int, []int](func(_ context.Context, nums []int) ([]int, error) {
					result := make([]int, len(nums))
					for j, n := range nums {
						result[j] = n * 2
					}
					return result, nil
				})
			} else {
				// A stage that converts integers back to strings
				stages[i] = fluxus.StageFunc[[]int, []string](func(_ context.Context, nums []int) ([]string, error) {
					result := make([]string, len(nums))
					for j, n := range nums {
						result[j] = strconv.Itoa(n)
					}
					return result, nil
				})
			}
		}

		// Use ChainMany
		var chainedStage interface{}

		// Handle special cases
		switch {
		case numStages == 1:
			chainedStage = fluxus.ChainMany[string, []string](stages...)
		case numStages == 2:
			chainedStage = fluxus.ChainMany[string, []int](stages...)
		case numStages%2 == 0: // Even number of stages >= 4
			chainedStage = fluxus.ChainMany[string, []int](stages...)
		default: // Odd number of stages >= 3
			chainedStage = fluxus.ChainMany[string, []string](stages...)
		}

		// Process the input
		ctx := context.Background()

		// Assert the correct type and process
		switch typedStage := chainedStage.(type) {
		case fluxus.Stage[string, []string]:
			result, err := typedStage.Process(ctx, input)
			if err != nil {
				t.Logf("Got error: %v", err)
				return
			}
			// Just check that we got some result
			t.Logf("Got string result with %d parts", len(result))

		case fluxus.Stage[string, []int]:
			result, err := typedStage.Process(ctx, input)
			if err != nil {
				t.Logf("Got error: %v", err)
				return
			}
			// Just check that we got some result
			t.Logf("Got int result with %d parts", len(result))

		default:
			t.Errorf("Unexpected stage type: %T", chainedStage)
		}
	})
}
