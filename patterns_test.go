package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/synoptiq/go-fluxus"
)

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

// TestBufferBasic tests the basic functionality of the Buffer stage.
func TestBufferBasic(t *testing.T) {
	testCases := []struct {
		name        string
		bufferSize  int
		input       []int
		expected    []int
		expectError bool
	}{
		{
			name:       "ExactMultipleBatches",
			bufferSize: 2,
			input:      []int{1, 2, 3, 4},
			expected:   []int{2, 4, 6, 8},
		},
		{
			name:       "PartialLastBatch",
			bufferSize: 3,
			input:      []int{1, 2, 3, 4, 5},
			expected:   []int{2, 4, 6, 8, 10},
		},
		{
			name:       "SingleBatchLessThanSize",
			bufferSize: 5,
			input:      []int{1, 2, 3},
			expected:   []int{2, 4, 6},
		},
		{
			name:       "SingleBatchExactSize",
			bufferSize: 3,
			input:      []int{1, 2, 3},
			expected:   []int{2, 4, 6},
		},
		{
			name:       "BufferSizeOne",
			bufferSize: 1,
			input:      []int{1, 2, 3, 4, 5},
			expected:   []int{2, 4, 6, 8, 10},
		},
		{
			name:       "EmptyInput",
			bufferSize: 3,
			input:      []int{},
			expected:   []int{},
		},
	}

	// Batch processor function: doubles each item
	processBatch := func(_ context.Context, batch []int) ([]int, error) {
		results := make([]int, len(batch))
		for i, item := range batch {
			results[i] = item * 2
		}
		return results, nil
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buffer := fluxus.NewBuffer(tc.bufferSize, processBatch)
			results, err := buffer.Process(context.Background(), tc.input)
			//nolint:nestif // This is a test, so nesting is acceptable for clarity.
			if tc.expectError {
				if err == nil {
					t.Fatal("Expected an error, but got nil")
				}
				// Add more specific error checks if needed
			} else {
				if err != nil {
					t.Fatalf("Buffer failed unexpectedly: %v", err)
				}
				if len(results) != len(tc.expected) {
					t.Fatalf("Expected %d results, got %d. Results: %v", len(tc.expected), len(results), results)
				}
				for i, result := range results {
					if result != tc.expected[i] {
						t.Errorf("At index %d: expected %d, got %d", i, tc.expected[i], result)
					}
				}
			}
		})
	}
}

// TestBufferErrorInBatch tests error handling when the batch processor fails.
func TestBufferErrorInBatch(t *testing.T) {
	expectedErr := errors.New("batch processing failed")

	// Batch processor that fails on the second batch
	var batchCount atomic.Int32
	processBatch := func(_ context.Context, batch []int) ([]int, error) {
		count := batchCount.Add(1)
		if count == 2 {
			return nil, expectedErr // Fail on the second batch
		}
		// Otherwise, succeed (return empty slice for simplicity)
		return make([]int, len(batch)), nil
	}

	buffer := fluxus.NewBuffer(2, processBatch) // Buffer size 2

	input := []int{1, 2, 3, 4, 5} // Will process {1, 2}, then {3, 4} (fails here)
	_, err := buffer.Process(context.Background(), input)

	if err == nil {
		t.Fatal("Expected an error, but got nil")
	}

	// Check if the error wraps the expected underlying error. This is the key check.
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to wrap '%v', but got '%v'", expectedErr, err)
	}

	// Optional: You could check if the error message contains the *original* error string,
	// which is less brittle than checking for the wrapper's specific text.
	// if !strings.Contains(err.Error(), expectedErr.Error()) {
	//  t.Errorf("Expected wrapped error message to contain '%s', got: %s", expectedErr.Error(), err.Error())
	// }

	// The previous check for "failed to process batch" is removed as it's too specific.
}

// TestBufferContextCancellation tests cancellation during batch processing.
func TestBufferContextCancellation(t *testing.T) {
	cancelTime := 50 * time.Millisecond
	batchProcessDuration := 100 * time.Millisecond

	// Batch processor that sleeps longer than the cancellation time
	processBatch := func(ctx context.Context, batch []int) ([]int, error) {
		select {
		case <-time.After(batchProcessDuration):
			// Process normally if no cancellation
			results := make([]int, len(batch))
			for i := range batch {
				results[i] = i // Dummy processing
			}
			return results, nil
		case <-ctx.Done():
			return nil, ctx.Err() // Propagate cancellation error
		}
	}

	buffer := fluxus.NewBuffer(2, processBatch)

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	input := []int{1, 2, 3, 4} // Enough data to trigger the slow batch processor
	_, err := buffer.Process(ctx, input)

	if err == nil {
		t.Fatal("Expected a cancellation error, got nil")
	}

	// Check if the error is context.DeadlineExceeded or context.Canceled
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.DeadlineExceeded or context.Canceled, got %v", err)
	}
}

// TestBufferInvalidBufferSize tests creating a buffer with invalid size.
func TestBufferInvalidBufferSize(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected NewBuffer to panic with non-positive buffer size, but it did not")
		}
	}()

	// Dummy batch processor
	processBatch := func(_ context.Context, batch []int) ([]int, error) {
		return batch, nil
	}

	// This should panic
	_ = fluxus.NewBuffer(0, processBatch)
}

// BenchmarkBuffer benchmarks the Buffer stage with varying buffer sizes and input sizes.
func BenchmarkBuffer(b *testing.B) {
	// Simple batch processor for benchmarking: sums items in batch
	// Note: The performance heavily depends on the *work done per batch*,
	// not just the buffering mechanism itself.
	processBatch := func(_ context.Context, batch []int) ([]int, error) {
		// Simulate some work proportional to batch size
		results := make([]int, len(batch))
		sum := 0
		for _, item := range batch {
			sum += item
		}
		// Return the sum in each result slot (just to return something)
		for i := range results {
			results[i] = sum
		}
		// Simulate some constant overhead per batch call
		time.Sleep(1 * time.Microsecond)
		return results, nil
	}

	inputSizes := []int{100, 1000, 10000}
	bufferSizes := []int{1, 10, 100, 1000}

	for _, size := range inputSizes {
		inputs := make([]int, size)
		for i := 0; i < size; i++ {
			inputs[i] = i
		}

		for _, bufSize := range bufferSizes {
			// Skip if buffer size is larger than input size for this benchmark run
			if bufSize > size && size > 0 {
				continue
			}
			// Ensure buffer size is at least 1
			actualBufSize := bufSize
			if actualBufSize < 1 {
				actualBufSize = 1
			}

			bufferStage := fluxus.NewBuffer(actualBufSize, processBatch)

			b.Run(fmt.Sprintf("Input%d_Buffer%d", size, actualBufSize), func(b *testing.B) {
				ctx := context.Background()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = bufferStage.Process(ctx, inputs)
				}
			})
		}
	}
}

// TestFanInBasic tests the basic functionality of the FanIn stage.
// (This expands on your existing TestFanIn)
func TestFanInBasic(t *testing.T) {
	testCases := []struct {
		name           string
		input          []int
		aggregator     func([]int) (int, error)
		expected       int
		expectError    bool
		expectedErrMsg string
	}{
		{
			name:  "Summing",
			input: []int{1, 2, 3, 4, 5},
			aggregator: func(inputs []int) (int, error) {
				sum := 0
				for _, input := range inputs {
					sum += input
				}
				return sum, nil
			},
			expected: 15,
		},
		{
			name:  "EmptyInput",
			input: []int{},
			aggregator: func(inputs []int) (int, error) {
				if len(inputs) != 0 {
					return 0, errors.New("expected empty input")
				}
				return 0, nil // Return 0 for empty sum
			},
			expected: 0,
		},
		{
			name:  "AggregatorError",
			input: []int{1, 2, 3},
			aggregator: func(_ []int) (int, error) {
				return 0, errors.New("aggregation failed")
			},
			expectError:    true,
			expectedErrMsg: "aggregation failed",
		},
		{
			name:  "SingleElement",
			input: []int{42},
			aggregator: func(inputs []int) (int, error) {
				if len(inputs) != 1 {
					return 0, errors.New("expected single element")
				}
				return inputs[0], nil
			},
			expected: 42,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fanIn := fluxus.NewFanIn(tc.aggregator)
			result, err := fanIn.Process(context.Background(), tc.input)
			//nolint:nestif // This is a test, so nesting is acceptable for clarity.
			if tc.expectError {
				if err == nil {
					t.Fatal("Expected an error, but got nil")
				}
				if !strings.Contains(err.Error(), tc.expectedErrMsg) {
					t.Errorf("Expected error message to contain %q, got: %v", tc.expectedErrMsg, err)
				}
			} else {
				if err != nil {
					t.Fatalf("FanIn failed unexpectedly: %v", err)
				}
				if result != tc.expected {
					t.Errorf("Expected result %d, got %d", tc.expected, result)
				}
			}
		})
	}
}

// TestFanInContextCancellation tests cancellation during aggregation.
func TestFanInContextCancellation(t *testing.T) {
	cancelTime := 50 * time.Millisecond
	aggregationDuration := 100 * time.Millisecond

	// This tests if FanIn.Process checks context before calling the aggregator.
	fanIn := fluxus.NewFanIn(func(_ []int) (int, error) {
		// This part won't be reached if context is cancelled before Process calls it.
		time.Sleep(aggregationDuration) // Simulate work anyway
		return 0, nil
	})

	_, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	// Simulate a scenario where the pipeline might be cancelled *before* FanIn gets to run
	// For this specific test, we'll cancel immediately and check Process.
	instantCtx, instantCancel := context.WithCancel(context.Background())
	instantCancel() // Cancel immediately

	_, err := fanIn.Process(instantCtx, []int{1, 2, 3})

	if err == nil {
		t.Fatal("Expected a cancellation error when context is cancelled before Process, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// BenchmarkFanInVsManual compares FanIn stage to a manual aggregation loop.
func BenchmarkFanInVsManual(b *testing.B) {
	// Simple aggregation function (not a stage)
	aggregateFunc := func(inputs []int) int {
		sum := 0
		for _, item := range inputs {
			sum += item
			for i := 0; i < 5; i++ {
				sum = (sum*17 + 13) % 10000000
			}
		}
		time.Sleep(1 * time.Microsecond) // Simulate overhead
		return sum
	}

	// Stage wrapper for the aggregation function
	//nolint:unparam // error is always nil, but we need to satisfy the StageFunc signature
	aggregateStageFunc := func(inputs []int) (int, error) {
		return aggregateFunc(inputs), nil
	}

	inputSize := 10000
	inputs := make([]int, inputSize)
	for i := 0; i < inputSize; i++ {
		inputs[i] = i
	}

	// Benchmark FanIn stage
	b.Run("FanInStage", func(b *testing.B) {
		fanInStage := fluxus.NewFanIn(aggregateStageFunc)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = fanInStage.Process(ctx, inputs)
		}
	})

	// Benchmark Manual Aggregation Loop
	b.Run("ManualAggregation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// --- Manual Implementation ---
			result := aggregateFunc(inputs)
			// --- End Manual Implementation ---
			_ = result // Use result to prevent optimization
		}
	})
}

// TestFanOutBasic tests basic fan-out functionality with multiple stages.
func TestFanOutBasic(t *testing.T) {
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

// TestFanOutWithError tests error handling when one stage fails.
func TestFanOutWithError(t *testing.T) {
	// Create stages, one of which returns an error
	stage1 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	expectedErr := errors.New("stage2 error")
	stage2 := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		time.Sleep(10 * time.Millisecond) // Ensure stage1 likely finishes
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

// TestFanOutSingleStage tests fan-out with only one stage.
func TestFanOutSingleStage(t *testing.T) {
	stage := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Value: %d", input), nil
	})

	fanOut := fluxus.NewFanOut(stage)
	results, err := fanOut.Process(context.Background(), 123)
	if err != nil {
		t.Fatalf("FanOut with single stage failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0] != "Value: 123" {
		t.Errorf("Expected result %q, got %q", "Value: 123", results[0])
	}
}

// TestFanOutNoStages tests creating a fan-out with zero stages.
func TestFanOutNoStages(t *testing.T) {
	// Assuming NewFanOut panics or returns an error for zero stages.
	// Let's assume it panics for consistency with Buffer's invalid size.
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected NewFanOut to panic with zero stages, but it did not")
		}
	}()

	// This should panic
	_ = fluxus.NewFanOut[int, string]() // Need to specify types if no stages are passed
}

// TestFanOutContextCancellation tests cancellation during stage execution.
func TestFanOutContextCancellation(t *testing.T) {
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

	// Fast stage that should finish before cancellation
	fastStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		return input * 2, nil
	})

	fanOut := fluxus.NewFanOut(slowStage, fastStage) // One slow, one fast

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	_, err := fanOut.Process(ctx, 10)

	if err == nil {
		t.Fatal("Expected a cancellation error, got nil")
	}

	// Check if the error is context.DeadlineExceeded or context.Canceled
	// It might also be wrapped by the FanOut error indicating which stage failed.
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected error to wrap context.DeadlineExceeded or context.Canceled, got %v", err)
	}

	// Check if the error message indicates a stage failure (likely the slow one)
	if !strings.Contains(err.Error(), "fan-out stage") {
		t.Errorf("Expected error message to contain 'fan-out stage', got: %s", err.Error())
	}
}

// TestFanOutContextCancellationBeforeStart tests cancellation before processing starts.
func TestFanOutContextCancellationBeforeStart(t *testing.T) {
	stage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		// This shouldn't be called if context is cancelled first
		time.Sleep(50 * time.Millisecond)
		return input, nil
	})

	fanOut := fluxus.NewFanOut(stage)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := fanOut.Process(ctx, 10)

	if err == nil {
		t.Fatal("Expected a cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// BenchmarkFanOut benchmarks the FanOut stage with varying numbers of stages.
func BenchmarkFanOut(b *testing.B) {
	// Simple stage for benchmarking
	processItem := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		// Simulate some CPU work
		result := input
		for i := 0; i < 50; i++ {
			result = (result*17 + 13) % 1000000
		}
		// Simulate tiny sleep for potential scheduling effects
		// time.Sleep(1 * time.Microsecond) // Optional: uncomment to add I/O-like delay
		return strconv.Itoa(result), nil
	})

	numStages := []int{1, 2, 4, 8, 16}
	inputValue := 12345

	for _, count := range numStages {
		stages := make([]fluxus.Stage[int, string], count)
		for i := 0; i < count; i++ {
			stages[i] = processItem // Use the same stage multiple times
		}

		fanOutStage := fluxus.NewFanOut(stages...)

		b.Run(fmt.Sprintf("Stages%d", count), func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = fanOutStage.Process(ctx, inputValue)
			}
		})
	}
}

// BenchmarkFanOutVsManual compares FanOut stage to manual goroutine launch.
func BenchmarkFanOutVsManual(b *testing.B) {
	// Simple processing function (not a stage)
	processItemFunc := func(input int) string {
		result := input
		for i := 0; i < 50; i++ {
			result = (result*17 + 13) % 1000000
		}
		return strconv.Itoa(result)
	}

	// Stage wrapper
	stageFunc := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return processItemFunc(input), nil
	})

	numStages := 8 // Number of concurrent tasks to run
	inputValue := 12345

	// Benchmark FanOut stage
	b.Run("FanOutStage", func(b *testing.B) {
		stages := make([]fluxus.Stage[int, string], numStages)
		for i := 0; i < numStages; i++ {
			stages[i] = stageFunc
		}
		fanOutStage := fluxus.NewFanOut(stages...)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = fanOutStage.Process(ctx, inputValue)
		}
	})

	// Benchmark Manual Goroutines
	b.Run("ManualGoroutines", func(b *testing.B) {
		// ctx := context.Background() // Context not actively used in this manual version for simplicity
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// --- Manual Implementation ---
			results := make([]string, numStages)
			var wg sync.WaitGroup
			wg.Add(numStages)

			for j := 0; j < numStages; j++ {
				go func(index int) {
					defer wg.Done()
					// In a real scenario, check ctx.Done() here
					results[index] = processItemFunc(inputValue)
				}(j)
			}
			wg.Wait()
			// --- End Manual Implementation ---
			_ = results // Use results to prevent optimization
		}
	})
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

// BenchmarkFanOutConcurrencyScaling tests how the system scales with different concurrency levels
func BenchmarkFanOutConcurrencyScaling(b *testing.B) {
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

// --- MapReduce Test Helpers ---

// Simple word count mapper: emits (word, 1)
func wordCountMapper(ctx context.Context, line string) (fluxus.KeyValue[string, int], error) {
	// Simulate some work
	select {
	case <-time.After(1 * time.Millisecond):
	case <-ctx.Done():
		return fluxus.KeyValue[string, int]{}, ctx.Err()
	}
	// Basic word splitting and lowercasing - assumes input is already single words for simplicity here
	word := strings.ToLower(strings.TrimSpace(line))
	if word == "" {
		return fluxus.KeyValue[string, int]{}, errors.New("empty word") // Example error
	}
	if word == "failmap" {
		return fluxus.KeyValue[string, int]{}, errors.New("intentional map failure")
	}
	return fluxus.KeyValue[string, int]{Key: word, Value: 1}, nil
}

// Simple word count reducer: sums values for a key
func wordCountReducer(ctx context.Context, input fluxus.ReduceInput[string, int]) (fluxus.KeyValue[string, int], error) {
	// Simulate some work
	select {
	case <-time.After(1 * time.Millisecond):
	case <-ctx.Done():
		return fluxus.KeyValue[string, int]{}, ctx.Err()
	}

	if input.Key == "failreduce" {
		return fluxus.KeyValue[string, int]{}, errors.New("intentional reduce failure")
	}

	sum := 0
	for _, v := range input.Values {
		sum += v
	}
	return fluxus.KeyValue[string, int]{Key: input.Key, Value: sum}, nil
}

// Helper to sort results for consistent testing
type ByKey []fluxus.KeyValue[string, int]

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// --- MapReduce Tests ---

// TestMapReduceBasicWordCount tests the fundamental MapReduce functionality using a word count example.
func TestMapReduceBasicWordCount(t *testing.T) {
	inputs := []string{"hello", "world", "hello", "fluxus", "world", "hello"}
	expected := []fluxus.KeyValue[string, int]{
		{Key: "fluxus", Value: 1},
		{Key: "hello", Value: 3},
		{Key: "world", Value: 2},
	}

	mapReduceStage := fluxus.NewMapReduce(wordCountMapper, wordCountReducer)
	ctx := context.Background()

	results, err := mapReduceStage.Process(ctx, inputs)

	require.NoError(t, err)
	require.NotNil(t, results)

	// Sort results for comparison as order is not guaranteed
	sort.Sort(ByKey(results))
	sort.Sort(ByKey(expected))

	assert.Equal(t, expected, results)
}

// TestMapReduceParallelism tests MapReduce with parallelism enabled in the map phase.
func TestMapReduceParallelism(t *testing.T) {
	inputs := []string{"a", "b", "a", "c", "b", "a", "d", "a"}
	expected := []fluxus.KeyValue[string, int]{
		{Key: "a", Value: 4},
		{Key: "b", Value: 2},
		{Key: "c", Value: 1},
		{Key: "d", Value: 1},
	}

	// Use parallelism > 1
	mapReduceStage := fluxus.NewMapReduce(wordCountMapper, wordCountReducer).WithParallelism(4)
	ctx := context.Background()

	results, err := mapReduceStage.Process(ctx, inputs)

	require.NoError(t, err)
	require.NotNil(t, results)

	sort.Sort(ByKey(results))
	sort.Sort(ByKey(expected))

	assert.Equal(t, expected, results)
}

// TestMapReduceEmptyInput tests the behavior with an empty input slice.
func TestMapReduceEmptyInput(t *testing.T) {
	inputs := []string{}
	mapReduceStage := fluxus.NewMapReduce(wordCountMapper, wordCountReducer)
	ctx := context.Background()

	results, err := mapReduceStage.Process(ctx, inputs)

	require.NoError(t, err)
	assert.Empty(t, results, "Expected empty results for empty input")
}

// TestMapReduceSingleInput tests the behavior with a single input element.
func TestMapReduceSingleInput(t *testing.T) {
	inputs := []string{"single"}
	expected := []fluxus.KeyValue[string, int]{
		{Key: "single", Value: 1},
	}
	mapReduceStage := fluxus.NewMapReduce(wordCountMapper, wordCountReducer)
	ctx := context.Background()

	results, err := mapReduceStage.Process(ctx, inputs)

	require.NoError(t, err)
	require.NotNil(t, results)
	assert.Equal(t, expected, results)
}

// TestMapReduceMapperError tests error handling when a mapper function fails.
func TestMapReduceMapperError(t *testing.T) {
	inputs := []string{"good", "failmap", "good", "another"} // Contains input causing mapper error
	mapReduceStage := fluxus.NewMapReduce(wordCountMapper, wordCountReducer).WithParallelism(2)
	ctx := context.Background()

	results, err := mapReduceStage.Process(ctx, inputs)

	require.Error(t, err)
	assert.Nil(t, results, "Results should be nil on mapper error")
	require.ErrorContains(t, err, "map phase failed")
	require.ErrorContains(t, err, "intentional map failure")
}

// TestMapReduceReducerError tests error handling when a reducer function fails.
func TestMapReduceReducerError(t *testing.T) {
	inputs := []string{"good", "failreduce", "good", "another"} // Contains input causing reducer error
	mapReduceStage := fluxus.NewMapReduce(wordCountMapper, wordCountReducer).WithParallelism(2)
	ctx := context.Background()

	results, err := mapReduceStage.Process(ctx, inputs)

	require.Error(t, err)
	assert.Nil(t, results, "Results should be nil on reducer error")
	require.ErrorContains(t, err, "reduce phase failed")
	require.ErrorContains(t, err, "intentional reduce failure")
}

// TestMapReduceContextCancellationMapPhase tests cancellation during the map phase.
func TestMapReduceContextCancellationMapPhase(t *testing.T) {
	inputs := make([]string, 100) // Many inputs
	for i := range inputs {
		inputs[i] = fmt.Sprintf("input-%d", i)
	}

	// Mapper with a longer delay to make cancellation easier to hit
	slowMapper := func(ctx context.Context, line string) (fluxus.KeyValue[string, int], error) {
		select {
		case <-time.After(20 * time.Millisecond): // Increased delay
			word := strings.ToLower(strings.TrimSpace(line))
			return fluxus.KeyValue[string, int]{Key: word, Value: 1}, nil
		case <-ctx.Done():
			return fluxus.KeyValue[string, int]{}, ctx.Err()
		}
	}

	mapReduceStage := fluxus.NewMapReduce(slowMapper, wordCountReducer).WithParallelism(4)

	// Create a context that cancels after a short time
	cancelTime := 30 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	startTime := time.Now()
	results, err := mapReduceStage.Process(ctx, inputs)
	duration := time.Since(startTime)

	require.Error(t, err)
	assert.Nil(t, results)
	// The error could be context.DeadlineExceeded directly or wrapped by map/shuffle phase error
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, duration, cancelTime+50*time.Millisecond, "Should fail quickly after cancellation") // Allow some buffer
}

// TestMapReduceContextCancellationReducePhase tests cancellation during the reduce phase.
func TestMapReduceContextCancellationReducePhase(t *testing.T) {
	inputs := []string{"a", "b", "a", "c", "b", "a", "d", "a"} // Inputs that will produce multiple reduce tasks

	// Reducer with a longer delay
	slowReducer := func(ctx context.Context, input fluxus.ReduceInput[string, int]) (fluxus.KeyValue[string, int], error) {
		select {
		case <-time.After(100 * time.Millisecond): // Increased delay
			sum := 0
			for _, v := range input.Values {
				sum += v
			}
			return fluxus.KeyValue[string, int]{Key: input.Key, Value: sum}, nil
		case <-ctx.Done():
			return fluxus.KeyValue[string, int]{}, ctx.Err()
		}
	}

	mapReduceStage := fluxus.NewMapReduce(wordCountMapper, slowReducer).WithParallelism(4)

	// Create a context that cancels after map phase but during reduce phase
	cancelTime := 50 * time.Millisecond // Enough time for map, maybe not all reduce
	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	startTime := time.Now()
	results, err := mapReduceStage.Process(ctx, inputs)
	duration := time.Since(startTime)

	require.Error(t, err)
	assert.Nil(t, results)
	// The error could be context.DeadlineExceeded directly or wrapped by reduce phase error
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, duration, cancelTime+50*time.Millisecond, "Should fail quickly after cancellation") // Allow some buffer
}

// BenchmarkMapReduce benchmarks the MapReduce stage with varying parallelism.
func BenchmarkMapReduce(b *testing.B) {
	inputSize := 1000
	inputs := make([]string, inputSize)
	for i := 0; i < inputSize; i++ {
		// Generate somewhat realistic data with repetitions
		inputs[i] = fmt.Sprintf("key_%d", rand.Intn(inputSize/10))
	}
	ctx := context.Background()

	// Mapper/Reducer with minimal delay for overhead measurement
	fastMapper := func(_ context.Context, line string) (fluxus.KeyValue[string, int], error) {
		return fluxus.KeyValue[string, int]{Key: line, Value: 1}, nil
	}
	fastReducer := func(_ context.Context, input fluxus.ReduceInput[string, int]) (fluxus.KeyValue[string, int], error) {
		sum := 0
		for _, v := range input.Values {
			sum += v
		}
		return fluxus.KeyValue[string, int]{Key: input.Key, Value: sum}, nil
	}

	parallelismLevels := []int{1, 2, 4, 8}

	for _, p := range parallelismLevels {
		b.Run(fmt.Sprintf("Parallelism%d", p), func(b *testing.B) {
			mapReduceStage := fluxus.NewMapReduce(fastMapper, fastReducer).WithParallelism(p)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = mapReduceStage.Process(ctx, inputs)
			}
		})
	}

	// Benchmark with simulated work
	b.Run("WithWorkDelay", func(b *testing.B) {
		// Use mappers/reducers with slight delay
		mapReduceStage := fluxus.NewMapReduce(wordCountMapper, wordCountReducer).WithParallelism(4) // Fixed parallelism
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = mapReduceStage.Process(ctx, inputs)
		}
	})
}

// BenchmarkMapReduceInputSize benchmarks MapReduce performance with varying input sizes.
func BenchmarkMapReduceInputSize(b *testing.B) {
	ctx := context.Background()
	parallelism := 4 // Fixed parallelism

	fastMapper := func(_ context.Context, line string) (fluxus.KeyValue[string, int], error) {
		return fluxus.KeyValue[string, int]{Key: line, Value: 1}, nil
	}
	fastReducer := func(_ context.Context, input fluxus.ReduceInput[string, int]) (fluxus.KeyValue[string, int], error) {
		sum := 0
		for _, v := range input.Values {
			sum += v
		}
		return fluxus.KeyValue[string, int]{Key: input.Key, Value: sum}, nil
	}

	inputSizes := []int{100, 1000, 10000}

	for _, size := range inputSizes {
		b.Run(fmt.Sprintf("InputSize%d", size), func(b *testing.B) {
			// Generate inputs inside the sub-benchmark run
			inputs := make([]string, size)
			for i := 0; i < size; i++ {
				inputs[i] = fmt.Sprintf("key_%d", rand.Intn(size/10+1))
			}
			mapReduceStage := fluxus.NewMapReduce(fastMapper, fastReducer).WithParallelism(parallelism)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = mapReduceStage.Process(ctx, inputs)
			}
		})
	}
}
