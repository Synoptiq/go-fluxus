package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/synoptiq/go-fluxus"
)

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

// TestPipelineCancellation verifies that pipeline respects context cancellation
func TestPipelineCancellation(t *testing.T) {
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

// --- Helper Stages for Stream Tests ---

// IntToStringStage converts int to string
type IntToStringStage struct{}

func (s *IntToStringStage) Process(_ context.Context, input int) (string, error) {
	return fmt.Sprintf("v:%d", input), nil
}

// StringToUpperStage converts string to uppercase
type StringToUpperStage struct{}

func (s *StringToUpperStage) Process(_ context.Context, input string) (string, error) {
	return strings.ToUpper(input), nil
}

// StringToIntStage tries to parse int from string (can fail)
type StringToIntStage struct{}

func (s *StringToIntStage) Process(_ context.Context, input string) (int, error) {
	// Example: "v:123" -> 123
	parts := strings.Split(input, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid format: %s", input)
	}
	return strconv.Atoi(parts[1])
}

// --- Stream Pipeline Tests ---

// TestStreamPipelineBasicFlow tests a simple successful data flow.
// This is your existing test, but I've included it for completeness
func TestStreamPipelineBasicFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := log.New(os.Stdout, "TestBasicFlow: ", log.LstdFlags)

	// Build pipeline: int -> string -> UPPERCASE_STRING
	builder := fluxus.NewStreamPipeline[int](
		fluxus.WithStreamLogger(logger),
		fluxus.WithStreamBufferSize(1), // Small buffer
	)
	builderStage2 := fluxus.AddStage(builder, "int_to_string", &IntToStringStage{})
	builderStage3 := fluxus.AddStage(builderStage2, "string_to_upper", &StringToUpperStage{})

	pipeline, err := fluxus.Finalize(builderStage3)
	require.NoError(t, err)
	require.NotNil(t, pipeline)

	// Prepare source and sink
	source := make(chan int)
	sink := make(chan string)

	// Use waitgroups to coordinate the goroutines
	var wg sync.WaitGroup
	var resultsWg sync.WaitGroup

	// We need to track the results
	results := []string{}
	var resultsMu sync.Mutex // Protect access to the results slice

	// Add waiters for both goroutines
	wg.Add(1)        // For the pipeline
	resultsWg.Add(1) // For the results collection

	// Run pipeline in a goroutine
	var runErr error
	go func() {
		defer wg.Done() // Signal pipeline completion
		runErr = fluxus.Run(ctx, pipeline, source, sink)
	}()

	// Consume results from sink in a separate goroutine
	go func() {
		defer resultsWg.Done() // Signal results collection completion
		for res := range sink {
			resultsMu.Lock()
			results = append(results, res)
			resultsMu.Unlock()
		}
		t.Log("Results collection complete")
	}()

	// Feed data to source
	inputData := []int{1, 2, 3, 4, 5}
	go func() {
		defer close(source) // IMPORTANT: Close source when done feeding
		for _, item := range inputData {
			select {
			case source <- item:
			case <-ctx.Done():
				t.Log("Context cancelled while sending source data")
				return
			}
		}
		t.Log("Source feeding complete")
	}()

	// Wait for pipeline Run to finish
	wg.Wait()
	t.Log("Pipeline execution complete")

	// Wait for results collection to finish (it will finish since the sink is now closed)
	resultsWg.Wait()
	t.Log("Results collection finished")

	// Assertions
	require.NoError(t, runErr, "Pipeline Run should complete without error")
	expectedResults := []string{"V:1", "V:2", "V:3", "V:4", "V:5"}
	resultsMu.Lock()
	assert.ElementsMatch(t, expectedResults, results, "Sink should receive all processed items")
	resultsMu.Unlock()
}

// TestStreamPipelineMultipleStages tests a pipeline with multiple stages of different types
func TestStreamPipelineMultipleStages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := log.New(os.Stdout, "TestMultiStage: ", log.LstdFlags)

	// Define stages for testing
	// Stage 1: int -> string (convert to hex)
	intToHex := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("0x%X", input), nil
	})

	// Stage 2: string -> []string (split by character)
	splitChars := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		result := make([]string, len(input))
		for i, c := range input {
			result[i] = string(c)
		}
		return result, nil
	})

	// Stage 3: []string -> map[string]int (count occurrences)
	countOccurrences := fluxus.StageFunc[[]string, map[string]int](
		func(_ context.Context, input []string) (map[string]int, error) {
			result := make(map[string]int)
			for _, s := range input {
				result[s]++
			}
			return result, nil
		},
	)

	// Build pipeline: int -> string -> []string -> map[string]int
	builder := fluxus.NewStreamPipeline[int](
		fluxus.WithStreamLogger(logger),
		fluxus.WithStreamBufferSize(2),
	)
	b2 := fluxus.AddStage(builder, "int_to_hex", intToHex)
	b3 := fluxus.AddStage(b2, "split_chars", splitChars)
	b4 := fluxus.AddStage(b3, "count_occurrences", countOccurrences)

	pipeline, err := fluxus.Finalize(b4)
	require.NoError(t, err)
	require.NotNil(t, pipeline)

	// Run pipeline
	source := make(chan int)
	sink := make(chan map[string]int)

	var wg sync.WaitGroup
	var resultsWg sync.WaitGroup
	wg.Add(1)
	resultsWg.Add(1)

	var results []map[string]int
	var resultsMu sync.Mutex

	// Run pipeline
	var runErr error
	go func() {
		defer wg.Done()
		runErr = fluxus.Run(ctx, pipeline, source, sink)
	}()

	// Collect results
	go func() {
		defer resultsWg.Done()
		for res := range sink {
			resultsMu.Lock()
			results = append(results, res)
			resultsMu.Unlock()
		}
	}()

	// Feed data
	inputs := []int{10, 15, 255} // 0xA, 0xF, 0xFF
	go func() {
		defer close(source)
		for _, val := range inputs {
			select {
			case source <- val:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for completion
	wg.Wait()
	resultsWg.Wait()

	// Verify results
	require.NoError(t, runErr)
	assert.Len(t, results, 3)

	// Verify first result (0xA)
	assert.Contains(t, results, map[string]int{"0": 1, "x": 1, "A": 1})

	// Verify second result (0xF)
	assert.Contains(t, results, map[string]int{"0": 1, "x": 1, "F": 1})

	// Verify third result (0xFF)
	assert.Contains(t, results, map[string]int{"0": 1, "x": 1, "F": 2})
}

// TestStreamPipelineErrorHandling tests how errors are propagated in a stream pipeline
func TestStreamPipelineErrorHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := log.New(os.Stdout, "TestErrorHandling: ", log.LstdFlags)

	// Define a stage that fails on even numbers
	failOnEven := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		if input%2 == 0 {
			return 0, fmt.Errorf("even number not allowed: %d", input)
		}
		return input * 10, nil
	})

	// Test cases for different error strategies
	testCases := []struct {
		name            string
		strategy        fluxus.ErrorHandlingStrategy
		expectedResults []int
		expectError     bool
	}{
		{
			name:            "SkipOnError",
			strategy:        fluxus.SkipOnError,
			expectedResults: []int{10, 30, 50, 70, 90}, // Only process odd numbers
			expectError:     false,
		},
		{
			name:            "StopOnError",
			strategy:        fluxus.StopOnError,
			expectedResults: []int{10}, // Only process the first odd number before error on 2
			expectError:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build pipeline with the specific error strategy
			builder := fluxus.NewStreamPipeline[int](
				fluxus.WithStreamLogger(logger),
			)

			b2 := fluxus.AddStage(builder, "fail_on_even", failOnEven,
				fluxus.WithAdapterErrorStrategy[int, int](tc.strategy),
			)

			pipeline, err := fluxus.Finalize(b2)
			require.NoError(t, err)
			require.NotNil(t, pipeline)

			// Run pipeline
			source := make(chan int)
			sink := make(chan int)

			var wg sync.WaitGroup
			wg.Add(1)

			var results []int
			var resultsMu sync.Mutex
			var resultsWg sync.WaitGroup
			resultsWg.Add(1)

			// Run pipeline
			var runErr error
			go func() {
				defer wg.Done()
				runErr = fluxus.Run(ctx, pipeline, source, sink)
			}()

			// Collect results
			go func() {
				defer resultsWg.Done()
				for res := range sink {
					resultsMu.Lock()
					results = append(results, res)
					resultsMu.Unlock()
				}
			}()

			// Feed data (1-10)
			go func() {
				defer close(source)
				for i := 1; i <= 10; i++ {
					select {
					case source <- i:
					case <-ctx.Done():
						return
					}
				}
			}()

			// Wait for completion
			wg.Wait()
			resultsWg.Wait()

			// Verify results
			if tc.expectError {
				require.Error(t, runErr)
				assert.Contains(t, runErr.Error(), "even number not allowed")
			} else {
				require.NoError(t, runErr)
			}

			assert.Equal(t, tc.expectedResults, results)
		})
	}
}

// TestStreamPipelineCancellation tests how the pipeline handles context cancellation
func TestStreamPipelineCancellation(t *testing.T) {
	// Create a context that will be cancelled during processing
	ctx, cancel := context.WithCancel(context.Background())

	logger := log.New(os.Stdout, "TestCancellation: ", log.LstdFlags)

	// Create a slow stage
	slowStage := fluxus.StageFunc[int, int](func(ctx context.Context, input int) (int, error) {
		// This stage will take some time to process each item
		select {
		case <-time.After(50 * time.Millisecond):
			return input * 10, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	// Build pipeline
	builder := fluxus.NewStreamPipeline[int](
		fluxus.WithStreamLogger(logger),
	)
	b2 := fluxus.AddStage(builder, "slow_stage", slowStage)

	pipeline, err := fluxus.Finalize(b2)
	require.NoError(t, err)
	require.NotNil(t, pipeline)

	// Run pipeline
	source := make(chan int)
	sink := make(chan int)

	var wg sync.WaitGroup
	wg.Add(1)

	var results []int
	var resultsWg sync.WaitGroup
	resultsWg.Add(1)

	// Run pipeline
	var runErr error
	go func() {
		defer wg.Done()
		runErr = fluxus.Run(ctx, pipeline, source, sink)
	}()

	// Collect results
	go func() {
		defer resultsWg.Done()
		for res := range sink {
			results = append(results, res)

			if len(results) == 3 {
				// Cancel after receiving 3 results
				t.Log("Cancelling context after 3 results")
				cancel()
			}
		}
	}()

	// Feed a lot of data, more than we expect to process
	go func() {
		defer close(source)
		for i := 1; i <= 100; i++ {
			select {
			case source <- i:
				// Small delay to ensure predictable behavior
				time.Sleep(10 * time.Millisecond)
			case <-ctx.Done():
				t.Log("Source feeding stopped due to cancellation")
				return
			}
		}
	}()

	// Wait for pipeline to complete or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Pipeline completed
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Pipeline did not complete in time after cancellation")
	}

	// Wait for results collection to finish
	resultsWg.Wait()

	// Verify error is context cancellation
	require.Error(t, runErr)
	require.ErrorIs(t, runErr, context.Canceled)

	// Verify we got some results before cancellation
	assert.GreaterOrEqual(t, len(results), 3)
	t.Logf("Received %d results before cancellation", len(results))
}

// TestStreamPipelineBufferSizes tests how different buffer sizes affect performance
func TestStreamPipelineBufferSizes(t *testing.T) {
	// Use a longer timeout to ensure we have enough time for all test cases
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use a discard logger to avoid log output affecting timing
	logger := log.New(io.Discard, "TestBufferSizes: ", log.LstdFlags)

	// Define slow producer and consumer stages
	slowProducer := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		// Simulate a slow producer
		time.Sleep(10 * time.Millisecond)
		return input, nil
	})

	slowConsumer := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		// Simulate a slow consumer
		time.Sleep(25 * time.Millisecond)
		return input * 10, nil
	})

	// Test different buffer sizes
	bufferSizes := []int{0, 1, 10, 50}
	// Fixed number of items to process for each test
	const itemCount = 50

	for _, size := range bufferSizes {
		t.Run(fmt.Sprintf("BufferSize_%d", size), func(t *testing.T) {
			// Create a new context for each test case
			testCtx, testCancel := context.WithCancel(ctx)
			defer testCancel()

			// Build pipeline with the specific buffer size
			builder := fluxus.NewStreamPipeline[int](
				fluxus.WithStreamLogger(logger),
				fluxus.WithStreamBufferSize(size),
			)

			// Add stages
			b2 := fluxus.AddStage(builder, "slow_producer", slowProducer)
			b3 := fluxus.AddStage(b2, "slow_consumer", slowConsumer)

			pipeline, err := fluxus.Finalize(b3)
			require.NoError(t, err)
			require.NotNil(t, pipeline)

			// Create channels with appropriate buffer sizes
			source := make(chan int, 5)       // Small buffer for source
			sink := make(chan int, itemCount) // Large buffer for sink

			// Wait for pipeline to complete
			var pipelineWg sync.WaitGroup
			pipelineWg.Add(1)

			var results []int
			var resultsMu sync.Mutex

			// Set up result collection before running the pipeline
			// We'll collect in a separate goroutine to avoid blocking
			var resultsWg sync.WaitGroup
			resultsWg.Add(1)

			go func() {
				defer resultsWg.Done()
				for result := range sink {
					resultsMu.Lock()
					results = append(results, result)
					resultsMu.Unlock()
				}
				t.Logf("Finished collecting results, got %d items", len(results))
			}()

			// Measure time taken
			startTime := time.Now()

			// Run pipeline - IMPORTANT: The pipeline will close the sink channel
			var runErr error
			go func() {
				defer pipelineWg.Done()
				runErr = fluxus.Run(testCtx, pipeline, source, sink)
				// Note: The pipeline will close the sink channel
				t.Log("Pipeline completed")
			}()

			// Feed data
			t.Log("Starting to send items to source")
			feedStartTime := time.Now()
			for i := 1; i <= itemCount; i++ {
				select {
				case source <- i:
					// Successfully sent
				case <-testCtx.Done():
					t.Logf("Context cancelled while sending item %d", i)
					t.FailNow()
				}
			}

			// Close source after all data sent
			close(source)
			feedDuration := time.Since(feedStartTime)
			t.Logf("All %d items sent to source and source closed in %v", itemCount, feedDuration)

			// Wait for pipeline to complete
			pipelineWg.Wait()

			// Wait for results collection to finish (it will finish when sink is closed by pipeline)
			resultsWg.Wait()

			// Check for pipeline errors
			if runErr != nil {
				t.Errorf("Pipeline execution error: %v", runErr)
			}

			duration := time.Since(startTime)

			// Verify we got all results
			resultsMu.Lock()
			itemsReceived := len(results)
			resultsMu.Unlock()

			assert.Equal(t, itemCount, itemsReceived,
				"Expected exactly %d results, got %d. Buffer size: %d",
				itemCount, itemsReceived, size)

			// Log performance data
			t.Logf("Buffer size %d: processed %d items in %v (%.2f items/sec)",
				size, itemCount, duration, float64(itemCount)/duration.Seconds())
		})
	}
}

// TestStreamAdapterBackpressure tests how the StreamAdapter handles backpressure
// when a slow consumer is connected to a fast producer
func TestStreamAdapterBackpressure(t *testing.T) {
	// Create a fast producer that generates numbers quickly
	fastProducer := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		return input * 2, nil // Just a simple, fast operation
	})

	// Create a very slow consumer
	slowConsumer := fluxus.StageFunc[int, int](func(ctx context.Context, input int) (int, error) {
		select {
		case <-time.After(50 * time.Millisecond): // Simulate slow processing
			return input, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	// Create adapters with different concurrency settings
	testCases := []struct {
		name          string
		concurrency   int
		expectedItems int
		maxDuration   time.Duration
	}{
		{
			name:          "Sequential",
			concurrency:   1,
			expectedItems: 10,
			maxDuration:   600 * time.Millisecond, // Sequential should be slowest
		},
		{
			name:          "LowConcurrency",
			concurrency:   2,
			expectedItems: 10,
			maxDuration:   400 * time.Millisecond, // Should be faster
		},
		{
			name:          "HighConcurrency",
			concurrency:   runtime.NumCPU(),
			expectedItems: 10,
			maxDuration:   200 * time.Millisecond, // Should be fastest
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create the adapter with specified concurrency
			fastAdapter := fluxus.NewStreamAdapter(
				fastProducer,
				fluxus.WithAdapterConcurrency[int, int](1), // Always sequential for producer
				fluxus.WithAdapterLogger[int, int](
					log.New(os.Stdout, fmt.Sprintf("Producer-%s: ", tc.name), log.LstdFlags),
				),
			)

			slowAdapter := fluxus.NewStreamAdapter(
				slowConsumer,
				fluxus.WithAdapterConcurrency[int, int](tc.concurrency), // Varying concurrency for consumer
				fluxus.WithAdapterLogger[int, int](
					log.New(os.Stdout, fmt.Sprintf("Consumer-%s: ", tc.name), log.LstdFlags),
				),
			)

			// Create channels
			source := make(chan int)    // Input to producer
			middle := make(chan int, 3) // Between producer and consumer, small buffer
			sink := make(chan int, 10)  // Output from consumer

			// Run the adapters in goroutines
			ctx, cancel := context.WithTimeout(context.Background(), tc.maxDuration+100*time.Millisecond)
			defer cancel()

			var producerWg, consumerWg sync.WaitGroup
			producerWg.Add(1)
			consumerWg.Add(1)

			// Start the producer
			go func() {
				defer producerWg.Done()
				err := fastAdapter.ProcessStream(ctx, source, middle)
				if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					t.Errorf("Producer error: %v", err)
				}
			}()

			// Start the consumer
			go func() {
				defer consumerWg.Done()
				err := slowAdapter.ProcessStream(ctx, middle, sink)
				if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					t.Errorf("Consumer error: %v", err)
				}
			}()

			// Feed data rapidly
			go func() {
				defer close(source)
				for i := 1; i <= 20; i++ { // Send more than we expect to process
					select {
					case source <- i:
					case <-ctx.Done():
						return
					}
				}
			}()

			// Collect results with timeout
			results := make([]int, 0, tc.expectedItems)
			timeout := time.After(tc.maxDuration)

		resultLoop:
			for {
				select {
				case result, ok := <-sink:
					if !ok {
						break resultLoop
					}
					results = append(results, result)
					if len(results) >= tc.expectedItems {
						break resultLoop // Collected enough items
					}
				case <-timeout:
					// We've waited long enough, check what we got
					break resultLoop
				case <-ctx.Done():
					break resultLoop
				}
			}

			// Cancel the context to stop the adapters
			cancel()
			producerWg.Wait()
			consumerWg.Wait()

			// Assertions
			t.Logf("%s: Processed %d items within the time limit (%v)",
				tc.name, len(results), tc.maxDuration)

			// Higher concurrency should process more items in the same time
			if tc.concurrency > 1 {
				assert.GreaterOrEqual(t, len(results), tc.expectedItems/2,
					"Higher concurrency should process more items")
			}
		})
	}
}

// TestStreamPipelineRobustness tests how the pipeline handles errors and recovery
func TestStreamPipelineRobustness(t *testing.T) {
	// Create a stage that occasionally fails
	flakyStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		// Fail on multiples of 3
		if input%3 == 0 {
			return 0, fmt.Errorf("flaky error on input %d", input)
		}
		return input * 10, nil
	})

	// Test different error strategies
	testCases := []struct {
		name              string
		strategy          fluxus.ErrorHandlingStrategy
		expectErrors      bool
		completesPipeline bool
	}{
		{
			name:              "SkipOnError",
			strategy:          fluxus.SkipOnError,
			expectErrors:      true,
			completesPipeline: true,
		},
		{
			name:              "StopOnError",
			strategy:          fluxus.StopOnError,
			expectErrors:      true,
			completesPipeline: false,
		},
		{
			name:              "SendToErrorChannel",
			strategy:          fluxus.SendToErrorChannel,
			expectErrors:      true,
			completesPipeline: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create error channel if needed
			var errChan chan fluxus.ProcessingError[int]
			if tc.strategy == fluxus.SendToErrorChannel {
				errChan = make(chan fluxus.ProcessingError[int], 10)
			}

			// Create adapter options
			adapterOpts := []fluxus.StreamAdapterOption[int, int]{
				fluxus.WithAdapterErrorStrategy[int, int](tc.strategy),
				fluxus.WithAdapterLogger[int, int](
					log.New(os.Stdout, fmt.Sprintf("Robust-%s: ", tc.name), log.LstdFlags),
				),
			}

			if tc.strategy == fluxus.SendToErrorChannel {
				adapterOpts = append(adapterOpts, fluxus.WithAdapterErrorChannel[int, int](errChan))
			}

			// Build the pipeline
			builder := fluxus.NewStreamPipeline[int](
				fluxus.WithStreamLogger(
					log.New(os.Stdout, fmt.Sprintf("Robust-%s-Pipeline: ", tc.name), log.LstdFlags),
				),
			)
			b2 := fluxus.AddStage(builder, "flaky_stage", flakyStage, adapterOpts...)
			pipeline, err := fluxus.Finalize(b2)
			require.NoError(t, err)

			// Run the pipeline
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			source := make(chan int)
			sink := make(chan int)

			// Run the pipeline
			var wg sync.WaitGroup
			wg.Add(1)
			var runErr error
			go func() {
				defer wg.Done()
				runErr = fluxus.Run(ctx, pipeline, source, sink)
			}()

			// Collect results
			var results []int
			var errResults []fluxus.ProcessingError[int]
			var resultsMu sync.Mutex
			var resultsWg sync.WaitGroup
			resultsWg.Add(1)
			go func() {
				defer resultsWg.Done()
				for result := range sink {
					resultsMu.Lock() // Lock before accessing results
					results = append(results, result)
					resultsMu.Unlock() // Unlock after accessing results
				}
			}()

			// Collect errors if using error channel
			var errMu sync.Mutex
			if tc.strategy == fluxus.SendToErrorChannel {
				var errWg sync.WaitGroup
				errWg.Add(1)
				go func() {
					defer errWg.Done()
					for err := range errChan {
						errMu.Lock() // <<< Lock before accessing errResults
						errResults = append(errResults, err)
						errMu.Unlock() // <<< Unlock after accessing errResults
					}
				}()
				// Ensure we close the error channel
				defer func() {
					close(errChan)
					errWg.Wait()
				}()
			}

			// Send test data (including numbers that will cause errors)
			const itemCount = 10
			go func() {
				defer close(source)
				for i := 1; i <= itemCount; i++ {
					select {
					case source <- i:
					case <-ctx.Done():
						return
					}
				}
			}()

			// Wait for completion
			wg.Wait()
			resultsWg.Wait()

			// Verify results
			if tc.completesPipeline {
				// For strategies that should complete, we expect all non-error items
				if tc.strategy == fluxus.SendToErrorChannel {
					errMu.Lock() // <<< Lock before reading errResults
					assert.Len(t, errResults, itemCount/3, "Expected errors for multiples of 3")
					errMu.Unlock() // <<< Unlock after reading errResults
				}
				resultsMu.Lock()
				assert.Len(t, results, itemCount-itemCount/3,
					"Expected all non-error items to be processed")
				assert.NotContains(t, results, 30, "Multiple of 3 should not be in results")
				resultsMu.Unlock()
			} else {
				// For StopOnError, we expect it to fail quickly
				require.Error(t, runErr)
				assert.Contains(t, runErr.Error(), "flaky error")
				resultsMu.Lock()
				assert.Less(t, len(results), itemCount-itemCount/3,
					"Expected pipeline to stop early on error")
				resultsMu.Unlock()
			}
		})
	}
}

// BenchmarkStreamPipelineConcurrency compares the performance of different concurrency settings
func BenchmarkStreamPipelineConcurrency(b *testing.B) {
	// Create a CPU-bound operation
	cpuBoundStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		// Simulate CPU-bound work
		result := 0
		for i := 0; i < 100000; i++ {
			result += input * i
		}
		return result, nil
	})

	// Test with different concurrency settings
	concurrencyLevels := []int{1, 2, 4, 8, runtime.NumCPU(), runtime.NumCPU() * 2}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			// Reset the timer to exclude setup
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Create a fresh context and adapter for each iteration
				ctx, cancel := context.WithCancel(context.Background())

				adapter := fluxus.NewStreamAdapter(cpuBoundStage,
					fluxus.WithAdapterConcurrency[int, int](concurrency),
				)

				const itemCount = 100
				source := make(chan int, itemCount)
				sink := make(chan int, itemCount)

				// Start the adapter
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					adapter.ProcessStream(ctx, source, sink)
				}()

				// Feed data
				for j := 1; j <= itemCount; j++ {
					source <- j
				}
				close(source)

				// Start the timer for processing time
				b.StartTimer()

				// Read all results
				for j := 0; j < itemCount; j++ {
					<-sink
				}

				// Stop the timer before cleanup
				b.StopTimer()

				// Clean up
				cancel()
				wg.Wait()
			}
		})
	}
}

// BenchmarkStreamPipelineComplexity measures how pipeline performance scales with complexity
func BenchmarkStreamPipelineComplexity(b *testing.B) {
	// Create stages of various complexity
	simpleStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		return input * 2, nil
	})

	moderateStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		result := input
		for i := 0; i < 1000; i++ {
			result = (result * 31) % 997 // Some arbitrary computation
		}
		return result, nil
	})

	complexStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		result := input
		for i := 0; i < 10000; i++ {
			result = (result*31 + i) % 997
		}
		return result, nil
	})

	// Define pipeline configurations of increasing complexity
	configs := []struct {
		name   string
		stages int    // Number of stages to use
		types  string // Configuration of stage types to use
	}{
		{
			name:   "Simple_1Stage",
			stages: 1,
			types:  "S", // S = simple
		},
		{
			name:   "Simple_3Stages",
			stages: 3,
			types:  "SSS", // All simple stages
		},
		{
			name:   "Mixed_3Stages",
			stages: 3,
			types:  "SMS", // Simple, Moderate, Simple
		},
		{
			name:   "Complex_3Stages",
			stages: 3,
			types:  "MCM", // Moderate, Complex, Moderate
		},
		{
			name:   "Complex_5Stages",
			stages: 5,
			types:  "SMCMS", // Simple, Moderate, Complex, Moderate, Simple
		},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Create a fresh context for each iteration
				ctx, cancel := context.WithCancel(context.Background())

				// Build the pipeline of the specified complexity manually instead of using reflection
				builder := fluxus.NewStreamPipeline[int]()

				var currentBuilder = builder

				// For each stage in the configuration
				for j := 0; j < cfg.stages && j < len(cfg.types); j++ {
					stageType := cfg.types[j]
					var stage fluxus.Stage[int, int]
					var name string

					// Select the appropriate stage type
					switch stageType {
					case 'S': // Simple
						stage = simpleStage
						name = "simple_stage"
					case 'M': // Moderate
						stage = moderateStage
						name = "moderate_stage"
					case 'C': // Complex
						stage = complexStage
						name = "complex_stage"
					default:
						stage = simpleStage // Default to simple
						name = "default_stage"
					}

					// Add the stage to the builder
					currentBuilder = fluxus.AddStage(currentBuilder, name, stage)
				}

				// Finalize the pipeline
				pipeline, _ := fluxus.Finalize(currentBuilder)

				const itemCount = 50
				source := make(chan int, itemCount)
				sink := make(chan int, itemCount)

				// Start the pipeline
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					fluxus.Run(ctx, pipeline, source, sink)
				}()

				// Feed data
				for j := 1; j <= itemCount; j++ {
					source <- j
				}
				close(source)

				// Start the timer for processing time
				b.StartTimer()

				// Read all results
				for j := 0; j < itemCount; j++ {
					<-sink
				}

				// Stop the timer before cleanup
				b.StopTimer()

				// Clean up
				cancel()
				wg.Wait()
			}
		})
	}
}

// BenchmarkStreamPipelineDataTypes tests how different data types affect performance
func BenchmarkStreamPipelineDataTypes(b *testing.B) {
	// Define benchmark cases for different data types
	type largeStruct struct {
		ID        int
		Name      string
		Values    [100]float64
		Timestamp time.Time
		Tags      []string
		Metadata  map[string]interface{}
	}

	// Helper function to create a large struct
	createLargeStruct := func(id int) largeStruct {
		values := [100]float64{}
		for i := 0; i < 100; i++ {
			values[i] = float64(i) * 1.5
		}

		return largeStruct{
			ID:        id,
			Name:      fmt.Sprintf("Item-%d", id),
			Values:    values,
			Timestamp: time.Now(),
			Tags:      []string{"tag1", "tag2", "tag3", "tag4", "tag5"},
			Metadata: map[string]interface{}{
				"key1": "value1",
				"key2": 123,
				"key3": true,
			},
		}
	}

	// Define different stage types
	intStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		return input * 2, nil
	})

	stringStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Use generics to create a function that processes any type
	createStructStage := func() fluxus.Stage[largeStruct, largeStruct] {
		return fluxus.StageFunc[largeStruct, largeStruct](
			func(_ context.Context, input largeStruct) (largeStruct, error) {
				// Do some work on the struct
				result := input
				result.ID *= 2
				result.Name = strings.ToUpper(input.Name)
				return result, nil
			},
		)
	}

	structStage := createStructStage()

	// Benchmark each data type
	b.Run("Int", func(b *testing.B) {
		benchmarkPipeline(b, "int_stage", intStage, 1, 100)
	})

	b.Run("String", func(b *testing.B) {
		// Create a string stage pipeline
		stringInput := "hello world this is a test string for benchmarking"
		benchmarkPipelineWithInput(b, "string_stage", stringStage, stringInput, 100)
	})

	b.Run("LargeStruct", func(b *testing.B) {
		// Create a struct stage pipeline
		structInput := createLargeStruct(1)
		benchmarkPipelineWithInput(b, "struct_stage", structStage, structInput, 100)
	})
}

// Helper function to benchmark a pipeline with a specific stage and input count
func benchmarkPipeline[T any](b *testing.B, name string, stage fluxus.Stage[T, T], input T, itemCount int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create a fresh context
		ctx, cancel := context.WithCancel(context.Background())

		// Build the pipeline
		builder := fluxus.NewStreamPipeline[T]()
		b2 := fluxus.AddStage(builder, name, stage)
		pipeline, _ := fluxus.Finalize(b2)

		source := make(chan T, itemCount)
		sink := make(chan T, itemCount)

		// Start the pipeline
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			fluxus.Run(ctx, pipeline, source, sink)
		}()

		// Feed data
		for j := 0; j < itemCount; j++ {
			source <- input
		}
		close(source)

		// Start the timer for processing time
		b.StartTimer()

		// Read all results
		for j := 0; j < itemCount; j++ {
			<-sink
		}

		// Stop the timer before cleanup
		b.StopTimer()

		// Clean up
		cancel()
		wg.Wait()
	}
}

// Helper function to benchmark a pipeline with a specific input value
func benchmarkPipelineWithInput[T any](b *testing.B, name string, stage fluxus.Stage[T, T], input T, itemCount int) {
	benchmarkPipeline(b, name, stage, input, itemCount)
}

// PassthroughStage simply passes the input value through without modification or delay.
type passthroughStage[T any] struct{}

func (s *passthroughStage[T]) Process(_ context.Context, input T) (T, error) {
	return input, nil
}

// BenchmarkStreamPipelineOverhead measures the framework overhead using pass-through stages.
func BenchmarkStreamPipelineOverhead(b *testing.B) {
	stageCounts := []int{1, 3, 5, 10} // Test pipelines of different lengths
	const itemCount = 100             // Number of items to push through

	for _, numStages := range stageCounts {
		b.Run(fmt.Sprintf("Stages_%d", numStages), func(b *testing.B) {
			// Create the pass-through stage instance (using int as example)
			passStage := &passthroughStage[int]{}

			// --- Benchmark Loop ---
			for i := 0; i < b.N; i++ {
				b.StopTimer() // Pause timer for setup

				// --- Setup for each iteration ---
				ctx, cancel := context.WithCancel(context.Background()) // Use cancellable context

				// Build the pipeline dynamically
				builder := fluxus.NewStreamPipeline[int]() // Start with int input
				currentBuilder := builder
				for j := 0; j < numStages; j++ {
					// Need to cast the builder type correctly in each step.
					// Since all stages are int->int, we know the type.
					currentBuilder = fluxus.AddStage(currentBuilder, "pass_stage", passStage)
				}
				pipeline, err := fluxus.Finalize(currentBuilder)
				if err != nil {
					b.Fatalf("Failed to finalize pipeline: %v", err)
				}

				source := make(chan int, itemCount) // Buffer source for quick feeding
				sink := make(chan int, itemCount)   // Buffer sink

				var wg sync.WaitGroup
				wg.Add(1)

				// Run the pipeline in a goroutine
				go func() {
					defer wg.Done()
					// We don't expect errors from pass-through stages, but handle context cancellation
					_ = fluxus.Run(ctx, pipeline, source, sink)
				}()

				// Feed data quickly
				for j := 0; j < itemCount; j++ {
					source <- j
				}
				close(source) // Close source to signal end of input

				// --- Start Timing ---
				b.StartTimer()

				// Drain the sink completely - this is the core work being measured
				for j := 0; j < itemCount; j++ {
					_, ok := <-sink
					if !ok {
						b.Fatalf("Sink closed prematurely after %d items", j)
					}
				}

				// --- Stop Timing ---
				b.StopTimer() // Pause timer for cleanup

				// --- Cleanup ---
				cancel()  // Signal pipeline to stop (if still running)
				wg.Wait() // Wait for Run goroutine to finish

				// Check if sink is closed (it should be by Run)
				_, ok := <-sink
				if ok {
					b.Fatalf("Sink channel was not closed after pipeline completion")
				}
				// --- End Cleanup ---
			}
			// --- End Benchmark Loop ---
		})
	}
}
