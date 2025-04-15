package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestStreamAdapterSequential tests the sequential processing mode of StreamAdapter
func TestStreamAdapterSequential(t *testing.T) {
	// Create a simple stage that doubles integers
	doubler := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		return input * 2, nil
	})

	// Create a stream adapter with sequential processing (concurrency=1)
	adapter := fluxus.NewStreamAdapter(doubler,
		fluxus.WithAdapterConcurrency[int, int](1),
		fluxus.WithAdapterLogger[int, int](log.New(os.Stdout, "TestSequential: ", log.LstdFlags)),
	)

	// Create channels
	in := make(chan int)
	out := make(chan int)

	// Run the adapter in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	var adapterErr error
	go func() {
		defer wg.Done()
		adapterErr = adapter.ProcessStream(ctx, in, out)
	}()

	// Send test data
	go func() {
		for i := 1; i <= 5; i++ {
			in <- i
		}
		close(in)
	}()

	// Collect results
	results := make([]int, 0, 5)
	for result := range out {
		results = append(results, result)
	}

	// Wait for adapter to finish
	wg.Wait()

	// Verify results
	require.NoError(t, adapterErr)
	assert.Equal(t, []int{2, 4, 6, 8, 10}, results)
}

// TestStreamAdapterConcurrent tests the concurrent processing mode of StreamAdapter
func TestStreamAdapterConcurrent(t *testing.T) {
	// Keep track of concurrent executions
	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	// Create a stage that tracks concurrency
	tracker := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		// Increment the current count and check if it's a new maximum
		current := currentConcurrent.Add(1)
		for {
			maxConcurrencyVal := maxConcurrent.Load()
			if current <= maxConcurrencyVal {
				break
			}
			if maxConcurrent.CompareAndSwap(maxConcurrencyVal, current) {
				break
			}
		}

		// Simulate some work
		time.Sleep(10 * time.Millisecond)

		// Decrement the count
		currentConcurrent.Add(-1)

		return input * 2, nil
	})

	// Create a stream adapter with concurrent processing
	expectedConcurrency := 3
	adapter := fluxus.NewStreamAdapter(tracker,
		fluxus.WithAdapterConcurrency[int, int](expectedConcurrency),
		fluxus.WithAdapterLogger[int, int](log.New(os.Stdout, "TestConcurrent: ", log.LstdFlags)),
	)

	// Create channels
	in := make(chan int)
	out := make(chan int)

	// Run the adapter in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	var adapterErr error
	go func() {
		defer wg.Done()
		adapterErr = adapter.ProcessStream(ctx, in, out)
	}()

	// Send test data
	go func() {
		for i := 1; i <= 10; i++ {
			in <- i
		}
		close(in)
	}()

	// Collect results
	results := make([]int, 0, 10)
	for result := range out {
		results = append(results, result)
	}

	// Wait for adapter to finish
	wg.Wait()

	// Verify results
	require.NoError(t, adapterErr)
	assert.Len(t, results, 10)
	for _, v := range results {
		// The order might not match the input order due to concurrency
		assert.Contains(t, []int{2, 4, 6, 8, 10, 12, 14, 16, 18, 20}, v)
	}

	// Verify concurrency
	assert.Equal(t, int32(expectedConcurrency), maxConcurrent.Load(),
		"Expected maximum concurrency to match the configured value")
}

// TestStreamAdapterErrorHandling tests different error handling strategies
func TestStreamAdapterErrorHandling(t *testing.T) {
	// Define error for testing
	testErr := errors.New("test error")

	// Create a stage that returns an error for even numbers
	errorOnEven := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		if input%2 == 0 {
			return 0, testErr
		}
		return input, nil
	})

	// Test cases for different error strategies
	testCases := []struct {
		name            string
		strategy        fluxus.ErrorHandlingStrategy
		expectedResults []int
		expectedError   bool
	}{
		{
			name:            "SkipOnError",
			strategy:        fluxus.SkipOnError,
			expectedResults: []int{1, 3, 5},
			expectedError:   false,
		},
		{
			name:            "StopOnError",
			strategy:        fluxus.StopOnError,
			expectedResults: []int{1}, // Only process until the first error (on 2)
			expectedError:   true,
		},
		{
			name:            "SendToErrorChannel",
			strategy:        fluxus.SendToErrorChannel,
			expectedResults: []int{1, 3, 5},
			expectedError:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up error channel if needed
			var errChan chan fluxus.ProcessingError[int]
			var errResults []fluxus.ProcessingError[int]
			var errWg sync.WaitGroup

			if tc.strategy == fluxus.SendToErrorChannel {
				errChan = make(chan fluxus.ProcessingError[int], 10)
				errResults = make([]fluxus.ProcessingError[int], 0)
				errWg.Add(1)

				go func() {
					defer errWg.Done()
					for err := range errChan {
						errResults = append(errResults, err)
					}
				}()
			}

			// Create adapter with the specified strategy
			var adapterOptions []fluxus.StreamAdapterOption[int, int]
			adapterOptions = append(adapterOptions,
				fluxus.WithAdapterErrorStrategy[int, int](tc.strategy),
				fluxus.WithAdapterLogger[int, int](log.New(os.Stdout, "TestErrorHandling: ", log.LstdFlags)),
			)

			if tc.strategy == fluxus.SendToErrorChannel {
				adapterOptions = append(adapterOptions, fluxus.WithAdapterErrorChannel[int, int](errChan))
			}

			adapter := fluxus.NewStreamAdapter(errorOnEven, adapterOptions...)

			// Create channels
			in := make(chan int)
			out := make(chan int)

			// Run the adapter in a goroutine
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(1)

			var adapterErr error
			go func() {
				defer wg.Done()
				adapterErr = adapter.ProcessStream(ctx, in, out)
			}()

			// Send test data
			go func() {
				for i := 1; i <= 5; i++ {
					select {
					case in <- i:
					case <-ctx.Done():
						return
					}
				}
				close(in)
			}()

			// Collect results
			results := make([]int, 0)
			for result := range out {
				results = append(results, result)
			}

			// Wait for adapter to finish
			wg.Wait()

			// Close error channel if it exists
			if errChan != nil {
				close(errChan)
				errWg.Wait()
			}

			// Verify results
			if tc.expectedError {
				require.Error(t, adapterErr)
				require.ErrorIs(t, adapterErr, testErr)
			} else {
				require.NoError(t, adapterErr)
			}

			assert.Equal(t, tc.expectedResults, results)

			// Verify error channel results if applicable
			if tc.strategy == fluxus.SendToErrorChannel {
				assert.Len(t, errResults, 2) // 2 and 4 should generate errors
				for _, e := range errResults {
					assert.Equal(t, 0, e.Item%2, "Expected only even numbers in error channel")
					assert.ErrorIs(t, e.Error, testErr)
				}
			}
		})
	}
}

// TestStreamAdapterCancellation tests how the StreamAdapter handles context cancellation
func TestStreamAdapterCancellation(t *testing.T) {
	// Create a slow stage
	slowStage := fluxus.StageFunc[int, int](func(ctx context.Context, input int) (int, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return input * 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	// Create adapter
	adapter := fluxus.NewStreamAdapter(slowStage,
		fluxus.WithAdapterConcurrency[int, int](2),
		fluxus.WithAdapterLogger[int, int](log.New(os.Stdout, "TestCancellation: ", log.LstdFlags)),
	)

	// Create channels
	in := make(chan int)
	out := make(chan int)

	// Create a context that will be cancelled after a short delay
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure adapter eventually stops

	var wg sync.WaitGroup
	wg.Add(1)

	var adapterErr error
	go func() {
		defer wg.Done()
		adapterErr = adapter.ProcessStream(ctx, in, out)
	}()

	// Send some data
	go func() {
		for i := 1; i <= 10; i++ {
			select {
			case in <- i:
			case <-ctx.Done():
				return
			}
		}
		// Don't close input channel yet
	}()

	// Collect results for a short time
	results := make([]int, 0)
	timeout := time.After(150 * time.Millisecond)

collectLoop:
	for {
		select {
		case result, ok := <-out:
			if !ok {
				break collectLoop
			}
			results = append(results, result)
		case <-timeout:
			// Cancel after timeout
			cancel()
			break collectLoop
		}
	}

	// Wait for adapter to finish or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Adapter completed
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Adapter did not complete in time after cancellation")
	}

	// Verify the error is a context cancellation
	require.Error(t, adapterErr)
	require.ErrorIs(t, adapterErr, context.Canceled)

	// Some results may have been processed before cancellation
	t.Logf("Processed %d items before cancellation", len(results))
}

// TestStreamAdapterBufferSize_Concurrent verifies the buffer allows sending ahead with concurrency >= 2.
func TestStreamAdapterBufferSizeConcurrent(t *testing.T) {
	// Stage that simulates work
	processingTime := 50 * time.Millisecond
	slowStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
		time.Sleep(processingTime)
		return input * 2, nil
	})

	// --- Test Setup ---
	bufferSize := 5
	concurrency := 2 // *** Set concurrency >= 2 to engage processConcurrent ***
	require.GreaterOrEqual(t, concurrency, 2, "This test requires concurrency >= 2")

	adapter := fluxus.NewStreamAdapter(slowStage,
		fluxus.WithAdapterConcurrency[int, int](concurrency),
		fluxus.WithAdapterBufferSize[int, int](bufferSize),
		fluxus.WithAdapterLogger[int, int](log.New(os.Stdout, "TestBufferConcurrent: ", log.LstdFlags)),
	)

	in := make(chan int)
	out := make(chan int)

	// Context for the adapter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure adapter eventually stops

	var adapterWg sync.WaitGroup
	adapterWg.Add(1)

	// Run the adapter
	go func() {
		defer adapterWg.Done()
		adapter.ProcessStream(ctx, in, out)
		t.Log("Adapter ProcessStream finished.")
	}()

	// --- Test Execution ---
	itemsToSend := bufferSize + concurrency // Send enough items to potentially fill buffer and engage all workers
	sendDelay := 1 * time.Millisecond

	startTime := time.Now()

	// Send items quickly and measure how long it takes
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 2*time.Second) // Increased timeout slightly
	defer sendCancel()

	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		defer close(in) // Close input channel when sender is done
		for i := 1; i <= itemsToSend; i++ {
			select {
			case in <- i:
				time.Sleep(sendDelay) // Small delay between sends
			case <-sendCtx.Done():
				t.Errorf("Sending timed out or cancelled: %v", sendCtx.Err())
				return
			}
		}
		t.Logf("Finished sending %d items.", itemsToSend)
	}()

	// Wait for the sender to finish
	<-senderDone
	sendDuration := time.Since(startTime)
	t.Logf("Time taken to send %d items: %v", itemsToSend, sendDuration)

	// --- Assertion ---
	// Calculate the minimum time it would take to process these items with the given concurrency.
	// Each item takes processingTime. With 'concurrency' workers, we can process 'concurrency' items in parallel.
	// The number of parallel batches needed is ceil(itemsToSend / concurrency).
	batches := math.Ceil(float64(itemsToSend) / float64(concurrency))
	minProcessingTime := time.Duration(batches) * processingTime

	// If the buffer is working, sending should complete much faster than the minimum concurrent processing time.
	// We expect sendDuration to be roughly itemsToSend * sendDelay + overhead.
	// Assert that sendDuration is significantly less than minProcessingTime.
	// Let's set a generous upper bound for sending time (e.g., half of minProcessingTime).
	maxExpectedSendDuration := minProcessingTime / 2

	assert.Lessf(
		t,
		sendDuration,
		maxExpectedSendDuration,
		"Sending %d items took %v, which is not significantly less than the minimum concurrent processing time %v (batches=%v). Buffer might not be effective.",
		itemsToSend,
		sendDuration,
		minProcessingTime,
		batches,
	)

	// --- Cleanup ---
	// Cancel the adapter's context
	cancel()

	// Drain the output channel to allow the adapter to fully exit
	go func() {
		//nolint:revive // ignore the output values, just drain the channel
		for range out {
		}
		t.Log("Output channel drained.")
	}()

	// Wait for the adapter goroutine to finish
	adapterWg.Wait()
	t.Log("Adapter goroutine finished.")
}
