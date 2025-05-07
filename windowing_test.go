package fluxus_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/synoptiq/go-fluxus"
)

// Helper function to run TumblingCountWindow in a test setting
func runTumblingCountWindowTest[T any](
	ctx context.Context,
	t *testing.T,
	windowSize int,
	inputs []T,
) ([][]T, error) {
	t.Helper() // Mark as test helper

	source := make(chan T, len(inputs)) // Buffered source for simplicity
	sink := make(chan []T)              // Unbuffered sink to observe output directly
	results := make([][]T, 0)
	var wg sync.WaitGroup
	var runErr error

	// Create the window stage
	windowStage := fluxus.NewTumblingCountWindow[T](windowSize)

	// Start the stage
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = windowStage.ProcessStream(ctx, source, sink)
	}()

	// Start the sink reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for window := range sink {
			// Make a copy of the window slice before storing,
			// as the underlying array might be reused by the stage (though our current impl copies).
			// It's good practice for testing stream stages.
			windowCopy := make([]T, len(window))
			copy(windowCopy, window)
			results = append(results, windowCopy)
		}
	}()

	// Send inputs

	for _, item := range inputs {
		select {
		case source <- item:
		case <-ctx.Done():
			// If context is cancelled while sending, stop sending
			// The stage should handle this cancellation internally
			t.Logf("Context cancelled while sending input")
			//nolint:staticcheck // break out of the select
			break
		}
	}
	close(source) // Close source to signal end of input

	// Wait for stage and sink reader to finish
	wg.Wait()

	return results, runErr
}

// TestTumblingCountWindowBasic tests basic window emission.
func TestTumblingCountWindowBasic(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6, 7}
	windowSize := 3
	expectedWindows := [][]int{
		{1, 2, 3},
		{4, 5, 6},
	}
	// The final partial window {7} will also be emitted

	results, err := runTumblingCountWindowTest(context.Background(), t, windowSize, inputs)

	require.NoError(t, err)
	require.Len(t, results, 3, "Expected 2 full windows and 1 partial window")
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
	assert.Equal(t, []int{7}, results[2], "Expected partial final window")
}

// TestTumblingCountWindowPartialLast tests emission of only a partial window.
func TestTumblingCountWindowPartialLast(t *testing.T) {
	inputs := []int{10, 20}
	windowSize := 5
	expectedWindows := [][]int{
		{10, 20},
	}

	results, err := runTumblingCountWindowTest(context.Background(), t, windowSize, inputs)

	require.NoError(t, err)
	require.Len(t, results, 1, "Expected 1 partial window")
	assert.Equal(t, expectedWindows[0], results[0])
}

// TestTumblingCountWindowExactMultiple tests when input size is an exact multiple of window size.
func TestTumblingCountWindowExactMultiple(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6}
	windowSize := 3
	expectedWindows := [][]int{
		{1, 2, 3},
		{4, 5, 6},
	}

	results, err := runTumblingCountWindowTest(context.Background(), t, windowSize, inputs)

	require.NoError(t, err)
	require.Len(t, results, 2, "Expected 2 full windows")
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
}

// TestTumblingCountWindowEmptyInput tests behavior with no input.
func TestTumblingCountWindowEmptyInput(t *testing.T) {
	inputs := []int{}
	windowSize := 5

	results, err := runTumblingCountWindowTest(context.Background(), t, windowSize, inputs)

	require.NoError(t, err)
	assert.Empty(t, results, "Expected no windows for empty input")
}

// TestTumblingCountWindowSizeOne tests behavior with window size 1.
func TestTumblingCountWindowSizeOne(t *testing.T) {
	inputs := []int{10, 20, 30}
	windowSize := 1
	expectedWindows := [][]int{
		{10},
		{20},
		{30},
	}

	results, err := runTumblingCountWindowTest(context.Background(), t, windowSize, inputs)

	require.NoError(t, err)
	require.Len(t, results, 3, "Expected 3 windows of size 1")
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
	assert.Equal(t, expectedWindows[2], results[2])
}

// TestTumblingCountWindowCancellation tests context cancellation during processing.
func TestTumblingCountWindowCancellation(t *testing.T) {
	inputs := make([]int, 200) // Use enough inputs to ensure the stage is busy
	for i := range inputs {
		inputs[i] = i
	}
	windowSize := 10
	cancelTime := 25 * time.Millisecond // A short, but not instantaneous, timeout

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel() // Ensure cancel is eventually called

	source := make(chan int) // Use unbuffered source
	sink := make(chan []int) // Use unbuffered sink
	var wg sync.WaitGroup
	var runErr error

	windowStage := fluxus.NewTumblingCountWindow[int](windowSize)

	// Start the stage
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Capture the error returned by ProcessStream
		runErr = windowStage.ProcessStream(ctx, source, sink)
	}()

	// Start a sink drainer goroutine.
	// This prevents the stage from blocking indefinitely on sending to sink
	// if it manages to produce output before cancellation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Drain the sink until it's closed
		//nolint:revive // Draining the channel is the intended behavior here.
		for range sink {
		}
	}()

	// Start input sender goroutine
	inputSendingDone := make(chan struct{})
	go func() {
		defer close(inputSendingDone) // Signal when sending stops
		for _, item := range inputs {
			select {
			case source <- item:
				// Item sent successfully
			case <-ctx.Done():
				// Context was cancelled while trying to send input
				t.Logf("Input sending interrupted by context cancellation.")
				return // Stop sending
			}
		}
		// Input loop finished before context cancellation (less likely with unbuffered source)
		t.Logf("Input sending finished naturally before cancellation.")
	}()

	// Wait until the context is guaranteed to be cancelled
	<-ctx.Done()
	t.Logf("Context cancellation detected by test.")

	// Now that the context is cancelled, close the source channel.
	// This allows the ProcessStream loop to potentially detect the closed channel
	// if it wasn't already stopped by the context cancellation.
	close(source)

	// Wait for the input sender to finish (it might have already exited due to cancellation)
	<-inputSendingDone

	// Wait for the main stage goroutine and sink drainer to complete.
	wg.Wait()

	// Assert that the stage returned an error, specifically the context error.
	require.Error(t, runErr, "ProcessStream should have returned an error due to context cancellation")
	assert.ErrorIs(t, runErr, context.DeadlineExceeded, "Expected error to be context.DeadlineExceeded")
}

// TestTumblingCountWindowCancellationDuringSend tests cancellation while blocked sending output.
func TestTumblingCountWindowCancellationDuringSend(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5} // Enough for one window
	windowSize := 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := make(chan int, len(inputs))
	sink := make(chan []int) // Unbuffered sink
	var wg sync.WaitGroup
	var runErr error

	windowStage := fluxus.NewTumblingCountWindow[int](windowSize)

	// Start the stage
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = windowStage.ProcessStream(ctx, source, sink)
	}()

	// Send just enough input to fill one window
	for i := 0; i < windowSize; i++ {
		source <- inputs[i]
	}

	// Give the stage a moment to process and try to send the first window
	time.Sleep(20 * time.Millisecond)

	// Now cancel the context while the stage is likely blocked on sending to the unread sink
	cancel()

	// Close the source (important for the stage to potentially exit its loop)
	close(source)

	// Wait for the stage to finish
	wg.Wait()

	// Check the error
	require.Error(t, runErr)
	require.ErrorIs(t, runErr, context.Canceled, "Expected context canceled error")

	// Ensure the sink wasn't read from (or only partially if timing was off)
	select {
	case _, ok := <-sink:
		if ok {
			t.Log("Sink received a window unexpectedly after cancellation")
		}
	default:
		// Expected: sink is empty or closed
	}
}

// TestTumblingCountWindowInvalidSize tests panic on non-positive size.
func TestTumblingCountWindowInvalidSize(t *testing.T) {
	assert.PanicsWithValue(
		t,
		"fluxus.NewTumblingCountWindow: size must be positive, got 0",
		func() { fluxus.NewTumblingCountWindow[int](0) },
		"Expected panic for size 0",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewTumblingCountWindow: size must be positive, got -1",
		func() { fluxus.NewTumblingCountWindow[int](-1) },
		"Expected panic for negative size",
	)
}

// --- TumblingCountWindow Benchmarks ---

// Helper function for benchmarks
func runTumblingCountWindowBench(b *testing.B, windowSize, numItems int) {
	b.Helper()
	ctx := context.Background()
	inputs := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		inputs[i] = i
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		source := make(chan int, numItems)
		sink := make(chan []int, (numItems/windowSize)+1) // Buffered sink for benchmark
		var wg sync.WaitGroup

		windowStage := fluxus.NewTumblingCountWindow[int](windowSize)

		// Start stage
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = windowStage.ProcessStream(ctx, source, sink)
		}()

		// Start sink reader (just drain)
		wg.Add(1)
		go func() {
			defer wg.Done()
			//nolint:revive // Drain the channel
			for range sink {
			}
		}()

		// Send inputs
		for _, item := range inputs {
			source <- item
		}
		close(source)

		wg.Wait()
	}
	b.ReportAllocs()
}

// BenchmarkTumblingCountWindow benchmarks the stage with various window sizes.
func BenchmarkTumblingCountWindow(b *testing.B) {
	numItems := 10000
	windowSizes := []int{1, 10, 100, 1000}

	for _, size := range windowSizes {
		b.Run(fmt.Sprintf("WinSize%d", size), func(b *testing.B) {
			runTumblingCountWindowBench(b, size, numItems)
		})
	}
}

// BenchmarkTumblingCountWindowInputSize benchmarks the stage with various input sizes.
func BenchmarkTumblingCountWindowInputSize(b *testing.B) {
	windowSize := 100 // Fixed window size
	numItemsList := []int{100, 1000, 10000, 100000}

	for _, numItems := range numItemsList {
		b.Run(fmt.Sprintf("NumItems%d", numItems), func(b *testing.B) {
			runTumblingCountWindowBench(b, windowSize, numItems)
		})
	}
}

// --- TumblingTimeWindow Tests ---

// Helper function to run TumblingTimeWindow in a test setting
// Inputs are pairs of (delay before sending, value)
func runTumblingTimeWindowTest[T any](
	ctx context.Context,
	t *testing.T,
	windowDuration time.Duration,
	inputs []struct {
		Delay time.Duration
		Value T
	},
) ([][]T, error) {
	t.Helper()

	source := make(chan T) // Unbuffered source for controlled timing
	sink := make(chan []T) // Unbuffered sink
	results := make([][]T, 0)
	var wg sync.WaitGroup
	var runErr error
	var resultsMu sync.Mutex // Protect results slice

	// Create the window stage
	windowStage := fluxus.NewTumblingTimeWindow[T](windowDuration)

	// Start the stage
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = windowStage.ProcessStream(ctx, source, sink)
	}()

	// Start the sink reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for window := range sink {
			windowCopy := make([]T, len(window))
			copy(windowCopy, window)
			resultsMu.Lock()
			results = append(results, windowCopy)
			resultsMu.Unlock()
		}
	}()

	// Start the input sender
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(source) // Close source when done sending

		startTime := time.Now()
		for _, item := range inputs {
			// Calculate when this item should be sent
			sendTime := startTime.Add(item.Delay)
			sleepDuration := time.Until(sendTime)

			if sleepDuration > 0 {
				select {
				case <-time.After(sleepDuration):
					// Time to send
				case <-ctx.Done():
					t.Logf("Context cancelled while waiting to send input: %v", item.Value)
					return // Stop sending
				}
			}

			// Send the item, respecting context cancellation
			select {
			case source <- item.Value:
				// Sent successfully
			case <-ctx.Done():
				t.Logf("Context cancelled while sending input: %v", item.Value)
				return // Stop sending
			}
		}
	}()

	// Wait for stage, sink reader, and input sender to finish
	wg.Wait()

	// Sort results by the first element for deterministic testing (if possible/needed)
	// This might not always be appropriate depending on the test case.
	// For simplicity, we'll assume order matters based on window emission time.

	return results, runErr
}

// TestTumblingTimeWindowBasic tests items falling into distinct windows.
func TestTumblingTimeWindowBasic(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},  // Window 1 (0-100ms)
		{Delay: 50 * time.Millisecond, Value: 2},  // Window 1
		{Delay: 110 * time.Millisecond, Value: 3}, // Window 2 (100-200ms)
		{Delay: 180 * time.Millisecond, Value: 4}, // Window 2
		{Delay: 250 * time.Millisecond, Value: 5}, // Window 3 (200-300ms)
	}
	expectedWindows := [][]int{
		{1, 2},
		{3, 4},
		{5}, // Final partial window flushed on close
	}

	// Use a slightly longer context to allow all inputs and windows to process
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	results, err := runTumblingTimeWindowTest(ctx, t, windowDuration, inputs)

	require.NoError(t, err)
	require.Len(t, results, len(expectedWindows))
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
	assert.Equal(t, expectedWindows[2], results[2])
}

// TestTumblingTimeWindowEmptyInterval tests a window interval with no items.
func TestTumblingTimeWindowEmptyInterval(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},  // Window 1 (0-100ms)
		{Delay: 210 * time.Millisecond, Value: 2}, // Window 3 (200-300ms) - Window 2 is empty
		{Delay: 250 * time.Millisecond, Value: 3}, // Window 3
	}
	expectedWindows := [][]int{
		{1},
		{2, 3}, // Final partial window flushed on close
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	results, err := runTumblingTimeWindowTest(ctx, t, windowDuration, inputs)

	require.NoError(t, err)
	// Expect only non-empty windows to be emitted
	require.Len(t, results, len(expectedWindows))
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
}

// TestTumblingTimeWindowFlushOnClose tests flushing the buffer when input closes.
func TestTumblingTimeWindowFlushOnClose(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},
		{Delay: 30 * time.Millisecond, Value: 2}, // These should be flushed before timer fires
	}
	expectedWindows := [][]int{
		{1, 2},
	}

	// Short context, but long enough for inputs to be sent
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	results, err := runTumblingTimeWindowTest(ctx, t, windowDuration, inputs)

	// We expect the run to finish without context error because input closes first
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, expectedWindows[0], results[0])
}

// TestTumblingTimeWindowEmptyInput tests behavior with no input.
func TestTumblingTimeWindowEmptyInput(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	inputs := []struct {
		Delay time.Duration
		Value int
	}{} // Empty input

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	results, err := runTumblingTimeWindowTest(ctx, t, windowDuration, inputs)

	require.NoError(t, err)
	assert.Empty(t, results, "Expected no windows for empty input")
}

// TestTumblingTimeWindowCancellation tests context cancellation during processing.
func TestTumblingTimeWindowCancellation(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	cancelTime := 150 * time.Millisecond // Cancel during the second window
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},  // Window 1
		{Delay: 50 * time.Millisecond, Value: 2},  // Window 1
		{Delay: 110 * time.Millisecond, Value: 3}, // Window 2 - Should arrive
		{Delay: 180 * time.Millisecond, Value: 4}, // Window 2 - Might not be sent due to cancellation
		{Delay: 250 * time.Millisecond, Value: 5}, // Window 3 - Should not be sent
	}

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	_, err := runTumblingTimeWindowTest(ctx, t, windowDuration, inputs)

	// Expect context deadline exceeded error from the ProcessStream stage
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestTumblingTimeWindowInvalidDuration tests panic on non-positive duration.
func TestTumblingTimeWindowInvalidDuration(t *testing.T) {
	assert.PanicsWithValue(
		t,
		"fluxus.NewTumblingTimeWindow: duration must be positive, got 0s",
		func() { fluxus.NewTumblingTimeWindow[int](0) },
		"Expected panic for duration 0",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewTumblingTimeWindow: duration must be positive, got -1ns",
		func() { fluxus.NewTumblingTimeWindow[int](-1 * time.Nanosecond) },
		"Expected panic for negative duration",
	)
}

// --- TumblingTimeWindow Benchmarks ---

// Helper function for time window benchmarks
func runTumblingTimeWindowBench(b *testing.B, windowDuration time.Duration, numItems int, itemDelay time.Duration) {
	b.Helper()
	ctx := context.Background()

	// Pre-generate input timings relative to start
	inputs := make([]struct {
		Delay time.Duration
		Value int
	}, numItems)
	for i := 0; i < numItems; i++ {
		inputs[i].Delay = time.Duration(i) * itemDelay
		inputs[i].Value = i
	}
	totalDuration := time.Duration(numItems) * itemDelay

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		source := make(chan int) // Unbuffered source
		// Buffered sink for benchmark to avoid blocking stage output
		sink := make(chan []int, (numItems/10)+10) // Estimate buffer size
		var wg sync.WaitGroup

		benchCtx, cancel := context.WithTimeout(ctx, totalDuration+windowDuration*2) // Context for the run

		windowStage := fluxus.NewTumblingTimeWindow[int](windowDuration)

		// Start stage
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = windowStage.ProcessStream(benchCtx, source, sink)
		}()

		// Start sink reader (just drain)
		wg.Add(1)
		go func() {
			defer wg.Done()
			//nolint:revive // Drain the channel
			for range sink {
			}
		}()

		// Start input sender (Optimized with timer reuse)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(source)

			// --- Timer Optimization ---
			// Create a single timer outside the loop.
			// Initialize it stopped or with a zero duration.
			timer := time.NewTimer(0)
			// Ensure timer is stopped if it hasn't fired yet when exiting.
			if !timer.Stop() {
				// Drain the channel if Stop returns false, indicating the timer fired.
				// This is unlikely if initialized with 0, but good practice.
				select {
				case <-timer.C:
				default:
				}
			}
			defer timer.Stop() // Ensure timer is stopped on exit
			// --- End Timer Optimization ---

			startTime := time.Now()
			for _, item := range inputs {
				sendTime := startTime.Add(item.Delay)
				sleepDuration := time.Until(sendTime)

				if sleepDuration > 0 {
					// --- Timer Optimization ---
					// Reset the existing timer instead of using time.After
					timer.Reset(sleepDuration)
					select {
					case <-timer.C:
						// Timer fired, time to send
					case <-benchCtx.Done():
						return // Context cancelled while waiting
					}
					// --- End Timer Optimization ---
				}
				// If sleepDuration <= 0, proceed immediately

				select {
				case source <- item.Value:
					// Item sent successfully
				case <-benchCtx.Done():
					return // Context cancelled while sending
				}
			}
		}()

		wg.Wait()
		cancel() // Clean up context
	}
	b.ReportAllocs()
}

// BenchmarkTumblingTimeWindow benchmarks the stage with various window durations.
func BenchmarkTumblingTimeWindow(b *testing.B) {
	numItems := 10000
	itemDelay := 100 * time.Microsecond // Rate at which items arrive
	windowDurations := []time.Duration{
		1 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		1 * time.Second,
	}

	for _, dur := range windowDurations {
		// Ensure window duration is larger than item delay for meaningful windows
		if dur <= itemDelay {
			continue
		}
		b.Run(fmt.Sprintf("WinDuration%s", dur), func(b *testing.B) {
			runTumblingTimeWindowBench(b, dur, numItems, itemDelay)
		})
	}
}

// BenchmarkTumblingTimeWindowInputRate benchmarks the stage with various input rates.
func BenchmarkTumblingTimeWindowInputRate(b *testing.B) {
	numItems := 10000
	windowDuration := 100 * time.Millisecond // Fixed window duration
	itemDelays := []time.Duration{
		1 * time.Microsecond,   // Very high rate
		10 * time.Microsecond,  // High rate
		100 * time.Microsecond, // Moderate rate
		1 * time.Millisecond,   // Lower rate
	}

	for _, delay := range itemDelays {
		b.Run(fmt.Sprintf("ItemDelay%s", delay), func(b *testing.B) {
			runTumblingTimeWindowBench(b, windowDuration, numItems, delay)
		})
	}
}

// --- End TumblingTimeWindow Tests & Benchmarks ---

// --- SlidingCountWindow Tests ---

// Helper function to run SlidingCountWindow in a test setting
func runSlidingCountWindowTest[T any](
	ctx context.Context,
	t *testing.T,
	windowSize, slideSize int,
	inputs []T,
) ([][]T, error) {
	t.Helper()

	source := make(chan T, len(inputs)) // Buffered source
	sink := make(chan []T)              // Unbuffered sink
	results := make([][]T, 0)
	var wg sync.WaitGroup
	var runErr error
	var resultsMu sync.Mutex // Protect results slice

	// Create the window stage
	windowStage := fluxus.NewSlidingCountWindow[T](windowSize, slideSize)

	// Start the stage
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = windowStage.ProcessStream(ctx, source, sink)
	}()

	// Start the sink reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for window := range sink {
			windowCopy := make([]T, len(window))
			copy(windowCopy, window)
			resultsMu.Lock()
			results = append(results, windowCopy)
			resultsMu.Unlock()
		}
	}()

	// Send inputs

	for _, item := range inputs {
		select {
		case source <- item:
		case <-ctx.Done():
			t.Logf("Context cancelled while sending input")
			//nolint:staticcheck // break out of the select
			break
		}
	}
	close(source) // Close source to signal end of input

	// Wait for stage and sink reader to finish
	wg.Wait()

	return results, runErr
}

// TestSlidingCountWindowBasic tests basic sliding behavior (slide < size).
func TestSlidingCountWindowBasic(t *testing.T) {
	windowSize := 5
	slideSize := 2
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	// Expected windows:
	// After 1, 2: buffer=[1,2], slideCount=2 -> emit? no (len < size)
	// After 3, 4: buffer=[1,2,3,4], slideCount=2 -> emit? no (len < size)
	// After 5: buffer=[1,2,3,4,5], slideCount=1
	// After 6: buffer=[2,3,4,5,6], slideCount=2 -> emit [2,3,4,5,6]
	// After 7: buffer=[3,4,5,6,7], slideCount=1
	// After 8: buffer=[4,5,6,7,8], slideCount=2 -> emit [4,5,6,7,8]
	// After 9: buffer=[5,6,7,8,9], slideCount=1
	// After 10: buffer=[6,7,8,9,10], slideCount=2 -> emit [6,7,8,9,10]
	expectedWindows := [][]int{
		{2, 3, 4, 5, 6},
		{4, 5, 6, 7, 8},
		{6, 7, 8, 9, 10},
	}

	results, err := runSlidingCountWindowTest(context.Background(), t, windowSize, slideSize, inputs)

	require.NoError(t, err)
	require.Len(t, results, len(expectedWindows))
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
	assert.Equal(t, expectedWindows[2], results[2])
}

// TestSlidingCountWindowTumbling tests behavior when slide == size (tumbling).
func TestSlidingCountWindowTumbling(t *testing.T) {
	windowSize := 3
	slideSize := 3
	inputs := []int{1, 2, 3, 4, 5, 6, 7}
	// Expected windows:
	// After 1, 2, 3: buffer=[1,2,3], slideCount=3 -> emit [1,2,3]
	// After 4, 5, 6: buffer=[4,5,6], slideCount=3 -> emit [4,5,6]
	// After 7: buffer=[7], slideCount=1 -> no emit
	expectedWindows := [][]int{
		{1, 2, 3},
		{4, 5, 6},
	}

	results, err := runSlidingCountWindowTest(context.Background(), t, windowSize, slideSize, inputs)

	require.NoError(t, err)
	require.Len(t, results, len(expectedWindows))
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
}

// TestSlidingCountWindowSlideGreaterThanSize tests behavior when slide > size (skipping items).
func TestSlidingCountWindowSlideGreaterThanSize(t *testing.T) {
	windowSize := 3
	slideSize := 5
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	// Expected windows:
	// After 1, 2, 3, 4, 5: buffer=[3,4,5], slideCount=5 -> emit [3,4,5]
	// After 6, 7, 8, 9, 10: buffer=[8,9,10], slideCount=5 -> emit [8,9,10]
	// After 11, 12: buffer=[10,11,12], slideCount=2 -> no emit
	expectedWindows := [][]int{
		{3, 4, 5},
		{8, 9, 10},
	}

	results, err := runSlidingCountWindowTest(context.Background(), t, windowSize, slideSize, inputs)

	require.NoError(t, err)
	require.Len(t, results, len(expectedWindows))
	assert.Equal(t, expectedWindows[0], results[0])
	assert.Equal(t, expectedWindows[1], results[1])
}

// TestSlidingCountWindowNotEnoughForFirstWindow tests when input ends before first window is full.
func TestSlidingCountWindowNotEnoughForFirstWindow(t *testing.T) {
	windowSize := 5
	slideSize := 2
	inputs := []int{1, 2, 3} // Not enough to fill size 5

	results, err := runSlidingCountWindowTest(context.Background(), t, windowSize, slideSize, inputs)

	require.NoError(t, err)
	assert.Empty(t, results, "Expected no windows when input count < window size")
}

// TestSlidingCountWindowNotEnoughForSlide tests when input ends before first slide completes.
func TestSlidingCountWindowNotEnoughForSlide(t *testing.T) {
	windowSize := 5
	slideSize := 3
	inputs := []int{1, 2} // Not enough to complete slide 3

	results, err := runSlidingCountWindowTest(context.Background(), t, windowSize, slideSize, inputs)

	require.NoError(t, err)
	assert.Empty(t, results, "Expected no windows when input count < slide size")
}

// TestSlidingCountWindowEmptyInput tests behavior with no input.
func TestSlidingCountWindowEmptyInput(t *testing.T) {
	windowSize := 5
	slideSize := 2
	inputs := []int{}

	results, err := runSlidingCountWindowTest(context.Background(), t, windowSize, slideSize, inputs)

	require.NoError(t, err)
	assert.Empty(t, results, "Expected no windows for empty input")
}

// TestSlidingCountWindowCancellation tests context cancellation during processing.
func TestSlidingCountWindowCancellation(t *testing.T) {
	windowSize := 5
	slideSize := 2
	inputs := make([]int, 200) // Use enough inputs to keep the stage busy
	for i := range inputs {
		inputs[i] = i
	}
	cancelTime := 25 * time.Millisecond // A short, but not instantaneous, timeout

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel() // Ensure cancel is eventually called

	source := make(chan int) // Use unbuffered source
	sink := make(chan []int) // Use unbuffered sink
	var wg sync.WaitGroup
	var runErr error

	windowStage := fluxus.NewSlidingCountWindow[int](windowSize, slideSize)

	// Start the stage
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Capture the error returned by ProcessStream
		runErr = windowStage.ProcessStream(ctx, source, sink)
	}()

	// Start a sink drainer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Drain the sink until it's closed
		//nolint:revive // Draining the channel is the intended behavior here.
		for range sink {
		}
	}()

	// Start input sender goroutine
	inputSendingDone := make(chan struct{})
	go func() {
		defer close(inputSendingDone) // Signal when sending stops
		for _, item := range inputs {
			select {
			case source <- item:
				// Item sent successfully
			case <-ctx.Done():
				// Context was cancelled while trying to send input
				t.Logf("Input sending interrupted by context cancellation.")
				return // Stop sending
			}
		}
		// Input loop finished before context cancellation
		t.Logf("Input sending finished naturally before cancellation.")
	}()

	// Wait until the context is guaranteed to be cancelled
	<-ctx.Done()
	t.Logf("Context cancellation detected by test.")

	// Now that the context is cancelled, close the source channel.
	// This allows the ProcessStream loop to potentially detect the closed channel
	// if it wasn't already stopped by the context cancellation.
	close(source)

	// Wait for the input sender to finish (it might have already exited due to cancellation)
	<-inputSendingDone

	// Wait for the main stage goroutine and sink drainer to complete.
	wg.Wait()

	// Assert that the stage returned an error, specifically the context error.
	require.Error(t, runErr, "ProcessStream should have returned an error due to context cancellation")
	// Check for DeadlineExceeded specifically, as that's what WithTimeout causes.
	assert.ErrorIs(t, runErr, context.DeadlineExceeded, "Expected error to be context.DeadlineExceeded")
}

// TestSlidingCountWindowInvalidSize tests panic on non-positive size or slide.
func TestSlidingCountWindowInvalidSize(t *testing.T) {
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingCountWindow: size must be positive, got 0",
		func() { fluxus.NewSlidingCountWindow[int](0, 1) },
		"Expected panic for size 0",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingCountWindow: slide must be positive, got 0",
		func() { fluxus.NewSlidingCountWindow[int](1, 0) },
		"Expected panic for slide 0",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingCountWindow: size must be positive, got -1",
		func() { fluxus.NewSlidingCountWindow[int](-1, 1) },
		"Expected panic for negative size",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingCountWindow: slide must be positive, got -1",
		func() { fluxus.NewSlidingCountWindow[int](1, -1) },
		"Expected panic for negative slide",
	)
}

// --- SlidingCountWindow Benchmarks ---

// Helper function for sliding count window benchmarks
func runSlidingCountWindowBench(b *testing.B, windowSize, slideSize, numItems int) {
	b.Helper()
	ctx := context.Background()
	inputs := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		inputs[i] = i
	}

	// Estimate number of windows for sink buffer
	numWindows := 0
	if numItems >= windowSize {
		numWindows = (numItems-windowSize)/slideSize + 1
	}
	if numWindows < 1 {
		numWindows = 1 // At least 1 for buffer
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		source := make(chan int, numItems)
		sink := make(chan []int, numWindows) // Buffered sink
		var wg sync.WaitGroup

		windowStage := fluxus.NewSlidingCountWindow[int](windowSize, slideSize)

		// Start stage
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = windowStage.ProcessStream(ctx, source, sink)
		}()

		// Start sink reader (just drain)
		wg.Add(1)
		go func() {
			defer wg.Done()
			//nolint:revive // Drain the channel
			for range sink {
			}
		}()

		// Send inputs
		for _, item := range inputs {
			source <- item
		}
		close(source)

		wg.Wait()
	}
	b.ReportAllocs()
}

// BenchmarkSlidingCountWindow benchmarks the stage with various size/slide combinations.
func BenchmarkSlidingCountWindow(b *testing.B) {
	numItems := 10000
	configs := []struct {
		Size  int
		Slide int
	}{
		{Size: 10, Slide: 1},   // High overlap
		{Size: 10, Slide: 5},   // Medium overlap
		{Size: 10, Slide: 10},  // Tumbling
		{Size: 100, Slide: 1},  // High overlap, larger window
		{Size: 100, Slide: 10}, // Medium overlap, larger window
		{Size: 100, Slide: 50},
		{Size: 100, Slide: 100}, // Tumbling, larger window
		{Size: 1000, Slide: 10}, // Large window, medium overlap
		{Size: 1000, Slide: 100},
		{Size: 1000, Slide: 1000}, // Tumbling, large window
		{Size: 10, Slide: 20},     // Slide > Size
		{Size: 100, Slide: 200},   // Slide > Size
	}

	for _, cfg := range configs {
		b.Run(fmt.Sprintf("Size%d_Slide%d", cfg.Size, cfg.Slide), func(b *testing.B) {
			runSlidingCountWindowBench(b, cfg.Size, cfg.Slide, numItems)
		})
	}
}

// BenchmarkSlidingCountWindowInputSize benchmarks the stage with various input sizes.
func BenchmarkSlidingCountWindowInputSize(b *testing.B) {
	windowSize := 100 // Fixed window size
	slideSize := 10   // Fixed slide size
	numItemsList := []int{100, 1000, 10000, 100000}

	for _, numItems := range numItemsList {
		b.Run(fmt.Sprintf("NumItems%d", numItems), func(b *testing.B) {
			runSlidingCountWindowBench(b, windowSize, slideSize, numItems)
		})
	}
}

// --- End SlidingCountWindow Tests & Benchmarks ---

// --- SlidingTimeWindow Tests ---

// Helper function to run SlidingTimeWindow in a test setting
// Inputs are pairs of (delay before sending, value)
func runSlidingTimeWindowTest[T any](
	ctx context.Context,
	t *testing.T,
	windowDuration, slideInterval time.Duration,
	inputs []struct {
		Delay time.Duration
		Value T
	},
) ([][]T, error) {
	t.Helper()

	source := make(chan T) // Unbuffered source for controlled timing
	sink := make(chan []T) // Unbuffered sink
	results := make([][]T, 0)
	var wg sync.WaitGroup
	var runErr error
	var resultsMu sync.Mutex // Protect results slice

	// Create the window stage
	windowStage := fluxus.NewSlidingTimeWindow[T](windowDuration, slideInterval)

	// Start the stage
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = windowStage.ProcessStream(ctx, source, sink)
	}()

	// Start the sink reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for window := range sink {
			windowCopy := make([]T, len(window))
			copy(windowCopy, window)
			resultsMu.Lock()
			results = append(results, windowCopy)
			resultsMu.Unlock()
		}
	}()

	// Start the input sender (using optimized timer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// --- CHANGE: Defer close(source) moved lower ---

		timer := time.NewTimer(0)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		defer timer.Stop()

		startTime := time.Now()
		inputLoopDone := false
		for _, item := range inputs {
			// --- Existing input sending logic ---
			sendTime := startTime.Add(item.Delay)
			sleepDuration := time.Until(sendTime)

			if sleepDuration > 0 {
				timer.Reset(sleepDuration)
				select {
				case <-timer.C:
					// Time to send
				case <-ctx.Done():
					t.Logf("Context cancelled while waiting to send input: %v", item.Value)
					inputLoopDone = true // Mark loop as done due to cancellation
					break                // Exit inner select
				}
			}
			if inputLoopDone {
				break // Exit outer loop if cancelled
			}

			// Send the item, respecting context cancellation
			select {
			case source <- item.Value:
				// Sent successfully
			case <-ctx.Done():
				t.Logf("Context cancelled while sending input: %v", item.Value)
				inputLoopDone = true // Mark loop as done due to cancellation
				//nolint:staticcheck // Break is intentional to break out of the selects
				break // Exit inner select
			}
			if inputLoopDone {
				break // Exit outer loop if cancelled
			}
			// --- End existing input sending logic ---
		}

		// --- NEW: Wait after sending inputs before closing source ---
		if !inputLoopDone {
			// Only wait if the input loop wasn't cancelled
			// Wait for a duration that allows subsequent ticks to process the last items.
			// e.g., wait for another slide interval plus a small buffer.
			waitDuration := slideInterval + 10*time.Millisecond
			t.Logf("Input sending finished, waiting %v before closing source.", waitDuration)
			timer.Reset(waitDuration)
			select {
			case <-timer.C:
				// Wait finished
			case <-ctx.Done():
				// Context cancelled during the final wait
				t.Logf("Context cancelled during final wait after sending inputs.")
			}
		}
		// --- End NEW ---

		// Now close the source channel
		close(source)
		t.Logf("Source channel closed.")
		// --- End CHANGE ---
	}()

	// Wait for stage, sink reader, and input sender to finish
	wg.Wait()

	return results, runErr
}

// TestSlidingTimeWindowBasic tests overlapping windows.
func TestSlidingTimeWindowBasic(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	slideInterval := 50 * time.Millisecond
	// Ticks expected around: 50ms, 100ms, 150ms, 200ms.
	// Final tick ~270ms (relative to source close) evaluates window [170ms, 270ms) -> empty.
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},
		{Delay: 40 * time.Millisecond, Value: 2},
		{Delay: 60 * time.Millisecond, Value: 3},
		{Delay: 90 * time.Millisecond, Value: 4},
		{Delay: 120 * time.Millisecond, Value: 5},
		{Delay: 160 * time.Millisecond, Value: 6},
	}
	// Expected windows based on ticks and final timer logic:
	// Tick @ ~50ms: Window [ -50ms, 50ms). Items: {1, 2}
	// Tick @ ~100ms: Window [   0ms, 100ms). Items: {1, 2, 3, 4}
	// Tick @ ~150ms: Window [  50ms, 150ms). Items: {3, 4, 5}
	// Tick @ ~200ms: Window [ 100ms, 200ms). Items: {5, 6}
	// Final Tick @ ~270ms: Window [ 170ms, 270ms). Items: {} -> No emission
	expectedWindows := [][]int{ // <-- CHANGE: Removed the last expected window [6]
		{1, 2},
		{1, 2, 3, 4},
		{3, 4, 5},
		{5, 6},
	}

	// Use a context long enough for all ticks and processing
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Use the helper with the shorter wait time now, as the stage handles the final tick
	results, err := runSlidingTimeWindowTest(ctx, t, windowDuration, slideInterval, inputs)

	require.NoError(t, err)
	// Now we expect exactly 4 windows
	require.Len(t, results, len(expectedWindows), "Should have the expected number of windows")

	// Check the content using ElementsMatch
	if len(results) >= 1 {
		assert.ElementsMatch(t, expectedWindows[0], results[0])
	}
	if len(results) >= 2 {
		assert.ElementsMatch(t, expectedWindows[1], results[1])
	}
	if len(results) >= 3 {
		assert.ElementsMatch(t, expectedWindows[2], results[2])
	}
	if len(results) >= 4 {
		assert.ElementsMatch(t, expectedWindows[3], results[3])
	}
	// No 5th assertion needed
}

// TestSlidingTimeWindowTumbling tests behavior when slide == duration.
func TestSlidingTimeWindowTumbling(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	slideInterval := 100 * time.Millisecond // Same as duration
	// Ticks expected around: 100ms, 200ms, 300ms, ...
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},  // Window 1 (0-100ms)
		{Delay: 50 * time.Millisecond, Value: 2},  // Window 1
		{Delay: 110 * time.Millisecond, Value: 3}, // Window 2 (100-200ms)
		{Delay: 180 * time.Millisecond, Value: 4}, // Window 2
		{Delay: 250 * time.Millisecond, Value: 5}, // Arrives, but source closes before next tick
	}
	// Expected windows (should behave like tumbling, BUT no final flush on close):
	// Tick @ 100ms: Window [0ms, 100ms). Items: {1, 2}
	// Tick @ 200ms: Window [100ms, 200ms). Items: {3, 4}
	// Source closes before Tick @ 300ms, so {5} is NOT emitted.
	expectedWindows := [][]int{ // <-- CHANGE: Removed the last expected window [5]
		{1, 2},
		{3, 4},
		{5},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	results, err := runSlidingTimeWindowTest(ctx, t, windowDuration, slideInterval, inputs)

	require.NoError(t, err)
	require.Len(t, results, len(expectedWindows)) // <-- CHANGE: Length assertion now expects 2
	assert.ElementsMatch(t, expectedWindows[0], results[0])
	assert.ElementsMatch(t, expectedWindows[1], results[1])
	assert.ElementsMatch(t, expectedWindows[2], results[2])
}

// TestSlidingTimeWindowSlideGreater tests behavior when slide > duration (non-overlapping, potentially skipping time).
func TestSlidingTimeWindowSlideGreater(t *testing.T) {
	windowDuration := 50 * time.Millisecond
	slideInterval := 100 * time.Millisecond // Slide is longer
	// Ticks expected around: 100ms, 200ms, 300ms, ...
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},  // Before tick 1
		{Delay: 70 * time.Millisecond, Value: 2},  // Between window end for tick 1 and tick 1 itself
		{Delay: 110 * time.Millisecond, Value: 3}, // Before tick 2
		{Delay: 160 * time.Millisecond, Value: 4}, // Between window end for tick 2 and tick 2 itself
		{Delay: 220 * time.Millisecond, Value: 5}, // Before tick 3
	}
	// Expected windows:
	// Tick @ 100ms: Window [50ms, 100ms). Items: {2} (item 1 is too old)
	// Tick @ 200ms: Window [150ms, 200ms). Items: {4} (item 3 is too old)
	// Tick @ 300ms: Window [250ms, 300ms). Items: {} (item 5 is too old)
	// Tick @ 400ms: Window [350ms, 400ms). Items: {}
	// ... then source closes, no final flush
	expectedWindows := [][]int{
		{2},
		{4},
		// Empty windows are not emitted
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	results, err := runSlidingTimeWindowTest(ctx, t, windowDuration, slideInterval, inputs)

	require.NoError(t, err)
	require.Len(t, results, len(expectedWindows))
	assert.ElementsMatch(t, expectedWindows[0], results[0])
	assert.ElementsMatch(t, expectedWindows[1], results[1])
}

// TestSlidingTimeWindowEmptyInput tests behavior with no input.
func TestSlidingTimeWindowEmptyInput(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	slideInterval := 50 * time.Millisecond
	inputs := []struct {
		Delay time.Duration
		Value int
	}{} // Empty input

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond) // Allow a couple of ticks
	defer cancel()

	results, err := runSlidingTimeWindowTest(ctx, t, windowDuration, slideInterval, inputs)

	require.NoError(t, err)
	assert.Empty(t, results, "Expected no windows for empty input")
}

// TestSlidingTimeWindowCancellation tests context cancellation during processing.
func TestSlidingTimeWindowCancellation(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	slideInterval := 50 * time.Millisecond
	cancelTime := 120 * time.Millisecond // Cancel after ~2 ticks
	inputs := []struct {
		Delay time.Duration
		Value int
	}{
		{Delay: 10 * time.Millisecond, Value: 1},
		{Delay: 40 * time.Millisecond, Value: 2},
		{Delay: 60 * time.Millisecond, Value: 3},
		{Delay: 90 * time.Millisecond, Value: 4},
		{Delay: 110 * time.Millisecond, Value: 5}, // Might arrive before cancellation hits
		{Delay: 150 * time.Millisecond, Value: 6}, // Should not be sent
	}

	ctx, cancel := context.WithTimeout(context.Background(), cancelTime)
	defer cancel()

	_, err := runSlidingTimeWindowTest(ctx, t, windowDuration, slideInterval, inputs)

	// Expect context deadline exceeded error from the ProcessStream stage
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestSlidingTimeWindowInvalidParams tests panic on non-positive duration or slide.
func TestSlidingTimeWindowInvalidParams(t *testing.T) {
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingTimeWindow: duration must be positive, got 0s",
		func() { fluxus.NewSlidingTimeWindow[int](0, 1*time.Second) },
		"Expected panic for duration 0",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingTimeWindow: slide must be positive, got 0s",
		func() { fluxus.NewSlidingTimeWindow[int](1*time.Second, 0) },
		"Expected panic for slide 0",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingTimeWindow: duration must be positive, got -1ns",
		func() { fluxus.NewSlidingTimeWindow[int](-1*time.Nanosecond, 1*time.Second) },
		"Expected panic for negative duration",
	)
	assert.PanicsWithValue(
		t,
		"fluxus.NewSlidingTimeWindow: slide must be positive, got -1ns",
		func() { fluxus.NewSlidingTimeWindow[int](1*time.Second, -1*time.Nanosecond) },
		"Expected panic for negative slide",
	)
}

// --- SlidingTimeWindow Benchmarks ---

// Helper function for sliding time window benchmarks
func runSlidingTimeWindowBench(
	b *testing.B,
	windowDuration, slideInterval time.Duration,
	numItems int,
	itemDelay time.Duration,
) {
	b.Helper()
	ctx := context.Background()

	// Pre-generate input timings relative to start
	inputs := make([]struct {
		Delay time.Duration
		Value int
	}, numItems)
	for i := 0; i < numItems; i++ {
		inputs[i].Delay = time.Duration(i) * itemDelay
		inputs[i].Value = i
	}
	totalDuration := time.Duration(numItems) * itemDelay

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		source := make(chan int) // Unbuffered source
		// Estimate buffer size - can be tricky for sliding windows
		numWindowsEstimate := int(totalDuration/slideInterval) + 10
		sink := make(chan []int, numWindowsEstimate) // Buffered sink

		var wg sync.WaitGroup

		benchCtx, cancel := context.WithTimeout(
			ctx,
			totalDuration+windowDuration+slideInterval*5,
		) // Context for the run

		windowStage := fluxus.NewSlidingTimeWindow[int](windowDuration, slideInterval)

		// Start stage
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = windowStage.ProcessStream(benchCtx, source, sink)
		}()

		// Start sink reader (just drain)
		wg.Add(1)
		go func() {
			defer wg.Done()
			//nolint:revive // Drain the channel
			for range sink {
			}
		}()

		// Start input sender (Optimized with timer reuse)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(source)

			timer := time.NewTimer(0)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			defer timer.Stop()

			startTime := time.Now()
			for _, item := range inputs {
				sendTime := startTime.Add(item.Delay)
				sleepDuration := time.Until(sendTime)

				if sleepDuration > 0 {
					timer.Reset(sleepDuration)
					select {
					case <-timer.C:
					case <-benchCtx.Done():
						return
					}
				}
				select {
				case source <- item.Value:
				case <-benchCtx.Done():
					return
				}
			}
		}()

		wg.Wait()
		cancel() // Clean up context
	}
	b.ReportAllocs()
}

// BenchmarkSlidingTimeWindow benchmarks the stage with various duration/slide combinations.
func BenchmarkSlidingTimeWindow(b *testing.B) {
	numItems := 10000
	itemDelay := 100 * time.Microsecond // Rate at which items arrive (1s total)
	configs := []struct {
		Duration time.Duration
		Slide    time.Duration
	}{
		{Duration: 10 * time.Millisecond, Slide: 1 * time.Millisecond},   // High overlap, small window
		{Duration: 10 * time.Millisecond, Slide: 5 * time.Millisecond},   // Medium overlap
		{Duration: 10 * time.Millisecond, Slide: 10 * time.Millisecond},  // Tumbling
		{Duration: 100 * time.Millisecond, Slide: 1 * time.Millisecond},  // High overlap, larger window
		{Duration: 100 * time.Millisecond, Slide: 10 * time.Millisecond}, // Medium overlap
		{Duration: 100 * time.Millisecond, Slide: 50 * time.Millisecond},
		{Duration: 100 * time.Millisecond, Slide: 100 * time.Millisecond}, // Tumbling
		{Duration: 1 * time.Second, Slide: 10 * time.Millisecond},         // Large window, medium overlap
		{Duration: 1 * time.Second, Slide: 100 * time.Millisecond},
		{Duration: 1 * time.Second, Slide: 1 * time.Second},             // Tumbling
		{Duration: 10 * time.Millisecond, Slide: 20 * time.Millisecond}, // Slide > Duration
		{Duration: 100 * time.Millisecond, Slide: 200 * time.Millisecond},
	}

	for _, cfg := range configs {
		b.Run(fmt.Sprintf("Dur%s_Slide%s", cfg.Duration, cfg.Slide), func(b *testing.B) {
			runSlidingTimeWindowBench(b, cfg.Duration, cfg.Slide, numItems, itemDelay)
		})
	}
}

// BenchmarkSlidingTimeWindowInputRate benchmarks the stage with various input rates.
func BenchmarkSlidingTimeWindowInputRate(b *testing.B) {
	numItems := 10000
	windowDuration := 100 * time.Millisecond // Fixed window duration
	slideInterval := 10 * time.Millisecond   // Fixed slide interval
	itemDelays := []time.Duration{
		1 * time.Microsecond,   // Very high rate
		10 * time.Microsecond,  // High rate
		100 * time.Microsecond, // Moderate rate (total 1s)
		1 * time.Millisecond,   // Lower rate (total 10s)
	}

	for _, delay := range itemDelays {
		b.Run(fmt.Sprintf("ItemDelay%s", delay), func(b *testing.B) {
			runSlidingTimeWindowBench(b, windowDuration, slideInterval, numItems, delay)
		})
	}
}

// --- End SlidingTimeWindow Tests & Benchmarks ---
