package fluxus

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// --- Windowing Patterns ---

// TumblingCountWindow collects items into fixed-size, non-overlapping batches (windows)
// based on item count. It implements the StreamStage interface.
// Input: T, Output: []T
type TumblingCountWindow[T any] struct {
	size   int
	buffer []T
	// Optional: Add logger, metrics collector if needed later via options
}

// NewTumblingCountWindow creates a new TumblingCountWindow stage.
// 'size' determines the number of items in each window.
// Panics if size is not positive.
func NewTumblingCountWindow[T any](size int) *TumblingCountWindow[T] {
	if size <= 0 {
		panic(fmt.Sprintf("fluxus.NewTumblingCountWindow: size must be positive, got %d", size))
	}
	return &TumblingCountWindow[T]{
		size: size,
		// Pre-allocate buffer capacity for efficiency
		buffer: make([]T, 0, size),
	}
}

// ProcessStream implements the StreamStage interface for TumblingCountWindow.
// It reads items from 'in', buffers them, and sends slices of 'size' items
// to 'out'. Any remaining items are sent as a final partial window when 'in' closes.
func (w *TumblingCountWindow[T]) ProcessStream(ctx context.Context, in <-chan T, out chan<- []T) error {
	// CRITICAL: Ensure output channel is closed on exit
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, exit gracefully.
			// Note: Any items currently in the buffer are discarded on cancellation.
			// This is typical behavior; flushing on cancellation can be complex.
			return ctx.Err()

		case item, ok := <-in:
			if !ok {
				// Input channel closed. Flush any remaining items in the buffer.
				if len(w.buffer) > 0 {
					// Create a copy of the final partial window
					finalWindow := make([]T, len(w.buffer))
					copy(finalWindow, w.buffer)

					// Send the final window, respecting context cancellation
					select {
					case out <- finalWindow:
						// Successfully sent final partial window
					case <-ctx.Done():
						return ctx.Err() // Cancelled while sending final window
					}
				}
				return nil // Normal exit after flushing
			}

			// Add item to the buffer
			w.buffer = append(w.buffer, item)

			// Check if the buffer is full
			if len(w.buffer) == w.size {
				// Create a copy of the window to send downstream.
				// This is crucial because we will reuse the buffer.
				windowToSend := make([]T, w.size)
				copy(windowToSend, w.buffer)

				// Send the full window, respecting context cancellation
				select {
				case out <- windowToSend:
					// Successfully sent window
					// Reset the buffer for the next window by slicing (keeps capacity)
					w.buffer = w.buffer[:0]
				case <-ctx.Done():
					return ctx.Err() // Cancelled while sending a full window
				}
			}
		}
	}
}

// TumblingTimeWindow collects items into fixed-duration, non-overlapping windows
// based on time. It implements the StreamStage interface.
// Input: T, Output: []T
type TumblingTimeWindow[T any] struct {
	duration time.Duration
	buffer   []T
	timer    *time.Timer
	mu       sync.Mutex // Protects access to buffer
	// Optional: Add logger, metrics collector if needed later via options
}

// NewTumblingTimeWindow creates a new TumblingTimeWindow stage.
// 'duration' specifies the fixed time length of each window.
// Panics if duration is not positive.
func NewTumblingTimeWindow[T any](duration time.Duration) *TumblingTimeWindow[T] {
	if duration <= 0 {
		panic(fmt.Sprintf("fluxus.NewTumblingTimeWindow: duration must be positive, got %v", duration))
	}
	return &TumblingTimeWindow[T]{
		duration: duration,
		buffer:   make([]T, 0), // Initial buffer, capacity can grow
		// Timer will be initialized in ProcessStream
	}
}

// processTick handles the logic when the timer fires for TumblingTimeWindow.
// It locks the mutex, prepares the window, resets the buffer, and sends the window.
// Returns an error if sending fails due to context cancellation.
func (w *TumblingTimeWindow[T]) processTick(ctx context.Context, out chan<- []T) error {
	var windowToSend []T
	w.mu.Lock()
	if len(w.buffer) > 0 {
		// Create a copy of the window to send downstream.
		windowToSend = make([]T, len(w.buffer))
		copy(windowToSend, w.buffer)
		// Reset the buffer for the next window by slicing (keeps capacity)
		w.buffer = w.buffer[:0]
	}
	w.mu.Unlock() // Unlock before potentially blocking send

	// Send the window if it's not empty
	if len(windowToSend) > 0 {
		select {
		case out <- windowToSend:
			// Successfully sent window
		case <-ctx.Done():
			return ctx.Err() // Cancelled while sending a window
		}
	}

	// IMPORTANT: Reset the timer for the next interval AFTER processing the current tick.
	// This should be done in the main loop after calling processTick.
	// w.timer.Reset(w.duration) // Moved to main loop

	return nil
}

// flushBuffer handles flushing the remaining buffer when the input channel closes.
// It locks the mutex, prepares the final window, and sends it.
// Returns an error if sending fails due to context cancellation.
func (w *TumblingTimeWindow[T]) flushBuffer(ctx context.Context, out chan<- []T) error {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return nil // Nothing to flush
	}

	// Create a copy of the final partial window
	finalWindow := make([]T, len(w.buffer))
	copy(finalWindow, w.buffer)
	w.buffer = w.buffer[:0] // Reset buffer
	w.mu.Unlock()           // Unlock before potentially blocking send

	// Send the final window, respecting context cancellation
	select {
	case out <- finalWindow:
		// Successfully sent final partial window
	case <-ctx.Done():
		return ctx.Err() // Cancelled while sending final window
	}
	return nil
}

// ProcessStream implements the StreamStage interface for TumblingTimeWindow.
// Refactored to reduce cognitive complexity.
func (w *TumblingTimeWindow[T]) ProcessStream(ctx context.Context, in <-chan T, out chan<- []T) error {
	defer close(out)

	w.timer = time.NewTimer(w.duration)
	defer w.timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case item, ok := <-in:
			if !ok {
				// Input channel closed. Stop timer and flush.
				if !w.timer.Stop() {
					// Timer already fired and channel might contain a value. Drain it.
					select {
					case <-w.timer.C:
					default:
					}
				}
				// Flush remaining buffer
				return w.flushBuffer(ctx, out) // Use helper
			}

			// Add item to the buffer under lock
			w.mu.Lock()
			w.buffer = append(w.buffer, item)
			w.mu.Unlock()

		case <-w.timer.C:
			// Timer fired, process the tick using the helper
			if err := w.processTick(ctx, out); err != nil {
				return err // Return error if sending failed due to cancellation
			}
			// Reset timer *after* processing the tick successfully
			w.timer.Reset(w.duration)
		}
	}
}

// SlidingCountWindow collects items into potentially overlapping windows based on item count.
// It emits a window containing the last 'size' items every 'slide' items received.
// It implements the StreamStage interface.
// Input: T, Output: []T
type SlidingCountWindow[T any] struct {
	size       int
	slide      int
	buffer     *circularBuffer[T] // Use the circular buffer
	slideCount int
	// Optional: Add logger, metrics collector if needed later via options
}

// NewSlidingCountWindow creates a new SlidingCountWindow stage.
// 'size' is the fixed number of items in each emitted window.
// 'slide' is the number of incoming items that trigger a window emission.
// Typically, slide <= size.
// Panics if size or slide are not positive.
func NewSlidingCountWindow[T any](size, slide int) *SlidingCountWindow[T] {
	if size <= 0 {
		panic(fmt.Sprintf("fluxus.NewSlidingCountWindow: size must be positive, got %d", size))
	}
	if slide <= 0 {
		panic(fmt.Sprintf("fluxus.NewSlidingCountWindow: slide must be positive, got %d", slide))
	}

	return &SlidingCountWindow[T]{
		size:       size,
		slide:      slide,
		buffer:     newCircularBuffer[T](size), // Initialize circular buffer
		slideCount: 0,
	}
}

// ProcessStream implements the StreamStage interface for SlidingCountWindow.
// It reads items from 'in', maintains a circular buffer of the last 'size' items,
// and emits a copy of the buffer every 'slide' items once the buffer is full.
func (w *SlidingCountWindow[T]) ProcessStream(ctx context.Context, in <-chan T, out chan<- []T) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case item, ok := <-in:
			if !ok {
				return nil // Input closed, normal exit
			}

			// Add item to the circular buffer (overwrites oldest if full)
			w.buffer.Add(item)

			// Increment the slide counter
			w.slideCount++

			// Check if a slide interval is complete
			if w.slideCount == w.slide {
				w.slideCount = 0 // Reset slide counter

				// Get the window content (only returns non-nil if buffer is full)
				windowToSend := w.buffer.GetWindow()

				// Only emit if the buffer was full (GetWindow returned non-nil)
				if windowToSend != nil {
					// Send the full window, respecting context cancellation
					select {
					case out <- windowToSend:
						// Successfully sent window
					case <-ctx.Done():
						return ctx.Err() // Cancelled while sending a window
					}
				}
				// If buffer isn't full yet (GetWindow returned nil), wait for more items.
			}
		}
	}
}

// timestampedItem wraps an item with its arrival timestamp.
type timestampedItem[T any] struct {
	Timestamp time.Time
	Value     T
}

// SlidingTimeWindow collects items into potentially overlapping windows based on time.
// It emits a window containing items that arrived within the last 'duration'
// every 'slide' interval.
// It implements the StreamStage interface.
// Input: T, Output: []T
type SlidingTimeWindow[T any] struct {
	duration time.Duration        // The time length of each window.
	slide    time.Duration        // How often windows are emitted.
	buffer   []timestampedItem[T] // Stores recent items with timestamps. Needs to be kept sorted.
	ticker   *time.Ticker         // Triggers window evaluation every 'slide'.
	mu       sync.Mutex           // Protects access to the buffer.
	// Optional: Add logger, metrics collector if needed later via options
}

// NewSlidingTimeWindow creates a new SlidingTimeWindow stage.
// 'duration' is the time length of each emitted window.
// 'slide' is the interval at which windows are emitted. Typically slide <= duration.
// Panics if duration or slide are not positive.
func NewSlidingTimeWindow[T any](duration, slide time.Duration) *SlidingTimeWindow[T] {
	if duration <= 0 {
		panic(fmt.Sprintf("fluxus.NewSlidingTimeWindow: duration must be positive, got %v", duration))
	}
	if slide <= 0 {
		panic(fmt.Sprintf("fluxus.NewSlidingTimeWindow: slide must be positive, got %v", slide))
	}
	// Optional: Warn if slide > duration?

	return &SlidingTimeWindow[T]{
		duration: duration,
		slide:    slide,
		buffer:   make([]timestampedItem[T], 0),
		// Ticker is initialized in ProcessStream
	}
}

// ProcessStream implements the StreamStage interface for SlidingTimeWindow.
func (w *SlidingTimeWindow[T]) ProcessStream(ctx context.Context, in <-chan T, out chan<- []T) error {
	defer close(out)

	w.ticker = time.NewTicker(w.slide)
	defer w.ticker.Stop() // Ensure regular ticker is stopped eventually

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case item, ok := <-in:
			if !ok {
				// --- Input Closed Handling ---
				// Input channel closed. Stop the periodic ticker.
				w.ticker.Stop()
				// Drain the ticker channel just in case a tick fired concurrently
				// but wasn't processed yet.
				select {
				case <-w.ticker.C:
				default:
				}

				// Start a final timer for one slide duration from now.
				// This gives a chance to emit a window including items that arrived
				// shortly before the input closed.
				finalTimer := time.NewTimer(w.slide)
				// Ensure finalTimer is stopped if we exit early due to context cancellation
				defer finalTimer.Stop()

				select {
				case <-ctx.Done():
					// Context cancelled before final tick could be processed
					return ctx.Err()
				case finalTickTime := <-finalTimer.C:
					// Process the final window based on this final tick time
					// We ignore the error from processTick here as we are exiting anyway.
					_ = w.processTick(ctx, finalTickTime, out)
					// Exit normally after processing the final tick
					return nil
				}
				// --- End Input Closed Handling ---
			}

			// Record arrival time and add to buffer (only if input was open)
			now := time.Now()
			w.mu.Lock()
			w.buffer = append(w.buffer, timestampedItem[T]{Timestamp: now, Value: item})
			w.mu.Unlock()

		case tickTime := <-w.ticker.C:
			// Slide interval elapsed, evaluate and emit a window.
			// Ignore error from processTick here; let the main loop handle context errors.
			_ = w.processTick(ctx, tickTime, out)
			// Ticker automatically resets in NewTicker
		}
	}
}

// Helper function to process a tick event (refactored from ProcessStream)
// This function remains unchanged from the previous version.
func (w *SlidingTimeWindow[T]) processTick(ctx context.Context, tickTime time.Time, out chan<- []T) error {
	// Define the window boundaries for this tick
	windowEndTime := tickTime
	windowStartTime := windowEndTime.Add(-w.duration)

	var itemsInWindow []T
	var cleanupRequired bool

	w.mu.Lock()
	// Find items within the window [windowStartTime, windowEndTime)
	firstKeepIndex := -1
	for i, tsItem := range w.buffer {
		if !tsItem.Timestamp.Before(windowStartTime) {
			if firstKeepIndex == -1 {
				firstKeepIndex = i
			}
			if tsItem.Timestamp.Before(windowEndTime) {
				itemsInWindow = append(itemsInWindow, tsItem.Value)
			}
		}
	}

	// Perform cleanup
	if firstKeepIndex > 0 {
		keptCount := len(w.buffer) - firstKeepIndex
		copy(w.buffer[0:], w.buffer[firstKeepIndex:])
		w.buffer = w.buffer[:keptCount]
		cleanupRequired = true
	} else if firstKeepIndex == -1 && len(w.buffer) > 0 {
		// All items are older than windowStartTime, clear the buffer.
		w.buffer = w.buffer[:0]
		cleanupRequired = true
	}
	// If firstKeepIndex is 0, no cleanup needed based on this window's start time.

	// Optional: Shrink buffer capacity if cleanup happened and buffer is small
	if cleanupRequired && cap(w.buffer) > 2*len(w.buffer) && cap(w.buffer) > 32 { // Heuristic
		newCap := len(w.buffer)
		if newCap < 32 {
			newCap = 32
		}
		newBuffer := make([]timestampedItem[T], len(w.buffer), newCap)
		copy(newBuffer, w.buffer)
		w.buffer = newBuffer
	}

	w.mu.Unlock() // Unlock before potentially blocking send

	// Emit the window if it contains items
	if len(itemsInWindow) > 0 {
		// Create a final copy to send downstream
		windowToSend := make([]T, len(itemsInWindow))
		copy(windowToSend, itemsInWindow)

		select {
		case out <- windowToSend:
			// Successfully sent window
		case <-ctx.Done():
			return ctx.Err() // Return error to signal cancellation during send
		}
	}
	return nil // Indicate success for this tick processing
}

// --- End Windowing Patterns ---
