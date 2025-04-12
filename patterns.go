package fluxus

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// Map applies a given stage to each element of an input slice concurrently.
type Map[I, O any] struct {
	stage         Stage[I, O]
	concurrency   int
	collectErrors bool // Option to collect all errors
	errHandler    func(error) error
}

// NewMap creates a new Map stage.
func NewMap[I, O any](stage Stage[I, O]) *Map[I, O] {
	return &Map[I, O]{
		stage:         stage,
		concurrency:   runtime.NumCPU(), // Default concurrency
		collectErrors: false,
		errHandler:    func(err error) error { return err },
	}
}

// WithConcurrency sets the concurrency limit.
func (m *Map[I, O]) WithConcurrency(n int) *Map[I, O] {
	if n < 1 {
		n = 1
	}
	m.concurrency = n
	return m
}

// WithCollectErrors configures error collection behavior.
func (m *Map[I, O]) WithCollectErrors(collect bool) *Map[I, O] {
	m.collectErrors = collect
	return m
}

// WithErrorHandler adds a custom error handler.
func (m *Map[I, O]) WithErrorHandler(handler func(error) error) *Map[I, O] {
	if handler == nil {
		handler = func(err error) error { return err }
	}
	m.errHandler = handler
	return m
}

// handleMapItemError encapsulates the logic for handling errors during item processing.
func (m *Map[I, O]) handleMapItemError(
	index int,
	err error,
	results []O,
	errs []error,
	firstErr *atomic.Value,
	cancel context.CancelFunc,
) {
	wrappedErr := NewMapItemError(index, err) // Use the specific error type

	if m.collectErrors {
		// Need to check if errs is nil here too
		if errs != nil {
			errs[index] = wrappedErr
		}
		// Store zero value for output on error when collecting
		var zero O
		results[index] = zero
	} else if firstErr.CompareAndSwap(nil, wrappedErr) {
		// Fail fast: store the first error and cancel the context
		cancel() // Cancel the context for other goroutines
	}
}

// processMapItem handles the processing of a single item within the Map stage.
// It's called concurrently by goroutines managed in the main Process method.
func (m *Map[I, O]) processMapItem(
	mapCtx context.Context,
	index int,
	inputItem I,
	results []O,
	errs []error,
	firstErr *atomic.Value,
	cancel context.CancelFunc,
) {
	// Check for cancellation before actual processing, in case it was cancelled
	// while this goroutine was waiting to run.
	select {
	case <-mapCtx.Done():
		// If collecting errors, record context cancellation as the error
		if m.collectErrors {
			errs[index] = fmt.Errorf("map cancelled for index %d: %w", index, mapCtx.Err())
		}
		// If not collecting errors, the cancellation will be handled after wg.Wait()
		return
	default:
		// Process the item using the provided stage
		output, err := m.stage.Process(mapCtx, inputItem)
		if err != nil {
			m.handleMapItemError(index, err, results, errs, firstErr, cancel)
		} else {
			// Store successful result
			results[index] = output
		}
	}
}

// Process implements the Stage interface for Map.
// It manages the concurrent execution of processMapItem for each input.
func (m *Map[I, O]) Process(ctx context.Context, inputs []I) ([]O, error) {
	inputLen := len(inputs)
	if inputLen == 0 {
		return []O{}, nil
	}

	results := make([]O, inputLen)
	var errs []error // Declare errs, but don't allocate yet
	if m.collectErrors {
		errs = make([]error, inputLen) // Allocate only if needed
	}

	concurrency := m.concurrency
	if concurrency > inputLen {
		concurrency = inputLen
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var firstErr atomic.Value // Stores the first error in fail-fast mode

	// Create a cancellable context specific to this map operation
	mapCtx, cancel := context.WithCancel(ctx)
	defer cancel() // Ensure cancellation propagates if an error occurs

	// Launch goroutines to process items concurrently
	for i := 0; i < inputLen; i++ {
		// Check for cancellation before dispatching new work
		// This is important for fail-fast behavior
		select {
		case <-mapCtx.Done():
			goto finish // Context cancelled (likely due to an error or external cancellation)
		default:
			// Continue dispatching
		}

		// Acquire semaphore slot
		select {
		case sem <- struct{}{}:
			// Acquired slot
		case <-mapCtx.Done():
			// Context cancelled while waiting for semaphore
			goto finish
		}

		wg.Add(1)
		go func(index int, inputItem I) {
			defer func() {
				<-sem // Release semaphore slot
				wg.Done()
			}()

			// Call the helper function to process the item
			m.processMapItem(mapCtx, index, inputItem, results, errs, &firstErr, cancel)
		}(i, inputs[i])
	}

finish:
	wg.Wait() // Wait for all dispatched goroutines to complete or acknowledge cancellation

	// --- Error Handling and Result Aggregation ---

	// Check for fail-fast error first
	if errVal := firstErr.Load(); errVal != nil {
		// An error occurred and we were in fail-fast mode
		//nolint:errcheck // False positive: Error is intentionally returned directly after handling.
		return nil, m.errHandler(errVal.(error))
	}

	// If collecting errors, aggregate them
	if m.collectErrors {
		multiErr := NewMultiError(errs) // Assumes NewMultiError filters out nil errors
		if multiErr != nil {
			// Return results along with the MultiError
			return results, m.errHandler(multiErr)
		}
	}

	// If the context was cancelled externally (and not due to a fail-fast error)
	// Check mapCtx specifically, as the original ctx might not show the cancellation yet.
	if mapCtx.Err() != nil && !errors.Is(mapCtx.Err(), context.Canceled) {
		// Return the cancellation error if no other specific error was returned
		return nil, m.errHandler(fmt.Errorf("map stage cancelled: %w", mapCtx.Err()))
	}

	// No errors (or errors were collected and there were none)
	return results, nil
}

// Buffer is a stage that buffers inputs and processes them in batches.
type Buffer[I, O any] struct {
	batchSize  int
	processor  func(ctx context.Context, batch []I) ([]O, error)
	errHandler func(error) error
}

// NewBuffer creates a new buffer stage.
func NewBuffer[I, O any](batchSize int, processor func(ctx context.Context, batch []I) ([]O, error)) *Buffer[I, O] {
	if batchSize <= 0 {
		// Panic to indicate a programmer error during setup.
		// This makes configuration errors explicit and prevents silent fallback behavior.
		panic(fmt.Sprintf("fluxus.NewBuffer: batchSize must be positive, got %d", batchSize))
	}
	return &Buffer[I, O]{
		batchSize:  batchSize,
		processor:  processor,
		errHandler: func(err error) error { return err },
	}
}

// WithErrorHandler adds a custom error handler to the buffer stage.
func (b *Buffer[I, O]) WithErrorHandler(handler func(error) error) *Buffer[I, O] {
	b.errHandler = handler
	return b
}

// Process implements the Stage interface for Buffer.
func (b *Buffer[I, O]) Process(ctx context.Context, inputs []I) ([]O, error) {
	inputLen := len(inputs)
	if inputLen == 0 {
		return []O{}, nil
	}

	// Pre-allocate the result slice with the expected capacity
	// This is a key optimization to avoid repeated slice growth
	estimatedOutputSize := (inputLen + b.batchSize - 1) / b.batchSize * b.batchSize
	allResults := make([]O, 0, estimatedOutputSize)

	// Process inputs in batches
	for i := 0; i < inputLen; i += b.batchSize {
		// Check for context cancellation
		if ctx.Err() != nil {
			return allResults, b.errHandler(ctx.Err())
		}

		// Get the current batch (reuse slice to avoid allocation)
		end := i + b.batchSize
		if end > inputLen {
			end = inputLen
		}

		// Avoid slice allocation by using the original slice
		batch := inputs[i:end]

		// Process the batch
		results, err := b.processor(ctx, batch)
		if err != nil {
			return allResults, b.errHandler(fmt.Errorf("batch processing error at offset %d: %w", i, err))
		}

		// Append batch results to main results slice
		allResults = append(allResults, results...)
	}

	return allResults, nil
}

// FanIn represents a stage that collects results from multiple parallel processing
// stages and combines them into a single output.
type FanIn[I, O any] struct {
	aggregator func([]I) (O, error)
	errHandler func(error) error
}

// NewFanIn creates a new fan-in stage with the given aggregator function.
func NewFanIn[I, O any](aggregator func([]I) (O, error)) *FanIn[I, O] {
	return &FanIn[I, O]{
		aggregator: aggregator,
		errHandler: func(err error) error { return err },
	}
}

// WithErrorHandler adds a custom error handler to the fan-in stage.
func (f *FanIn[I, O]) WithErrorHandler(handler func(error) error) *FanIn[I, O] {
	f.errHandler = handler
	return f
}

// Process implements the Stage interface for FanIn.
func (f *FanIn[I, O]) Process(ctx context.Context, inputs []I) (O, error) {
	var result O

	// Check for context cancellation
	if ctx.Err() != nil {
		return result, f.errHandler(ctx.Err())
	}

	// Aggregate results
	var err error
	result, err = f.aggregator(inputs)
	if err != nil {
		return result, f.errHandler(fmt.Errorf("fan-in aggregation: %w", err))
	}

	return result, nil
}

// FanOut represents a parallel processing step where the input is processed
// by multiple stages concurrently, and the outputs are collected.
type FanOut[I, O any] struct {
	stages      []Stage[I, O]
	errHandler  func(error) error
	concurrency int
}

// NewFanOut creates a new fan-out stage with the given stages.
func NewFanOut[I, O any](stages ...Stage[I, O]) *FanOut[I, O] {
	if len(stages) == 0 {
		// Panic to indicate a programmer error during setup.
		// This makes configuration errors explicit and prevents silent fallback behavior.
		panic("fluxus.NewFanOut: at least one stage is required")
	}

	return &FanOut[I, O]{
		stages:      stages,
		errHandler:  func(err error) error { return err },
		concurrency: 0, // 0 means no limit
	}
}

// WithErrorHandler adds a custom error handler to the fan-out stage.
func (f *FanOut[I, O]) WithErrorHandler(handler func(error) error) *FanOut[I, O] {
	f.errHandler = handler
	return f
}

// WithConcurrency limits the number of concurrent operations.
// A value of 0 means no limit (all stages run concurrently).
func (f *FanOut[I, O]) WithConcurrency(n int) *FanOut[I, O] {
	f.concurrency = n
	return f
}

// Process implements the Stage interface for FanOut.
func (f *FanOut[I, O]) Process(ctx context.Context, input I) ([]O, error) {
	if err := ctx.Err(); err != nil {
		// Return immediately if the context is already cancelled.
		return nil, err
	}

	numStages := len(f.stages)
	if numStages == 0 {
		return []O{}, nil
	}

	// Fast path for single stage (avoid goroutine overhead)
	if numStages == 1 {
		result, err := f.stages[0].Process(ctx, input)
		if err != nil {
			return nil, f.errHandler(fmt.Errorf("fan-out stage 0: %w", err))
		}
		return []O{result}, nil
	}

	// Pre-allocate results slice
	results := make([]O, numStages)

	// Determine actual concurrency
	concurrency := f.concurrency
	if concurrency <= 0 || concurrency > numStages {
		concurrency = numStages
	}

	// Use worker pool pattern
	return f.processWithWorkerPool(ctx, input, results, concurrency)
}

// processWithWorkerPool processes stages using a worker pool pattern
func (f *FanOut[I, O]) processWithWorkerPool(ctx context.Context, input I, results []O, concurrency int) ([]O, error) {
	// Create cancelable context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create worker pool
	var wg sync.WaitGroup
	wg.Add(concurrency)

	// Task channel - buffer to avoid blocking
	tasks := make(chan int, len(f.stages))

	// Error channel - buffer for one error is enough
	errChan := make(chan error, 1)

	// Start worker goroutines
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()

			for i := range tasks {
				select {
				case <-ctx.Done():
					// Context cancelled, stop processing
					return
				default:
					// Process the stage
					result, err := f.stages[i].Process(ctx, input)
					if err != nil {
						// Send error and cancel context
						select {
						case errChan <- fmt.Errorf("fan-out stage %d: %w", i, err):
							// Error sent
						default:
							// Channel full, another error already reported
						}
						cancel()
						return
					}

					// Store result
					results[i] = result
				}
			}
		}()
	}

	// Send all task indices to the workers
	for i := range f.stages {
		tasks <- i
	}
	close(tasks)

	// Wait for all workers to finish
	wg.Wait()

	// Check if there was an error
	select {
	case err := <-errChan:
		return nil, f.errHandler(err)
	default:
		// No error
		return results, nil
	}
}

// Parallel creates a pipeline that fans out to the given stages and then fans in.
func Parallel[I, M, O any](
	fanOutStages []Stage[I, M],
	aggregator func([]M) (O, error),
) Stage[I, O] {
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		// Fan out
		fanOut := NewFanOut(fanOutStages...)
		results, err := fanOut.Process(ctx, input)
		if err != nil {
			var zero O
			return zero, err
		}

		// Fan in
		fanIn := NewFanIn(aggregator)
		return fanIn.Process(ctx, results)
	})
}

// KeyValue represents a key-value pair used as intermediate data in MapReduce.
// K must be comparable to be used as a map key during the shuffle phase.
type KeyValue[K comparable, V any] struct {
	Key   K
	Value V
}

// ReduceInput encapsulates the data passed to a ReducerFunc: a key and all its associated values.
type ReduceInput[K comparable, V any] struct {
	Key    K
	Values []V
}

// MapperFunc defines the function signature for the Map phase.
// It processes a single input element (I) and produces an intermediate KeyValue pair (K, V).
type MapperFunc[I any, K comparable, V any] func(ctx context.Context, input I) (KeyValue[K, V], error)

// ReducerFunc defines the function signature for the Reduce phase.
// It processes a key (K) and all its associated values ([]V) to produce a single result (R).
type ReducerFunc[K comparable, V any, R any] func(ctx context.Context, input ReduceInput[K, V]) (R, error)

// MapReduce implements the MapReduce pattern as a Stage.
// It takes a slice of input elements ([]I), applies a mapper to each, groups the results by key,
// applies a reducer to each group, and returns a slice of the reduced results ([]R).
type MapReduce[I any, K comparable, V any, R any] struct {
	mapper      MapperFunc[I, K, V]
	reducer     ReducerFunc[K, V, R]
	parallelism int // Degree of parallelism for the map phase (0 or 1 means sequential)
}

// NewMapReduce creates a new MapReduce stage with the provided mapper and reducer functions.
// By default, the map phase runs sequentially. Use WithParallelism to enable parallel mapping.
func NewMapReduce[I any, K comparable, V any, R any](
	mapper MapperFunc[I, K, V],
	reducer ReducerFunc[K, V, R],
) *MapReduce[I, K, V, R] {
	return &MapReduce[I, K, V, R]{
		mapper:      mapper,
		reducer:     reducer,
		parallelism: 1, // Default to sequential map phase
	}
}

// WithParallelism sets the degree of parallelism for the map phase.
// If n <= 1, the map phase will execute sequentially.
// Otherwise, up to 'n' mapper functions may run concurrently.
func (mr *MapReduce[I, K, V, R]) WithParallelism(n int) *MapReduce[I, K, V, R] {
	if n < 1 {
		n = 1
	}
	mr.parallelism = n
	return mr
}

// Process implements the Stage interface for MapReduce.
// It orchestrates the map, shuffle, and reduce phases.
func (mr *MapReduce[I, K, V, R]) Process(ctx context.Context, inputs []I) ([]R, error) {
	var zero []R // Zero value for error returns

	// --- Map Phase ---
	intermediateChan, mapErrChan := mr.mapPhase(ctx, inputs)

	// --- Shuffle/Group Phase ---
	// This runs concurrently with the map phase until intermediateChan is closed.
	grouped, shuffleErr := mr.shufflePhase(ctx, intermediateChan, mapErrChan)
	if shuffleErr != nil {
		return zero, shuffleErr // Error from map or context cancellation during shuffle
	}

	// --- Reduce Phase ---
	results, reduceErr := mr.reducePhase(ctx, grouped)
	if reduceErr != nil {
		return zero, reduceErr // Error from reduce or context cancellation during reduce
	}

	return results, nil
}

// mapPhase executes the mapping logic concurrently.
// It returns a channel for intermediate KeyValue pairs and a channel for the first error encountered.
func (mr *MapReduce[I, K, V, R]) mapPhase(ctx context.Context, inputs []I) (<-chan KeyValue[K, V], <-chan error) {
	intermediateChan := make(chan KeyValue[K, V], len(inputs))
	mapErrChan := make(chan error, 1)
	var mapWg sync.WaitGroup

	// Use a controlling context to signal early exit on error within this phase
	mapCtx, mapCancel := context.WithCancel(ctx)

	// Create a channel for inputs to distribute work among mappers
	inputChan := make(chan I, len(inputs))
	for _, input := range inputs {
		inputChan <- input
	}
	close(inputChan)

	mapWg.Add(mr.parallelism)
	for i := 0; i < mr.parallelism; i++ {
		go func() {
			defer mapWg.Done()
			for input := range inputChan {
				// Check for cancellation before processing
				select {
				case <-mapCtx.Done():
					return
				default:
					// Proceed with mapping
				}

				kv, err := mr.mapper(mapCtx, input)
				if err != nil {
					// Report the first error and cancel other mappers
					select {
					case mapErrChan <- err:
						mapCancel() // Signal other goroutines in this phase to stop
					case <-mapCtx.Done(): // Already cancelled
					}
					return // Stop this worker
				}

				// Send result, checking for cancellation
				select {
				case intermediateChan <- kv:
				case <-mapCtx.Done():
					return
				}
			}
		}()
	}

	// Goroutine to wait for mappers and close channels
	go func() {
		mapWg.Wait()
		close(intermediateChan)
		close(mapErrChan)
		mapCancel() // Cancel context once map phase is fully done (cleans up resources)
	}()

	return intermediateChan, mapErrChan
}

// shufflePhase collects intermediate results from the map phase and groups them by key.
// It also checks for errors from the map phase or context cancellation.
func (mr *MapReduce[I, K, V, R]) shufflePhase(ctx context.Context, intermediateChan <-chan KeyValue[K, V], mapErrChan <-chan error) (map[K][]V, error) {
	grouped := make(map[K][]V)
	for {
		select {
		case kv, ok := <-intermediateChan:
			if !ok {
				// Channel closed, shuffling is done. Now check for map errors.
				if err := <-mapErrChan; err != nil {
					return nil, fmt.Errorf("map phase failed: %w", err)
				}
				// Check parent context cancellation *after* ensuring map didn't error
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return grouped, nil // Success
			}
			grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
		case err := <-mapErrChan:
			// Map phase reported an error while we were shuffling. Drain intermediateChan.
			if err != nil {
				go func() {
					//nolint:revive // This is a known issue with the linter. We need to drain the channel.
					for range intermediateChan {
						// Drain remaining items
					}
				}()
				return nil, fmt.Errorf("map phase failed: %w", err)
			}
			// If err is nil, mapErrChan was closed, continue draining intermediateChan
		case <-ctx.Done():
			// Parent context cancelled during shuffle. Drain intermediateChan.
			go func() {
				//nolint:revive // This is a known issue with the linter. We need to drain the channel.
				for range intermediateChan {
					// Drain remaining items
				}
			}()
			return nil, ctx.Err()
		}
	}
}

// reducePhase executes the reduction logic concurrently for each key group.
func (mr *MapReduce[I, K, V, R]) reducePhase(ctx context.Context, grouped map[K][]V) ([]R, error) {
	if len(grouped) == 0 {
		return []R{}, nil // Nothing to reduce
	}

	results := make([]R, 0, len(grouped))
	reduceErrChan := make(chan error, 1)
	var reduceWg sync.WaitGroup
	var resultsMu sync.Mutex

	// Use a new context for the reduce phase based on the original context
	reduceCtx, reduceCancel := context.WithCancel(ctx)
	defer reduceCancel() // Ensure cancellation propagates

	reduceWg.Add(len(grouped))
	for key, values := range grouped {
		// Check for cancellation before launching goroutine
		select {
		case <-reduceCtx.Done():
			// Don't launch more goroutines if already cancelled
			reduceWg.Done() // Decrement counter for the skipped goroutine
			continue
		default:
			// Proceed to launch goroutine
		}

		go func(k K, v []V) {
			defer reduceWg.Done()
			reduceInput := ReduceInput[K, V]{Key: k, Values: v}

			// Check for cancellation again inside goroutine
			select {
			case <-reduceCtx.Done():
				return
			default:
				// Proceed with reduction
			}

			reducedVal, err := mr.reducer(reduceCtx, reduceInput)
			if err != nil {
				// Report first error and cancel other reducers
				select {
				case reduceErrChan <- err:
					reduceCancel()
				case <-reduceCtx.Done():
				}
				return // Stop this worker
			}

			// Add result safely
			resultsMu.Lock()
			// Check cancellation *again* before appending, as it might have happened
			// between the reducer call and acquiring the lock.
			if reduceCtx.Err() == nil {
				results = append(results, reducedVal)
			}
			resultsMu.Unlock()
		}(key, values)
	}

	// Goroutine to wait for reducers and close the error channel
	go func() {
		reduceWg.Wait()
		close(reduceErrChan)
	}()

	// Wait for the first error or completion
	if err := <-reduceErrChan; err != nil {
		return nil, fmt.Errorf("reduce phase failed: %w", err)
	}

	// Check parent context again after reduce phase is complete
	// Use reduceCtx as it reflects cancellations triggered within the phase
	if reduceCtx.Err() != nil && !errors.Is(reduceCtx.Err(), context.Canceled) {
		// Return error only if it's not the cancellation we triggered internally on error
		return nil, reduceCtx.Err()
	}
	// Check original context in case it was cancelled externally after reduce started
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return results, nil
}
