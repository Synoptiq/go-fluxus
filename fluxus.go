package fluxus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrPipelineCancelled is returned when a pipeline is cancelled.
var ErrPipelineCancelled = errors.New("pipeline cancelled")

// Stage represents a processing stage in a pipeline.
// It takes an input of type I and produces an output of type O.
// The stage can also return an error if processing fails.
type Stage[I, O any] interface {
	Process(ctx context.Context, input I) (O, error)
}

// StageFunc is a function that implements the Stage interface.
type StageFunc[I, O any] func(ctx context.Context, input I) (O, error)

// Process implements the Stage interface for StageFunc.
func (f StageFunc[I, O]) Process(ctx context.Context, input I) (O, error) {
	return f(ctx, input)
}

// Chain combines two stages where the output of the first becomes input to the second.
// This allows creating pipelines with stages of different input and output types.
func Chain[A, B, C any](first Stage[A, B], second Stage[B, C]) Stage[A, C] {
	return StageFunc[A, C](func(ctx context.Context, input A) (C, error) {
		// Process through the first stage
		intermediate, err := first.Process(ctx, input)
		if err != nil {
			var zero C
			return zero, err
		}

		// Process through the second stage
		return second.Process(ctx, intermediate)
	})
}

// ChainMany combines multiple stages into a single stage.
// The output type of each stage must match the input type of the next stage.
func ChainMany[I, O any](stages ...interface{}) Stage[I, O] {
	if len(stages) == 0 {
		return StageFunc[I, O](func(_ context.Context, _ I) (O, error) {
			var zero O
			return zero, errors.New("cannot chain zero stages")
		})
	}

	// Validate the first stage
	firstStage, ok := stages[0].(Stage[I, any])
	if !ok {
		return StageFunc[I, O](func(_ context.Context, _ I) (O, error) {
			var zero O
			return zero, errors.New("first stage has incorrect input type")
		})
	}

	// For a single stage, check if it's already the right type
	if len(stages) == 1 {
		if directStage, okDirect := stages[0].(Stage[I, O]); okDirect {
			return directStage
		}
		return StageFunc[I, O](func(_ context.Context, _ I) (O, error) {
			var zero O
			return zero, errors.New("single stage has incorrect output type")
		})
	}

	// For multiple stages, we need to build a function that chains them all
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		var currentInput any = input

		for i, stage := range stages {
			// Skip type checking for the first stage as we already did it
			if i == 0 {
				result, err := firstStage.Process(ctx, input)
				if err != nil {
					var zero O
					return zero, fmt.Errorf("stage %d: %w", i, err)
				}
				currentInput = result
				continue
			}

			// For the last stage, it must have output type O
			if i == len(stages)-1 {
				lastStage, okLast := stage.(Stage[any, O])
				if !okLast {
					var zero O
					return zero, errors.New("last stage has incorrect output type")
				}

				return lastStage.Process(ctx, currentInput)
			}

			// For intermediate stages
			intermediateStage, okIntermediate := stage.(Stage[any, any])
			if !okIntermediate {
				var zero O
				return zero, fmt.Errorf("stage %d has incorrect type", i)
			}

			result, err := intermediateStage.Process(ctx, currentInput)
			if err != nil {
				var zero O
				return zero, fmt.Errorf("stage %d: %w", i, err)
			}

			currentInput = result
		}

		// This should never happen if the stages are validated correctly
		var zero O
		return zero, errors.New("unexpected error in ChainMany")
	})
}

// Pipeline represents a sequence of processing stages.
type Pipeline[I, O any] struct {
	stage      Stage[I, O]
	errHandler func(error) error
}

// NewPipeline creates a new pipeline with a single stage.
func NewPipeline[I, O any](stage Stage[I, O]) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		stage:      stage,
		errHandler: func(err error) error { return err },
	}
}

// WithErrorHandler adds a custom error handler to the pipeline.
func (p *Pipeline[I, O]) WithErrorHandler(handler func(error) error) *Pipeline[I, O] {
	p.errHandler = handler
	return p
}

// Process runs the pipeline on the given input.
func (p *Pipeline[I, O]) Process(ctx context.Context, input I) (O, error) {
	var zero O

	// Check for context cancellation
	if ctx.Err() != nil {
		return zero, p.errHandler(ctx.Err())
	}

	// Process through the stage
	result, err := p.stage.Process(ctx, input)
	if err != nil {
		return zero, p.errHandler(err)
	}

	return result, nil
}

// Fully optimized FanOut implementation

// FanOut represents a parallel processing step where the input is processed
// by multiple stages concurrently, and the outputs are collected.
type FanOut[I, O any] struct {
	stages      []Stage[I, O]
	errHandler  func(error) error
	concurrency int
}

// NewFanOut creates a new fan-out stage with the given stages.
func NewFanOut[I, O any](stages ...Stage[I, O]) *FanOut[I, O] {
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

// Optimized Buffer implementation to reduce allocations

// Buffer is a stage that buffers inputs and processes them in batches.
type Buffer[I, O any] struct {
	batchSize  int
	processor  func(ctx context.Context, batch []I) ([]O, error)
	errHandler func(error) error
}

// NewBuffer creates a new buffer stage.
func NewBuffer[I, O any](batchSize int, processor func(ctx context.Context, batch []I) ([]O, error)) *Buffer[I, O] {
	if batchSize <= 0 {
		batchSize = 1 // Default to processing items one by one
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

// Retry is a stage that retries the given stage a specified number of times
// if it fails, with an optional backoff strategy.
type Retry[I, O any] struct {
	stage       Stage[I, O]
	maxAttempts int
	shouldRetry func(error) bool
	backoff     func(attempt int) int // Returns delay in milliseconds
	errHandler  func(error) error
}

// NewRetry creates a new retry stage.
func NewRetry[I, O any](stage Stage[I, O], maxAttempts int) *Retry[I, O] {
	return &Retry[I, O]{
		stage:       stage,
		maxAttempts: maxAttempts,
		shouldRetry: func(_ error) bool { return true }, // Retry all errors by default
		backoff:     func(_ int) int { return 0 },       // No backoff by default
		errHandler:  func(err error) error { return err },
	}
}

// WithShouldRetry adds a predicate to determine if an error should be retried.
func (r *Retry[I, O]) WithShouldRetry(shouldRetry func(error) bool) *Retry[I, O] {
	r.shouldRetry = shouldRetry
	return r
}

// WithBackoff adds a backoff strategy to the retry stage.
func (r *Retry[I, O]) WithBackoff(backoff func(attempt int) int) *Retry[I, O] {
	r.backoff = backoff
	return r
}

// WithErrorHandler adds a custom error handler to the retry stage.
func (r *Retry[I, O]) WithErrorHandler(handler func(error) error) *Retry[I, O] {
	r.errHandler = handler
	return r
}

// Process implements the Stage interface for Retry.
func (r *Retry[I, O]) Process(ctx context.Context, input I) (O, error) {
	var result O
	var lastErr error

	for attempt := 0; attempt < r.maxAttempts; attempt++ {
		// Check for context cancellation
		if ctx.Err() != nil {
			if lastErr != nil {
				return result, r.errHandler(fmt.Errorf("retry failed after %d attempts: %w (context cancelled: %w)",
					attempt, lastErr, ctx.Err()))
			}
			return result, r.errHandler(ctx.Err())
		}

		// Try to process
		var err error
		result, err = r.stage.Process(ctx, input)
		if err == nil {
			return result, nil // Success
		}

		// Handle error
		lastErr = err
		if !r.shouldRetry(err) {
			return result, r.errHandler(fmt.Errorf("retry giving up after %d attempts: %w",
				attempt+1, err))
		}

		// Apply backoff if this is not the last attempt
		if attempt < r.maxAttempts-1 {
			backoffMs := r.backoff(attempt)
			if backoffMs > 0 {
				select {
				case <-ctx.Done():
					return result, r.errHandler(fmt.Errorf("retry interrupted during backoff: %w", ctx.Err()))
				case <-time.After(time.Duration(backoffMs) * time.Millisecond):
					// Continue after backoff
				}
			}
		}
	}

	return result, r.errHandler(fmt.Errorf("retry exhausted %d attempts: %w", r.maxAttempts, lastErr))
}

// Timeout adds a timeout to a stage.
type Timeout[I, O any] struct {
	stage      Stage[I, O]
	timeout    time.Duration
	errHandler func(error) error
}

// NewTimeout creates a new timeout stage.
func NewTimeout[I, O any](stage Stage[I, O], timeout time.Duration) *Timeout[I, O] {
	return &Timeout[I, O]{
		stage:      stage,
		timeout:    timeout,
		errHandler: func(err error) error { return err },
	}
}

// WithErrorHandler adds a custom error handler to the timeout stage.
func (t *Timeout[I, O]) WithErrorHandler(handler func(error) error) *Timeout[I, O] {
	t.errHandler = handler
	return t
}

// Process implements the Stage interface for Timeout.
func (t *Timeout[I, O]) Process(ctx context.Context, input I) (O, error) {
	var result O

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	// Process with timeout
	var err error
	result, err = t.stage.Process(ctx, input)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return result, t.errHandler(fmt.Errorf("stage timed out after %v: %w", t.timeout, err))
		}
		return result, t.errHandler(err)
	}

	return result, nil
}
