package fluxus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// Starter defines an optional interface for stages requiring initialization before processing begins.
// This is useful for stages that need to acquire resources (like connections) or start background tasks.
type Starter interface {
	// Start initializes the stage. It may block until initialization is complete.
	// The context can be used for cancellation or timeouts during startup.
	Start(ctx context.Context) error
}

// Stopper defines an optional interface for stages requiring graceful shutdown.
// This is useful for stages that need to release resources, flush buffers, or stop background tasks.
type Stopper interface {
	// Stop signals the stage to shut down gracefully. It may block until shutdown is complete.
	// The context can be used to enforce a timeout or cancellation for the shutdown process itself.
	Stop(ctx context.Context) error
}

// ErrorHandlingStrategy defines how item-level errors are handled in a stream adapter.
type ErrorHandlingStrategy int

const (
	// SkipOnError logs the error (if logger is provided) and continues processing the next item.
	// This is the default strategy.
	SkipOnError ErrorHandlingStrategy = iota
	// StopOnError logs the error and returns it, causing the ProcessStream method to terminate,
	// effectively stopping this stage and potentially the entire pipeline.
	StopOnError
	// SendToErrorChannel sends the item and the processing error to a dedicated error channel.
	// Processing continues with the next item unless the error channel blocks indefinitely or the context is cancelled.
	SendToErrorChannel
)

// ErrPipelineCancelled is returned when a pipeline is cancelled.
var ErrPipelineCancelled = errors.New("pipeline cancelled")

// ProcessingError bundles an item with the error encountered during its processing.
// Used with the SendToErrorChannel strategy.
type ProcessingError[I any] struct {
	Item  I
	Error error
}

// Stage represents a processing stage in a pipeline.
// It takes an input of type I and produces an output of type O.
// The stage can also return an error if processing fails.
type Stage[I, O any] interface {
	Process(ctx context.Context, input I) (O, error)
}

// StreamStage defines the interface for a continuous stream processing stage.
type StreamStage[I, O any] interface {
	// ProcessStream reads items from 'in', processes them, and sends results to 'out'.
	// It should run until 'in' is closed or 'ctx' is cancelled.
	//
	// Implementations MUST ensure 'out' is closed before returning,
	// typically using `defer close(out)`, to signal downstream stages that no more
	// data will be sent.
	//
	// Returning a non-nil error indicates a fatal error for this stage,
	// which should typically lead to the pipeline shutting down. Item-level
	// processing errors should be handled within the stage (e.g., logged, skipped,
	// sent to a separate error channel).
	ProcessStream(ctx context.Context, in <-chan I, out chan<- O) error
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

// StreamAdapterOption is a function type used to configure a StreamAdapter.
type StreamAdapterOption[I, O any] func(*StreamAdapter[I, O])

// WithoutAdapterMetrics is a StreamAdapterOption that explicitly disables
// StageWorker* metrics for the adapter instance it's applied to by setting
// its collector to the NoopMetricsCollector.
func WithoutAdapterMetrics[I, O any]() StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		sa.metricsCollector = DefaultMetricsCollector // DefaultMetricsCollector is the Noop one
	}
}

// WithAdapterMetrics sets the metrics collector and stage name for adapter-specific metrics.
// This is typically called internally by the StreamPipeline builder.
func WithAdapterMetrics[I, O any](collector MetricsCollector) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		if collector != nil {
			sa.metricsCollector = collector
		} else {
			sa.metricsCollector = DefaultMetricsCollector // Use default if nil
		}
	}
}

// WithAdapterName sets the name for the StreamAdapter stage.
// This is typically used for metrics collection and logging purposes.
func WithAdapterName[I, O any](name string) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		// Allow empty name, though pipeline usually provides one
		sa.adapterName = name
	}
}

// WithAdapterConcurrency sets the number of concurrent workers for the StreamAdapter.
//   - If n > 0, up to 'n' items will be processed concurrently. Output order is not guaranteed.
//   - If n == 0, it defaults to runtime.NumCPU().
//   - If n <= 0, processing will be sequential (concurrency = 1).
//
// Default is 1 (sequential).
func WithAdapterConcurrency[I, O any](n int) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		switch {
		case n == 0:
			// If n is 0, use the number of CPUs.
			sa.concurrency = runtime.NumCPU()
		case n < 0:
			// If n is negative, force sequential processing (concurrency 1).
			sa.concurrency = 1
		default: // n > 0
			// If n is positive, use the specified value.
			sa.concurrency = n
		}
	}
}

// WithAdapterErrorStrategy sets the error handling strategy for item-level errors
// encountered when calling the wrapped stage's Process method.
// Default is SkipOnError.
func WithAdapterErrorStrategy[I, O any](strategy ErrorHandlingStrategy) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		sa.errStrategy = strategy
	}
}

// WithAdapterLogger sets a custom logger for the StreamAdapter to use for logging
// skipped items or other internal warnings/errors.
// If nil, logging defaults to a logger that discards output.
// WithAdapterLogger creates an option to set the logger for a StreamAdapter.
func WithAdapterLogger[I, O any](logger *log.Logger) StreamAdapterOption[I, O] {
	return func(adapter *StreamAdapter[I, O]) { // Assuming StreamAdapterOption is func(*StreamAdapter[I, O])
		if logger == nil {
			adapter.logger = log.New(io.Discard, "", 0) // Default to discard if nil
		} else {
			adapter.logger = logger
		}
		// Ensure StreamAdapter has a 'logger *log.Logger' field
	}
}

// WithAdapterErrorChannel provides a channel for sending processing errors when
// the strategy is SendToErrorChannel. The channel must be created and managed
// (e.g., read from) by the caller. The StreamAdapter will block trying to send
// to this channel if it's full, respecting backpressure, but will stop trying
// if the main context is cancelled.
// This option is mandatory if using SendToErrorChannel strategy.
func WithAdapterErrorChannel[I, O any](errChan chan<- ProcessingError[I]) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		sa.errChan = errChan
	}
}

// WithAdapterBufferSize sets the buffer size for internal channels used by the StreamAdapter.
// A larger buffer can improve throughput by reducing blocking, especially when processing rates
// vary between stages. If size <= 0, channels will be unbuffered.
func WithAdapterBufferSize[I, O any](size int) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		if size < 0 {
			size = 0
		}
		sa.bufferSize = size
	}
}

// WithAdapterTracerProvider sets the TracerProvider for the adapter.
// Defaults to DefaultTracerProvider.
func WithAdapterTracerProvider[I, O any](provider TracerProvider) StreamAdapterOption[I, O] { // <-- Add
	return func(sa *StreamAdapter[I, O]) {
		if provider != nil {
			sa.tracerProvider = provider
		} else {
			sa.tracerProvider = DefaultTracerProvider // Ensure it's never nil
		}
	}
}

// StreamAdapter wraps a Stage to make it usable as a StreamStage, adding concurrency and error handling.
type StreamAdapter[I, O any] struct {
	wrappedStage     Stage[I, O]
	concurrency      int
	errStrategy      ErrorHandlingStrategy
	logger           *log.Logger
	errChan          chan<- ProcessingError[I] // Channel for SendToErrorChannel strategy
	bufferSize       int
	metricsCollector MetricsCollector // <<< Add Metrics Collector
	adapterName      string           // <<< Add Stage Name
	tracerProvider   TracerProvider   // <-- Add
	tracer           trace.Tracer     // <-- Add
}

// NewStreamAdapter creates a StreamStage adapter for a given Stage with optional configuration.
func NewStreamAdapter[I, O any](stage Stage[I, O], options ...StreamAdapterOption[I, O]) *StreamAdapter[I, O] {
	if stage == nil {
		panic("fluxus.NewStreamAdapter: wrappedStage cannot be nil")
	}

	// Set defaults
	sa := &StreamAdapter[I, O]{
		wrappedStage:     stage,
		concurrency:      1,                        // Default to sequential
		errStrategy:      SkipOnError,              // Default to skipping errors
		logger:           nil,                      // Default to nil, will be replaced by discard logger if needed
		errChan:          nil,                      // Default to nil
		bufferSize:       0,                        // Default to unbuffered channels
		metricsCollector: DefaultMetricsCollector,  // <<< Initialize collector
		adapterName:      "unnamed_stream_adapter", // <<< Initialize name
	}

	// Apply options
	for _, option := range options {
		option(sa)
	}

	// Set discard logger if none was provided
	if sa.logger == nil {
		sa.logger = log.New(io.Discard, "", 0)
	}

	// Ensure metrics collector is not nil
	if sa.metricsCollector == nil {
		sa.metricsCollector = DefaultMetricsCollector
	}

	// Ensure tracer provider is not nil
	if sa.tracerProvider == nil { // <-- Add check
		sa.tracerProvider = DefaultTracerProvider
	}

	// Create the tracer instance using the provider and adapter name
	sa.tracer = sa.tracerProvider.Tracer(fmt.Sprintf("fluxus/adapter/%s", sa.adapterName))

	// Validate configuration
	if sa.errStrategy == SendToErrorChannel && sa.errChan == nil {
		panic("fluxus.NewStreamAdapter: WithErrorChannel must be provided when using SendToErrorChannel strategy")
	}

	// --- Metrics: Report Concurrency ---
	// Report concurrency after all options are applied
	sa.metricsCollector.StageWorkerConcurrency(
		context.Background(),
		sa.adapterName,
		sa.concurrency,
	) // Use background context as this isn't per-item

	return sa
}

// ProcessStream implements the StreamStage interface for the adapter.
// It handles both sequential and concurrent processing based on the concurrency setting.
func (a *StreamAdapter[I, O]) ProcessStream(ctx context.Context, in <-chan I, out chan<- O) error {
	// CRITICAL: Ensure the output channel is closed when this function returns,
	// regardless of whether it's a normal exit or an error.
	defer close(out)

	if a.concurrency <= 1 {
		// Use sequential processing for concurrency 1 or less
		return a.processSequential(ctx, in, out)
	}

	// Use concurrent processing for concurrency > 1
	return a.processConcurrent(ctx, in, out)
}

// processSequential handles processing items one by one.
func (a *StreamAdapter[I, O]) processSequential(ctx context.Context, in <-chan I, out chan<- O) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Context cancelled

		case item, ok := <-in:
			if !ok {
				return nil // Input channel closed, normal exit
			}

			// --- Start Tracing Span ---
			itemCtx, itemSpan := a.tracer.Start(
				ctx,
				fmt.Sprintf("%s.process", a.adapterName), // Span name: adapterName.process
				trace.WithAttributes(
					attribute.String("fluxus.adapter.name", a.adapterName),
					attribute.Int("fluxus.adapter.concurrency", 1), // Explicitly 1 for sequential
					attribute.String("fluxus.adapter.error_strategy", a.errStrategy.String()),
					// Add item-specific attributes here if possible/relevant
				),
				// Link to parent span from ctx automatically
			)
			// ---

			// Process the item
			itemStartTime := time.Now()
			result, err := a.wrappedStage.Process(ctx, item)
			itemDuration := time.Since(itemStartTime)

			if err != nil {
				// --- Record error and set status in span ---
				itemSpan.RecordError(err)
				itemSpan.SetStatus(codes.Error, err.Error())
				// ---

				// Handle error based on strategy
				_, processErr := a.handleItemError(ctx, item, err)
				itemSpan.End() // End the span after processing the item
				if processErr != nil {
					// StopOnError strategy returned an error
					return processErr
				}
				// SkipOnError or SendToErrorChannel handled, continue loop
				continue
			}

			// --- Set success status and duration attribute ---
			itemSpan.SetStatus(codes.Ok, "")
			itemSpan.SetAttributes(attribute.Int64("fluxus.adapter.stage.duration_ms", itemDuration.Milliseconds()))
			// ---

			// --- Metrics: Report successful processing ---
			a.metricsCollector.StageWorkerItemProcessed(ctx, a.adapterName, itemDuration)
			// --- End Metrics ---

			// Send successful result downstream, checking for cancellation
			select {
			case out <- result:
			// Successfully sent
			case <-itemCtx.Done(): // Use itemCtx
				itemSpan.SetAttributes(attribute.Bool("fluxus.adapter.cancelled_on_send", true))
				itemSpan.SetStatus(codes.Error, "cancelled while sending output")
				itemSpan.End() // End span before returning error
				return itemCtx.Err()
			}
			itemSpan.End() // --- End Tracing Span ---
		}
	}
}

// processConcurrent handles processing items using multiple goroutines.
func (a *StreamAdapter[I, O]) processConcurrent(ctx context.Context, in <-chan I, out chan<- O) error {
	g, gctx := errgroup.WithContext(ctx)
	jobs := make(chan I, a.bufferSize) // Consider adding a small buffer if needed, e.g., make(chan I, a.concurrency)

	// Start dispatcher goroutine
	g.Go(func() error {
		defer close(jobs) // Ensure jobs channel is closed when dispatcher finishes
		return a.dispatchJobs(gctx, in, jobs)
	})

	// Start worker goroutines
	for i := 0; i < a.concurrency; i++ {
		g.Go(func() error {
			return a.runWorker(gctx, jobs, out)
		})
	}

	// Wait for all goroutines (dispatcher and workers)
	return g.Wait()
}

// dispatchJobs reads from the input channel and sends items to the jobs channel.
func (a *StreamAdapter[I, O]) dispatchJobs(gctx context.Context, in <-chan I, jobs chan<- I) error {
	for {
		select {
		case <-gctx.Done(): // Check for cancellation first
			return gctx.Err()
		case item, ok := <-in:
			if !ok {
				return nil // Input channel closed, normal exit for dispatcher
			}
			// Send item to workers, checking for cancellation during send
			select {
			case jobs <- item:
				// Item sent successfully
			case <-gctx.Done():
				return gctx.Err() // Cancelled while waiting to send job
			}
		}
	}
}

// runWorker reads items from the jobs channel, processes them, and sends results/handles errors.
func (a *StreamAdapter[I, O]) runWorker(gctx context.Context, jobs <-chan I, out chan<- O) error {
	for item := range jobs { // Loop until 'jobs' channel is closed and empty
		// --- Start Tracing Span ---
		// Use gctx as parent, but create a new context for the item processing span
		itemCtx, itemSpan := a.tracer.Start(
			gctx,
			fmt.Sprintf("%s.process", a.adapterName), // Span name: adapterName.process
			trace.WithAttributes(
				attribute.String("fluxus.adapter.name", a.adapterName),
				attribute.Int("fluxus.adapter.concurrency", a.concurrency),
				attribute.String("fluxus.adapter.error_strategy", a.errStrategy.String()),
				// Add item-specific attributes here if possible/relevant
			),
		)
		// ---

		// --- Metrics: Track item processing time ---
		itemStartTime := time.Now()
		result, err := a.wrappedStage.Process(gctx, item)
		itemDuration := time.Since(itemStartTime)
		// --- End Metrics ---

		if err != nil {
			// --- Record error and set status in span ---
			itemSpan.RecordError(err)
			itemSpan.SetStatus(codes.Error, err.Error())
			// ---

			// Handle error based on strategy
			_, processErr := a.handleItemError(gctx, item, err)
			itemSpan.End()
			if processErr != nil {
				// StopOnError strategy: return the error to errgroup
				return processErr // This cancels gctx for others
			}
			// SkipOnError or SendToErrorChannel handled, continue loop
			continue
		}

		// --- Set success status and duration attribute ---
		itemSpan.SetStatus(codes.Ok, "")
		itemSpan.SetAttributes(attribute.Int64("fluxus.adapter.stage.duration_ms", itemDuration.Milliseconds()))
		// ---

		// --- Metrics: Report successful processing ---
		a.metricsCollector.StageWorkerItemProcessed(gctx, a.adapterName, itemDuration)
		// --- End Metrics ---

		// Send successful result downstream, checking for cancellation
		select {
		case out <- result:
		// Successfully sent
		case <-itemCtx.Done(): // Use itemCtx
			itemSpan.SetAttributes(attribute.Bool("fluxus.adapter.cancelled_on_send", true))
			itemSpan.SetStatus(codes.Error, "cancelled while sending output")
			itemSpan.End() // End span before returning error
			return itemCtx.Err()
		}
		itemSpan.End() // --- End Tracing Span ---
	}
	return nil // Worker finished normally after jobs channel closed
}

// handleItemError centralizes the logic for dealing with errors from wrappedStage.Process.
// It returns whether processing should continue for the current item (relevant for concurrent)
// and any fatal error (only for StopOnError strategy).
func (a *StreamAdapter[I, O]) handleItemError(ctx context.Context, item I, err error) (bool, error) {
	switch a.errStrategy {
	case StopOnError:
		// Log and return the error to stop the stage/pipeline
		a.logf("ERROR: fluxus.StreamAdapter stopping due to error: %v", err)
		return false, err // Signal to stop and return the error

	case SendToErrorChannel:
		// Send the item and error to the configured error channel
		processingErr := ProcessingError[I]{Item: item, Error: err}
		select {
		case a.errChan <- processingErr:
			// --- Metrics: Report error sent ---
			a.metricsCollector.StageWorkerErrorSent(ctx, a.adapterName, err)
			// --- End Metrics ---
			// Successfully sent error
			a.logf("DEBUG: fluxus.StreamAdapter sent item error to error channel: %v", err)
		case <-ctx.Done():
			// Context cancelled while trying to send error
			a.logf("WARN: fluxus.StreamAdapter context cancelled while sending item error: %v", err)
			// Depending on requirements, you might want to return ctx.Err() here
			// but typically SendToErrorChannel implies continuing if possible.
		}
		return true, nil // Signal to continue processing next item

	case SkipOnError:
		fallthrough // Fallthrough to default skip behavior
	default: // SkipOnError is the default
		// --- Metrics: Report item skipped ---
		a.metricsCollector.StageWorkerItemSkipped(ctx, a.adapterName, err)
		// --- End Metrics ---
		// Log and continue to the next item
		a.logf("WARN: fluxus.StreamAdapter skipping item due to error: %v", err)
		return true, nil // Signal to continue processing next item
	}
}

// logf is a helper to log using the configured logger.
func (a *StreamAdapter[I, O]) logf(format string, v ...interface{}) {
	// Check logger again in case it was initially nil and replaced by discard logger
	if a.logger != nil {
		a.logger.Printf(format, v...)
	}
}

// String representation for ErrorHandlingStrategy (useful for attributes)
func (e ErrorHandlingStrategy) String() string {
	switch e {
	case SkipOnError:
		return "SkipOnError"
	case StopOnError:
		return "StopOnError"
	case SendToErrorChannel:
		return "SendToErrorChannel"
	default:
		return fmt.Sprintf("Unknown(%d)", int(e))
	}
}
