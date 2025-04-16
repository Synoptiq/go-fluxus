// Package fluxus provides tools for building robust, type-safe, and concurrent
// data processing pipelines in Go. It leverages generics for strong type checking
// during pipeline construction and execution.
//
// Core Concepts:
//   - Stage[I, O]: A single processing step transforming input I to output O.
//   - StreamStage[I, O]: A stage designed for continuous stream processing using channels.
//   - StreamAdapter[I, O]: Wraps a Stage to make it a StreamStage, adding concurrency,
//     error handling, metrics, and tracing.
//   - StreamPipelineBuilder: A type-safe builder for constructing multi-stage pipelines.
//   - StreamPipeline: The finalized, runnable pipeline object.
//   - Resilience Patterns: Built-in stages for Retry, Circuit Breaker, Timeout, etc.
//   - Observability: Integration points for metrics (MetricsCollector) and tracing (TracerProvider).
//
// Example Usage (Conceptual):
//
//	source := make(chan int)
//	sink := make(chan string)
//
//	// Go func to feed source...
//
//	builder := fluxus.NewStreamPipeline[int](
//	    fluxus.WithStreamPipelineName("my_data_pipeline"),
//	    // Add other pipeline options...
//	)
//
//	pipeline, err := fluxus.Finalize(
//	    fluxus.AddStage(builder, "stage1_double", fluxus.StageFunc[int, int](func(ctx context.Context, i int) (int, error) {
//	        return i * 2, nil
//	    })),
//	    fluxus.AddStage(fluxus.AddStage(builder, "stage2_format", fluxus.StageFunc[int, string](func(ctx context.Context, i int) (string, error) {
//	        return fmt.Sprintf("Value: %d", i), nil
//	    })),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	// Run the pipeline (blocks until completion or error)
//	runErr := fluxus.Run(ctx, pipeline, source, sink)
//
//	// Go func to read from sink...
//
//	if runErr != nil {
//	    log.Printf("Pipeline failed: %v", runErr)
//	}
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

// ErrorHandlingStrategy defines how item-level errors are handled within a StreamAdapter
// when the wrapped Stage's Process method returns an error.
type ErrorHandlingStrategy int

const (
	// SkipOnError logs the error (if a logger is provided) using WARN level,
	// increments the skipped item metric (if metrics are enabled), and continues
	// processing the next item. This is the default strategy.
	SkipOnError ErrorHandlingStrategy = iota
	// StopOnError logs the error using ERROR level and returns it immediately.
	// This causes the StreamAdapter's ProcessStream method to terminate,
	// which will typically propagate up through the pipeline's error group,
	// cancelling the pipeline's context and stopping all stages.
	StopOnError
	// SendToErrorChannel attempts to send a ProcessingError struct (containing the
	// original item and the error) to the configured error channel (must be provided
	// via WithAdapterErrorChannel). Processing continues with the next item.
	// If the error channel is full, the send operation will block until space is
	// available or the adapter's context (ctx) is cancelled. If the context is
	// cancelled during the send, the error is logged at WARN level, and processing continues.
	// The error sent metric is incremented upon successful send.
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

// Stage represents a single, discrete processing step in a pipeline.
// It defines a transformation from an input type I to an output type O.
// Stages are typically stateless but can manage state internally if needed.
// For stream processing, Stages are usually wrapped by a StreamAdapter.
type Stage[I, O any] interface {
	// Process takes a single input item and the context, performs the processing,
	// and returns the output item or an error.
	// The context should be checked for cancellation, especially for long-running operations.
	// Returning an error signifies a failure for this specific item. How this error
	// is handled depends on the execution context (e.g., StreamAdapter's strategy).
	Process(ctx context.Context, input I) (O, error)
}

// StreamStage defines the interface for a stage designed for continuous stream processing.
// Unlike Stage, which processes item by item, StreamStage manages the flow of items
// from an input channel to an output channel directly. This is useful for stages
// that need more control over the stream, perform stateful operations across items,
// or implement custom concurrency/batching logic.

type StreamStage[I, O any] interface {
	// ProcessStream reads items from the input channel 'in', processes them, and sends
	// results to the output channel 'out'.
	// It should respect the cancellation signal from the context 'ctx'.
	// The method must run until the 'in' channel is closed and all processing related
	// to received items is complete, OR until 'ctx' is cancelled.
	//
	// CRITICAL: Implementations MUST ensure the 'out' channel is closed before returning,
	// regardless of success or failure. This signals downstream stages that no more
	// data will be sent by this stage. Using `defer close(out)` is the standard practice.
	//
	// Returning a non-nil error indicates a fatal error for this stage. This error
	// will typically propagate up and cause the entire pipeline to shut down.
	// Item-level processing errors that should not stop the pipeline must be handled
	// internally within the ProcessStream implementation (e.g., logged, skipped,
	// sent to a separate error channel if applicable).
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
//
// IMPORTANT: Due to limitations in Go's generics for variadic functions with
// changing types, this function uses interface{} and runtime type assertions
// internally. This means type mismatches between stages might only be caught
// at runtime when the chained stage is processed, unlike the compile-time
// safety provided by direct, nested calls to the Chain function.
// Use with caution or prefer direct Chain calls for maximum type safety.
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

// StreamAdapterOption is a function type used to configure a StreamAdapter instance
// during its creation via NewStreamAdapter.
type StreamAdapterOption[I, O any] func(*StreamAdapter[I, O])

// WithoutAdapterMetrics provides an option to explicitly disable StageWorker* metrics
// for the StreamAdapter it's applied to. It sets the adapter's internal metrics
// collector to the NoopMetricsCollector.
func WithoutAdapterMetrics[I, O any]() StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		sa.metricsCollector = DefaultMetricsCollector // DefaultMetricsCollector is the Noop one
	}
}

// WithAdapterMetrics provides an option to set the MetricsCollector instance for the
// StreamAdapter. This collector will receive StageWorker* metrics (processed, skipped, error sent).
// If the provided collector is nil, the NoopMetricsCollector is used.
// This is typically configured via the pipeline builder using the pipeline's collector.
func WithAdapterMetrics[I, O any](collector MetricsCollector) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		if collector != nil {
			sa.metricsCollector = collector
		} else {
			sa.metricsCollector = DefaultMetricsCollector // Use default if nil
		}
	}
}

// WithAdapterName provides an option to set a descriptive name for the StreamAdapter stage.
// This name is used in logging output and as a dimension/attribute in metrics and traces,
// helping to identify the specific adapter instance.
// Default: "unnamed_stream_adapter".
func WithAdapterName[I, O any](name string) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		// Allow empty name, though pipeline usually provides one
		sa.adapterName = name
	}
}

// WithAdapterConcurrency provides an option to set the number of concurrent goroutines
// the StreamAdapter uses to process items from the input channel by calling the
// wrapped Stage's Process method.
//   - n > 1: Enables concurrent processing with up to 'n' goroutines. Output order
//     relative to input order is NOT guaranteed.
//   - n == 0: Defaults to using runtime.NumCPU() goroutines. Output order is NOT guaranteed.
//   - n == 1 or n < 0: Enforces sequential processing (a single goroutine). Output order
//     matches input order.
//
// Default: 1 (sequential).
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

// WithAdapterErrorStrategy provides an option to set the strategy for handling errors
// returned by the wrapped Stage's Process method. See the ErrorHandlingStrategy constants
// for details on each strategy (SkipOnError, StopOnError, SendToErrorChannel).
// Default: SkipOnError.
func WithAdapterErrorStrategy[I, O any](strategy ErrorHandlingStrategy) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		sa.errStrategy = strategy
	}
}

// WithAdapterLogger provides an option to set a custom *log.Logger for the StreamAdapter.
// This logger is used for internal messages, such as logging skipped items (WARN level)
// or errors during error handling (ERROR/WARN level).
// If nil is provided, logging defaults to a logger that discards all output (io.Discard).
// Default: nil (results in io.Discard logger).
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

// WithAdapterErrorChannel provides an option to set the channel used when the
// ErrorHandlingStrategy is SendToErrorChannel. The adapter will send ProcessingError[I]
// structs containing the failed item and the error to this channel.
//
// The caller is responsible for creating this channel and ensuring there is a consumer
// reading from it to prevent the adapter from blocking indefinitely. The adapter respects
// backpressure; if the channel is full, the sending goroutine will block until space is
// available or the adapter's context is cancelled.
//
// This option is MANDATORY if using the SendToErrorChannel strategy; otherwise,
// NewStreamAdapter will panic.
// Default: nil.
func WithAdapterErrorChannel[I, O any](errChan chan<- ProcessingError[I]) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		sa.errChan = errChan
	}
}

// WithAdapterBufferSize provides an option to set the buffer size for the internal
// 'jobs' channel used in concurrent mode (when concurrency > 1). This channel sits
// between the dispatcher reading from the input channel and the worker goroutines.
// A buffer can help smooth out processing rates and potentially improve throughput
// by reducing contention, especially if the wrapped Stage's processing time varies.
// If size <= 0, the channel will be unbuffered.
// This option has no effect in sequential mode (concurrency <= 1).
// Default: 0 (unbuffered).
func WithAdapterBufferSize[I, O any](size int) StreamAdapterOption[I, O] {
	return func(sa *StreamAdapter[I, O]) {
		if size < 0 {
			size = 0
		}
		sa.bufferSize = size
	}
}

// WithAdapterTracerProvider provides an option to set the TracerProvider used by the
// StreamAdapter to create OpenTelemetry traces for each processed item.
// If nil is provided, the DefaultTracerProvider (which uses otel.GetGlobalTracerProvider)
// is used. Spans are created for each item processed, recording success/failure and duration.
// Default: DefaultTracerProvider.
func WithAdapterTracerProvider[I, O any](provider TracerProvider) StreamAdapterOption[I, O] { // <-- Add
	return func(sa *StreamAdapter[I, O]) {
		if provider != nil {
			sa.tracerProvider = provider
		} else {
			sa.tracerProvider = DefaultTracerProvider // Ensure it's never nil
		}
	}
}

// StreamAdapter wraps a standard Stage[I, O] to adapt it for use within a channel-based
// stream processing pipeline (making it implement StreamStage[I, O]).
//
// It enhances the wrapped Stage with:
//   - Concurrency: Optionally processes items using multiple goroutines.
//   - Error Handling: Provides strategies (Skip, Stop, SendToErrorChannel) for item-level errors.
//   - Observability: Integrates with MetricsCollector and TracerProvider for metrics and tracing.
//   - Logging: Uses a configurable logger for internal messages.
//   - Lifecycle Management: Respects context cancellation.
//
// It forms the bridge between the item-by-item processing logic of a Stage and the
// continuous flow of a StreamPipeline.
type StreamAdapter[I, O any] struct {
	wrappedStage     Stage[I, O]
	concurrency      int                       // Number of worker goroutines (1 for sequential)
	errStrategy      ErrorHandlingStrategy     // How to handle errors from wrappedStage.Process
	logger           *log.Logger               // Logger for internal messages
	errChan          chan<- ProcessingError[I] // Channel for SendToErrorChannel strategy
	bufferSize       int                       // Buffer size for the internal jobs channel (concurrent mode)
	metricsCollector MetricsCollector          // Collector for StageWorker* metrics
	adapterName      string                    // Name for logging, metrics, tracing
	tracerProvider   TracerProvider            // Provider for OpenTelemetry tracing
	tracer           trace.Tracer              // Tracer instance created from provider
}

// NewStreamAdapter creates a new StreamAdapter instance.
// It takes the Stage[I, O] to wrap and a variable number of StreamAdapterOption functions
// to customize its behavior (concurrency, error handling, etc.).
// Panics if the provided stage is nil or if SendToErrorChannel strategy is chosen
// without providing an error channel via WithAdapterErrorChannel.
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
		metricsCollector: DefaultMetricsCollector,  // Default to NoopMetricsCollectors
		adapterName:      "unnamed_stream_adapter", // Default name
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

// ProcessStream implements the StreamStage interface. It reads from 'in', processes items
// using the wrapped Stage (sequentially or concurrently based on configuration),
// handles errors according to the strategy, and sends results to 'out'.
// It respects context cancellation and ensures the 'out' channel is closed upon return.
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

// processSequential handles processing items one by one in the order they arrive.
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

// processConcurrent handles processing items using a pool of worker goroutines.
// It uses an errgroup to manage goroutine lifecycle and error propagation.
// Order is not preserved.
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

// dispatchJobs is run by a single goroutine. It reads items from the pipeline's
// input channel ('in') and forwards them to the internal 'jobs' channel, which
// is consumed by the worker pool. It stops when 'in' is closed or the context 'gctx'
// is cancelled.
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

// runWorker is executed by each concurrent worker goroutine. It reads items ('jobs')
// from the internal channel, processes them using the wrapped Stage, handles errors,
// and sends results to the pipeline's output channel ('out'). It stops when the
// 'jobs' channel is closed or the context 'gctx' is cancelled.
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

// handleItemError centralizes the logic for dealing with errors from wrappedStage.Process,
// including logging, metrics reporting, and applying the configured ErrorHandlingStrategy.
// It returns (true, nil) if processing should continue, (false, err) if StopOnError is triggered.
// The returned error for StopOnError is wrapped with adapter context.
func (a *StreamAdapter[I, O]) handleItemError(ctx context.Context, item I, err error) (bool, error) {
	switch a.errStrategy {
	case StopOnError:
		// Log and return the error to stop the stage/pipeline
		wrappedErr := fmt.Errorf("adapter '%s' failed with StopOnError strategy: %w", a.adapterName, err)
		a.logf(
			"ERROR: fluxus.StreamAdapter '%s' stopping due to error: %v",
			a.adapterName,
			err,
		) // Log original error for clarity
		return false, wrappedErr // Signal to stop and return the error
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

// logf is a helper to safely log using the configured logger, checking for nil.
func (a *StreamAdapter[I, O]) logf(format string, v ...interface{}) {
	// Check logger again in case it was initially nil and replaced by discard logger
	if a.logger != nil {
		a.logger.Printf(format, v...)
	}
}

// String provides a human-readable representation of the ErrorHandlingStrategy.
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
