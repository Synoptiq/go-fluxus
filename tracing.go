package fluxus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerProvider is an interface wrapper around otel's TracerProvider.
// Allows for custom implementations or defaults.
type TracerProvider interface {
	Tracer(name string, options ...trace.TracerOption) trace.Tracer
}

// NoopTracerProvider provides a tracer that does nothing.
type NoopTracerProvider struct{}

func (p *NoopTracerProvider) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	return noop.NewTracerProvider().Tracer(name, options...)
}

// DefaultTracerProvider is the default used when none is specified.
var DefaultTracerProvider TracerProvider = &NoopTracerProvider{}

// Ensure NoopTracerProvider implements TracerProvider
var _ TracerProvider = (*NoopTracerProvider)(nil)

// TracedStage wraps any Stage with OpenTelemetry tracing
type TracedStage[I, O any] struct {
	// The underlying stage
	stage Stage[I, O]

	// Name for tracing
	name string

	// Tracer to use
	tracer trace.Tracer

	// Provider to use for creating spans
	tracerProvider TracerProvider

	// Attributes to add to spans
	attributes []attribute.KeyValue
}

// TracedStageOption is a function that configures a TracedStage.
type TracedStageOption[I, O any] func(*TracedStage[I, O])

// WithTracerStageName sets a custom name for the TracedStage.
func WithTracerStageName[I, O any](name string) TracedStageOption[I, O] {
	return func(ts *TracedStage[I, O]) {
		ts.name = name
	}
}

// WithTracerProvider sets a custom tracer provider for the TracedStage.
func WithTracerProvider[I, O any](tracer TracerProvider) TracedStageOption[I, O] {
	return func(ts *TracedStage[I, O]) {
		if tracer != nil {
			ts.tracerProvider = tracer
		} else {
			ts.tracerProvider = DefaultTracerProvider
		}
	}
}

// WithTracerAttributes adds custom attributes to spans created by the TracedStage.
func WithTracerAttributes[I, O any](attrs ...attribute.KeyValue) TracedStageOption[I, O] {
	return func(ts *TracedStage[I, O]) {
		ts.attributes = append(ts.attributes, attrs...)
	}
}

// NewTracedStage creates a new TracedStage that wraps the given stage.
func NewTracedStage[I, O any](
	stage Stage[I, O],
	options ...TracedStageOption[I, O],
) *TracedStage[I, O] {
	ts := &TracedStage[I, O]{
		stage:          stage,
		name:           "traced_stage",
		tracer:         nil,
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	// Apply options
	for _, option := range options {
		option(ts)
	}

	ts.tracer = ts.tracerProvider.Tracer(fmt.Sprintf("fluxus/stage/%s", ts.name))

	return ts
}

// Process implements the Stage interface for TracedStage.
func (ts *TracedStage[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Create a span for this stage
	ctx, span := ts.tracer.Start(
		ctx,
		ts.name,
		trace.WithAttributes(ts.attributes...),
	)
	defer span.End()

	// Start timing
	startTime := time.Now()

	// Process the stage
	output, err := ts.stage.Process(ctx, input)

	// Record duration
	duration := time.Since(startTime)
	span.SetAttributes(attribute.Int64("fluxus.stage.duration_ms", duration.Milliseconds()))

	// Record error if any
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return output, err
}

// TracedStreamStage wraps any StreamStage with OpenTelemetry tracing.
type TracedStreamStage[T, O any] struct {
	stage          StreamStage[T, O]
	name           string
	tracer         trace.Tracer
	tracerProvider TracerProvider
	attributes     []attribute.KeyValue // Static attributes for all spans from this stage
}

// TracedStreamStageOption configures a TracedStreamStage.
type TracedStreamStageOption[T, O any] func(*TracedStreamStage[T, O])

// WithTracerStreamStageName sets the name for the traced stream stage.
func WithTracerStreamStageName[T, O any](name string) TracedStreamStageOption[T, O] {
	return func(tss *TracedStreamStage[T, O]) {
		if name != "" {
			tss.name = name
		}
	}
}

// WithTracerStreamProvider sets the TracerProvider for the TracedStreamStage.
func WithTracerStreamProvider[T, O any](provider TracerProvider) TracedStreamStageOption[T, O] {
	return func(tss *TracedStreamStage[T, O]) {
		if provider != nil {
			tss.tracerProvider = provider
		}
	}
}

// WithTracerStreamAttributes adds static attributes to all spans created by this TracedStreamStage.
func WithTracerStreamAttributes[T, O any](attrs ...attribute.KeyValue) TracedStreamStageOption[T, O] {
	return func(tss *TracedStreamStage[T, O]) {
		tss.attributes = append(tss.attributes, attrs...)
	}
}

// NewTracedStreamStage creates a new TracedStreamStage that wraps the given stream stage.
func NewTracedStreamStage[T, O any](
	stage StreamStage[T, O],
	options ...TracedStreamStageOption[T, O],
) StreamStage[T, O] {
	if stage == nil {
		panic("fluxus.NewTracedStreamStage: stage cannot be nil")
	}

	tss := &TracedStreamStage[T, O]{
		stage:          stage,
		name:           "traced_stream_stage", // Default name
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}
	for _, option := range options {
		option(tss)
	}
	tss.tracer = tss.tracerProvider.Tracer(fmt.Sprintf("fluxus/stream_stage/%s", tss.name))
	return tss
}

// ProcessStream implements the StreamStage interface, adding tracing.
func (tss *TracedStreamStage[T, O]) ProcessStream(ctx context.Context, in <-chan T, out chan<- O) error {
	ctx, span := tss.tracer.Start(ctx, tss.name, trace.WithAttributes(tss.attributes...))
	defer span.End()

	startTime := time.Now()
	err := tss.stage.ProcessStream(ctx, in, out) // Call the wrapped stage
	duration := time.Since(startTime)

	span.SetAttributes(attribute.Int64("fluxus.stage.duration_ms", duration.Milliseconds()))

	if err != nil {
		// Avoid recording context.Canceled or context.DeadlineExceeded if they are the direct error,
		// as these are often normal terminations for streams. The span status will reflect it.
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Error, err.Error()) // Set status for cancellation too
		}
	} else {
		span.SetStatus(codes.Ok, "")
	}
	return err
}

type tracedPipelineConfig[I, O any] struct {
	name           string
	tracerProvider TracerProvider
	attributes     []attribute.KeyValue
}

type TracedPipelineOption[I, O any] func(*tracedPipelineConfig[I, O])

// WithTracerPipelineName sets the name for the traced pipeline.
func WithTracerPipelineName[T, O any](name string) TracedPipelineOption[T, O] {
	return func(tss *tracedPipelineConfig[T, O]) {
		if name != "" {
			tss.name = name
		}
	}
}

// WithTracerPipelineProvider sets the TracerProvider for the TracedPipeline.
func WithTracerPipelineProvider[T, O any](provider TracerProvider) TracedPipelineOption[T, O] {
	return func(tss *tracedPipelineConfig[T, O]) {
		if provider != nil {
			tss.tracerProvider = provider
		}
	}
}

// WithTracerPipelineAttributes adds static attributes to all spans created by this TracedPipeline.
func WithTracerPipelineAttributes[T, O any](attrs ...attribute.KeyValue) TracedPipelineOption[T, O] {
	return func(tss *tracedPipelineConfig[T, O]) {
		tss.attributes = append(tss.attributes, attrs...)
	}
}

// NewTracedPipeline creates a traced wrapper around a Pipeline stage.
func NewTracedPipeline[I, O any](
	pipeline *Pipeline[I, O],
	options ...TracedPipelineOption[I, O],
) Stage[I, O] {
	if pipeline == nil {
		panic("fluxus.NewTracedPipeline: pipeline cannot be nil")
	}

	// For Pipeline, there aren't many "pipeline-specific" attributes from the Pipeline struct itself
	// that NewTracedStage wouldn't already handle if passed as options.
	// So, this can be a direct wrap.
	// The TracedStageOption can be used to set the name to "traced_pipeline" or similar.

	cfg := &tracedPipelineConfig[I, O]{
		name:           "traced_pipeline",
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		option(cfg)
	}

	// Create the tracer instance based on the configured provider and name
	tracer := cfg.tracerProvider.Tracer(fmt.Sprintf("fluxus/pipeline/%s", cfg.name))

	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		// Create a span for this pipeline execution
		ctx, span := tracer.Start(
			ctx,
			cfg.name, // Use the configured pipeline name for the span
			trace.WithAttributes(cfg.attributes...),
		)
		defer span.End()

		startTime := time.Now()
		var zero O // For returning in case of error

		output, err := pipeline.Process(ctx, input) // Call the original pipeline

		duration := time.Since(startTime)
		span.SetAttributes(attribute.Int64("fluxus.pipeline.duration_ms", duration.Milliseconds()))

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return zero, err
		}
		span.SetStatus(codes.Ok, "")
		return output, nil
	})
}

// NewTracedMap creates a traced wrapper around a Map stage.
func NewTracedMap[I, O any](
	mapStage *Map[I, O],
	options ...TracedStageOption[[]I, []O], // Map's Process takes []I and returns []O
) Stage[[]I, []O] {
	if mapStage == nil {
		panic("fluxus.NewTracedMap: mapStage cannot be nil")
	}

	// Attributes known at construction time
	constructionAttrs := []attribute.KeyValue{
		attribute.String("fluxus.stage.type", "map"),
		attribute.Int("fluxus.map.concurrency", mapStage.concurrency),
		attribute.Bool("fluxus.map.collect_errors", mapStage.collectErrors),
	}

	// Intermediate stage to add output-dependent attributes
	intermediateStage := StageFunc[[]I, []O](func(ctx context.Context, inputs []I) ([]O, error) {
		outputs, err := mapStage.Process(ctx, inputs) // Call original Map stage
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() { // Check if span is recording before setting attributes
			if err == nil {
				span.SetAttributes(attribute.Int("fluxus.map.num_results", len(outputs)))
			}
			span.SetAttributes(attribute.Int("fluxus.map.num_inputs", len(inputs)))
		}
		return outputs, err
	})

	// Combine construction attributes with any user-provided attributes via options
	allOptions := append(
		[]TracedStageOption[[]I, []O]{
			WithTracerStageName[[]I, []O](fmt.Sprintf("map<%T_to_%T>", new(I), new(O))), // Example dynamic name
			WithTracerAttributes[[]I, []O](constructionAttrs...),
		},
		options...,
	)

	return NewTracedStage(intermediateStage, allOptions...)
}

// NewTracedFanOut creates a traced wrapper around a FanOut stage.
func NewTracedFanOut[I, O any](
	fanOut *FanOut[I, O],
	options ...TracedStageOption[I, []O],
) Stage[I, []O] {
	if fanOut == nil {
		panic("fluxus.NewTracedFanOut: fanOut cannot be nil") // Add nil check
	}

	// Configure generic TracedStage settings from options
	configHolder := &TracedStage[I, []O]{
		name:           "traced_fan_out",
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		option(configHolder)
	}

	// Prepare FanOut-specific attributes known at construction
	constructionTimeAttrs := []attribute.KeyValue{
		attribute.Int("fluxus.stage.num_stages", len(fanOut.stages)),
		attribute.Int("fluxus.stage.concurrency", fanOut.concurrency),
	}

	//nolint:gocritic // this is for clarity, not a linting issue
	allConstructionAttrs := append(configHolder.attributes, constructionTimeAttrs...)

	// Intermediate stage to add output-dependent attributes
	intermediateStage := StageFunc[I, []O](func(ctx context.Context, input I) ([]O, error) {
		outputs, err := fanOut.Process(ctx, input) // Call original FanOut
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() && err == nil {
			span.SetAttributes(attribute.Int("fluxus.stage.num_results", len(outputs)))
		}
		return outputs, err
	})

	return NewTracedStage(intermediateStage,
		WithTracerStageName[I, []O](configHolder.name),
		WithTracerProvider[I, []O](configHolder.tracerProvider),
		WithTracerAttributes[I, []O](allConstructionAttrs...),
	)
}

// NewTracedFanIn creates a traced wrapper around a FanIn stage.
func NewTracedFanIn[I, O any](
	fanIn *FanIn[I, O],
	options ...TracedStageOption[[]I, O],
) Stage[[]I, O] {
	if fanIn == nil {
		panic("fluxus.NewTracedFanIn: fanIn cannot be nil")
	}

	configHolder := &TracedStage[[]I, O]{
		name:           "traced_fan_in",
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}
	for _, option := range options {
		option(configHolder)
	}

	// FanIn-specific attributes (num_inputs is known from input, not construction)
	// So, the intermediate stage will handle this.

	intermediateStage := StageFunc[[]I, O](func(ctx context.Context, inputs []I) (O, error) {
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			span.SetAttributes(attribute.Int("fluxus.stage.num_inputs", len(inputs)))
		}
		return fanIn.Process(ctx, inputs)
	})

	return NewTracedStage(intermediateStage,
		WithTracerStageName[[]I, O](configHolder.name),
		WithTracerProvider[[]I, O](configHolder.tracerProvider),
		WithTracerAttributes[[]I, O](configHolder.attributes...),
	)
}

// NewTracedBuffer creates a traced wrapper around a Buffer stage.
func NewTracedBuffer[I, O any](
	buffer *Buffer[I, O],
	options ...TracedStageOption[[]I, []O],
) Stage[[]I, []O] {
	if buffer == nil {
		panic("fluxus.NewTracedBuffer: buffer cannot be nil") // Add nil check
	}

	configHolder := &TracedStage[[]I, []O]{
		name:           "traced_buffer",
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		// Apply the generic option to the proxy
		option(configHolder)
	}

	for _, option := range options {
		option(configHolder)
	}

	constructionTimeAttrs := []attribute.KeyValue{
		attribute.Int("fluxus.stage.batch_size", buffer.batchSize),
		// num_inputs is input-dependent, handled by intermediate stage
	}

	//nolint:gocritic // this is for clarity, not a linting issue
	allConstructionAttrs := append(configHolder.attributes, constructionTimeAttrs...)

	intermediateStage := StageFunc[[]I, []O](func(ctx context.Context, inputs []I) ([]O, error) {
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			span.SetAttributes(attribute.Int("fluxus.stage.num_inputs", len(inputs)))
		}
		outputs, err := buffer.Process(ctx, inputs) // Call original Buffer
		if span.IsRecording() && err == nil {
			span.SetAttributes(attribute.Int("fluxus.stage.num_outputs", len(outputs)))
		}
		return outputs, err
	})

	return NewTracedStage(intermediateStage,
		WithTracerStageName[[]I, []O](configHolder.name),
		WithTracerProvider[[]I, []O](configHolder.tracerProvider),
		WithTracerAttributes[[]I, []O](allConstructionAttrs...),
	)
}

// NewTracedRetry creates a traced wrapper around a Retry stage.
// This one is more complex due to needing to trace individual attempts.
func NewTracedRetry[I, O any](
	retryStage *Retry[I, O],
	options ...TracedStageOption[I, O],
) Stage[I, O] {
	if retryStage == nil {
		panic("fluxus.NewTracedRetry: retry cannot be nil")
	}

	configHolder := &TracedStage[I, O]{
		name:           "traced_retry",
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}
	for _, option := range options {
		option(configHolder)
	}

	constructionTimeAttrs := []attribute.KeyValue{
		attribute.Int("fluxus.stage.max_attempts", retryStage.maxAttempts),
	}

	//nolint:gocritic // this is for clarity, not a linting issue
	allConstructionAttrs := append(configHolder.attributes, constructionTimeAttrs...)

	tracer := configHolder.tracerProvider.Tracer(fmt.Sprintf("fluxus/stage/%s", configHolder.name))

	// Return a StageFunc that implements the retry tracing logic
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		// Create the main span for the entire retry operation
		ctx, span := tracer.Start(
			ctx,
			configHolder.name,
			trace.WithAttributes(allConstructionAttrs...),
		)
		defer span.End()

		attemptCount := 0
		originalInternalStage := retryStage.stage // The stage *inside* the Retry struct

		// Wrapper for the internal stage to trace each attempt
		tracedInternalAttemptStage := StageFunc[I, O](func(attemptCtx context.Context, attemptInput I) (O, error) {
			attemptCount++

			// Create a child span for this specific attempt
			_, attemptSpan := tracer.Start(
				attemptCtx, // Parent is the main retry span's context
				fmt.Sprintf("%s.attempt.%d", configHolder.name, attemptCount),
				trace.WithAttributes(attribute.Int("attempt", attemptCount)),
			)
			defer attemptSpan.End()

			output, err := originalInternalStage.Process(attemptCtx, attemptInput)

			if err != nil {
				attemptSpan.RecordError(err)
				attemptSpan.SetStatus(codes.Error, err.Error())
				attemptSpan.SetAttributes(attribute.Bool("fluxus.stage.success", false))
			} else {
				attemptSpan.SetStatus(codes.Ok, "")
				attemptSpan.SetAttributes(attribute.Bool("fluxus.stage.success", true))
			}
			return output, err
		})

		// Temporarily replace the Retry stage's internal stage
		retryStage.stage = tracedInternalAttemptStage

		startTime := time.Now()
		output, err := retryStage.Process(ctx, input) // Call the original Retry stage's Process method

		// Restore the original internal stage
		retryStage.stage = originalInternalStage

		// Record overall results in the main span
		duration := time.Since(startTime)
		span.SetAttributes(
			attribute.Int64("fluxus.stage.duration_ms", duration.Milliseconds()),
			attribute.Int("fluxus.stage.attempts", attemptCount),
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		return output, err
	})
}

// NewTracedMapReduce creates a traced wrapper around a MapReduce stage.
func NewTracedMapReduce[I any, K comparable, V any, R any](
	mapReduceStage *MapReduce[I, K, V, R],
	options ...TracedStageOption[[]I, []R], // MapReduce's Process takes []I and returns []R
) Stage[[]I, []R] {
	if mapReduceStage == nil {
		panic("fluxus.NewTracedMapReduce: mapReduceStage cannot be nil")
	}

	constructionAttrs := []attribute.KeyValue{
		attribute.String("fluxus.stage.type", "map_reduce"),
		attribute.Int("fluxus.map_reduce.parallelism", mapReduceStage.parallelism),
	}

	intermediateStage := StageFunc[[]I, []R](func(ctx context.Context, inputs []I) ([]R, error) {
		outputs, err := mapReduceStage.Process(ctx, inputs)
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			span.SetAttributes(attribute.Int("fluxus.map_reduce.num_inputs", len(inputs)))
			if err == nil {
				span.SetAttributes(attribute.Int("fluxus.map_reduce.num_results", len(outputs)))
			}
		}
		return outputs, err
	})

	// Determine types for naming (can be a bit verbose with generics)
	var iType I
	var rType R
	allOptions := append(
		[]TracedStageOption[[]I, []R]{
			WithTracerStageName[[]I, []R](fmt.Sprintf("map_reduce<%T_to_%T>", iType, rType)),
			WithTracerAttributes[[]I, []R](constructionAttrs...),
		},
		options...,
	)

	return NewTracedStage(intermediateStage, allOptions...)
}

// NewTracedTumblingCountWindow creates a traced wrapper for TumblingCountWindow.
func NewTracedTumblingCountWindow[T any](
	windowStage *TumblingCountWindow[T], // Input T, Output []T
	options ...TracedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	if windowStage == nil {
		panic("fluxus.NewTracedTumblingCountWindow: windowStage cannot be nil")
	}

	constructionAttrs := []attribute.KeyValue{
		attribute.String("fluxus.stage.type", "tumbling_count_window"),
		attribute.Int("fluxus.window.size", windowStage.size),
	}

	allOptions := append(
		[]TracedStreamStageOption[T, []T]{
			WithTracerStreamStageName[T, []T](fmt.Sprintf("tumbling_count_window<%T>", new(T))),
			WithTracerStreamAttributes[T, []T](constructionAttrs...),
		},
		options...,
	)

	return NewTracedStreamStage(windowStage, allOptions...)
}

// NewTracedTumblingTimeWindow creates a traced wrapper for TumblingTimeWindow.
func NewTracedTumblingTimeWindow[T any](
	windowStage *TumblingTimeWindow[T], // Input T, Output []T
	options ...TracedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	if windowStage == nil {
		panic("fluxus.NewTracedTumblingTimeWindow: windowStage cannot be nil")
	}

	constructionAttrs := []attribute.KeyValue{
		attribute.String("fluxus.stage.type", "tumbling_time_window"),
		attribute.Int64("fluxus.window.duration_ms", windowStage.duration.Milliseconds()),
	}

	var tType T
	allOptions := append(
		[]TracedStreamStageOption[T, []T]{
			WithTracerStreamStageName[T, []T](fmt.Sprintf("tumbling_time_window<%T>", tType)),
			WithTracerStreamAttributes[T, []T](constructionAttrs...),
		},
		options...,
	)

	return NewTracedStreamStage(windowStage, allOptions...)
}

// NewTracedSlidingCountWindow creates a traced wrapper for SlidingCountWindow.
func NewTracedSlidingCountWindow[T any](
	windowStage *SlidingCountWindow[T], // Input T, Output []T
	options ...TracedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	if windowStage == nil {
		panic("fluxus.NewTracedSlidingCountWindow: windowStage cannot be nil")
	}

	constructionAttrs := []attribute.KeyValue{
		attribute.String("fluxus.stage.type", "sliding_count_window"),
		attribute.Int("fluxus.window.size", windowStage.size),
		attribute.Int("fluxus.window.slide", windowStage.slide),
	}

	var tType T
	allOptions := append(
		[]TracedStreamStageOption[T, []T]{
			WithTracerStreamStageName[T, []T](fmt.Sprintf("sliding_count_window<%T>", tType)),
			WithTracerStreamAttributes[T, []T](constructionAttrs...),
		},
		options...,
	)
	return NewTracedStreamStage(windowStage, allOptions...)
}

// NewTracedSlidingTimeWindow creates a traced wrapper for SlidingTimeWindow.
func NewTracedSlidingTimeWindow[T any](
	windowStage *SlidingTimeWindow[T], // Input T, Output []T
	options ...TracedStreamStageOption[T, []T],
) StreamStage[T, []T] {
	if windowStage == nil {
		panic("fluxus.NewTracedSlidingTimeWindow: windowStage cannot be nil")
	}

	constructionAttrs := []attribute.KeyValue{
		attribute.String("fluxus.stage.type", "sliding_time_window"),
		attribute.Int64("fluxus.window.duration_ms", windowStage.duration.Milliseconds()),
		attribute.Int64("fluxus.window.slide_ms", windowStage.slide.Milliseconds()),
	}

	var tType T
	allOptions := append(
		[]TracedStreamStageOption[T, []T]{
			WithTracerStreamStageName[T, []T](fmt.Sprintf("sliding_time_window<%T>", tType)),
			WithTracerStreamAttributes[T, []T](constructionAttrs...),
		},
		options...,
	)
	return NewTracedStreamStage(windowStage, allOptions...)
}
