package fluxus

import (
	"context"
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

// Factory functions for creating traced versions of specific stages

// TracedFanOutStage wraps a FanOut with additional fan-out specific tracing
type TracedFanOutStage[I, O any] struct {
	stage          *FanOut[I, O]
	name           string
	tracer         trace.Tracer
	tracerProvider TracerProvider
	attributes     []attribute.KeyValue
}

// NewTracedFanOut creates a traced wrapper around a FanOut stage.
func NewTracedFanOut[I, O any](
	fanOut *FanOut[I, O],
	options ...TracedStageOption[I, []O], // Accept options instead
) Stage[I, []O] {
	if fanOut == nil {
		panic("fluxus.NewTracedFanOut: fanOut cannot be nil") // Add nil check
	}

	// Create a proxy for handling options
	ts := &TracedStage[I, []O]{
		stage:          fanOut,
		name:           "traced_fan_out", // Default name
		tracer:         nil,
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		// Apply the generic option to the proxy
		option(ts)
	}

	// create the tracer *after* all options (especially provider) have been applied
	ts.tracer = ts.tracerProvider.Tracer(fmt.Sprintf("fluxus/stage/%s", ts.name))

	return &TracedFanOutStage[I, O]{
		stage:          fanOut,
		name:           ts.name,
		tracer:         ts.tracer,
		tracerProvider: ts.tracerProvider,
		attributes:     ts.attributes,
	}
}

// Process implements the Stage interface for TracedFanOutStage
func (ts *TracedFanOutStage[I, O]) Process(ctx context.Context, input I) ([]O, error) {
	// Create a span for the fan-out operation
	ctx, span := ts.tracer.Start(
		ctx,
		ts.name,
		trace.WithAttributes(
			append(
				ts.attributes,
				attribute.Int("fluxus.stage.num_stages", len(ts.stage.stages)),
				attribute.Int("fluxus.stage.concurrency", ts.stage.concurrency),
			)...,
		),
	)
	defer span.End()

	// Start timing
	startTime := time.Now()

	// Process the fan-out
	outputs, err := ts.stage.Process(ctx, input)

	// Record duration
	duration := time.Since(startTime)
	span.SetAttributes(attribute.Int64("fluxus.stage.duration_ms", duration.Milliseconds()))

	// Record error if any
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int("fluxus.stage.num_results", len(outputs)))
	}

	return outputs, err
}

// TracedFanInStage wraps a FanIn with additional fan-in specific tracing
type TracedFanInStage[I, O any] struct {
	stage          *FanIn[I, O]
	name           string
	tracer         trace.Tracer
	tracerProvider TracerProvider
	attributes     []attribute.KeyValue
}

// NewTracedFanIn creates a traced wrapper around a FanIn stage.
func NewTracedFanIn[I, O any](
	fanIn *FanIn[I, O],
	options ...TracedStageOption[[]I, O], // Accept options instead
) Stage[[]I, O] {
	if fanIn == nil {
		panic("fluxus.NewTracedFanIn: fanIn cannot be nil") // Add nil check
	}

	// Create a proxy for handling options
	ts := &TracedStage[[]I, O]{
		stage:          fanIn,
		name:           "traced_fan_in", // Default name
		tracer:         nil,
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		// Apply the generic option to the proxy
		option(ts)
	}

	// create the tracer *after* all options (especially provider) have been applied
	ts.tracer = ts.tracerProvider.Tracer(fmt.Sprintf("fluxus/stage/%s", ts.name))

	return &TracedFanInStage[I, O]{
		stage:          fanIn,
		name:           ts.name,
		tracer:         ts.tracer,
		tracerProvider: ts.tracerProvider,
		attributes:     ts.attributes,
	}
}

// Process implements the Stage interface for TracedFanInStage
func (ts *TracedFanInStage[I, O]) Process(ctx context.Context, inputs []I) (O, error) {
	// Create a span for the fan-in operation
	ctx, span := ts.tracer.Start(
		ctx,
		ts.name,
		trace.WithAttributes(
			append(
				ts.attributes,
				attribute.Int("fluxus.stage.num_inputs", len(inputs)),
			)...,
		),
	)
	defer span.End()

	// Start timing
	startTime := time.Now()

	// Process the fan-in
	output, err := ts.stage.Process(ctx, inputs)

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

// TracedBufferStage wraps a Buffer with buffer-specific tracing
type TracedBufferStage[I, O any] struct {
	stage          *Buffer[I, O]
	name           string
	tracer         trace.Tracer
	tracerProvider TracerProvider
	attributes     []attribute.KeyValue
}

// NewTracedBuffer creates a traced wrapper around a Buffer stage.
func NewTracedBuffer[I, O any](
	buffer *Buffer[I, O],
	options ...TracedStageOption[[]I, []O], // Accept options instead
) Stage[[]I, []O] {
	if buffer == nil {
		panic("fluxus.NewTracedBuffer: buffer cannot be nil") // Add nil check
	}

	// Create a proxy for handling options
	ts := &TracedStage[[]I, []O]{
		stage:          buffer,
		name:           "traced_buffer", // Default name
		tracer:         nil,
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		// Apply the generic option to the proxy
		option(ts)
	}

	// create the tracer *after* all options (especially provider) have been applied
	ts.tracer = ts.tracerProvider.Tracer(fmt.Sprintf("fluxus/stage/%s", ts.name))

	return &TracedBufferStage[I, O]{
		stage:          buffer,
		name:           ts.name,
		tracer:         ts.tracer,
		tracerProvider: ts.tracerProvider,
		attributes:     ts.attributes,
	}
}

// Process implements the Stage interface for TracedBufferStage
func (ts *TracedBufferStage[I, O]) Process(ctx context.Context, inputs []I) ([]O, error) {
	// Create a span for the buffer operation
	ctx, span := ts.tracer.Start(
		ctx,
		ts.name,
		trace.WithAttributes(
			append(
				ts.attributes,
				attribute.Int("fluxus.stage.num_inputs", len(inputs)),
				attribute.Int("fluxus.stage.batch_size", ts.stage.batchSize),
			)...,
		),
	)
	defer span.End()

	// Start timing
	startTime := time.Now()

	// Process the buffer
	outputs, err := ts.stage.Process(ctx, inputs)

	// Record duration
	duration := time.Since(startTime)
	span.SetAttributes(
		attribute.Int64("fluxus.stage.duration_ms", duration.Milliseconds()),
		attribute.Int("fluxus.stage.num_outputs", len(outputs)),
	)

	// Record error if any
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return outputs, err
}

// TracedRetryStage wraps a Retry with additional retry-specific tracing
type TracedRetryStage[I, O any] struct {
	stage          *Retry[I, O]
	name           string
	tracer         trace.Tracer
	tracerProvider TracerProvider
	attributes     []attribute.KeyValue
}

// NewTracedRetry creates a traced wrapper around a Retry stage.
func NewTracedRetry[I, O any](
	retry *Retry[I, O],
	options ...TracedStageOption[I, O], // Accept options instead
) Stage[I, O] {
	if retry == nil {
		panic("fluxus.NewTracedRetry: retry cannot be nil") // Add nil check
	}

	// Create a proxy for handling options
	ts := &TracedStage[I, O]{
		stage:          retry,
		name:           "traced_retry", // Default name
		tracer:         nil,
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		// Apply the generic option to the proxy
		option(ts)
	}

	// create the tracer *after* all options (especially provider) have been applied
	ts.tracer = ts.tracerProvider.Tracer(fmt.Sprintf("fluxus/stage/%s", ts.name))

	return &TracedRetryStage[I, O]{
		stage:          retry,
		name:           ts.name,
		tracer:         ts.tracer,
		tracerProvider: ts.tracerProvider,
		attributes:     ts.attributes,
	}
}

// Process implements the Stage interface for TracedRetryStage
func (ts *TracedRetryStage[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Create a span for the retry operation
	ctx, span := ts.tracer.Start(
		ctx,
		ts.name,
		trace.WithAttributes(
			append(
				ts.attributes,
				attribute.Int("fluxus.stage.max_attempts", ts.stage.maxAttempts),
			)...,
		),
	)
	defer span.End()

	// Create a counter for tracking attempts
	attemptCount := 0

	// Temporarily replace the original stage with our counted version
	originalStage := ts.stage.stage

	// Create a wrapper stage that counts attempts and creates spans for each attempt
	countingStage := StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		// Increment attempt counter before each attempt
		attemptCount++

		// Create a child span for the retry attempt
		attemptCtx, attemptSpan := ts.tracer.Start(
			ctx,
			fmt.Sprintf("fluxus.stage.%s.attempt.%d", ts.name, attemptCount),
			trace.WithAttributes(
				attribute.Int("attempt", attemptCount),
			),
		)

		// Process the attempt
		output, err := originalStage.Process(attemptCtx, input)

		// Record result in the span
		if err != nil {
			attemptSpan.RecordError(err)
			attemptSpan.SetStatus(codes.Error, err.Error())
			attemptSpan.SetAttributes(attribute.Bool("fluxus.stage.success", false))
		} else {
			attemptSpan.SetStatus(codes.Ok, "")
			attemptSpan.SetAttributes(attribute.Bool("fluxus.stage.success", true))
		}

		// End the attempt span
		attemptSpan.End()

		return output, err
	})

	// Temporarily replace the stage with our counting version
	ts.stage.stage = countingStage

	// Start timing
	startTime := time.Now()

	// Process using the modified retry stage
	output, err := ts.stage.Process(ctx, input)

	// Restore the original stage
	ts.stage.stage = originalStage

	// Record duration and attemptss
	duration := time.Since(startTime)
	span.SetAttributes(
		attribute.Int64("fluxus.stage.duration_ms", duration.Milliseconds()),
		attribute.Int("fluxus.stage.attempts", attemptCount),
	)

	// Record error if any
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return output, err
}

// TracedPipelineStage wraps a Pipeline with pipeline-specific tracing
type TracedPipelineStage[I, O any] struct {
	stage          *Pipeline[I, O]
	name           string
	tracer         trace.Tracer
	tracerProvider TracerProvider
	attributes     []attribute.KeyValue
}

// NewTracedPipeline creates a traced wrapper around a Pipeline stage.
func NewTracedPipeline[I, O any](
	pipeline *Pipeline[I, O],
	options ...TracedStageOption[I, O], // Accept options instead
) Stage[I, O] {
	if pipeline == nil {
		panic("fluxus.NewTracedPipeline: pipeline cannot be nil") // Add nil check
	}

	// Create a proxy for handling options
	ts := &TracedStage[I, O]{
		stage:          pipeline,
		name:           "traced_pipeline", // Default name
		tracer:         nil,
		tracerProvider: DefaultTracerProvider,
		attributes:     []attribute.KeyValue{},
	}

	for _, option := range options {
		// Apply the generic option to the proxy
		option(ts)
	}

	// create the tracer *after* all options (especially provider) have been applied
	ts.tracer = ts.tracerProvider.Tracer(fmt.Sprintf("fluxus/stage/%s", ts.name))

	return &TracedPipelineStage[I, O]{
		stage:          pipeline,
		name:           ts.name,
		tracer:         ts.tracer,
		tracerProvider: ts.tracerProvider,
		attributes:     ts.attributes,
	}
}

// Process implements the Stage interface for TracedPipelineStage
func (ts *TracedPipelineStage[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Create a span for the pipeline operation
	ctx, span := ts.tracer.Start(
		ctx,
		ts.name,
		trace.WithAttributes(ts.attributes...),
	)
	defer span.End()

	// Start timing
	startTime := time.Now()

	// Process the pipeline
	output, err := ts.stage.Process(ctx, input)

	// Record duration
	duration := time.Since(startTime)
	span.SetAttributes(attribute.Int64("fluxus.pipeline.duration_ms", duration.Milliseconds()))

	// Record error if any
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return output, err
}
