package fluxus

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TracedStage wraps any Stage with OpenTelemetry tracing
type TracedStage[I, O any] struct {
	// The underlying stage
	stage Stage[I, O]

	// Name for tracing
	name string

	// Tracer to use
	tracer trace.Tracer

	// Attributes to add to spans
	attributes []attribute.KeyValue
}

// TracedStageOption is a function that configures a TracedStage.
type TracedStageOption[I, O any] func(*TracedStage[I, O])

// WithTracerName sets a custom name for the TracedStage.
func WithTracerName[I, O any](name string) TracedStageOption[I, O] {
	return func(ts *TracedStage[I, O]) {
		ts.name = name
	}
}

// WithTracer sets a custom tracer for the TracedStage.
func WithTracer[I, O any](tracer trace.Tracer) TracedStageOption[I, O] {
	return func(ts *TracedStage[I, O]) {
		ts.tracer = tracer
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
		stage:      stage,
		name:       "fluxus.stage",
		tracer:     otel.Tracer("github.com/synoptiq/go-fluxus"),
		attributes: []attribute.KeyValue{},
	}

	// Apply options
	for _, option := range options {
		option(ts)
	}

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
	span.SetAttributes(attribute.Float64("duration_ms", float64(duration.Milliseconds())))

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
	stage      *FanOut[I, O]
	name       string
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

// NewTracedFanOut creates a traced wrapper around a FanOut stage.
func NewTracedFanOut[I, O any](
	fanOut *FanOut[I, O],
	name string,
	attributes ...attribute.KeyValue,
) Stage[I, []O] {
	return &TracedFanOutStage[I, O]{
		stage:      fanOut,
		name:       name,
		tracer:     otel.Tracer("github.com/synoptiq/go-fluxus"),
		attributes: attributes,
	}
}

// WithTracer sets a custom tracer for the traced fan-out stage.
func (ts *TracedFanOutStage[I, O]) WithTracer(tracer trace.Tracer) *TracedFanOutStage[I, O] {
	ts.tracer = tracer
	return ts
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
				attribute.Int("num_stages", len(ts.stage.stages)),
				attribute.Int("concurrency", ts.stage.concurrency),
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
	span.SetAttributes(attribute.Float64("duration_ms", float64(duration.Milliseconds())))

	// Record error if any
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int("num_results", len(outputs)))
	}

	return outputs, err
}

// TracedFanInStage wraps a FanIn with additional fan-in specific tracing
type TracedFanInStage[I, O any] struct {
	stage      *FanIn[I, O]
	name       string
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

// NewTracedFanIn creates a traced wrapper around a FanIn stage.
func NewTracedFanIn[I, O any](
	fanIn *FanIn[I, O],
	name string,
	attributes ...attribute.KeyValue,
) Stage[[]I, O] {
	return &TracedFanInStage[I, O]{
		stage:      fanIn,
		name:       name,
		tracer:     otel.Tracer("github.com/synoptiq/go-fluxus"),
		attributes: attributes,
	}
}

// WithTracer sets a custom tracer for the traced fan-in stage.
func (ts *TracedFanInStage[I, O]) WithTracer(tracer trace.Tracer) *TracedFanInStage[I, O] {
	ts.tracer = tracer
	return ts
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
				attribute.Int("num_inputs", len(inputs)),
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
	span.SetAttributes(attribute.Float64("duration_ms", float64(duration.Milliseconds())))

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
	stage      *Buffer[I, O]
	name       string
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

// NewTracedBuffer creates a traced wrapper around a Buffer stage.
func NewTracedBuffer[I, O any](
	buffer *Buffer[I, O],
	name string,
	attributes ...attribute.KeyValue,
) Stage[[]I, []O] {
	return &TracedBufferStage[I, O]{
		stage:      buffer,
		name:       name,
		tracer:     otel.Tracer("github.com/synoptiq/go-fluxus"),
		attributes: attributes,
	}
}

// WithTracer sets a custom tracer for the traced buffer stage.
func (ts *TracedBufferStage[I, O]) WithTracer(tracer trace.Tracer) *TracedBufferStage[I, O] {
	ts.tracer = tracer
	return ts
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
				attribute.Int("num_inputs", len(inputs)),
				attribute.Int("batch_size", ts.stage.batchSize),
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
		attribute.Float64("duration_ms", float64(duration.Milliseconds())),
		attribute.Int("num_outputs", len(outputs)),
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
	stage      *Retry[I, O]
	name       string
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

// NewTracedRetry creates a traced wrapper around a Retry stage.
func NewTracedRetry[I, O any](
	retry *Retry[I, O],
	name string,
	attributes ...attribute.KeyValue,
) Stage[I, O] {
	return &TracedRetryStage[I, O]{
		stage:      retry,
		name:       name,
		tracer:     otel.Tracer("github.com/synoptiq/go-fluxus"),
		attributes: attributes,
	}
}

// WithTracer sets a custom tracer for the traced retry stage.
func (ts *TracedRetryStage[I, O]) WithTracer(tracer trace.Tracer) *TracedRetryStage[I, O] {
	ts.tracer = tracer
	return ts
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
				attribute.Int("max_attempts", ts.stage.maxAttempts),
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
			fmt.Sprintf("%s.attempt.%d", ts.name, attemptCount),
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
			attemptSpan.SetAttributes(attribute.Bool("success", false))
		} else {
			attemptSpan.SetStatus(codes.Ok, "")
			attemptSpan.SetAttributes(attribute.Bool("success", true))
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

	// Record duration and attempts
	duration := time.Since(startTime)
	span.SetAttributes(
		attribute.Float64("duration_ms", float64(duration.Milliseconds())),
		attribute.Int("attempts", attemptCount),
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
	stage      *Pipeline[I, O]
	name       string
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

// NewTracedPipeline creates a traced wrapper around a Pipeline stage.
func NewTracedPipeline[I, O any](
	pipeline *Pipeline[I, O],
	name string,
	attributes ...attribute.KeyValue,
) Stage[I, O] {
	return &TracedPipelineStage[I, O]{
		stage:      pipeline,
		name:       name,
		tracer:     otel.Tracer("github.com/synoptiq/go-fluxus"),
		attributes: attributes,
	}
}

// WithTracer sets a custom tracer for the traced pipeline stage.
func (ts *TracedPipelineStage[I, O]) WithTracer(tracer trace.Tracer) *TracedPipelineStage[I, O] {
	ts.tracer = tracer
	return ts
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
	span.SetAttributes(attribute.Float64("duration_ms", float64(duration.Milliseconds())))

	// Record error if any
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return output, err
}
