package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/synoptiq/go-fluxus"
)

// Create a test-ready tracer using the actual SDK's implementation
// but with a test exporter to capture spans
func createTestTracer() (*tracetest.SpanRecorder, oteltrace.TracerProvider) {
	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	return spanRecorder, provider
}

// Helper function to find a span by name
func findSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	return nil
}

// Helper function to find attribute in span
func findAttribute(span sdktrace.ReadOnlySpan, key string) (attribute.KeyValue, bool) {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr, true
		}
	}
	return attribute.KeyValue{}, false
}

// Helper function to test if attribute exists with specific value
func hasAttributeWithValue(span sdktrace.ReadOnlySpan, key string, value string) bool {
	attr, found := findAttribute(span, key)
	if !found {
		return false
	}
	return attr.Value.AsString() == value
}

// TestTracedStage tests the basic TracedStage functionality
func TestTracedStage(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a simple stage
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Wrap it with tracing
	traced := fluxus.NewTracedStage(
		stage,
		fluxus.WithTracerStageName[string, string]("test_stage"),
		fluxus.WithTracerProvider[string, string](provider),
		fluxus.WithTracerAttributes[string, string](
			attribute.String("test", "value"),
		),
	)

	// Process input
	result, err := traced.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify result
	if result != "HELLO" {
		t.Errorf("Expected 'HELLO', got '%s'", result)
	}

	// Wait for spans to be exported
	spans := recorder.Ended()

	// Verify that a span was recorded
	if len(spans) == 0 {
		t.Fatal("No spans were recorded")
	}

	// Find the span with our name
	span := findSpanByName(spans, "test_stage")
	if span == nil {
		t.Fatal("Span 'test_stage' not found")
	}

	// Check for custom attribute
	if !hasAttributeWithValue(span, "test", "value") {
		t.Errorf("Custom attribute 'test=value' not found in span attributes: %v", span.Attributes())
	}

	// Check for duration attribute
	_, hasDuration := findAttribute(span, "fluxus.stage.duration_ms")
	if !hasDuration {
		t.Errorf("Duration attribute not found in span attributes: %v", span.Attributes())
	}

	// Check status
	if span.Status().Code != codes.Ok {
		t.Errorf("Expected status code Ok, got %v", span.Status().Code)
	}
}

// TestTracedStageError tests error handling in TracedStage
func TestTracedStageError(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a stage that returns an error
	expectedErr := errors.New("test error")
	stage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", expectedErr
	})

	// Wrap it with tracing
	traced := fluxus.NewTracedStage(
		stage,
		fluxus.WithTracerStageName[string, string]("error_stage"),
		fluxus.WithTracerProvider[string, string](provider),
	)

	// Process input
	_, err := traced.Process(context.Background(), "hello")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Expected error %v, got %v", expectedErr, err)
	}

	// Wait for spans to be exported
	spans := recorder.Ended()

	// Verify that a span was recorded
	if len(spans) == 0 {
		t.Fatal("No spans were recorded")
	}

	// Find the span with our name
	span := findSpanByName(spans, "error_stage")
	if span == nil {
		t.Fatal("Span 'error_stage' not found")
	}

	// Check status code
	if span.Status().Code != codes.Error {
		t.Errorf("Expected status code Error, got %v", span.Status().Code)
	}

	// Error events should be recorded
	events := span.Events()
	foundErrorEvent := false
	for _, event := range events {
		if event.Name == "exception" {
			foundErrorEvent = true
			break
		}
	}
	if !foundErrorEvent {
		t.Errorf("Expected error event not recorded: %v", events)
	}
}

// TestTracedFanOut tests the TracedFanOut functionality
func TestTracedFanOut(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create stages for FanOut
	stage1 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	stage2 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToLower(input), nil
	})

	// Create a FanOut
	fanOut := fluxus.NewFanOut(stage1, stage2)

	// Create a traced fan-out wrapper
	tracedFanOut := fluxus.NewTracedFanOut(
		fanOut,
		fluxus.WithTracerStageName[string, []string]("test_fan_out"),
		fluxus.WithTracerAttributes[string, []string](
			attribute.String("test", "value"),
		),
		fluxus.WithTracerProvider[string, []string](provider),
	)

	// Process input
	results, err := tracedFanOut.Process(context.Background(), "Hello")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify results
	if len(results) != 2 || results[0] != "HELLO" || results[1] != "hello" {
		t.Errorf("Unexpected results: %v", results)
	}

	// Wait for spans to be exported
	spans := recorder.Ended()

	// Verify that a span was recorded
	if len(spans) == 0 {
		t.Fatal("No spans were recorded")
	}

	// Find the span with our name
	span := findSpanByName(spans, "test_fan_out")
	if span == nil {
		t.Fatal("Span 'test_fan_out' not found")
	}

	// Check for num_stages attribute
	numStagesAttr, hasNumStages := findAttribute(span, "fluxus.stage.num_stages")
	if !hasNumStages || numStagesAttr.Value.AsInt64() != 2 {
		t.Errorf("Attribute 'num_stages=2' not found or incorrect in span attributes: %v", span.Attributes())
	}

	// Check for num_results attribute
	numResultsAttr, hasNumResults := findAttribute(span, "fluxus.stage.num_results")
	if !hasNumResults || numResultsAttr.Value.AsInt64() != 2 {
		t.Errorf("Attribute 'num_results=2' not found or incorrect in span attributes: %v", span.Attributes())
	}
}

// TestTracedRetry tests the TracedRetry functionality
func TestTracedRetry(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a stage that fails a few times then succeeds
	var mu sync.Mutex
	attemptCount := 0
	maxFailures := 2
	stage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		attemptCount++
		currentAttempt := attemptCount
		mu.Unlock()

		if currentAttempt <= maxFailures {
			return "", fmt.Errorf("attempt %d failed", currentAttempt)
		}
		return fmt.Sprintf("Success on attempt %d", currentAttempt), nil
	})

	// Create a retry stage
	retry := fluxus.NewRetry(stage, maxFailures+1)

	// Create a traced retry wrapper
	tracedRetry := fluxus.NewTracedRetry(
		retry,
		fluxus.WithTracerStageName[string, string]("test_retry"),
		fluxus.WithTracerProvider[string, string](provider),
		fluxus.WithTracerAttributes[string, string](
			attribute.String("test", "value"),
		),
	)

	// Process input
	result, err := tracedRetry.Process(context.Background(), "test")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify result
	expected := "Success on attempt 3"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}

	// Wait for spans to be exported
	spans := recorder.Ended()

	// Verify that spans were recorded
	if len(spans) <= 1 {
		t.Fatal("Expected multiple spans for retry attempts")
	}

	// Find the main retry span
	mainSpan := findSpanByName(spans, "test_retry")
	if mainSpan == nil {
		t.Fatal("Main span 'test_retry' not found")
	}

	// Check for max_attempts attribute
	maxAttemptsAttr, hasMaxAttempts := findAttribute(mainSpan, "fluxus.stage.max_attempts")
	if !hasMaxAttempts || maxAttemptsAttr.Value.AsInt64() != int64(maxFailures+1) {
		t.Errorf("Attribute 'max_attempts=%d' not found or incorrect in span attributes: %v",
			maxFailures+1, mainSpan.Attributes())
	}

	// Look for attempt spans
	var attemptSpans []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if strings.Contains(s.Name(), "test_retry.attempt.") {
			attemptSpans = append(attemptSpans, s)
		}
	}

	if len(attemptSpans) < maxFailures+1 {
		t.Errorf("Expected at least %d attempt spans, got %d", maxFailures+1, len(attemptSpans))
	}
}

// createNoopTracer creates a tracer that doesn't record any spans
// This is useful for benchmarking to separate the overhead of
// trace instrumentation from the overhead of recording traces
func createNoopTracer() oteltrace.TracerProvider {
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.NeverSample()),
	)
	return provider
}

// createRecordingTracer creates a tracer that records all spans
func createRecordingTracer() (*tracetest.SpanRecorder, oteltrace.TracerProvider) {
	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	return spanRecorder, provider
}

// BenchmarkTracing compares the performance of a regular stage versus a traced stage
func BenchmarkTracing(b *testing.B) {
	// Create a simple stage for testing
	simpleStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Create a more complex stage for testing
	complexStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		// Do some string operations to simulate work
		result := strings.ToUpper(input)
		result = strings.ReplaceAll(result, "A", "X")
		result = strings.ReplaceAll(result, "E", "Y")
		result = strings.ReplaceAll(result, "I", "Z")
		result = strings.ReplaceAll(result, "O", "W")
		result = strings.ReplaceAll(result, "U", "V")
		return result, nil
	})

	// Create tracers for testing
	noopProvider := createNoopTracer()

	_, recordingProvider := createRecordingTracer()

	// Test inputs of different sizes
	inputs := []string{
		"hello", // Short input
		"hello world this is a test of tracing overhead", // Medium input
		strings.Repeat("abcdefghijklmnopqrstuvwxyz", 10), // Long input (260 chars)
	}

	for _, input := range inputs {
		inputName := "short"
		if len(input) > 10 && len(input) < 100 {
			inputName = "medium"
		} else if len(input) >= 100 {
			inputName = "long"
		}

		// Benchmark simple stage without tracing
		b.Run("SimpleStage_NoTracing_"+inputName, func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = simpleStage.Process(ctx, input)
			}
		})

		// Benchmark simple stage with noop tracing
		b.Run("SimpleStage_NoopTracing_"+inputName, func(b *testing.B) {
			tracedStage := fluxus.NewTracedStage(
				simpleStage,
				fluxus.WithTracerStageName[string, string]("benchmark-simple"),
				fluxus.WithTracerProvider[string, string](noopProvider),
				fluxus.WithTracerAttributes[string, string](
					attribute.String("benchmark", "true"),
				),
			)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = tracedStage.Process(ctx, input)
			}
		})

		// Benchmark simple stage with real tracing
		b.Run("SimpleStage_Recording_"+inputName, func(b *testing.B) {
			tracedStage := fluxus.NewTracedStage(
				simpleStage,
				fluxus.WithTracerStageName[string, string]("benchmark-simple"),
				fluxus.WithTracerProvider[string, string](recordingProvider),
				fluxus.WithTracerAttributes[string, string](
					attribute.String("benchmark", "true"),
				),
			)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = tracedStage.Process(ctx, input)
			}
		})

		// Benchmark complex stage without tracing
		b.Run("ComplexStage_NoTracing_"+inputName, func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = complexStage.Process(ctx, input)
			}
		})

		// Benchmark complex stage with noop tracing
		b.Run("ComplexStage_NoopTracing_"+inputName, func(b *testing.B) {
			tracedStage := fluxus.NewTracedStage(
				complexStage,
				fluxus.WithTracerStageName[string, string]("benchmark-complex"),
				fluxus.WithTracerProvider[string, string](noopProvider),
				fluxus.WithTracerAttributes[string, string](
					attribute.String("benchmark", "true"),
				),
			)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = tracedStage.Process(ctx, input)
			}
		})

		// Benchmark complex stage with real tracing
		b.Run("ComplexStage_Recording_"+inputName, func(b *testing.B) {
			tracedStage := fluxus.NewTracedStage(
				complexStage,
				fluxus.WithTracerStageName[string, string]("benchmark-complex"),
				fluxus.WithTracerProvider[string, string](recordingProvider),
				fluxus.WithTracerAttributes[string, string](
					attribute.String("benchmark", "true"),
				),
			)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = tracedStage.Process(ctx, input)
			}
		})
	}
}

// BenchmarkTracedComponents benchmarks different traced components to see their relative overhead
func BenchmarkTracedComponents(b *testing.B) {
	// Create a noop tracer for benchmarking
	noopProvider := createNoopTracer()

	// Create simple stages for the test
	simpleStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Input for all tests
	input := "hello world"
	ctx := context.Background()

	// Benchmark simple stage without tracing as baseline
	b.Run("Baseline_NoTracing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = simpleStage.Process(ctx, input)
		}
	})

	// Benchmark traced simple stage
	b.Run("TracedStage", func(b *testing.B) {
		tracedStage := fluxus.NewTracedStage(
			simpleStage,
			fluxus.WithTracerStageName[string, string]("benchmark"),
			fluxus.WithTracerProvider[string, string](noopProvider),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tracedStage.Process(ctx, input)
		}
	})

	// Benchmark traced fan-out
	b.Run("TracedFanOut", func(b *testing.B) {
		fanOut := fluxus.NewFanOut(simpleStage, simpleStage)
		tracedFanOut := fluxus.NewTracedFanOut(
			fanOut,
			fluxus.WithTracerStageName[string, []string]("benchmark-fanout"),
			fluxus.WithTracerProvider[string, []string](noopProvider),
			// "benchmark-fanout",
		)
		// if traceable, ok := tracedFanOut.(*fluxus.TracedFanOutStage[string, string]); ok {
		// 	traceable.WithTracerProvider(noopProvider)
		// }
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tracedFanOut.Process(ctx, input)
		}
	})

	// Benchmark traced retry
	b.Run("TracedRetry", func(b *testing.B) {
		retry := fluxus.NewRetry(simpleStage, 1) // Just 1 attempt since it won't fail
		tracedRetry := fluxus.NewTracedRetry(
			retry,
			fluxus.WithTracerStageName[string, string]("benchmark-retry"),
			fluxus.WithTracerProvider[string, string](noopProvider),
			// "benchmark-retry",
		)
		// if traceable, ok := tracedRetry.(*fluxus.TracedRetryStage[string, string]); ok {
		// 	traceable.WithTracerProvider(noopProvider)
		// }
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tracedRetry.Process(ctx, input)
		}
	})

	// Benchmark traced pipeline
	b.Run("TracedPipeline", func(b *testing.B) {
		pipeline := fluxus.NewPipeline(simpleStage)

		tracedPipeline := fluxus.NewTracedPipeline(
			pipeline,
			fluxus.WithTracerPipelineName[string, string]("benchmark-pipeline"),
			fluxus.WithTracerPipelineProvider[string, string](noopProvider),
		)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tracedPipeline.Process(ctx, input)
		}
	})
}

type Key string
type Value any

// BenchmarkTracingWithContext benchmarks how context propagation affects tracing performance
func BenchmarkTracingWithContext(b *testing.B) {
	// Create a noop tracer for benchmarking
	noopProvider := createNoopTracer()

	// Create a stage that reads from context
	ctxAwareStage := fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		// Simulate reading values from context
		_, _ = ctx.Value("key1").(string)
		_, _ = ctx.Value("key2").(int)
		return strings.ToUpper(input), nil
	})

	// Input for all tests
	input := "hello world"

	// Context with no values
	emptyCtx := context.Background()

	var key Key = "key1"
	var value Value = "value1"

	var key2 Key = "key2"
	var value2 Value = 42

	// Context with some values
	ctxWithValues := context.WithValue(context.Background(), key, value)
	ctxWithValues = context.WithValue(ctxWithValues, key2, value2)

	// Traced stage
	tracedStage := fluxus.NewTracedStage(
		ctxAwareStage,
		fluxus.WithTracerStageName[string, string]("benchmark-context"),
		fluxus.WithTracerProvider[string, string](noopProvider),
	)

	// Benchmark with empty context
	b.Run("EmptyContext", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tracedStage.Process(emptyCtx, input)
		}
	})

	// Benchmark with context values
	b.Run("ContextWithValues", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tracedStage.Process(ctxWithValues, input)
		}
	})
}
