package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestTracedTimeout tests the TracedTimeout functionality
func TestTracedTimeout(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a timeout stage that succeeds
	successStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	timeout := fluxus.NewTimeout(successStage, 100*time.Millisecond)

	// Create a traced timeout wrapper
	tracedTimeout := fluxus.NewTracedTimeout(
		timeout,
		fluxus.WithTracerStageName[string, string]("test_timeout"),
		fluxus.WithTracerProvider[string, string](provider),
		fluxus.WithTracerAttributes[string, string](
			attribute.String("test", "value"),
		),
	)

	// Process input
	result, err := tracedTimeout.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify result
	if result != "HELLO" {
		t.Errorf("Expected 'HELLO', got '%s'", result)
	}

	// Wait for spans to be exported
	spans := recorder.Ended()

	// Find the span with our name
	span := findSpanByName(spans, "test_timeout")
	if span == nil {
		t.Fatal("Span 'test_timeout' not found")
	}

	// Check for timeout duration attribute
	durationAttr, hasDuration := findAttribute(span, "fluxus.timeout.duration_ms")
	if !hasDuration || durationAttr.Value.AsInt64() != 100 {
		t.Errorf("Expected timeout duration attribute with value 100, got %v", durationAttr)
	}

	// Test timeout scenario
	t.Run("TimeoutOccurred", func(t *testing.T) {
		recorder2, provider2 := createTestTracer()

		// Create a stage that times out
		slowStage := fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
			select {
			case <-time.After(100 * time.Millisecond):
				return strings.ToUpper(input), nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})

		timeout2 := fluxus.NewTimeout(slowStage, 10*time.Millisecond)
		tracedTimeout2 := fluxus.NewTracedTimeout(
			timeout2,
			fluxus.WithTracerStageName[string, string]("test_timeout_occurred"),
			fluxus.WithTracerProvider[string, string](provider2),
		)

		// Process input (should timeout)
		_, timeoutErr := tracedTimeout2.Process(context.Background(), "hello")
		if !errors.Is(timeoutErr, context.DeadlineExceeded) {
			t.Errorf("Expected DeadlineExceeded error, got %v", timeoutErr)
		}

		// Wait for spans
		spans2 := recorder2.Ended()
		span2 := findSpanByName(spans2, "test_timeout_occurred")
		if span2 == nil {
			t.Fatal("Timeout span not found")
		}

		// Check for timeout occurred attribute
		timeoutAttr, hasTimeout := findAttribute(span2, "fluxus.timeout.occurred")
		if !hasTimeout || !timeoutAttr.Value.AsBool() {
			t.Errorf("Expected timeout.occurred=true attribute, got %v", timeoutAttr)
		}
	})
}

// TestTracedCircuitBreaker tests the TracedCircuitBreaker functionality
func TestTracedCircuitBreaker(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a stage that can fail
	var callCount int
	var mu sync.Mutex
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		mu.Lock()
		callCount++
		currentCall := callCount
		mu.Unlock()

		if currentCall <= 2 {
			return "", errors.New("service unavailable")
		}
		return strings.ToUpper(input), nil
	})

	// Create circuit breaker
	cb := fluxus.NewCircuitBreaker(stage, 2, 100*time.Millisecond)

	// Create traced circuit breaker
	tracedCB := fluxus.NewTracedCircuitBreaker(
		cb,
		fluxus.WithTracerStageName[string, string]("test_circuit_breaker"),
		fluxus.WithTracerProvider[string, string](provider),
	)

	// First call should succeed (circuit closed)
	_, err1 := tracedCB.Process(context.Background(), "test1")
	if err1 == nil {
		t.Error("Expected first call to fail")
	}

	// Second call should fail and open the circuit
	_, err2 := tracedCB.Process(context.Background(), "test2")
	if err2 == nil {
		t.Error("Expected second call to fail")
	}

	// Third call should be rejected (circuit open)
	_, err3 := tracedCB.Process(context.Background(), "test3")
	if !errors.Is(err3, fluxus.ErrCircuitOpen) {
		t.Errorf("Expected ErrCircuitOpen, got %v", err3)
	}

	// Wait for spans
	spans := recorder.Ended()

	// Find circuit breaker spans
	var cbSpans []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == "test_circuit_breaker" {
			cbSpans = append(cbSpans, span)
		}
	}

	if len(cbSpans) < 3 {
		t.Fatalf("Expected at least 3 circuit breaker spans, got %d", len(cbSpans))
	}

	// Check the third span (rejected)
	rejectedSpan := cbSpans[2]
	rejectedAttr, hasRejected := findAttribute(rejectedSpan, "fluxus.circuit_breaker.rejected")
	if !hasRejected || !rejectedAttr.Value.AsBool() {
		t.Errorf("Expected rejected=true attribute in third span")
	}

	// Check for state change
	foundStateChange := false
	for _, span := range cbSpans {
		if attr, found := findAttribute(span, "fluxus.circuit_breaker.state_change"); found {
			foundStateChange = true
			if !strings.Contains(attr.Value.AsString(), "closed->open") {
				t.Errorf("Expected state change from closed to open, got %s", attr.Value.AsString())
			}
			break
		}
	}
	if !foundStateChange {
		t.Error("Expected to find state change attribute")
	}
}

// TestTracedDeadLetterQueue tests the TracedDeadLetterQueue functionality
func TestTracedDeadLetterQueue(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a stage that always fails
	failingStage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", errors.New("processing failed")
	})

	// Create a DLQ handler that tracks calls
	var dlqCalls []string
	var mu sync.Mutex
	dlqHandler := fluxus.DLQHandlerFunc[string](func(_ context.Context, item string, _ error) error {
		mu.Lock()
		dlqCalls = append(dlqCalls, item)
		mu.Unlock()
		return nil
	})

	// Create DLQ stage
	dlq := fluxus.NewDeadLetterQueue(
		failingStage,
		fluxus.WithDLQHandler[string, string](dlqHandler),
	)

	// Create traced DLQ
	tracedDLQ := fluxus.NewTracedDeadLetterQueue(
		dlq,
		fluxus.WithTracerStageName[string, string]("test_dlq"),
		fluxus.WithTracerProvider[string, string](provider),
	)

	// Process input
	_, err := tracedDLQ.Process(context.Background(), "test-item")
	if err == nil {
		t.Error("Expected error from failing stage")
	}

	// Verify DLQ was called
	mu.Lock()
	if len(dlqCalls) != 1 || dlqCalls[0] != "test-item" {
		t.Errorf("Expected DLQ to be called with 'test-item', got %v", dlqCalls)
	}
	mu.Unlock()

	// Wait for spans
	spans := recorder.Ended()

	// Find DLQ span
	span := findSpanByName(spans, "test_dlq")
	if span == nil {
		t.Fatal("DLQ span not found")
	}

	// Check for DLQ attributes
	itemSentAttr, hasItemSent := findAttribute(span, "fluxus.dlq.item_sent")
	if !hasItemSent || !itemSentAttr.Value.AsBool() {
		t.Error("Expected dlq.item_sent=true attribute")
	}

	errorAttr, hasError := findAttribute(span, "fluxus.dlq.processing_error")
	if !hasError || !strings.Contains(errorAttr.Value.AsString(), "processing failed") {
		t.Errorf("Expected processing error attribute, got %v", errorAttr)
	}
}

// TestTracedRouter tests the TracedRouter functionality
func TestTracedRouter(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create route stages
	upperStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	lowerStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToLower(input), nil
	})

	reverseStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		runes := []rune(input)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	})

	// Create router with selector
	router := fluxus.NewRouter(
		func(_ context.Context, input string) ([]int, error) {
			if len(input) < 5 {
				return []int{0}, nil // Short strings go to upper
			} else if len(input) < 10 {
				return []int{1}, nil // Medium strings go to lower
			}
			return []int{0, 2}, nil // Long strings go to upper and reverse
		},
		fluxus.Route[string, string]{Name: "uppercase", Stage: upperStage},
		fluxus.Route[string, string]{Name: "lowercase", Stage: lowerStage},
		fluxus.Route[string, string]{Name: "reverse", Stage: reverseStage},
	)

	// Create traced router
	tracedRouter := fluxus.NewTracedRouter(
		router,
		fluxus.WithTracerStageName[string, []string]("test_router"),
		fluxus.WithTracerProvider[string, []string](provider),
	)

	// Test short input (single route)
	result1, err1 := tracedRouter.Process(context.Background(), "hi")
	if err1 != nil {
		t.Fatalf("Unexpected error: %v", err1)
	}
	if len(result1) != 1 || result1[0] != "HI" {
		t.Errorf("Expected ['HI'], got %v", result1)
	}

	// Test long input (multiple routes)
	result2, err2 := tracedRouter.Process(context.Background(), "hello world")
	if err2 != nil {
		t.Fatalf("Unexpected error: %v", err2)
	}
	if len(result2) != 2 || result2[0] != "HELLO WORLD" || result2[1] != "dlrow olleh" {
		t.Errorf("Expected ['HELLO WORLD', 'dlrow olleh'], got %v", result2)
	}

	// Wait for spans
	spans := recorder.Ended()

	// Find router spans
	var routerSpans []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == "test_router" {
			routerSpans = append(routerSpans, span)
		}
	}

	if len(routerSpans) < 2 {
		t.Fatalf("Expected at least 2 router spans, got %d", len(routerSpans))
	}

	// Check attributes on the first span
	span1 := routerSpans[0]
	routesSelectedAttr, hasRoutes := findAttribute(span1, "fluxus.router.num_routes_selected")
	if !hasRoutes || routesSelectedAttr.Value.AsInt64() != 1 {
		t.Errorf("Expected num_routes_selected=1, got %v", routesSelectedAttr)
	}

	// Check selected routes attribute
	_, hasSelectedRoutes := findAttribute(span1, "fluxus.router.selected_routes")
	if !hasSelectedRoutes {
		t.Error("Expected selected_routes attribute")
	}

	// Check the second span (multiple routes)
	span2 := routerSpans[1]
	routesSelectedAttr2, _ := findAttribute(span2, "fluxus.router.num_routes_selected")
	if routesSelectedAttr2.Value.AsInt64() != 2 {
		t.Errorf("Expected num_routes_selected=2, got %v", routesSelectedAttr2)
	}
}

// TestTracedFilter tests the TracedFilter functionality
func TestTracedFilter(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a filter that passes even length strings
	filter := fluxus.NewFilter(func(_ context.Context, input string) (bool, error) {
		return len(input)%2 == 0, nil
	})

	// Create traced filter
	tracedFilter := fluxus.NewTracedFilter(
		filter,
		fluxus.WithTracerStageName[string, string]("test_filter"),
		fluxus.WithTracerProvider[string, string](provider),
	)

	// Test item that passes
	result1, err1 := tracedFilter.Process(context.Background(), "test")
	if err1 != nil {
		t.Errorf("Expected no error for passing item, got %v", err1)
	}
	if result1 != "test" {
		t.Errorf("Expected 'test', got '%s'", result1)
	}

	// Test item that is filtered
	_, err2 := tracedFilter.Process(context.Background(), "hello")
	if !errors.Is(err2, fluxus.ErrItemFiltered) {
		t.Errorf("Expected ErrItemFiltered, got %v", err2)
	}

	// Wait for spans
	spans := recorder.Ended()

	// Find filter spans
	var filterSpans []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == "test_filter" {
			filterSpans = append(filterSpans, span)
		}
	}

	if len(filterSpans) < 2 {
		t.Fatalf("Expected at least 2 filter spans, got %d", len(filterSpans))
	}

	// Check first span (passed)
	span1 := filterSpans[0]
	passedAttr, hasPassed := findAttribute(span1, "fluxus.filter.item_passed")
	if !hasPassed || !passedAttr.Value.AsBool() {
		t.Error("Expected item_passed=true for first span")
	}

	// Check second span (dropped)
	span2 := filterSpans[1]
	droppedAttr, hasDropped := findAttribute(span2, "fluxus.filter.item_dropped")
	if !hasDropped || !droppedAttr.Value.AsBool() {
		t.Error("Expected item_dropped=true for second span")
	}
}

// TestTracedJoinByKey tests the TracedJoinByKey functionality
func TestTracedJoinByKey(t *testing.T) {
	// Create a test tracer with recorder
	recorder, provider := createTestTracer()

	// Create a join by key stage that groups by first letter
	joinByKey := fluxus.NewJoinByKey(func(_ context.Context, item string) (string, error) {
		if len(item) == 0 {
			return "", nil
		}
		return string(item[0]), nil
	})

	// Create traced join by key
	tracedJoin := fluxus.NewTracedJoinByKey(
		joinByKey,
		fluxus.WithTracerStageName[[]string, map[string][]string]("test_join_by_key"),
		fluxus.WithTracerProvider[[]string, map[string][]string](provider),
	)

	// Process input
	inputs := []string{"apple", "apricot", "banana", "berry", "cherry"}
	result, err := tracedJoin.Process(context.Background(), inputs)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify results
	if len(result) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(result))
	}
	if len(result["a"]) != 2 || len(result["b"]) != 2 || len(result["c"]) != 1 {
		t.Errorf("Unexpected grouping: %v", result)
	}

	// Wait for spans
	spans := recorder.Ended()

	// Find join span
	span := findSpanByName(spans, "test_join_by_key")
	if span == nil {
		t.Fatal("Join by key span not found")
	}

	// Check attributes
	numInputsAttr, hasInputs := findAttribute(span, "fluxus.join_by_key.num_inputs")
	if !hasInputs || numInputsAttr.Value.AsInt64() != 5 {
		t.Errorf("Expected num_inputs=5, got %v", numInputsAttr)
	}

	numKeysAttr, hasKeys := findAttribute(span, "fluxus.join_by_key.num_keys")
	if !hasKeys || numKeysAttr.Value.AsInt64() != 3 {
		t.Errorf("Expected num_keys=3, got %v", numKeysAttr)
	}

	maxGroupSizeAttr, hasMaxSize := findAttribute(span, "fluxus.join_by_key.max_group_size")
	if !hasMaxSize || maxGroupSizeAttr.Value.AsInt64() != 2 {
		t.Errorf("Expected max_group_size=2, got %v", maxGroupSizeAttr)
	}
}
