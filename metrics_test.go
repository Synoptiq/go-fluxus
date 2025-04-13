package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// mockMetricsCollector records metrics for testing
type mockMetricsCollector struct {
	stageStarted             int64
	stageCompleted           int64
	stageErrors              int64
	retryAttempts            int64
	bufferBatchCalled        int64
	fanOutStarted            int64
	fanOutCompleted          int64
	fanInStarted             int64
	fanInCompleted           int64
	pipelineStarted          int64 // Added
	pipelineCompleted        int64 // Added
	stageWorkerConcurrency   int64 // Added
	stageWorkerItemProcessed int64 // Added
	stageWorkerItemSkipped   int64 // Added
	stageWorkerErrorSent     int64 // Added
}

func (m *mockMetricsCollector) PipelineStarted(_ context.Context, _ string) {
	atomic.AddInt64(&m.pipelineStarted, 1)
}

func (m *mockMetricsCollector) PipelineCompleted(_ context.Context, _ string, _ time.Duration, _ error) {
	atomic.AddInt64(&m.pipelineCompleted, 1)
}

func (m *mockMetricsCollector) StageWorkerConcurrency(_ context.Context, _ string, _ int) {
	atomic.AddInt64(&m.stageWorkerConcurrency, 1)
}

func (m *mockMetricsCollector) StageWorkerItemProcessed(_ context.Context, _ string, _ time.Duration) {
	atomic.AddInt64(&m.stageWorkerItemProcessed, 1)
}

func (m *mockMetricsCollector) StageWorkerItemSkipped(_ context.Context, _ string, _ error) {
	atomic.AddInt64(&m.stageWorkerItemSkipped, 1)
}

func (m *mockMetricsCollector) StageWorkerErrorSent(_ context.Context, _ string, _ error) {
	atomic.AddInt64(&m.stageWorkerErrorSent, 1)
}

func (m *mockMetricsCollector) StageStarted(_ context.Context, _ string) {
	atomic.AddInt64(&m.stageStarted, 1)
}

func (m *mockMetricsCollector) StageCompleted(_ context.Context, _ string, _ time.Duration) {
	atomic.AddInt64(&m.stageCompleted, 1)
}

func (m *mockMetricsCollector) StageError(_ context.Context, _ string, _ error) {
	atomic.AddInt64(&m.stageErrors, 1)
}

func (m *mockMetricsCollector) RetryAttempt(_ context.Context, _ string, _ int, _ error) {
	atomic.AddInt64(&m.retryAttempts, 1)
}

func (m *mockMetricsCollector) BufferBatchProcessed(_ context.Context, _ int, _ time.Duration) {
	atomic.AddInt64(&m.bufferBatchCalled, 1)
}

func (m *mockMetricsCollector) FanOutStarted(_ context.Context, _ string, _ int) {
	atomic.AddInt64(&m.fanOutStarted, 1)
}

func (m *mockMetricsCollector) FanOutCompleted(_ context.Context, _ string, _ int, _ time.Duration) {
	atomic.AddInt64(&m.fanOutCompleted, 1)
}

func (m *mockMetricsCollector) FanInStarted(_ context.Context, _ string, _ int) {
	atomic.AddInt64(&m.fanInStarted, 1)
}

func (m *mockMetricsCollector) FanInCompleted(_ context.Context, _ string, _ int, _ time.Duration) {
	atomic.AddInt64(&m.fanInCompleted, 1)
}

// TestMetricatedStage tests the basic MetricatedStage functionality
func TestMetricatedStage(t *testing.T) {
	// Create a mock metrics collector
	collector := &mockMetricsCollector{}

	// Create a simple stage
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Wrap it with metrics
	metricated := fluxus.NewMetricatedStage(
		stage,
		fluxus.WithStageName[string, string]("test_stage"),
		fluxus.WithMetricsCollector[string, string](collector),
	)

	// Process input
	result, err := metricated.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify result
	if result != "HELLO" {
		t.Errorf("Expected 'HELLO', got '%s'", result)
	}

	// Verify metrics
	if collector.stageStarted != 1 {
		t.Errorf("Expected StageStarted to be called once, got %d", collector.stageStarted)
	}
	if collector.stageCompleted != 1 {
		t.Errorf("Expected StageCompleted to be called once, got %d", collector.stageCompleted)
	}
	if collector.stageErrors != 0 {
		t.Errorf("Expected StageError not to be called, got %d", collector.stageErrors)
	}
}

// TestMetricatedStageError tests error metrics
func TestMetricatedStageError(t *testing.T) {
	// Create a mock metrics collector
	collector := &mockMetricsCollector{}

	// Create a stage that returns an error
	expectedErr := errors.New("test error")
	stage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		return "", expectedErr
	})

	// Wrap it with metrics
	metricated := fluxus.NewMetricatedStage(
		stage,
		fluxus.WithStageName[string, string]("error_stage"),
		fluxus.WithMetricsCollector[string, string](collector),
	)

	// Process input
	_, err := metricated.Process(context.Background(), "hello")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Expected error %v, got %v", expectedErr, err)
	}

	// Verify metrics
	if collector.stageStarted != 1 {
		t.Errorf("Expected StageStarted to be called once, got %d", collector.stageStarted)
	}
	if collector.stageCompleted != 0 {
		t.Errorf("Expected StageCompleted not to be called, got %d", collector.stageCompleted)
	}
	if collector.stageErrors != 1 {
		t.Errorf("Expected StageError to be called once, got %d", collector.stageErrors)
	}
}

// TestMetricatedFanOut tests the FanOut metrics
func TestMetricatedFanOut(t *testing.T) {
	// Create a mock metrics collector
	collector := &mockMetricsCollector{}

	// Create stages for FanOut
	stage1 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	stage2 := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToLower(input), nil
	})

	// Create a FanOut
	fanOut := fluxus.NewFanOut(stage1, stage2)

	// Wrap with metrics
	metricated := fluxus.NewMetricatedFanOut(
		fanOut,
		fluxus.WithStageName[string, []string]("test_fan_out"),
		fluxus.WithMetricsCollector[string, []string](collector),
	)

	// Process input
	results, err := metricated.Process(context.Background(), "Hello")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify results
	if len(results) != 2 || results[0] != "HELLO" || results[1] != "hello" {
		t.Errorf("Unexpected results: %v", results)
	}

	// Verify metrics
	if collector.stageStarted != 1 {
		t.Errorf("Expected StageStarted to be called once, got %d", collector.stageStarted)
	}
	if collector.stageCompleted != 1 {
		t.Errorf("Expected StageCompleted to be called once, got %d", collector.stageCompleted)
	}
	if collector.fanOutStarted != 1 {
		t.Errorf("Expected FanOutStarted to be called once, got %d", collector.fanOutStarted)
	}
	if collector.fanOutCompleted != 1 {
		t.Errorf("Expected FanOutCompleted to be called once, got %d", collector.fanOutCompleted)
	}
}

// TestMetricatedFanIn tests the FanIn metrics
func TestMetricatedFanIn(t *testing.T) {
	// Create a mock metrics collector
	collector := &mockMetricsCollector{}

	// Create a FanIn
	fanIn := fluxus.NewFanIn(func(inputs []string) (string, error) {
		return strings.Join(inputs, ", "), nil
	})

	// Wrap with metrics
	metricated := fluxus.NewMetricatedFanIn(
		fanIn,
		fluxus.WithStageName[[]string, string]("test_fan_in"),
		fluxus.WithMetricsCollector[[]string, string](collector),
	)

	// Process input
	result, err := metricated.Process(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify result
	if result != "hello, world" {
		t.Errorf("Expected 'hello, world', got '%s'", result)
	}

	// Verify metrics
	if collector.stageStarted != 1 {
		t.Errorf("Expected StageStarted to be called once, got %d", collector.stageStarted)
	}
	if collector.stageCompleted != 1 {
		t.Errorf("Expected StageCompleted to be called once, got %d", collector.stageCompleted)
	}
	if collector.fanInStarted != 1 {
		t.Errorf("Expected FanInStarted to be called once, got %d", collector.fanInStarted)
	}
	if collector.fanInCompleted != 1 {
		t.Errorf("Expected FanInCompleted to be called once, got %d", collector.fanInCompleted)
	}
}

// TestMetricatedRetry tests the Retry metrics
func TestMetricatedRetry(t *testing.T) {
	// Create a mock metrics collector
	collector := &mockMetricsCollector{}

	// Create a stage that fails a few times then succeeds
	attemptCount := 0
	maxFailures := 2
	stage := fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
		attemptCount++
		if attemptCount <= maxFailures {
			return "", fmt.Errorf("attempt %d failed", attemptCount)
		}
		return fmt.Sprintf("Success on attempt %d", attemptCount), nil
	})

	// Create a retry stage
	retry := fluxus.NewRetry(stage, maxFailures+1)

	// Wrap with metrics
	metricated := fluxus.NewMetricatedRetry(
		retry,
		fluxus.WithStageName[string, string]("test_retry"),
		fluxus.WithMetricsCollector[string, string](collector),
	)

	// Process input
	result, err := metricated.Process(context.Background(), "test")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify result
	expected := "Success on attempt 3"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}

	// Verify metrics
	if collector.stageStarted != 1 {
		t.Errorf("Expected StageStarted to be called once, got %d", collector.stageStarted)
	}
	if collector.stageCompleted != 1 {
		t.Errorf("Expected StageCompleted to be called once, got %d", collector.stageCompleted)
	}
	if collector.retryAttempts != int64(maxFailures+1) {
		t.Errorf("Expected RetryAttempt to be called %d times, got %d", maxFailures+1, collector.retryAttempts)
	}
}

// BenchmarkMetricatedStage benchmarks the overhead of adding metrics to a stage
func BenchmarkMetricatedStage(b *testing.B) {
	// Create a simple stage
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Benchmark without metrics
	b.Run("WithoutMetrics", func(b *testing.B) {
		ctx := context.Background()
		input := "benchmark"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = stage.Process(ctx, input)
		}
	})

	// Benchmark with metrics
	b.Run("WithMetrics", func(b *testing.B) {
		// Create a no-op metrics collector to avoid external dependencies in benchmark
		collector := &mockMetricsCollector{}
		metricated := fluxus.NewMetricatedStage(
			stage,
			fluxus.WithStageName[string, string]("benchmark_stage"),
			fluxus.WithMetricsCollector[string, string](collector),
		)

		ctx := context.Background()
		input := "benchmark"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = metricated.Process(ctx, input)
		}
	})
}
