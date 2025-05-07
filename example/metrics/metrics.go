package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// --- Simple In-Memory Metrics Collector ---

// InMemoryMetricsCollector implements the fluxus.MetricsCollector interface
// and stores metrics counts in memory using atomic operations.
type InMemoryMetricsCollector struct {
	stageStartedCount    int64
	stageCompletedCount  int64
	stageErrorCount      int64
	retryAttemptCount    int64
	fanOutStartedCount   int64
	fanOutCompletedCount int64
	fanInStartedCount    int64
	fanInCompletedCount  int64
	bufferBatchCount     int64

	pipelineStartedCount          int64 // Added
	pipelineCompletedCount        int64 // Added
	stageWorkerConcurrencyCount   int64 // Added
	stageWorkerItemProcessedCount int64 // Added
	stageWorkerItemSkippedCount   int64 // Added
	stageWorkerErrorSentCount     int64 // Added

	windowEmittedCount int64 // Added

	// Store durations (use mutex for slice access)
	stageDurations map[string][]time.Duration
	mu             sync.Mutex
}

// NewInMemoryMetricsCollector creates a new collector.
func NewInMemoryMetricsCollector() *InMemoryMetricsCollector {
	return &InMemoryMetricsCollector{
		stageDurations: make(map[string][]time.Duration),
	}
}

func (m *InMemoryMetricsCollector) StageStarted(ctx context.Context, stageName string) {
	atomic.AddInt64(&m.stageStartedCount, 1)
	fmt.Printf("  📊 Metric: Stage '%s' started\n", stageName)
}

func (m *InMemoryMetricsCollector) StageCompleted(ctx context.Context, stageName string, duration time.Duration) {
	atomic.AddInt64(&m.stageCompletedCount, 1)
	m.mu.Lock()
	m.stageDurations[stageName] = append(m.stageDurations[stageName], duration)
	m.mu.Unlock()
	fmt.Printf("  📊 Metric: Stage '%s' completed in %v\n", stageName, duration)
}

func (m *InMemoryMetricsCollector) StageError(ctx context.Context, stageName string, err error) {
	atomic.AddInt64(&m.stageErrorCount, 1)
	fmt.Printf("  📊 Metric: Stage '%s' errored: %v\n", stageName, err)
}

func (m *InMemoryMetricsCollector) RetryAttempt(ctx context.Context, stageName string, attempt int, err error) {
	atomic.AddInt64(&m.retryAttemptCount, 1)
	fmt.Printf("  📊 Metric: Retry attempt #%d for stage '%s' (error: %v)\n", attempt, stageName, err)
}

func (m *InMemoryMetricsCollector) BufferBatchProcessed(ctx context.Context, batchSize int, duration time.Duration) {
	atomic.AddInt64(&m.bufferBatchCount, 1)
	fmt.Printf("  📊 Metric: Buffer batch processed (size %d) in %v\n", batchSize, duration)
}

func (m *InMemoryMetricsCollector) FanOutStarted(ctx context.Context, stageName string, numStages int) {
	atomic.AddInt64(&m.fanOutStartedCount, 1)
	fmt.Printf("  📊 Metric: FanOut[%s] started (%d stages)\n", stageName, numStages)
}

func (m *InMemoryMetricsCollector) FanOutCompleted(ctx context.Context, stageName string, numStages int, duration time.Duration) {
	atomic.AddInt64(&m.fanOutCompletedCount, 1)
	fmt.Printf("  📊 Metric: FanOut[%s] completed (%d stages) in %v\n", stageName, numStages, duration)
}

func (m *InMemoryMetricsCollector) FanInStarted(ctx context.Context, stageName string, numInputs int) {
	atomic.AddInt64(&m.fanInStartedCount, 1)
	fmt.Printf("  📊 Metric: FanIn[%s] started (%d inputs)\n", stageName, numInputs)
}

func (m *InMemoryMetricsCollector) FanInCompleted(ctx context.Context, stageName string, numInputs int, duration time.Duration) {
	atomic.AddInt64(&m.fanInCompletedCount, 1)
	fmt.Printf("  📊 Metric: FanIn[%s] completed (%d inputs) in %v\n", stageName, numInputs, duration)
}

func (m *InMemoryMetricsCollector) PipelineStarted(ctx context.Context, pipelineName string) {
	atomic.AddInt64(&m.pipelineStartedCount, 1)
	fmt.Printf("  📊 Metric: Pipeline[%s] started\n", pipelineName)
}

func (m *InMemoryMetricsCollector) PipelineCompleted(ctx context.Context, name string, duration time.Duration, err error) {
	atomic.AddInt64(&m.pipelineCompletedCount, 1)
	fmt.Printf("  📊 Metric: Pipeline[%s] completed in %v with (%v)\n", name, duration, err)
}

func (m *InMemoryMetricsCollector) StageWorkerConcurrency(ctx context.Context, stageName string, concurrency int) {
	atomic.AddInt64(&m.stageWorkerConcurrencyCount, 1)
	fmt.Printf("  📊 Metric: StageWorker[%s] concurrency: %d\n", stageName, concurrency)
}

func (m *InMemoryMetricsCollector) StageWorkerItemProcessed(ctx context.Context, stageName string, duration time.Duration) {
	atomic.AddInt64(&m.stageWorkerItemProcessedCount, 1)
	fmt.Printf("  📊 Metric: StageWorker[%s] item processed in %v\n", stageName, duration)
}

func (m *InMemoryMetricsCollector) StageWorkerItemSkipped(ctx context.Context, stageName string, err error) {
	atomic.AddInt64(&m.stageWorkerItemSkippedCount, 1)
	fmt.Printf("  📊 Metric: StageWorker[%s] item skipped: %v\n", stageName, err)
}

func (m *InMemoryMetricsCollector) StageWorkerErrorSent(ctx context.Context, stageName string, err error) {
	atomic.AddInt64(&m.stageWorkerErrorSentCount, 1)
	fmt.Printf("  📊 Metric: StageWorker[%s] error sent: %v\n", stageName, err)
}

func (m *InMemoryMetricsCollector) WindowEmitted(ctx context.Context, stageName string, itemCount int) {
	atomic.AddInt64(&m.windowEmittedCount, 1)
	fmt.Printf("  📊 Metric: Window[%s] emitted %d items\n", stageName, itemCount)
}

// PrintStats displays the collected metrics.
func (m *InMemoryMetricsCollector) PrintStats() {
	fmt.Println("\n📈 Collected Metrics Summary:")
	fmt.Println("----------------------------")
	fmt.Printf("Pipeline Started: %d\n", atomic.LoadInt64(&m.pipelineStartedCount))
	fmt.Printf("Pipeline Completed: %d\n", atomic.LoadInt64(&m.pipelineCompletedCount))
	fmt.Printf("Stage Started: %d\n", atomic.LoadInt64(&m.stageStartedCount))
	fmt.Printf("Stage Completed: %d\n", atomic.LoadInt64(&m.stageCompletedCount))
	fmt.Printf("Stage Errors: %d\n", atomic.LoadInt64(&m.stageErrorCount))
	fmt.Printf("Retry Attempts: %d\n", atomic.LoadInt64(&m.retryAttemptCount))
	fmt.Printf("FanOut Started: %d\n", atomic.LoadInt64(&m.fanOutStartedCount))
	fmt.Printf("FanOut Completed: %d\n", atomic.LoadInt64(&m.fanOutCompletedCount))
	fmt.Printf("FanIn Started: %d\n", atomic.LoadInt64(&m.fanInStartedCount))
	fmt.Printf("FanIn Completed: %d\n", atomic.LoadInt64(&m.fanInCompletedCount))
	fmt.Printf("Buffer Batches: %d\n", atomic.LoadInt64(&m.bufferBatchCount))

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.stageDurations) > 0 {
		fmt.Println("\nStage Durations:")
		for name, durations := range m.stageDurations {
			var total time.Duration
			for _, d := range durations {
				total += d
			}
			avg := time.Duration(0)
			if len(durations) > 0 {
				avg = total / time.Duration(len(durations))
			}
			fmt.Printf("  - %s: %d calls, Avg: %v, Total: %v\n", name, len(durations), avg, total)
		}
	}
}

// --- Pipeline Stages ---

// Stage 1: Prepare the input string (e.g., trim spaces)
func prepareStageFunc(ctx context.Context, input string) (string, error) {
	fmt.Println("   ⚙️ Running Prepare Stage...")
	time.Sleep(20 * time.Millisecond) // Simulate work
	return strings.TrimSpace(input), nil
}

// Stage 2a: Uppercase the input string (part of FanOut)
func uppercaseStageFunc(ctx context.Context, input string) (string, error) {
	fmt.Println("      ⚙️ Running Uppercase Stage (Parallel)...")
	time.Sleep(100 * time.Millisecond) // Simulate work
	return strings.ToUpper(input), nil
}

// Stage 2b: Reverse the string (part of FanOut, simulates potential failure)
var reverseAttemptCount int

func reverseStageFunc(ctx context.Context, input string) (string, error) {
	fmt.Println("      ⚙️ Running Reverse Stage (Parallel)...")
	time.Sleep(150 * time.Millisecond) // Simulate work

	reverseAttemptCount++
	// Fail the first time this stage is called in the demo
	if reverseAttemptCount <= 1 {
		fmt.Println("      ⚠️ Reverse Stage simulating transient failure...")
		return "", errors.New("reverse service temporarily unavailable")
	}

	runes := []rune(input)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	fmt.Println("      ✅ Reverse Stage succeeded.")
	return string(runes), nil
}

// Stage 3: Aggregate results from FanOut (FanIn)
func aggregateResultsFunc(results []string) (string, error) {
	fmt.Println("   ⚙️ Running Aggregate Stage (FanIn)...")
	time.Sleep(40 * time.Millisecond) // Simulate work
	if len(results) != 2 {
		return "", fmt.Errorf("aggregator expected 2 results, got %d", len(results))
	}
	// Combine results: "UPPERCASE | DESREVER"
	return fmt.Sprintf("%s | %s", results[0], results[1]), nil
}

// Stage 4: Add a prefix to the final result
func prefixStageFunc(ctx context.Context, input string) (string, error) {
	fmt.Println("   ⚙️ Running Prefix Stage...")
	time.Sleep(30 * time.Millisecond) // Simulate work
	return "Final: " + input, nil
}

// --- Main Application ---

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Println("📊 Fluxus Metrics Demonstration (with FanOut/FanIn)")
	fmt.Println("==================================================")
	fmt.Println("This example shows how to collect metrics from pipeline stages,")
	fmt.Println("including FanOut and FanIn operations.")

	// 1. Create our custom metrics collector
	collector := NewInMemoryMetricsCollector()

	// 2. Define the pipeline stages
	prepareStage := fluxus.StageFunc[string, string](prepareStageFunc)
	uppercaseStage := fluxus.StageFunc[string, string](uppercaseStageFunc)
	reverseStage := fluxus.StageFunc[string, string](reverseStageFunc)
	prefixStage := fluxus.StageFunc[string, string](prefixStageFunc)

	// 3. Wrap stages with metrics collection
	fmt.Println("\n🔧 Wrapping stages with metrics...")

	// Wrap initial stage
	metricatedPrepare := fluxus.NewMetricatedStage(
		prepareStage,
		fluxus.WithMetricsStageName[string, string]("prepare"),
		fluxus.WithMetricsCollector[string, string](collector),
	)
	fmt.Println("   - Wrapped 'prepare' stage.")

	// Wrap stages for FanOut
	metricatedUppercase := fluxus.NewMetricatedStage(
		uppercaseStage,
		fluxus.WithMetricsStageName[string, string]("uppercase"),
		fluxus.WithMetricsCollector[string, string](collector),
	)
	fmt.Println("   - Wrapped 'uppercase' stage (for FanOut).")

	// Wrap the reverse stage with Retry first, then metrics for the retry operation
	retryReverse := fluxus.NewRetry(reverseStage, 2).WithBackoff(func(attempt int) int {
		return 50 * (1 << attempt) // 50ms, 100ms
	})
	metricatedRetryReverse := fluxus.NewMetricatedRetry(
		retryReverse,
		fluxus.WithMetricsStageName[string, string]("retry-reverse"), // Name for the retry wrapper
		fluxus.WithMetricsCollector[string, string](collector),
	)
	fmt.Println("   - Wrapped 'reverse' stage with Retry and Metrics (for FanOut).")

	// Create FanOut stage
	fanOut := fluxus.NewFanOut(metricatedUppercase, metricatedRetryReverse)

	// Wrap FanOut with metrics
	metricatedFanOut := fluxus.NewMetricatedFanOut(
		fanOut,
		fluxus.WithMetricsStageName[string, []string]("parallel-transform"),
		fluxus.WithMetricsCollector[string, []string](collector),
	)
	fmt.Println("   - Wrapped FanOut stage ('parallel-transform') with Metrics.")

	// Create FanIn stage
	fanIn := fluxus.NewFanIn(aggregateResultsFunc)

	// Wrap FanIn with metrics
	metricatedFanIn := fluxus.NewMetricatedFanIn(
		fanIn,
		fluxus.WithMetricsStageName[[]string, string]("aggregate-results"),
		fluxus.WithMetricsCollector[[]string, string](collector),
	)
	fmt.Println("   - Wrapped FanIn stage ('aggregate-results') with Metrics.")

	// Wrap final stage
	metricatedPrefix := fluxus.NewMetricatedStage(
		prefixStage,
		fluxus.WithMetricsStageName[string, string]("prefix"),
		fluxus.WithMetricsCollector[string, string](collector),
	)
	fmt.Println("   - Wrapped 'prefix' stage.")

	// 4. Chain the metricated stages: Prepare -> FanOut -> FanIn -> Prefix
	fmt.Println("\n🔗 Chaining metricated stages...")
	chainedStage := fluxus.Chain(
		metricatedPrepare,
		fluxus.Chain(
			metricatedFanOut,
			fluxus.Chain(
				metricatedFanIn,
				metricatedPrefix,
			),
		),
	)

	// 5. Create the pipeline
	pipeline := fluxus.NewPipeline(chainedStage)
	fmt.Println("✅ Pipeline built.")

	metricatedPipeline := fluxus.NewMetricatedPipeline(
		pipeline,
		fluxus.WithPipelineName[string, string]("demo-pipeline"),
		fluxus.WithPipelineMetricsCollector[string, string](collector),
	)
	fmt.Println("   - Wrapped pipeline with metrics.")
	fmt.Println("✅ Pipeline wrapped with metrics.")

	// 6. Process some data
	fmt.Println("\n▶️ Processing input ' Hello Metrics World '...")
	ctx := context.Background()
	startTime := time.Now()

	result, err := metricatedPipeline.Process(ctx, " Hello Metrics World ")

	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("\n❌ Pipeline processing failed after %v: %v\n", duration, err)
		// Note: The StageError metric would have already been recorded by the failing stage's wrapper.
	} else {
		fmt.Printf("\n✅ Pipeline processing successful in %v\n", duration)
		fmt.Printf("   Result: %s\n", result)
	}

	// 7. Display collected metrics
	collector.PrintStats()

	fmt.Println("\nDemo Complete!")
}
