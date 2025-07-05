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

// TimeoutOccurred implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) TimeoutOccurred(ctx context.Context, stageName string, configuredTimeout time.Duration, actualDuration time.Duration) {
	fmt.Printf("  📊 Metric: Timeout[%s] occurred - configured: %v, actual: %v\n", stageName, configuredTimeout, actualDuration)
}

// CircuitStateChanged implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) CircuitStateChanged(ctx context.Context, stageName string, fromState, toState string) {
	fmt.Printf("  📊 Metric: CircuitBreaker[%s] state changed: %s -> %s\n", stageName, fromState, toState)
}

// CircuitBreakerRejected implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) CircuitBreakerRejected(ctx context.Context, stageName string) {
	fmt.Printf("  📊 Metric: CircuitBreaker[%s] rejected request (open)\n", stageName)
}

// CircuitBreakerFailureRecorded implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) CircuitBreakerFailureRecorded(ctx context.Context, stageName string, failureCount int, threshold int) {
	fmt.Printf("  📊 Metric: CircuitBreaker[%s] failure recorded: %d/%d\n", stageName, failureCount, threshold)
}

// CircuitBreakerSuccessRecorded implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) CircuitBreakerSuccessRecorded(ctx context.Context, stageName string, successCount int, threshold int) {
	fmt.Printf("  📊 Metric: CircuitBreaker[%s] success recorded: %d/%d\n", stageName, successCount, threshold)
}

// DeadLetterQueueItemSent implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) DeadLetterQueueItemSent(ctx context.Context, stageName string, originalError error) {
	fmt.Printf("  📊 Metric: DLQ[%s] item sent due to: %v\n", stageName, originalError)
}

// DeadLetterQueueHandlerError implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) DeadLetterQueueHandlerError(ctx context.Context, stageName string, dlqError error) {
	fmt.Printf("  📊 Metric: DLQ[%s] handler error: %v\n", stageName, dlqError)
}

// RouterRoutesSelected implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) RouterRoutesSelected(ctx context.Context, stageName string, numRoutesSelected int, totalRoutes int) {
	fmt.Printf("  📊 Metric: Router[%s] selected %d/%d routes\n", stageName, numRoutesSelected, totalRoutes)
}

// RouterNoRouteMatched implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) RouterNoRouteMatched(ctx context.Context, stageName string) {
	fmt.Printf("  📊 Metric: Router[%s] no route matched\n", stageName)
}

// RouterRouteProcessed implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) RouterRouteProcessed(ctx context.Context, stageName string, routeName string, routeIndex int, duration time.Duration) {
	fmt.Printf("  📊 Metric: Router[%s] route '%s' (#%d) processed in %v\n", stageName, routeName, routeIndex, duration)
}

// MapItemProcessed implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) MapItemProcessed(ctx context.Context, stageName string, itemIndex int, duration time.Duration) {
	fmt.Printf("  📊 Metric: Map[%s] item #%d processed in %v\n", stageName, itemIndex, duration)
}

// MapItemError implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) MapItemError(ctx context.Context, stageName string, itemIndex int, err error) {
	fmt.Printf("  📊 Metric: Map[%s] item #%d error: %v\n", stageName, itemIndex, err)
}

// MapConcurrencyLevel implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) MapConcurrencyLevel(ctx context.Context, stageName string, concurrencyLevel int, totalItems int) {
	fmt.Printf("  📊 Metric: Map[%s] concurrency %d for %d items\n", stageName, concurrencyLevel, totalItems)
}

// MapReduceMapPhaseCompleted implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) MapReduceMapPhaseCompleted(ctx context.Context, stageName string, numItems int, numKeys int, duration time.Duration) {
	fmt.Printf("  📊 Metric: MapReduce[%s] map phase: %d items -> %d keys in %v\n", stageName, numItems, numKeys, duration)
}

// MapReduceShufflePhaseCompleted implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) MapReduceShufflePhaseCompleted(ctx context.Context, stageName string, numKeys int, duration time.Duration) {
	fmt.Printf("  📊 Metric: MapReduce[%s] shuffle phase: %d keys in %v\n", stageName, numKeys, duration)
}

// MapReduceReducePhaseCompleted implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) MapReduceReducePhaseCompleted(ctx context.Context, stageName string, numKeys int, numResults int, duration time.Duration) {
	fmt.Printf("  📊 Metric: MapReduce[%s] reduce phase: %d keys -> %d results in %v\n", stageName, numKeys, numResults, duration)
}

// MapReduceKeyGroupSize implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) MapReduceKeyGroupSize(ctx context.Context, stageName string, key string, groupSize int) {
	fmt.Printf("  📊 Metric: MapReduce[%s] key '%s' has %d values\n", stageName, key, groupSize)
}

// FilterItemPassed implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) FilterItemPassed(ctx context.Context, stageName string) {
	fmt.Printf("  📊 Metric: Filter[%s] item passed\n", stageName)
}

// FilterItemDropped implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) FilterItemDropped(ctx context.Context, stageName string) {
	fmt.Printf("  📊 Metric: Filter[%s] item dropped\n", stageName)
}

// FilterPredicateError implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) FilterPredicateError(ctx context.Context, stageName string, err error) {
	fmt.Printf("  📊 Metric: Filter[%s] predicate error: %v\n", stageName, err)
}

// JoinByKeyGroupCreated implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) JoinByKeyGroupCreated(ctx context.Context, stageName string, keyStr string, groupSize int) {
	fmt.Printf("  📊 Metric: JoinByKey[%s] group created for key '%s' with %d items\n", stageName, keyStr, groupSize)
}

// JoinByKeyCompleted implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) JoinByKeyCompleted(ctx context.Context, stageName string, numKeys int, totalItems int, duration time.Duration) {
	fmt.Printf("  📊 Metric: JoinByKey[%s] completed: %d keys, %d items in %v\n", stageName, numKeys, totalItems, duration)
}

// CustomStageMetric implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) CustomStageMetric(ctx context.Context, stageName string, metricName string, value interface{}) {
	fmt.Printf("  📊 Metric: Custom[%s] %s = %v\n", stageName, metricName, value)
}

// CustomStageEvent implements MetricsCollector interface.
func (m *InMemoryMetricsCollector) CustomStageEvent(ctx context.Context, stageName string, eventName string, metadata map[string]interface{}) {
	fmt.Printf("  📊 Metric: Custom[%s] event '%s' with metadata: %v\n", stageName, eventName, metadata)
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
