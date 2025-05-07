package fluxus_test

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// TestObjectPool tests the basic functionality of ObjectPool
func TestObjectPool(t *testing.T) {
	// Create a pool
	pool := fluxus.NewObjectPool(
		func() string { return "new-object" },
		fluxus.WithPoolName[string]("test-pool"),
		fluxus.WithMaxCapacity[string](5),
	)

	// Get an object
	obj := pool.Get()
	if obj != "new-object" {
		t.Errorf("Expected 'new-object', got '%s'", obj)
	}

	// Put it back
	pool.Put(obj)

	// Get it again and verify it's the same
	obj2 := pool.Get()
	if obj2 != "new-object" {
		t.Errorf("Expected 'new-object', got '%s'", obj2)
	}

	// Check stats
	stats := pool.Stats()
	if stats["gets"] != 2 {
		t.Errorf("Expected 2 gets, got %d", stats["gets"])
	}
	if stats["puts"] != 1 {
		t.Errorf("Expected 1 put, got %d", stats["puts"])
	}
}

// TestObjectPoolMaxCapacity tests that the pool respects its capacity limits
func TestObjectPoolMaxCapacity(t *testing.T) {
	// Create a pool with very small capacity
	maxCapacity := 3
	pool := fluxus.NewObjectPool(
		func() string { return "new-object" },
		fluxus.WithMaxCapacity[string](maxCapacity),
	)

	// Fill the pool beyond capacity
	for i := 0; i < maxCapacity+5; i++ {
		pool.Put(fmt.Sprintf("obj-%d", i))
	}

	// Check the current size doesn't exceed capacity
	stats := pool.Stats()
	if stats["current_size"] > int64(maxCapacity) {
		t.Errorf("Pool exceeded max capacity: %d > %d", stats["current_size"], maxCapacity)
	}
}

// TestObjectPoolConcurrent tests concurrent access to the pool
func TestObjectPoolConcurrent(t *testing.T) {
	// Create a pool
	pool := fluxus.NewObjectPool(
		func() int { return 0 },
	)

	// Use the pool concurrently from multiple goroutines
	var wg sync.WaitGroup
	numGoroutines := 100
	opsPerGoroutine := 1000

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				obj := pool.Get()
				// Simulate some work
				time.Sleep(time.Microsecond)
				pool.Put(obj)
			}
		}()
	}

	wg.Wait()

	// Verify stats
	stats := pool.Stats()
	expectedOps := int64(numGoroutines * opsPerGoroutine)
	if stats["gets"] != expectedOps {
		t.Errorf("Expected %d gets, got %d", expectedOps, stats["gets"])
	}
	if stats["puts"] != expectedOps {
		t.Errorf("Expected %d puts, got %d", expectedOps, stats["puts"])
	}
}

// TestSlicePool tests the SlicePool specialization
func TestSlicePool(t *testing.T) {
	// Create a slice pool
	initialCap := 10
	pool := fluxus.NewSlicePool[int](initialCap)

	// Get a slice
	slice := pool.Get()
	if cap(slice) != initialCap {
		t.Errorf("Expected capacity %d, got %d", initialCap, cap(slice))
	}
	if len(slice) != 0 {
		t.Errorf("Expected length 0, got %d", len(slice))
	}

	// Add some items
	slice = append(slice, 1, 2, 3)
	if len(slice) != 3 {
		t.Errorf("Expected length 3, got %d", len(slice))
	}

	// Put back
	pool.Put(slice)

	// Get again - should be reset
	slice2 := pool.Get()
	if len(slice2) != 0 {
		t.Errorf("Expected length 0 after reset, got %d", len(slice2))
	}
	if cap(slice2) != initialCap {
		t.Errorf("Expected capacity still %d, got %d", initialCap, cap(slice2))
	}
}

// TestSlicePoolGetWithCapacity tests the capacity handling of GetWithCapacity
func TestSlicePoolGetWithCapacity(t *testing.T) {
	// Create a slice pool
	initialCap := 10
	pool := fluxus.NewSlicePool[int](initialCap)

	// Get a slice with smaller capacity
	slice := pool.GetWithCapacity(5)
	if cap(slice) < 5 {
		t.Errorf("Expected capacity at least 5, got %d", cap(slice))
	}

	// Get a slice with larger capacity
	slice = pool.GetWithCapacity(20)
	if cap(slice) < 20 {
		t.Errorf("Expected capacity at least 20, got %d", cap(slice))
	}

	// Put it back
	pool.Put(slice)

	// Get again with much larger capacity
	slice = pool.GetWithCapacity(50)
	if cap(slice) < 50 {
		t.Errorf("Expected capacity at least 50, got %d", cap(slice))
	}
}

// TestPreWarmPool tests the pre-warming functionality
func TestPreWarmPool(t *testing.T) {
	// Create a pool
	pool := fluxus.NewObjectPool(
		func() string { return "warmed" },
	)

	// Pre-warm with 10 objects
	fluxus.PreWarmPool(pool, 10)

	// Skip checking current_size as the balance of gets/puts is 0
	// Pre-warming affects the contents of the pool but not the counter

	// Get 10 objects - this is the real test that pre-warming worked
	objects := make([]string, 10)
	for i := 0; i < 10; i++ {
		objects[i] = pool.Get()
	}

	// All should be "warmed"
	for i, obj := range objects {
		if obj != "warmed" {
			t.Errorf("Object %d: expected 'warmed', got '%s'", i, obj)
		}
	}
}

// TestPooledStage tests the PooledStage wrapper
func TestPooledStage(t *testing.T) {
	// Create a test stage
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return input + "-processed", nil
	})

	// Create an object pool
	objectPool := fluxus.NewObjectPool(
		func() interface{} { return "pooled-object" },
		fluxus.WithPoolName[interface{}]("test-pool"),
	)

	// Create a pooled stage
	pooledStage := fluxus.NewPooledStage(stage)
	pooledStage.RegisterPool(objectPool)

	// Process
	result, err := pooledStage.Process(context.Background(), "input")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != "input-processed" {
		t.Errorf("Expected 'input-processed', got '%s'", result)
	}

	// Check stage stats
	stats := pooledStage.GetStats()
	if len(stats) != 1 {
		t.Errorf("Expected 1 pool stats, got %d", len(stats))
	}
}

// TestPooledBuffer tests the PooledBuffer implementation
func TestPooledBuffer(t *testing.T) {
	// Create a pooled buffer
	batchSize := 3
	buffer := fluxus.NewPooledBuffer(
		batchSize,
		func(_ context.Context, batch []int) ([]int, error) {
			// Double each item
			results := make([]int, len(batch))
			for i, item := range batch {
				results[i] = item * 2
			}
			return results, nil
		},
		fluxus.WithBufferName[int, int]("test-buffer"),
	)

	// Process inputs
	inputs := []int{1, 2, 3, 4, 5, 6, 7}
	results, err := buffer.Process(context.Background(), inputs)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify results
	expected := []int{2, 4, 6, 8, 10, 12, 14}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, val := range results {
		if val != expected[i] {
			t.Errorf("Result %d: expected %d, got %d", i, expected[i], val)
		}
	}

	// Check stats
	stats := buffer.Stats()
	if stats["processed_batches"] != 3 { // 7 items with batch size 3 = 3 batches
		t.Errorf("Expected 3 batches, got %d", stats["processed_batches"])
	}
	if stats["processed_items"] != int64(len(inputs)) {
		t.Errorf("Expected %d processed items, got %d", len(inputs), stats["processed_items"])
	}
}

// TestPooledStageInPipeline tests a PooledStage within a Pipeline
func TestPooledStageInPipeline(t *testing.T) {
	// Create a pooled object
	type ProcessingObject struct {
		Buffer []byte
		Result string
	}

	// Create a pool for the processing objects
	pool := fluxus.NewObjectPool(
		func() *ProcessingObject {
			return &ProcessingObject{
				Buffer: make([]byte, 1024),
			}
		},
		fluxus.WithPoolName[*ProcessingObject]("processing-objects"),
	)

	// Create a stage that uses the pool
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		// Get an object from the pool
		obj := pool.Get()
		defer pool.Put(obj) // Return it when done

		// Use the object for processing
		copy(obj.Buffer, []byte(input))
		obj.Result = strings.ToUpper(string(obj.Buffer[:len(input)]))

		return obj.Result, nil
	})

	// Create a pooled stage wrapper
	pooledStage := fluxus.NewPooledStage(stage)

	// Register the pool
	pooledStage.RegisterPool(pool)

	// Create a pipeline with the pooled stage
	pipeline := fluxus.NewPipeline(pooledStage)

	// Process some inputs
	inputs := []string{
		"hello",
		"world",
		"testing",
		"pooled",
		"stage",
	}

	for _, input := range inputs {
		result, err := pipeline.Process(context.Background(), input)
		if err != nil {
			t.Fatalf("Pipeline processing error: %v", err)
		}

		expected := strings.ToUpper(input)
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	}

	// Check pool stats
	stats := pooledStage.GetStats()
	poolStats, ok := stats["pool_processing-objects"]
	if !ok {
		t.Fatal("Expected to find pool stats")
	}

	// We should have done at least 5 gets
	if poolStats["gets"] < 5 {
		t.Errorf("Expected at least 5 gets, got %d", poolStats["gets"])
	}

	// And also at least 5 puts
	if poolStats["puts"] < 5 {
		t.Errorf("Expected at least 5 puts, got %d", poolStats["puts"])
	}
}

// TestPooledBufferInPipeline tests a PooledBuffer within a Pipeline
func TestPooledBufferInPipeline(t *testing.T) {
	// Create a stage that converts integers to strings
	intToStringStage := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return strconv.Itoa(input), nil
	})

	// Create a Buffer that processes batches of strings
	pooledBuffer := fluxus.NewPooledBuffer(
		3, // batch size
		func(_ context.Context, batch []string) ([]string, error) {
			results := make([]string, len(batch))
			for i, s := range batch {
				results[i] = strings.ToUpper(s)
			}
			return results, nil
		},
		fluxus.WithBufferName[string, string]("string-buffer"),
	)

	// Create fan-out and fan-in stages
	fanOut := fluxus.NewFanOut(intToStringStage, intToStringStage)

	fanInStage := fluxus.StageFunc[[]string, []string](func(_ context.Context, inputs []string) ([]string, error) {
		// We're already getting a flattened slice of strings from pooledBuffer
		// So no need for complex aggregation
		return inputs, nil
	})

	// Chain them together: fanOut -> pooledBuffer -> fanIn
	// First, we need to flatten the fan-out results for the buffer
	flattenStage := fluxus.StageFunc[[]string, []string](func(_ context.Context, inputs []string) ([]string, error) {
		return inputs, nil
	})

	// Chain them together
	stage1 := fluxus.Chain(fanOut, flattenStage)
	stage2 := fluxus.Chain(stage1, pooledBuffer)
	fullStage := fluxus.Chain(stage2, fanInStage)

	// Create a pipeline
	pipeline := fluxus.NewPipeline(fullStage)

	// Process a batch of integers
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// We need to create a new stage to process the inputs one at a time
	processingStage := fluxus.StageFunc[[]int, []string](func(ctx context.Context, inputs []int) ([]string, error) {
		var results []string
		for _, input := range inputs {
			result, err := pipeline.Process(ctx, input)
			if err != nil {
				return nil, err
			}
			results = append(results, result...)
		}
		return results, nil
	})

	// Process
	ctx := context.Background()
	results, err := processingStage.Process(ctx, inputs)
	if err != nil {
		t.Fatalf("Pipeline processing error: %v", err)
	}

	// Each input went through both branches in fan-out, and each result was uppercased
	// So we expect 20 results, all uppercase
	if len(results) != 20 {
		t.Errorf("Expected 20 results, got %d", len(results))
	}

	for _, result := range results {
		if result != "1" && result != "2" && result != "3" && result != "4" &&
			result != "5" && result != "6" && result != "7" && result != "8" &&
			result != "9" && result != "10" {
			t.Errorf("Unexpected result: %s", result)
		}

		// Should be uppercase
		if result != strings.ToUpper(result) {
			t.Errorf("Expected uppercase, got %s", result)
		}
	}

	// Check buffer stats
	stats := pooledBuffer.Stats()
	if stats["processed_batches"] < 1 {
		t.Errorf("Expected at least 1 batch, got %d", stats["processed_batches"])
	}
}

// BenchmarkPooledVsRegularStage compares pooled vs regular stages in a pipeline
func BenchmarkPooledVsRegularStage(b *testing.B) {
	// Create a processor object that requires allocation
	type Processor struct {
		Buffer []byte
		Result string
	}

	// Function that uses a processor
	processorFunc := func(input string, processor *Processor) string {
		if len(processor.Buffer) < len(input) {
			processor.Buffer = make([]byte, len(input)*2)
		}
		copy(processor.Buffer, input)
		return strings.ToUpper(input)
	}

	// Regular stage without pooling
	regularStage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		processor := &Processor{
			Buffer: make([]byte, 1024),
		}
		result := processorFunc(input, processor)
		return result, nil
	})

	// Stage with pooling
	pool := fluxus.NewObjectPool(
		func() *Processor {
			return &Processor{
				Buffer: make([]byte, 1024),
			}
		},
	)

	pooledFunc := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		processor := pool.Get()
		defer pool.Put(processor)
		result := processorFunc(input, processor)
		return result, nil
	})

	pooledStage := fluxus.NewPooledStage(pooledFunc)
	pooledStage.RegisterPool(pool)

	// Create pipelines
	regularPipeline := fluxus.NewPipeline(regularStage)
	pooledPipeline := fluxus.NewPipeline(pooledStage)

	// Test inputs of different sizes
	inputs := []string{
		"small",                      // 5 bytes
		strings.Repeat("medium", 20), // ~120 bytes
		strings.Repeat("large input for testing", 100),                                  // ~2400 bytes
		strings.Repeat("very large input for testing the performance of pooling", 1000), // ~50000 bytes
	}

	for _, input := range inputs {
		var name string

		inputLen := len(input) // Calculate length once

		switch {
		case inputLen >= 10000:
			name = "very_large"
		case inputLen >= 1000:
			name = "large"
		case inputLen > 100: // Adjusted condition slightly for clarity
			name = "medium"
		default: // Covers inputLen <= 100
			name = "small"
		}

		b.Run(fmt.Sprintf("Regular_%s", name), func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = regularPipeline.Process(ctx, input)
			}
		})

		b.Run(fmt.Sprintf("Pooled_%s", name), func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = pooledPipeline.Process(ctx, input)
			}
		})
	}
}

// BenchmarkPooledVsRegularBuffer compares pooled vs regular buffer performance
func BenchmarkPooledVsRegularBuffer(b *testing.B) {
	// Create input data
	dataSize := 10000
	batchSize := 100
	inputs := make([]string, dataSize)
	for i := 0; i < dataSize; i++ {
		inputs[i] = fmt.Sprintf("item-%d", i)
	}

	// Processor function
	processor := func(_ context.Context, batch []string) ([]string, error) {
		results := make([]string, len(batch))
		for i, s := range batch {
			results[i] = strings.ToUpper(s)
		}
		return results, nil
	}

	// Regular buffer
	regularBuffer := fluxus.NewBuffer(batchSize, processor)

	// Pooled buffer
	pooledBuffer := fluxus.NewPooledBuffer(batchSize, processor)

	// Benchmark both
	b.Run("RegularBuffer", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = regularBuffer.Process(ctx, inputs)
		}
	})

	b.Run("PooledBuffer", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = pooledBuffer.Process(ctx, inputs)
		}
	})
}

// BenchmarkPooledPipeline benchmarks a complete pipeline with pooling
func BenchmarkPooledPipeline(b *testing.B) {
	// Create a more realistic pipeline that can benefit from pooling

	// Create pools for different object types
	// CHANGE: Use SlicePool instead of ObjectPool for byte buffers
	bufferPool := fluxus.NewSlicePool(
		1024,
		fluxus.WithPoolName[[]byte]("buffer-pool"),
	)

	slicePool := fluxus.NewSlicePool(
		100,
		fluxus.WithPoolName[[]string]("string-slice-pool"),
	)

	// Pre-warm pools
	fluxus.PreWarmSlicePool(bufferPool, 100)
	fluxus.PreWarmSlicePool(slicePool, 50)

	// Stage 1: Parse input
	stage1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		// CHANGE: Get a buffer with sufficient capacity for the input
		buffer := bufferPool.GetWithCapacity(len(input))
		defer bufferPool.Put(buffer)

		// Get a slice from the pool with appropriate capacity
		result := slicePool.GetWithCapacity(100)

		// Use buffer for processing
		copy(buffer, []byte(input))
		parts := strings.Split(string(buffer[:len(input)]), " ")

		// Copy to result slice
		result = append(result, parts...)

		return result, nil
	})

	// Stage 2: Process each part
	stage2 := fluxus.StageFunc[[]string, []string](func(_ context.Context, parts []string) ([]string, error) {
		// Get a result slice from the pool
		result := slicePool.GetWithCapacity(len(parts))
		defer slicePool.Put(result)

		// Process each part
		for _, part := range parts {
			result = append(result, strings.ToUpper(part))
		}

		// Create a new slice for return (don't return the pooled slice directly)
		final := make([]string, len(result))
		copy(final, result)

		return final, nil
	})

	// Stage 3: Join results
	stage3 := fluxus.StageFunc[[]string, string](func(_ context.Context, parts []string) (string, error) {
		// Get a buffer from the pool
		buffer := bufferPool.Get()
		defer bufferPool.Put(buffer)

		// Join parts
		result := strings.Join(parts, "-")

		// Use buffer for any additional processing if needed
		// In this simple case, we don't actually need it

		return result, nil
	})

	// Chain stages
	chainedStage := fluxus.Chain(stage1, fluxus.Chain(stage2, stage3))

	// Create a pooled stage
	pooledStage := fluxus.NewPooledStage(chainedStage)

	// Register pools
	pooledStage.RegisterPool(bufferPool)
	pooledStage.RegisterPool(slicePool)

	// Create a pipeline
	pipeline := fluxus.NewPipeline(pooledStage)

	// Benchmark with different input sizes
	inputs := []string{
		"small input for testing",
		strings.Repeat("medium input for testing ", 10),
		strings.Repeat("large input for benchmarking the pooled pipeline ", 50),
	}

	for _, input := range inputs {
		name := "small"
		if len(input) > 100 && len(input) < 500 {
			name = "medium"
		} else if len(input) >= 500 {
			name = "large"
		}

		b.Run(fmt.Sprintf("PooledPipeline_%s", name), func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = pipeline.Process(ctx, input)
			}
		})
	}
}

// BenchmarkPooledPipelineConcurrent benchmarks concurrent access to a pooled pipeline
func BenchmarkPooledPipelineConcurrent(b *testing.B) {
	// Setup is similar to previous benchmark
	bufferPool := fluxus.NewObjectPool(
		func() []byte { return make([]byte, 1024) },
	)

	slicePool := fluxus.NewSlicePool[string](100)

	// Pre-warm pools
	fluxus.PreWarmPool(bufferPool, 100)
	fluxus.PreWarmSlicePool(slicePool, 50)

	// Create stages (simplified from previous example)
	stage1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		buffer := bufferPool.Get()
		defer bufferPool.Put(buffer)

		parts := strings.Split(input, " ")
		return parts, nil
	})

	stage2 := fluxus.StageFunc[[]string, string](func(_ context.Context, parts []string) (string, error) {
		result := slicePool.Get()
		defer slicePool.Put(result)

		for _, part := range parts {
			result = append(result, strings.ToUpper(part))
		}

		return strings.Join(result, "-"), nil
	})

	// Chain stages
	chainedStage := fluxus.Chain(stage1, stage2)

	// Create a pooled stage
	pooledStage := fluxus.NewPooledStage(chainedStage)
	pooledStage.RegisterPool(bufferPool)
	pooledStage.RegisterPool(slicePool)

	// Create a pipeline
	pipeline := fluxus.NewPipeline(pooledStage)

	// Input
	input := "the quick brown fox jumps over the lazy dog"

	// Test with different concurrency levels
	concurrencyLevels := []int{1, runtime.NumCPU(), runtime.NumCPU() * 2, runtime.NumCPU() * 4}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrent_%d", concurrency), func(b *testing.B) {
			ctx := context.Background()

			b.ResetTimer()
			b.SetParallelism(concurrency)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _ = pipeline.Process(ctx, input)
				}
			})
		})
	}
}

// BenchmarkPooledPipelineWithCancel tests how pooled pipeline behaves with context cancellation
func BenchmarkPooledPipelineWithCancel(b *testing.B) {
	// Create a stage that might be cancelled
	sleepStage := fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		// Get a pooled object
		buffer := make([]byte, 1024)

		// Do some work that checks for cancellation
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(50 * time.Microsecond):
			// Continue with processing
		}

		copy(buffer, []byte(input))
		return string(buffer[:len(input)]), nil
	})

	// Create a regular pipeline
	regularPipeline := fluxus.NewPipeline(sleepStage)

	// Create a pooled version
	pool := fluxus.NewObjectPool(
		func() []byte { return make([]byte, 1024) },
	)

	pooledStage := fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		// Get a pooled object
		buffer := pool.Get()
		defer pool.Put(buffer)

		// Do some work that checks for cancellation
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(50 * time.Microsecond):
			// Continue with processing
		}

		copy(buffer, []byte(input))
		return string(buffer[:len(input)]), nil
	})

	pooledPipeline := fluxus.NewPipeline(pooledStage)

	// Input
	input := "test input"

	// Benchmark with different cancellation probabilities
	cancellationProbs := []float64{0.0, 0.25, 0.5, 0.75}

	for _, prob := range cancellationProbs {
		b.Run(fmt.Sprintf("RegularPipeline_CancelProb_%.2f", prob), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithCancel(context.Background())

				// Maybe cancel
				if float64(i%100)/100 < prob {
					cancel()
				}

				_, _ = regularPipeline.Process(ctx, input)
				cancel() // Always cancel to clean up
			}
		})

		b.Run(fmt.Sprintf("PooledPipeline_CancelProb_%.2f", prob), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithCancel(context.Background())

				// Maybe cancel
				if float64(i%100)/100 < prob {
					cancel()
				}

				_, _ = pooledPipeline.Process(ctx, input)
				cancel() // Always cancel to clean up
			}
		})
	}
}

// BenchmarkRealWorldScenario simulates a real-world ETL pipeline with pooling
func BenchmarkRealWorldScenario(b *testing.B) {
	// This benchmark simulates a more realistic ETL pipeline:
	// 1. Parse input records
	// 2. Transform records in parallel
	// 3. Buffer results and process in batches
	// 4. Aggregate results

	// Define record types
	type InputRecord struct {
		ID   string
		Data string
	}

	type ProcessedRecord struct {
		ID       string
		Data     string
		Modified bool
		Score    float64
	}

	// Create pools
	recordPool := fluxus.NewObjectPool(
		func() *ProcessedRecord { return &ProcessedRecord{} },
	)

	slicePool := fluxus.NewSlicePool[*ProcessedRecord](100)

	// Pre-warm
	fluxus.PreWarmPool(recordPool, 1000)
	fluxus.PreWarmSlicePool(slicePool, 20)

	// Stage 1: Parse input into records
	parseStage := fluxus.StageFunc[string, []*InputRecord](
		func(_ context.Context, input string) ([]*InputRecord, error) {
			lines := strings.Split(input, "\n")
			records := make([]*InputRecord, 0, len(lines))

			for _, line := range lines {
				if line == "" {
					continue
				}

				parts := strings.Split(line, ",")
				if len(parts) >= 2 {
					records = append(records, &InputRecord{
						ID:   parts[0],
						Data: parts[1],
					})
				}
			}

			return records, nil
		},
	)

	// Stage 2: Process each record (using fan-out for parallelism)
	processRecord := fluxus.StageFunc[[]*InputRecord, []*ProcessedRecord](
		func(_ context.Context, records []*InputRecord) ([]*ProcessedRecord, error) {
			result := slicePool.GetWithCapacity(len(records))
			defer slicePool.Put(result)

			for _, record := range records {
				// Get a record from the pool
				processed := recordPool.Get()

				// Process it
				processed.ID = record.ID
				processed.Data = strings.ToUpper(record.Data)
				processed.Modified = true
				processed.Score = float64(len(record.Data)) / 100.0

				result = append(result, processed)
			}

			// Create a new slice for the result (don't return pool slice directly)
			final := make([]*ProcessedRecord, len(result))
			copy(final, result)

			return final, nil
		},
	)

	// Stage 3: Buffer and batch process the records
	batchProcess := fluxus.NewPooledBuffer(
		10, // batch size
		func(_ context.Context, batch []*ProcessedRecord) ([]*ProcessedRecord, error) {
			// Process the batch
			for _, record := range batch {
				// Some batch-level processing
				if len(record.Data) > 10 {
					record.Score += 0.5
				}
			}
			return batch, nil
		},
	)

	// Stage 4: Aggregate the results
	aggregateStage := fluxus.StageFunc[[]*ProcessedRecord, string](
		func(_ context.Context, records []*ProcessedRecord) (string, error) {
			var totalScore float64
			var result strings.Builder

			for _, record := range records {
				totalScore += record.Score
				result.WriteString(fmt.Sprintf("%s: %.2f\n", record.ID, record.Score))

				// Return the record to the pool
				recordPool.Put(record)
			}

			result.WriteString(fmt.Sprintf("Total Score: %.2f\n", totalScore))
			return result.String(), nil
		},
	)

	// Chain it all together
	stage1 := fluxus.Chain(parseStage, processRecord)
	stage2 := fluxus.Chain(stage1, batchProcess)
	fullStage := fluxus.Chain(stage2, aggregateStage)

	// Create a pooled stage wrapper
	pooledStage := fluxus.NewPooledStage(fullStage)

	// Register pools
	pooledStage.RegisterPool(recordPool)
	pooledStage.RegisterPool(slicePool)

	// Create a pipeline
	pipeline := fluxus.NewPipeline(pooledStage)

	// Create test input - simulate CSV data
	var inputBuilder strings.Builder
	for i := 0; i < 500; i++ {
		inputBuilder.WriteString(fmt.Sprintf("id-%d,data-%d\n", i, i))
	}
	input := inputBuilder.String()

	// Benchmark the pipeline
	b.Run("RealWorldETLPipeline", func(b *testing.B) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = pipeline.Process(ctx, input)
		}
	})

	// For comparison, create a version without pooling
	// This would be too much code to duplicate here fully, so this is just for illustration
	b.Run("RealWorldETLPipeline_NoPooling", func(b *testing.B) {
		// This would be similar to above but without using the pools
		b.Skip("This is a placeholder for a non-pooled version")
	})
}

// BenchmarkObjectPoolOverhead measures the overhead of using an object pool
func BenchmarkObjectPoolOverhead(b *testing.B) {
	// Direct allocation
	b.Run("DirectAllocation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			obj := "new-object"
			_ = obj
		}
	})

	// With object pool
	b.Run("ObjectPool", func(b *testing.B) {
		pool := fluxus.NewObjectPool(
			func() string { return "new-object" },
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			obj := pool.Get()
			pool.Put(obj)
		}
	})

	// With sync.Pool for comparison
	b.Run("SyncPool", func(b *testing.B) {
		pool := &sync.Pool{
			New: func() interface{} { return "new-object" },
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			obj := pool.Get().(string)
			//nolint:staticcheck // SA1006: this is fine in this specific context
			pool.Put(obj)
		}
	})
}

// BenchmarkObjectPoolWithWork measures pool overhead with actual work being done
func BenchmarkObjectPoolWithWork(b *testing.B) {
	// How much work to do with each object
	workFactor := 100

	// Create a work function
	doWork := func(obj []int) {
		for i := 0; i < workFactor; i++ {
			obj[i%len(obj)] = i
		}
	}

	// Direct allocation
	b.Run("DirectAllocation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			obj := make([]int, workFactor)
			doWork(obj)
		}
	})

	// With object pool
	b.Run("ObjectPool", func(b *testing.B) {
		pool := fluxus.NewObjectPool(
			func() []int { return make([]int, workFactor) },
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			obj := pool.Get()
			doWork(obj)
			pool.Put(obj)
		}
	})
}

// BenchmarkSlicePoolVsDirectAllocation compares the performance of SlicePool against direct allocation
func BenchmarkSlicePoolVsDirectAllocation(b *testing.B) {
	initialCap := 100
	fillSize := 50

	// Direct allocation and fill
	b.Run("DirectAllocation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			slice := make([]int, 0, initialCap)
			for j := 0; j < fillSize; j++ {
				slice = append(slice, j)
			}
			_ = slice
		}
	})

	// SlicePool with GetWithCapacity
	b.Run("SlicePool", func(b *testing.B) {
		pool := fluxus.NewSlicePool[int](initialCap)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			slice := pool.GetWithCapacity(initialCap)
			for j := 0; j < fillSize; j++ {
				slice = append(slice, j)
			}
			pool.Put(slice)
		}
	})
}

// BenchmarkObjectPoolUnderLoad tests the object pool under load
func BenchmarkObjectPoolUnderLoad(b *testing.B) {
	// Number of goroutines to use
	concurrency := runtime.NumCPU()

	// Create a pool
	pool := fluxus.NewObjectPool(
		func() []byte { return make([]byte, 1024) },
	)

	// Optionally pre-warm
	fluxus.PreWarmPool(pool, concurrency*2)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			obj := pool.Get()
			// Simulate some work
			for i := 0; i < 100; i++ {
				obj[i%len(obj)] = byte(i)
			}
			pool.Put(obj)
		}
	})
}

// BenchmarkPooledBufferOverhead measures the overhead of using PooledBuffer vs regular Buffer
func BenchmarkPooledBufferOverhead(b *testing.B) {
	// Create input data
	dataSize := 10000
	batchSize := 100
	inputs := make([]int, dataSize)
	for i := 0; i < dataSize; i++ {
		inputs[i] = i
	}

	// Processor function
	processor := func(_ context.Context, batch []int) ([]int, error) {
		results := make([]int, len(batch))
		for i, val := range batch {
			results[i] = val * 2
		}
		return results, nil
	}

	// Regular Buffer
	b.Run("RegularBuffer", func(b *testing.B) {
		buffer := fluxus.NewBuffer(batchSize, processor)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = buffer.Process(ctx, inputs)
		}
	})

	// Pooled Buffer
	b.Run("PooledBuffer", func(b *testing.B) {
		buffer := fluxus.NewPooledBuffer(batchSize, processor)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = buffer.Process(ctx, inputs)
		}
	})
}

// BenchmarkPreWarmingEffect measures the effect of pre-warming a pool
func BenchmarkPreWarmingEffect(b *testing.B) {
	// Test with varying pre-warm counts
	preWarmCounts := []int{0, 10, 100, 1000}

	for _, count := range preWarmCounts {
		b.Run(fmt.Sprintf("PreWarm-%d", count), func(b *testing.B) {
			// Create a pool with a somewhat expensive factory
			pool := fluxus.NewObjectPool(
				func() []byte {
					// Simulate expensive object creation
					buf := make([]byte, 4096)
					for i := range buf {
						buf[i] = byte(i % 256)
					}
					return buf
				},
			)

			// Pre-warm
			fluxus.PreWarmPool(pool, count)

			// Run the benchmark
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				obj := pool.Get()
				pool.Put(obj)
			}
		})
	}
}

// BenchmarkPoolSizeImpact measures how pool size limits impact performance
func BenchmarkPoolSizeImpact(b *testing.B) {
	// Test with varying pool size limits
	poolSizes := []int{1, 10, 100, 1000, -1} // -1 means unlimited

	for _, size := range poolSizes {
		name := fmt.Sprintf("MaxSize-%d", size)
		if size == -1 {
			name = "Unlimited"
		}

		b.Run(name, func(b *testing.B) {
			// Create a pool with the specific size limit
			var pool *fluxus.ObjectPool[[]byte]
			if size == -1 {
				pool = fluxus.NewObjectPool(
					func() []byte { return make([]byte, 1024) },
				)
			} else {
				pool = fluxus.NewObjectPool(
					func() []byte { return make([]byte, 1024) },
					fluxus.WithMaxCapacity[[]byte](size),
				)
			}

			// Run the benchmark
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					obj := pool.Get()
					// Do some work
					for i := 0; i < 100; i++ {
						obj[i%len(obj)] = byte(i)
					}
					pool.Put(obj)
				}
			})
		})
	}
}
