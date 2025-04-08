package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// LargeObject simulates a large object that is expensive to create
type LargeObject struct {
	ID        int
	Name      string
	Data      []byte            // Simulates large data
	Metadata  map[string]string // Maps are expensive to allocate
	Timestamp time.Time
	Tags      []string
	Processed bool
}

// ProcessedResult represents a transformed object
type ProcessedResult struct {
	ObjectID    int
	SizeBytes   int
	TagCount    int
	MetaCount   int
	Status      string // Keep status short for demo output
	ProcessedAt time.Time
}

// generateLargeObjects creates test data with larger objects
func generateLargeObjects(count int) []LargeObject {
	fmt.Printf("⚙️ Generating %d large test objects...\n", count)
	objects := make([]LargeObject, count)

	for i := 0; i < count; i++ {
		dataSize := 5*1024 + rand.Intn(5*1024) // 5-10KB
		metaSize := 20 + rand.Intn(20)         // 20-40 entries
		tagCount := 5 + rand.Intn(10)          // 5-15 tags

		metadata := make(map[string]string, metaSize)
		for j := 0; j < metaSize; j++ {
			key := fmt.Sprintf("meta-key-%d", j)
			value := fmt.Sprintf("meta-value-%d-%d-%s", i, j, strings.Repeat("x", 10)) // Shorter value
			metadata[key] = value
		}

		tags := make([]string, tagCount)
		for j := 0; j < tagCount; j++ {
			tags[j] = fmt.Sprintf("tag-%d-%s", j, strings.Repeat("y", 5)) // Shorter tag
		}

		data := make([]byte, dataSize)
		// rand.Read(data) // Skip filling for faster generation in demo

		objects[i] = LargeObject{
			ID:        i + 1,
			Name:      fmt.Sprintf("Object-%d", i+1), // Shorter name
			Data:      data,
			Metadata:  metadata,
			Timestamp: time.Now().Add(-time.Duration(rand.Intn(10000)) * time.Minute),
			Tags:      tags,
			Processed: false,
		}
	}
	fmt.Println("✅ Generation complete.")
	return objects
}

// --- Processing Logic (Common) ---

// processObject simulates processing an object, potentially using pooled resources
func processObject(
	obj LargeObject,
	builder *strings.Builder, // Pass builder explicitly
	tempBuffer []byte, // Pass buffer explicitly
	tempStrings []string, // Pass strings slice explicitly
	tempMap1 map[string]int, // Pass maps explicitly
	tempMap2 map[string][]byte,
	tempMap3 map[string]string,
	allKeys []string,
) (ProcessedResult, error) {
	// Reset builder and slices
	builder.Reset()
	tempStrings = tempStrings[:0]
	allKeys = allKeys[:0]

	// Clear maps
	for k := range tempMap1 {
		delete(tempMap1, k)
	}
	for k := range tempMap2 {
		delete(tempMap2, k)
	}
	for k := range tempMap3 {
		delete(tempMap3, k)
	}

	// Simulate processing that requires temporary allocations
	for i := 0; i < 5; i++ { // Reduced iterations for demo speed
		// Use the provided buffer
		workBuffer := tempBuffer
		if len(workBuffer) < 1024 { // Ensure minimum size
			workBuffer = make([]byte, 1024)
		}

		// Do some work with the buffer
		for j := 0; j < 100; j++ { // Reduced inner loop
			if j < len(obj.Data) {
				workBuffer[j] = obj.Data[j] ^ byte(i)
			} else {
				workBuffer[j] = byte(j % 256)
			}
		}

		// Use the provided string slice
		workStrings := tempStrings
		if cap(workStrings) < 5 { // Ensure minimum capacity
			workStrings = make([]string, 0, 5)
		}
		for j := 0; j < 5; j++ { // Reduced inner loop
			workStrings = append(workStrings, fmt.Sprintf("Item%d", j))
		}

		fmt.Fprintf(builder, "B%d ", i) // Shorter status
		tempStrings = workStrings       // Update slice reference if reallocated
	}

	// Use the provided maps
	for _, tag := range obj.Tags {
		tempMap1[tag] = len(tag)
		tempMap2[tag] = []byte(tag)
		tempMap3[tag] = strings.ToUpper(tag)
	}
	for key, value := range obj.Metadata {
		tempMap1[key] = len(key) + len(value)
		tempMap2[key] = []byte(value)
		tempMap3[key] = strings.ToUpper(key) + ":" + value
	}

	// Use the provided keys slice
	workKeys := allKeys
	if cap(workKeys) < len(tempMap1) {
		workKeys = make([]string, 0, len(tempMap1))
	}
	for k := range tempMap1 {
		workKeys = append(workKeys, k)
	}
	// Simplified processing for demo
	_, err := fmt.Fprintf(builder, "Keys:%d ", len(workKeys))

	if err != nil {
		return ProcessedResult{}, err
	}

	allKeys = workKeys // Update slice reference if reallocated
	_ = allKeys

	// Create a processed result
	result := ProcessedResult{
		ObjectID:    obj.ID,
		SizeBytes:   len(obj.Data),
		TagCount:    len(obj.Tags),
		MetaCount:   len(obj.Metadata),
		Status:      "Processed", // Simplified status
		ProcessedAt: time.Now(),
	}

	return result, nil
}

// --- Non-Pooled Implementation ---

// processBatchWithoutPooling processes a batch of objects without pooling
func processBatchWithoutPooling(ctx context.Context, batch []LargeObject) ([]ProcessedResult, error) {
	batchStart := time.Now()
	fmt.Printf("📦 Processing batch of %d records (No Pooling)...\n", len(batch))
	results := make([]ProcessedResult, len(batch))

	for i, obj := range batch {
		// Allocate resources for each object
		builder := &strings.Builder{}
		tempBuffer := make([]byte, 1024)
		tempStrings := make([]string, 0, 5)
		tempMap1 := make(map[string]int)
		tempMap2 := make(map[string][]byte)
		tempMap3 := make(map[string]string)
		allKeys := make([]string, 0, 50)

		result, err := processObject(obj, builder, tempBuffer, tempStrings, tempMap1, tempMap2, tempMap3, allKeys)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}

	fmt.Printf("✅ Batch complete (No Pooling) in %v.\n", time.Since(batchStart))
	return results, nil
}

// --- Pooled Implementation ---

// PoolResources holds the pools needed for processing
type PoolResources struct {
	builderPool *fluxus.ObjectPool[*strings.Builder]
	bufferPool  *fluxus.ObjectPool[[]byte]
	stringsPool *fluxus.ObjectPool[[]string]
	map1Pool    *fluxus.ObjectPool[map[string]int]
	map2Pool    *fluxus.ObjectPool[map[string][]byte]
	map3Pool    *fluxus.ObjectPool[map[string]string]
	keysPool    *fluxus.ObjectPool[[]string]
}

// createPoolResources initializes all necessary pools
func createPoolResources() *PoolResources {
	fmt.Println("🔧 Creating object pools...")
	resources := &PoolResources{
		builderPool: fluxus.NewObjectPool(func() *strings.Builder { return &strings.Builder{} }),
		bufferPool:  fluxus.NewObjectPool(func() []byte { return make([]byte, 1024) }),
		stringsPool: fluxus.NewObjectPool(func() []string { return make([]string, 0, 10) }),
		map1Pool:    fluxus.NewObjectPool(func() map[string]int { return make(map[string]int, 50) }),
		map2Pool:    fluxus.NewObjectPool(func() map[string][]byte { return make(map[string][]byte, 50) }),
		map3Pool:    fluxus.NewObjectPool(func() map[string]string { return make(map[string]string, 50) }),
		keysPool:    fluxus.NewObjectPool(func() []string { return make([]string, 0, 50) }),
	}
	// Pre-warm pools slightly
	fluxus.PreWarmPool(resources.builderPool, 10)
	fluxus.PreWarmPool(resources.bufferPool, 10)
	fmt.Println("🔥 Pools pre-warmed.")
	return resources
}

// processBatchWithPooling processes a batch of objects with pooling
func processBatchWithPooling(pools *PoolResources) func(ctx context.Context, batch []LargeObject) ([]ProcessedResult, error) {
	return func(ctx context.Context, batch []LargeObject) ([]ProcessedResult, error) {
		batchStart := time.Now()
		fmt.Printf("📦 Processing batch of %d records (Pooling)...\n", len(batch))
		results := make([]ProcessedResult, len(batch))

		for i, obj := range batch {
			// Get resources from pools
			builder := pools.builderPool.Get()
			tempBuffer := pools.bufferPool.Get()
			tempStrings := pools.stringsPool.Get()
			tempMap1 := pools.map1Pool.Get()
			tempMap2 := pools.map2Pool.Get()
			tempMap3 := pools.map3Pool.Get()
			allKeys := pools.keysPool.Get()

			result, err := processObject(obj, builder, tempBuffer, tempStrings, tempMap1, tempMap2, tempMap3, allKeys)

			// Return resources to pools
			pools.builderPool.Put(builder)
			pools.bufferPool.Put(tempBuffer)
			pools.stringsPool.Put(tempStrings)
			pools.map1Pool.Put(tempMap1)
			pools.map2Pool.Put(tempMap2)
			pools.map3Pool.Put(tempMap3)
			pools.keysPool.Put(allKeys)

			if err != nil {
				return nil, err
			}
			results[i] = result
		}
		fmt.Printf("✅ Batch complete (Pooling) in %v.\n", time.Since(batchStart))
		return results, nil
	}
}

// --- Demonstration Functions ---

// runProcessingDemo runs the processing with a given buffer stage
func runProcessingDemo(name string, ctx context.Context, buffer fluxus.Stage[[]LargeObject, []ProcessedResult], objects []LargeObject) (time.Duration, int64, uint64) {
	fmt.Printf("\n▶️ Running demo: %s\n", name)
	// Force GC before measurement
	runtime.GC()
	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)
	gcTotalPauseNsBefore := memStatsBefore.PauseTotalNs
	gcStart := memStatsBefore.NumGC

	startTime := time.Now()
	_, err := buffer.Process(ctx, objects)
	if err != nil {
		log.Printf("❌ Processing error in %s: %v", name, err)
	}
	duration := time.Since(startTime)

	// Force GC after measurement
	runtime.GC()
	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)
	gcCount := memStatsAfter.NumGC - gcStart
	gcPauseNs := memStatsAfter.PauseTotalNs - gcTotalPauseNsBefore
	allocDelta := memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc

	fmt.Printf("🏁 %s finished in %v.\n", name, duration)
	fmt.Printf("  - Allocations: %d MB (%d bytes/object)\n", allocDelta/(1024*1024), allocDelta/uint64(len(objects)))
	fmt.Printf("  - GC Runs: %d\n", gcCount)
	fmt.Printf("  - GC Pause: %v\n", time.Duration(gcPauseNs))

	return duration, int64(allocDelta), uint64(gcCount)
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	fmt.Println("🌀 Fluxus Pooling Demonstration")
	fmt.Println("==============================")
	fmt.Println("This example compares processing large objects with and without")
	fmt.Println("object pooling to demonstrate the impact on performance and memory usage.")

	// Generate test data
	objectCount := 500 // Reduced count for faster demo
	objects := generateLargeObjects(objectCount)
	batchSize := 50

	// --- Run Without Pooling ---
	bufferNoPooling := fluxus.NewBuffer(batchSize, processBatchWithoutPooling)
	durationNoPool, allocNoPool, gcNoPool := runProcessingDemo("Without Pooling", ctx, bufferNoPooling, objects)

	// --- Run With Pooling ---
	poolResources := createPoolResources()
	bufferWithPooling := fluxus.NewPooledBuffer(batchSize, processBatchWithPooling(poolResources))
	durationPool, allocPool, gcPool := runProcessingDemo("With Pooling", ctx, bufferWithPooling, objects)

	// --- Comparison ---
	fmt.Println("\n📊 Comparison Results")
	fmt.Println("--------------------")
	fmt.Printf("                     | %-15s | %-15s | Improvement\n", "Without Pooling", "With Pooling")
	fmt.Println("---------------------|-----------------|-----------------|------------")
	fmt.Printf("Duration             | %-15v | %-15v | %.2fx\n",
		durationNoPool.Round(time.Millisecond),
		durationPool.Round(time.Millisecond),
		float64(durationNoPool)/float64(durationPool))
	fmt.Printf("Allocations (MB)     | %-15d | %-15d | %.2fx less\n",
		allocNoPool/(1024*1024),
		allocPool/(1024*1024),
		float64(allocNoPool)/float64(allocPool))
	fmt.Printf("GC Runs              | %-15d | %-15d | %d fewer\n",
		gcNoPool, gcPool, gcNoPool-gcPool)

	// --- Profiling Info ---
	// Setup CPU profiling
	cpuFile, err := os.Create("pooling_cpu.prof")
	if err != nil {
		log.Printf("⚠️ Could not create CPU profile: %v", err)
	} else {
		defer cpuFile.Close()
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			log.Printf("⚠️ Could not start CPU profile: %v", err)
		} else {
			// Rerun the pooled version briefly for profiling
			fmt.Println("\n⏱️ Running pooled version again for CPU profiling...")
			ctxProfile, cancel := context.WithTimeout(ctx, 5*time.Second)
			runProcessingDemo("Profiling Run", ctxProfile, bufferWithPooling, objects)
			cancel()
			pprof.StopCPUProfile()
			fmt.Println("✅ CPU profile written to pooling_cpu.prof")
		}
	}

	// Create memory profile
	memFile, err := os.Create("pooling_mem.prof")
	if err != nil {
		log.Printf("⚠️ Could not create memory profile: %v", err)
	} else {
		defer memFile.Close()
		runtime.GC() // Run GC right before heap dump
		if err := pprof.WriteHeapProfile(memFile); err != nil {
			log.Printf("⚠️ Could not write memory profile: %v", err)
		} else {
			fmt.Println("✅ Memory profile written to pooling_mem.prof")
		}
	}

	fmt.Println("\n💡 To analyze profiles, use: go tool pprof <file>")
	fmt.Println("\nDemo Complete!")
}
