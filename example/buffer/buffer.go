package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// DataRecord represents a single data record to be processed
type DataRecord struct {
	ID        string
	Timestamp time.Time
	Value     float64
	Tags      []string
	Metadata  map[string]string
}

// ProcessedRecord represents the processed output
type ProcessedRecord struct {
	OriginalID   string
	ProcessedAt  time.Time
	Result       float64
	TagCount     int
	BatchID      int
	ProcessingMS int64
}

// BatchStats tracks statistics for each batch
type BatchStats struct {
	TotalRecords      int64
	TotalProcessingMS int64
	BatchesProcessed  int64
	AverageValue      float64
	MinValue          float64
	MaxValue          float64

	// mutex
	mu sync.Mutex
}

// DataProcessor handles the processing of data records
type DataProcessor struct {
	stats       *BatchStats
	batchNumber int32
}

// NewDataProcessor creates a new data processor
func NewDataProcessor() *DataProcessor {
	return &DataProcessor{
		stats: &BatchStats{
			MinValue: float64(^uint64(0) >> 1), // Initialize to max value
		},
	}
}

// Process processes a batch of records
func (p *DataProcessor) Process(ctx context.Context, batch []DataRecord) ([]ProcessedRecord, error) {
	batchStart := time.Now()
	batchID := atomic.AddInt32(&p.batchNumber, 1)

	fmt.Printf("📦 Processing batch #%d with %d records...\n", batchID, len(batch))

	// Simulate some CPU-intensive work
	time.Sleep(50 * time.Millisecond)

	results := make([]ProcessedRecord, 0, len(batch))
	batchSum := 0.0

	// Process each record in the batch
	for _, record := range batch {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Simulate per-record processing
			processingTime := 10 + rand.Intn(30)
			time.Sleep(time.Duration(processingTime) * time.Millisecond)

			// Apply a transformation to the value
			transformedValue := record.Value * 1.5

			// Track min/max values
			atomic.AddInt64(&p.stats.TotalRecords, 1)
			atomic.AddInt64(&p.stats.TotalProcessingMS, int64(processingTime))

			p.stats.mu.Lock()
			if transformedValue < p.stats.MinValue {
				p.stats.MinValue = transformedValue
			}
			if transformedValue > p.stats.MaxValue {
				p.stats.MaxValue = transformedValue
			}
			p.stats.mu.Unlock()

			batchSum += transformedValue

			// Create processed record
			processed := ProcessedRecord{
				OriginalID:   record.ID,
				ProcessedAt:  time.Now(),
				Result:       transformedValue,
				TagCount:     len(record.Tags),
				BatchID:      int(batchID),
				ProcessingMS: int64(processingTime),
			}

			results = append(results, processed)
		}
	}

	// Update batch statistics
	atomic.AddInt64(&p.stats.BatchesProcessed, 1)
	newAvg := (p.stats.AverageValue*float64(p.stats.BatchesProcessed-1) + batchSum/float64(len(batch))) / float64(p.stats.BatchesProcessed)
	p.stats.mu.Lock()
	p.stats.AverageValue = newAvg
	p.stats.mu.Unlock()

	batchTime := time.Since(batchStart)
	fmt.Printf("✅ Completed batch #%d in %.2f ms\n", batchID, float64(batchTime.Microseconds())/1000)

	return results, nil
}

// GetStats returns the current processing statistics
func (p *DataProcessor) GetStats() BatchStats {
	return *p.stats
}

// generateTestData creates sample data records for processing
func generateTestData(count int) []DataRecord {
	records := make([]DataRecord, count)

	// Sample tags to choose from
	possibleTags := []string{
		"sensor", "temperature", "pressure", "humidity",
		"voltage", "current", "power", "energy",
		"indoor", "outdoor", "mobile", "stationary",
	}

	// Generate records
	for i := 0; i < count; i++ {
		// Generate a random number of tags (1-5)
		tagCount := 1 + rand.Intn(5)
		tags := make([]string, tagCount)
		for j := 0; j < tagCount; j++ {
			tags[j] = possibleTags[rand.Intn(len(possibleTags))]
		}

		// Generate metadata with 2-5 entries
		metaCount := 2 + rand.Intn(4)
		metadata := make(map[string]string, metaCount)
		for j := 0; j < metaCount; j++ {
			key := fmt.Sprintf("meta%d", j)
			value := fmt.Sprintf("value-%d-%d", i, j)
			metadata[key] = value
		}

		// Create the record
		records[i] = DataRecord{
			ID:        fmt.Sprintf("REC-%04d", i),
			Timestamp: time.Now().Add(-time.Duration(rand.Intn(3600)) * time.Second),
			Value:     10 + rand.Float64()*90, // Random value between 10 and 100
			Tags:      tags,
			Metadata:  metadata,
		}
	}

	return records
}

// runBasicBufferDemo demonstrates basic buffer functionality
func runBasicBufferDemo(processor *DataProcessor) {
	fmt.Println("\n🔄 Running Basic Buffer Demo")
	fmt.Println("============================")

	// Create test data
	recordCount := 25
	fmt.Printf("Generating %d test records...\n", recordCount)
	records := generateTestData(recordCount)

	// Create a buffer stage with batch size of 5
	batchSize := 5
	buffer := fluxus.NewBuffer(batchSize, processor.Process)

	// Set up a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Process the records through the buffer
	fmt.Printf("Processing records with batch size %d...\n", batchSize)
	startTime := time.Now()

	results, err := buffer.Process(ctx, records)
	if err != nil {
		log.Fatalf("Buffer processing failed: %v", err)
	}

	totalTime := time.Since(startTime)

	// Print results summary
	fmt.Printf("\n📊 Basic Buffer Demo Results\n")
	fmt.Printf("  Processed %d records in %.2f ms\n",
		len(results), float64(totalTime.Microseconds())/1000)
	fmt.Printf("  Batches processed: %d\n", processor.stats.BatchesProcessed)
	fmt.Printf("  Average processing time per record: %.2f ms\n",
		float64(processor.stats.TotalProcessingMS)/float64(processor.stats.TotalRecords))
	fmt.Printf("  Throughput: %.2f records/second\n",
		float64(len(results))*1000/float64(totalTime.Milliseconds()))
}

// runVariableBatchSizeDemo demonstrates how different batch sizes affect performance
func runVariableBatchSizeDemo() {
	fmt.Println("\n📏 Running Variable Batch Size Demo")
	fmt.Println("==================================")

	// Create a large dataset
	recordCount := 100
	fmt.Printf("Generating %d test records...\n", recordCount)
	records := generateTestData(recordCount)

	// Test different batch sizes
	batchSizes := []int{1, 5, 10, 20, 50}

	for _, batchSize := range batchSizes {
		// Create a new processor for each test
		processor := NewDataProcessor()

		// Create a buffer with this batch size
		buffer := fluxus.NewBuffer(batchSize, processor.Process)

		// Set up a context
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		// Process the records
		fmt.Printf("\nTesting batch size: %d\n", batchSize)
		startTime := time.Now()

		results, err := buffer.Process(ctx, records)
		if err != nil {
			log.Printf("Error with batch size %d: %v", batchSize, err)
			cancel()
			continue
		}

		totalTime := time.Since(startTime)
		cancel()

		// Print results for this batch size
		fmt.Printf("  Processed %d records in %.2f ms (%.2f records/second)\n",
			len(results), float64(totalTime.Microseconds())/1000,
			float64(len(results))*1000/float64(totalTime.Milliseconds()))
		fmt.Printf("  Number of batches: %d\n", processor.stats.BatchesProcessed)
		fmt.Printf("  Average batch processing time: %.2f ms\n",
			float64(processor.stats.TotalProcessingMS)/float64(processor.stats.BatchesProcessed))
	}
}

// runPooledBufferDemo demonstrates using a pooled buffer for better performance
func runPooledBufferDemo(initialProcessor *DataProcessor) { // Renamed for clarity
	fmt.Println("\n♻️ Running Pooled Buffer Demo")
	fmt.Println("===========================")

	// Create test data
	recordCount := 50
	fmt.Printf("Generating %d test records...\n", recordCount)
	records := generateTestData(recordCount)

	// --- Regular Buffer Test ---
	batchSize := 10
	regularBuffer := fluxus.NewBuffer(batchSize, initialProcessor.Process) // Use initial processor

	ctxRegular, cancelRegular := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRegular()

	fmt.Println("\nProcessing with regular buffer...")
	startRegular := time.Now()
	regularResults, err := regularBuffer.Process(ctxRegular, records)
	if err != nil {
		log.Printf("Regular buffer error: %v", err)
	}
	regularTime := time.Since(startRegular)
	// Capture stats from the initial processor if needed for comparison *before* reset
	regularStats := initialProcessor.GetStats()

	// --- Pooled Buffer Test ---
	// Create a *new* processor specifically for the pooled buffer test
	pooledProcessor := NewDataProcessor()
	pooledBuffer := fluxus.NewPooledBuffer(batchSize, pooledProcessor.Process, // Use the new processor
		fluxus.WithBufferName[DataRecord, ProcessedRecord]("data-processor"))

	ctxPooled, cancelPooled := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPooled()

	fmt.Println("\nProcessing with pooled buffer...")
	startPooled := time.Now()
	pooledResults, err := pooledBuffer.Process(ctxPooled, records) // This uses pooledProcessor
	if err != nil {
		log.Printf("Pooled buffer error: %v", err)
	}
	pooledTime := time.Since(startPooled)
	// Get stats from the pooled processor
	pooledStats := pooledProcessor.GetStats()

	// Print comparison (using regularStats and pooledStats)
	fmt.Printf("\n📊 Buffer Comparison\n")
	fmt.Printf("  Regular Buffer: %.2f ms (%.2f records/second) [%d batches]\n",
		float64(regularTime.Microseconds())/1000,
		float64(len(regularResults))*1000/float64(regularTime.Milliseconds()),
		regularStats.BatchesProcessed) // Use stats from the correct processor

	fmt.Printf("  Pooled Buffer:  %.2f ms (%.2f records/second) [%d batches]\n",
		float64(pooledTime.Microseconds())/1000,
		float64(len(pooledResults))*1000/float64(pooledTime.Milliseconds()),
		pooledStats.BatchesProcessed) // Use stats from the correct processor

	if pooledTime < regularTime {
		improvement := 100 * (1 - float64(pooledTime)/float64(regularTime))
		fmt.Printf("  Performance improvement: %.2f%%\n", improvement)
	}
}

// runErrorHandlingDemo demonstrates error handling in buffer processing
func runErrorHandlingDemo() {
	fmt.Println("\n⚠️ Running Error Handling Demo")
	fmt.Println("============================")

	// Create test data
	recordCount := 20
	fmt.Printf("Generating %d test records...\n", recordCount)
	records := generateTestData(recordCount)

	// Create a processor that will fail on certain conditions
	errorProcessor := func(ctx context.Context, batch []DataRecord) ([]ProcessedRecord, error) {
		results := make([]ProcessedRecord, 0, len(batch))

		// Fail if batch contains a record with ID ending in "3"
		for _, record := range batch {
			if strings.HasSuffix(record.ID, "3") {
				return nil, fmt.Errorf("error processing record %s: value out of range", record.ID)
			}

			// Otherwise process normally
			processed := ProcessedRecord{
				OriginalID:   record.ID,
				ProcessedAt:  time.Now(),
				Result:       record.Value * 2,
				TagCount:     len(record.Tags),
				ProcessingMS: 10,
			}

			results = append(results, processed)
		}

		return results, nil
	}

	// Create a buffer with the error-generating processor
	buffer := fluxus.NewBuffer(5, errorProcessor)

	// Add custom error handling
	buffer = buffer.WithErrorHandler(func(err error) error {
		return fmt.Errorf("batch processing failed: %w", err)
	})

	// Process the records
	ctx := context.Background()
	fmt.Println("Processing records with error-generating processor...")

	_, err := buffer.Process(ctx, records)

	// We expect an error
	if err != nil {
		fmt.Printf("❌ Received expected error: %v\n", err)

		// Check if it's properly wrapped
		if strings.Contains(err.Error(), "batch processing failed") {
			fmt.Println("✅ Error was properly wrapped by custom error handler")
		} else {
			fmt.Println("❌ Error was not properly wrapped")
		}
	} else {
		fmt.Println("❌ Expected an error but none occurred")
	}

	// Now create a buffer that will handle errors at the batch level
	batchErrorHandler := func(ctx context.Context, batch []DataRecord) ([]ProcessedRecord, error) {
		results := make([]ProcessedRecord, 0, len(batch))

		for _, record := range batch {
			// Instead of failing the whole batch, we'll handle errors per record
			if strings.HasSuffix(record.ID, "3") {
				// Skip problematic records but continue processing
				fmt.Printf("⚠️ Skipping problematic record: %s\n", record.ID)
				continue
			}

			// Process valid records
			processed := ProcessedRecord{
				OriginalID:   record.ID,
				ProcessedAt:  time.Now(),
				Result:       record.Value * 2,
				TagCount:     len(record.Tags),
				ProcessingMS: 10,
			}

			results = append(results, processed)
		}

		return results, nil
	}

	// Create a buffer with the error-handling processor
	resilientBuffer := fluxus.NewBuffer(5, batchErrorHandler)

	fmt.Println("\nProcessing records with error-handling processor...")

	results, err := resilientBuffer.Process(ctx, records)
	if err != nil {
		fmt.Printf("❌ Unexpected error: %v\n", err)
	} else {
		fmt.Printf("✅ Successfully processed %d out of %d records\n",
			len(results), recordCount)

		// Calculate how many records were skipped
		skipped := recordCount - len(results)
		fmt.Printf("  Skipped %d problematic records\n", skipped)
	}
}

func main() {
	// Set seed for reproducible results
	rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Println("Fluxus Buffer Mechanism Demonstration")
	fmt.Println("====================================")
	fmt.Println("This example demonstrates batch processing using buffers,")
	fmt.Println("which allow processing data in efficient batches instead of")
	fmt.Println("one item at a time, improving throughput and resource usage.")

	// Create a data processor for demos
	processor := NewDataProcessor()

	// Run demos
	runBasicBufferDemo(processor)
	runVariableBatchSizeDemo()
	runPooledBufferDemo(processor)
	runErrorHandlingDemo()

	fmt.Println("\nDemo Complete!")
}
