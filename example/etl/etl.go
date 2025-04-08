package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/synoptiq/go-fluxus"
	"golang.org/x/time/rate"
)

// UserRecord represents a record in our input data
type UserRecord struct {
	ID        int
	Name      string
	Email     string
	Age       int
	CreatedAt time.Time
}

// UserSummary represents our transformed output
type UserSummary struct {
	TotalUsers           int
	AverageAge           float64
	EmailDomains         map[string]int
	RegistrationsByMonth map[string]int
	ProcessingTimeMs     int64
}

// CSVProcessor handles reading and parsing CSV files
type CSVProcessor struct {
	filename string
}

// NewCSVProcessor creates a new CSV processor
func NewCSVProcessor(filename string) *CSVProcessor {
	return &CSVProcessor{
		filename: filename,
	}
}

// ReadCSV reads the CSV file and returns the raw records
func (p *CSVProcessor) ReadCSV(ctx context.Context, filename string) ([][]string, error) {
	fmt.Printf("📂 Reading CSV file: %s\n", filename)
	startTime := time.Now()

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("✅ Read %d rows in %.2f ms\n", len(records), float64(elapsed.Microseconds())/1000)

	return records, nil
}

// ParseRecords parses raw CSV records into structured UserRecord objects
func (p *CSVProcessor) ParseRecords(ctx context.Context, records [][]string) ([]UserRecord, error) {
	fmt.Printf("🔄 Parsing %d records...\n", len(records)-1) // -1 for header
	startTime := time.Now()

	if len(records) < 2 { // Header + at least one record
		return nil, fmt.Errorf("insufficient records in CSV")
	}

	// Skip the header row
	dataRecords := records[1:]
	users := make([]UserRecord, 0, len(dataRecords))

	skippedRecords := 0
	for i, record := range dataRecords {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Continue processing
		}

		if len(record) < 5 {
			skippedRecords++
			fmt.Printf("⚠️ Record %d has insufficient fields, skipping\n", i+1)
			continue
		}

		id, err := strconv.Atoi(record[0])
		if err != nil {
			skippedRecords++
			fmt.Printf("⚠️ Invalid ID in record %d: %v, skipping\n", i+1, err)
			continue
		}

		age, err := strconv.Atoi(record[3])
		if err != nil {
			skippedRecords++
			fmt.Printf("⚠️ Invalid age in record %d: %v, skipping\n", i+1, err)
			continue
		}

		createdAt, err := time.Parse("2006-01-02", record[4])
		if err != nil {
			skippedRecords++
			fmt.Printf("⚠️ Invalid date in record %d: %v, skipping\n", i+1, err)
			continue
		}

		users = append(users, UserRecord{
			ID:        id,
			Name:      record[1],
			Email:     record[2],
			Age:       age,
			CreatedAt: createdAt,
		})
	}

	elapsed := time.Since(startTime)
	fmt.Printf("✅ Parsed %d valid records (skipped %d) in %.2f ms\n",
		len(users), skippedRecords, float64(elapsed.Microseconds())/1000)

	return users, nil
}

// TransformData transforms user records into a summary
func (p *CSVProcessor) TransformData(ctx context.Context, users []UserRecord) (UserSummary, error) {
	fmt.Printf("🔄 Transforming data from %d records...\n", len(users))
	startTime := time.Now()

	if len(users) == 0 {
		return UserSummary{}, nil
	}

	// Calculate statistics
	totalAge := 0
	emailDomains := make(map[string]int)
	registrationsByMonth := make(map[string]int)

	for _, user := range users {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return UserSummary{}, ctx.Err()
		default:
			// Continue processing
		}

		totalAge += user.Age

		// Extract email domain
		if parts := strings.Split(user.Email, "@"); len(parts) == 2 {
			domain := parts[1]
			emailDomains[domain]++
		}

		// Group by registration month
		month := user.CreatedAt.Format("2006-01")
		registrationsByMonth[month]++
	}

	summary := UserSummary{
		TotalUsers:           len(users),
		AverageAge:           float64(totalAge) / float64(len(users)),
		EmailDomains:         emailDomains,
		RegistrationsByMonth: registrationsByMonth,
		ProcessingTimeMs:     time.Since(startTime).Milliseconds(),
	}

	fmt.Printf("✅ Transformed data in %.2f ms\n", float64(summary.ProcessingTimeMs))

	return summary, nil
}

// buildETLPipeline creates a complete ETL pipeline
func buildETLPipeline() *fluxus.Pipeline[string, UserSummary] {
	// Stage 1: Read the CSV file
	readStage := fluxus.StageFunc[string, [][]string](func(ctx context.Context, filename string) ([][]string, error) {
		processor := NewCSVProcessor(filename)
		return processor.ReadCSV(ctx, filename)
	})

	// Stage 2: Parse the records into structured data
	parseStage := fluxus.StageFunc[[][]string, []UserRecord](func(ctx context.Context, records [][]string) ([]UserRecord, error) {
		processor := NewCSVProcessor("")
		return processor.ParseRecords(ctx, records)
	})

	// Wrap the parse stage with a rate limiter (for demonstration purposes)
	rateLimitedParseStage := fluxus.NewRateLimiter(
		parseStage,
		rate.Limit(100), // 100 operations per second
		10,              // Burst of 10
		fluxus.WithLimiterTimeout[[][]string, []UserRecord](500*time.Millisecond),
	)

	// Stage 3: Transform the data into a summary
	transformStage := fluxus.StageFunc[[]UserRecord, UserSummary](func(ctx context.Context, users []UserRecord) (UserSummary, error) {
		processor := NewCSVProcessor("")
		return processor.TransformData(ctx, users)
	})

	// Chain the stages together
	chainedStage := fluxus.Chain(
		readStage,
		fluxus.Chain(
			rateLimitedParseStage,
			transformStage,
		),
	)

	// Create a pipeline with error handling
	pipeline := fluxus.NewPipeline(chainedStage).WithErrorHandler(func(err error) error {
		// Log the error details before returning
		fmt.Printf("❌ Pipeline error: %v\n", err)
		return err
	})

	return pipeline
}

// runBasicETLDemo demonstrates a complete ETL process
func runBasicETLDemo(filename string) {
	fmt.Println("\n🔄 Running Basic ETL Demo")
	fmt.Println("========================")
	fmt.Printf("Input file: %s\n", filename)

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup a pipeline for ETL processing
	pipeline := buildETLPipeline()

	// Process the input file
	startTime := time.Now()
	result, err := pipeline.Process(ctx, filename)
	if err != nil {
		log.Fatalf("❌ Error processing pipeline: %v", err)
	}
	totalTime := time.Since(startTime)

	// Output the results
	fmt.Printf("\n📊 ETL Processing Results\n")
	fmt.Printf("  Total processing time: %.2f ms\n", float64(totalTime.Microseconds())/1000)
	fmt.Printf("  Total users processed: %d\n", result.TotalUsers)
	fmt.Printf("  Average age: %.2f years\n", result.AverageAge)

	fmt.Printf("\n  Top email domains:\n")
	// Find top 3 domains
	type domainCount struct {
		domain string
		count  int
	}

	domains := make([]domainCount, 0, len(result.EmailDomains))
	for domain, count := range result.EmailDomains {
		domains = append(domains, domainCount{domain, count})
	}

	// Sort by count (descending)
	for i := 0; i < len(domains); i++ {
		for j := i + 1; j < len(domains); j++ {
			if domains[j].count > domains[i].count {
				domains[i], domains[j] = domains[j], domains[i]
			}
		}
	}

	// Print top domains
	max := 3
	if len(domains) < max {
		max = len(domains)
	}

	for i := 0; i < max; i++ {
		fmt.Printf("    %s: %d users\n", domains[i].domain, domains[i].count)
	}

	fmt.Printf("\n  Monthly registration trend:\n")
	// Sort months chronologically
	months := make([]string, 0, len(result.RegistrationsByMonth))
	for month := range result.RegistrationsByMonth {
		months = append(months, month)
	}

	// Sort chronologically
	for i := 0; i < len(months); i++ {
		for j := i + 1; j < len(months); j++ {
			if months[i] > months[j] {
				months[i], months[j] = months[j], months[i]
			}
		}
	}

	for _, month := range months {
		fmt.Printf("    %s: %d registrations\n", month, result.RegistrationsByMonth[month])
	}
}

// runRateLimitDemo demonstrates rate limiting in the ETL pipeline
func runRateLimitDemo(filename string) {
	fmt.Println("\n🚦 Running Rate Limit Demo")
	fmt.Println("=========================")

	// Create two pipelines with different rate limits
	// First: Create a parser stage with very low rate limit
	slowParseStage := fluxus.StageFunc[[][]string, []UserRecord](func(ctx context.Context, records [][]string) ([]UserRecord, error) {
		processor := NewCSVProcessor("")
		return processor.ParseRecords(ctx, records)
	})

	slowRateLimiter := fluxus.NewRateLimiter(
		slowParseStage,
		rate.Limit(1), // 1 operation per second (very restrictive)
		1,             // Burst of 1
		fluxus.WithLimiterTimeout[[][]string, []UserRecord](300*time.Millisecond),
	)

	// Second: Create a parser stage with higher rate limit
	fastParseStage := fluxus.StageFunc[[][]string, []UserRecord](func(ctx context.Context, records [][]string) ([]UserRecord, error) {
		processor := NewCSVProcessor("")
		return processor.ParseRecords(ctx, records)
	})

	fastRateLimiter := fluxus.NewRateLimiter(
		fastParseStage,
		rate.Limit(1000), // 1000 operations per second (essentially unlimited)
		100,              // Burst of 100
		fluxus.WithLimiterTimeout[[][]string, []UserRecord](300*time.Millisecond),
	)

	// Create read stage (common to both pipelines)
	readStage := fluxus.StageFunc[string, [][]string](func(ctx context.Context, filename string) ([][]string, error) {
		processor := NewCSVProcessor(filename)
		return processor.ReadCSV(ctx, filename)
	})

	// Create transform stage (common to both pipelines)
	transformStage := fluxus.StageFunc[[]UserRecord, UserSummary](func(ctx context.Context, users []UserRecord) (UserSummary, error) {
		processor := NewCSVProcessor("")
		return processor.TransformData(ctx, users)
	})

	// Create the slow pipeline
	slowChainedStage := fluxus.Chain(
		readStage,
		fluxus.Chain(
			slowRateLimiter,
			transformStage,
		),
	)

	slowPipeline := fluxus.NewPipeline(slowChainedStage).WithErrorHandler(func(err error) error {
		fmt.Printf("❌ Slow pipeline error: %v\n", err)
		return err
	})

	// Create the fast pipeline
	fastChainedStage := fluxus.Chain(
		readStage,
		fluxus.Chain(
			fastRateLimiter,
			transformStage,
		),
	)

	fastPipeline := fluxus.NewPipeline(fastChainedStage).WithErrorHandler(func(err error) error {
		fmt.Printf("❌ Fast pipeline error: %v\n", err)
		return err
	})

	// Create contexts
	slowCtx, slowCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer slowCancel()

	fastCtx, fastCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer fastCancel()

	// Test the slow pipeline
	fmt.Println("Testing pipeline with restrictive rate limit (1 RPS)...")
	slowStart := time.Now()

	_, slowErr := slowPipeline.Process(slowCtx, filename)

	slowDuration := time.Since(slowStart)

	if slowErr != nil {
		if strings.Contains(slowErr.Error(), "would exceed context deadline") {
			fmt.Printf("⏱️ Slow pipeline timed out after %.2f ms due to rate limiting\n",
				float64(slowDuration.Microseconds())/1000)
		} else {
			fmt.Printf("❌ Slow pipeline failed: %v\n", slowErr)
		}
	} else {
		fmt.Printf("✅ Slow pipeline completed in %.2f ms\n",
			float64(slowDuration.Microseconds())/1000)
	}

	// Test the fast pipeline
	fmt.Println("\nTesting pipeline with generous rate limit (1000 RPS)...")
	fastStart := time.Now()

	fastResult, fastErr := fastPipeline.Process(fastCtx, filename)

	fastDuration := time.Since(fastStart)

	if fastErr != nil {
		fmt.Printf("❌ Fast pipeline failed: %v\n", fastErr)
	} else {
		fmt.Printf("✅ Fast pipeline completed in %.2f ms\n",
			float64(fastDuration.Microseconds())/1000)
		fmt.Printf("   Processed %d users\n", fastResult.TotalUsers)
	}

	// Compare results
	if slowErr == nil && fastErr == nil {
		speedup := float64(slowDuration) / float64(fastDuration)
		fmt.Printf("\n📊 Rate Limiting Comparison\n")
		fmt.Printf("  Slow pipeline: %.2f ms\n", float64(slowDuration.Microseconds())/1000)
		fmt.Printf("  Fast pipeline: %.2f ms\n", float64(fastDuration.Microseconds())/1000)
		fmt.Printf("  Speedup: %.2fx\n", speedup)
	}
}

// runErrorHandlingDemo demonstrates error handling in the ETL pipeline
func runErrorHandlingDemo() {
	fmt.Println("\n⚠️ Running Error Handling Demo")
	fmt.Println("============================")

	// Create a completely invalid CSV in memory (missing required columns)
	invalidCSV := []byte(`id,name,email
1,John Doe,john@example.com
2,Jane Smith,jane@example.com
3,Bob Johnson,bob@example.com
4,Alice Brown,alice@example.com
`)

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "invalid-*.csv")
	if err != nil {
		log.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write invalid data
	if _, err := tempFile.Write(invalidCSV); err != nil {
		log.Fatalf("Failed to write to temp file: %v", err)
	}
	tempFile.Close()

	fmt.Printf("Created invalid CSV file: %s\n", tempFile.Name())

	// Create special versions of stages that fail appropriately
	readStage := fluxus.StageFunc[string, [][]string](func(ctx context.Context, filename string) ([][]string, error) {
		processor := NewCSVProcessor(filename)
		return processor.ReadCSV(ctx, filename)
	})

	parseStage := fluxus.StageFunc[[][]string, []UserRecord](func(ctx context.Context, records [][]string) ([]UserRecord, error) {
		processor := NewCSVProcessor("")
		users, err := processor.ParseRecords(ctx, records)

		// For demo purposes, explicitly fail if all records were skipped
		if len(users) == 0 {
			return nil, fmt.Errorf("failed to parse any valid records from input")
		}

		return users, err
	})

	transformStage := fluxus.StageFunc[[]UserRecord, UserSummary](func(ctx context.Context, users []UserRecord) (UserSummary, error) {
		processor := NewCSVProcessor("")
		return processor.TransformData(ctx, users)
	})

	// Chain stages for basic error handling
	basicChainedStage := fluxus.Chain(
		readStage,
		fluxus.Chain(
			parseStage,
			transformStage,
		),
	)

	basicPipeline := fluxus.NewPipeline(basicChainedStage)

	// Create a pipeline with custom error handling
	customErrorPipeline := fluxus.NewPipeline(basicChainedStage).WithErrorHandler(func(err error) error {
		// Wrap the error with more context
		newErr := fmt.Errorf("ETL pipeline error (with custom handling): %w", err)
		fmt.Printf("🔍 Error intercepted by custom handler: %v\n", err)
		return newErr
	})

	// Process with basic pipeline
	fmt.Println("\nProcessing with basic error handling...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = basicPipeline.Process(ctx, tempFile.Name())
	cancel()

	if err != nil {
		fmt.Printf("❌ Expected error occurred: %v\n", err)
	} else {
		fmt.Println("⚠️ Unexpected success - error should have occurred")
	}

	// Process with custom error handling
	fmt.Println("\nProcessing with custom error handling...")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = customErrorPipeline.Process(ctx, tempFile.Name())
	cancel()

	if err != nil {
		fmt.Printf("❌ Expected error occurred with custom handler: %v\n", err)
		if strings.Contains(err.Error(), "ETL pipeline error (with custom handling)") {
			fmt.Println("✅ Error was properly wrapped by custom handler")
		} else {
			fmt.Println("❌ Error was not properly wrapped")
		}
	} else {
		fmt.Println("⚠️ Unexpected success - error should have occurred")
	}
}

// generateSampleCSV creates a sample CSV file for testing
func generateSampleCSV() string {
	// Create sample data
	sampleData := []byte(`id,name,email,age,created_at
1,John Doe,john@example.com,32,2023-01-15
2,Jane Smith,jane@example.net,28,2023-02-20
3,Bob Johnson,bob@gmail.com,45,2023-01-10
4,Alice Brown,alice@example.com,36,2023-03-05
5,Charlie Wilson,charlie@gmail.com,29,2023-02-12
6,Diana Miller,diana@example.org,41,2023-01-28
7,Edward Davis,edward@gmail.com,33,2023-03-15
8,Fiona Garcia,fiona@example.net,37,2023-02-08
9,George Martinez,george@gmail.com,42,2023-01-19
10,Hannah Robinson,hannah@example.com,31,2023-03-22
`)

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "sample-*.csv")
	if err != nil {
		log.Fatalf("Failed to create temp file: %v", err)
	}

	// Write sample data
	if _, err := tempFile.Write(sampleData); err != nil {
		log.Fatalf("Failed to write to temp file: %v", err)
	}
	tempFile.Close()

	return tempFile.Name()
}

func main() {
	fmt.Println("Fluxus ETL Pipeline Demonstration")
	fmt.Println("=================================")
	fmt.Println("This example demonstrates Extract-Transform-Load (ETL) pipelines")
	fmt.Println("for processing data through multiple stages with rate limiting,")
	fmt.Println("error handling, and performance monitoring capabilities.")

	// Create a sample CSV for testing
	sampleFile := generateSampleCSV()
	defer os.Remove(sampleFile)

	fmt.Printf("Created sample CSV file: %s\n", sampleFile)

	// Run the demos
	runBasicETLDemo(sampleFile)
	runRateLimitDemo(sampleFile)
	runErrorHandlingDemo()

	fmt.Println("\nDemo Complete!")
}
