package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/synoptiq/go-fluxus" // Assuming fluxus is in your GOPATH or module
)

// --- Data Structures ---

// RawEvent represents the initial input string
type RawEvent string

// ParsedEvent represents a structured event after parsing
type ParsedEvent struct {
	Timestamp time.Time `json:"timestamp"`
	UserID    string    `json:"user_id"`
	EventType string    `json:"event_type"`
	Payload   string    `json:"payload"` // Raw payload for simplicity
	IsValid   bool      // Flag set during parsing
}

// EnrichedEvent includes data from an external source
type EnrichedEvent struct {
	ParsedEvent
	UserRegion string `json:"user_region"` // Data from enrichment service
	IsPriority bool   `json:"is_priority"` // Determined during enrichment
}

// ProcessingResult represents the final outcome for an event
type ProcessingResult struct {
	UserID    string
	EventType string
	Region    string
	Status    string // e.g., "Processed", "Filtered", "Failed"
}

// --- Mock External Service ---

// MockUserService simulates fetching user details
type MockUserService struct {
	mu      sync.RWMutex
	regions map[string]string
}

func NewMockUserService() *MockUserService {
	return &MockUserService{
		regions: map[string]string{
			"user_1": "EMEA",
			"user_2": "NA",
			"user_3": "APAC",
			"user_4": "NA",
			"user_5": "EMEA",
		},
	}
}

// GetUserRegion simulates an API call with delay
func (s *MockUserService) GetUserRegion(ctx context.Context, userID string) (string, error) {
	// Simulate network latency + processing time
	delay := time.Duration(50+rand.Intn(100)) * time.Millisecond
	select {
	case <-time.After(delay):
		s.mu.RLock()
		region, ok := s.regions[userID]
		s.mu.RUnlock()
		if !ok {
			return "Unknown", nil // Simulate finding unknown users
		}
		return region, nil
	case <-ctx.Done():
		return "", ctx.Err() // Respect context cancellation
	}
}

// --- Pipeline Stages ---

// 1. ParseStage: RawEvent -> ParsedEvent
type ParseStage struct {
	logger *log.Logger
}

func (s *ParseStage) Process(_ context.Context, raw RawEvent) (ParsedEvent, error) {
	var event ParsedEvent
	err := json.Unmarshal([]byte(raw), &event)
	if err != nil {
		s.logger.Printf("WARN: Failed to parse event: %v. Raw: %s", err, raw)
		// Return an invalid event instead of failing the whole pipeline stage
		return ParsedEvent{IsValid: false, Payload: string(raw)}, nil
	}
	event.IsValid = true
	// s.logger.Printf("DEBUG: Parsed event for user %s", event.UserID)
	return event, nil
}

// 2. EnrichStage: ParsedEvent -> EnrichedEvent (Uses MockUserService)
type EnrichStage struct {
	userService *MockUserService
	logger      *log.Logger
}

func (s *EnrichStage) Process(ctx context.Context, event ParsedEvent) (EnrichedEvent, error) {
	// Skip invalid events directly
	if !event.IsValid {
		return EnrichedEvent{ParsedEvent: event}, nil // Pass through invalid events
	}

	region, err := s.userService.GetUserRegion(ctx, event.UserID)
	if err != nil {
		// Log error but potentially continue with default values
		s.logger.Printf("ERROR: Failed to get region for user %s: %v", event.UserID, err)
		// Depending on requirements, you might return the error or default
		// return EnrichedEvent{}, fmt.Errorf("enrichment failed for %s: %w", event.UserID, err)
		region = "Error" // Default value on error
	}

	// Determine priority (example logic)
	isPriority := (event.EventType == "purchase" && region == "NA") || event.EventType == "alert"

	enriched := EnrichedEvent{
		ParsedEvent: event,
		UserRegion:  region,
		IsPriority:  isPriority,
	}
	// s.logger.Printf("DEBUG: Enriched event for user %s, Region: %s, Priority: %t", enriched.UserID, enriched.UserRegion, enriched.IsPriority)
	return enriched, nil
}

// 3. FilterStage: EnrichedEvent -> EnrichedEvent (Filters non-priority events)
// This stage demonstrates filtering *within* a stage.
// Alternatively, you could use a dedicated Filter stage type if Fluxus provided one,
// or implement a StreamStage that selectively forwards items.
type FilterStage struct {
	logger *log.Logger
}

func (s *FilterStage) Process(_ context.Context, event EnrichedEvent) (EnrichedEvent, error) {
	// Only pass through valid, priority events
	if event.IsValid && event.IsPriority {
		// s.logger.Printf("DEBUG: Forwarding priority event for user %s", event.UserID)
		return event, nil
	}
	// Signal to skip this event downstream (e.g., by returning a specific error or modifying the event)
	// Here, we'll return a special error that the adapter can handle if configured with SkipOnError.
	// Or, more simply, return the event but mark it somehow, and let the next stage handle it.
	// Let's modify the event for simplicity in this example.
	event.IsValid = false // Mark as invalid if not priority
	// s.logger.Printf("DEBUG: Filtering non-priority/invalid event for user %s", event.UserID)
	return event, nil
}

// 4. ActionStage: EnrichedEvent -> ProcessingResult (Simulates final action)
type ActionStage struct {
	logger *log.Logger
}

func (s *ActionStage) Process(_ context.Context, event EnrichedEvent) (ProcessingResult, error) {
	// Only process events that made it through the filter
	if !event.IsValid {
		// This event was either invalid initially or filtered out
		return ProcessingResult{
			UserID:    event.UserID, // May be empty if parsing failed badly
			EventType: event.EventType,
			Region:    event.UserRegion,
			Status:    "Filtered/Invalid",
		}, nil
	}

	// Simulate sending a notification or updating a metric for priority events
	s.logger.Printf("INFO: Taking action for priority event: User %s, Type %s, Region %s",
		event.UserID, event.EventType, event.UserRegion)
	// Simulate action time
	time.Sleep(time.Duration(10+rand.Intn(20)) * time.Millisecond)

	return ProcessingResult{
		UserID:    event.UserID,
		EventType: event.EventType,
		Region:    event.UserRegion,
		Status:    "Processed",
	}, nil
}

// --- Main Execution ---

func main() {
	fmt.Println("🚀 Starting Event Processing Pipeline...")
	rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Overall timeout
	defer cancel()

	// --- Configuration ---
	pipelineLogger := log.New(os.Stdout, "Pipeline: ", log.Ltime)
	stageLogger := log.New(os.Stdout, "Stage: ", log.Ltime|log.Lshortfile)
	concurrency := runtime.NumCPU() // Concurrency for the enrichment stage
	bufferSize := 10                // Buffer between stages
	eventCount := 100               // Number of events to generate

	// --- Dependencies ---
	userService := NewMockUserService()

	// --- Build Pipeline ---
	builder := fluxus.NewStreamPipeline[RawEvent](
		fluxus.WithStreamBufferSize(bufferSize),
		fluxus.WithStreamLogger(pipelineLogger),   // Added the logger option based on surrounding code
		fluxus.WithStreamConcurrency(concurrency), // Set concurrency for the pipeline
	) // Correct closing parenthesis for the function call

	// Stage 1: Parse
	parseStage := &ParseStage{logger: stageLogger}
	b2 := fluxus.AddStage(builder, "parse_stage", parseStage)

	// Stage 2: Enrich (Concurrent)
	enrichStage := &EnrichStage{userService: userService, logger: stageLogger}
	// Use StreamAdapter options for concurrency and error handling
	b3 := fluxus.AddStage(b2, "enrich_stage", enrichStage,
		fluxus.WithAdapterConcurrency[ParsedEvent, EnrichedEvent](concurrency),          // <-- Correct usage
		fluxus.WithAdapterErrorStrategy[ParsedEvent, EnrichedEvent](fluxus.SkipOnError), // Assuming SkipOnError was intended
		fluxus.WithAdapterLogger[ParsedEvent, EnrichedEvent](stageLogger),               // Pass the logger instance
	)

	// Stage 3: Filter (Mark non-priority as invalid)
	filterStage := &FilterStage{logger: stageLogger}
	b4 := fluxus.AddStage(b3, "filter_stage", filterStage)

	// Stage 4: Action
	actionStage := &ActionStage{logger: stageLogger}
	b5 := fluxus.AddStage(b4, "action_stage", actionStage)

	// Finalize
	pipeline, err := fluxus.Finalize(b5)
	if err != nil {
		log.Fatalf("❌ Failed to build pipeline: %v", err)
	}
	fmt.Printf("✅ Pipeline built successfully with %d stages.\n", 5) // Includes internal adapter stages

	// --- Prepare Channels ---
	source := make(chan RawEvent, bufferSize)
	sink := make(chan ProcessingResult, eventCount) // Buffer sink to avoid blocking collection

	// --- Run Pipeline ---
	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		fmt.Println("⏳ Pipeline Run started...")
		runErr = fluxus.Run(ctx, pipeline, source, sink)
		if runErr != nil {
			// Log errors other than context cancellation
			if !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
				pipelineLogger.Printf("❌ Pipeline Run finished with error: %v", runErr)
			} else {
				pipelineLogger.Printf("ℹ️ Pipeline Run cancelled or timed out.")
			}
		} else {
			pipelineLogger.Println("✅ Pipeline Run completed successfully.")
		}
	}()

	// --- Generate and Feed Data ---
	go func() {
		defer close(source) // IMPORTANT: Close source when done
		fmt.Printf("⚙️ Generating and feeding %d events...\n", eventCount)
		eventTypes := []string{"login", "view_page", "purchase", "logout", "alert"}
		for i := 0; i < eventCount; i++ {
			userID := fmt.Sprintf("user_%d", rand.Intn(7)+1) // Include some unknown users
			eventType := eventTypes[rand.Intn(len(eventTypes))]
			payload := fmt.Sprintf("data for event %d", i)
			timestamp := time.Now().Add(-time.Duration(rand.Intn(1000)) * time.Millisecond)

			// Occasionally send invalid JSON
			var rawEvent RawEvent
			if rand.Intn(10) == 0 {
				rawEvent = RawEvent(fmt.Sprintf(`{"timestamp": "%s", "user_id": "%s", "event_type": "%s", "payload": "%s"`, // Missing closing brace
					timestamp.Format(time.RFC3339Nano), userID, eventType, payload))
			} else {
				event := ParsedEvent{
					Timestamp: timestamp,
					UserID:    userID,
					EventType: eventType,
					Payload:   payload,
				}
				jsonData, _ := json.Marshal(event)
				rawEvent = RawEvent(jsonData)
			}

			select {
			case source <- rawEvent:
				// Add a small delay to simulate realistic arrival rate
				time.Sleep(time.Duration(5+rand.Intn(10)) * time.Millisecond)
			case <-ctx.Done():
				fmt.Println("⚠️ Source generation stopped due to context cancellation.")
				return
			}
		}
		fmt.Println("✅ Finished feeding events.")
	}()

	// --- Collect Results ---
	results := make([]ProcessingResult, 0, eventCount)
	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		fmt.Println("👂 Listening for results on sink channel...")
		for result := range sink {
			results = append(results, result)
		}
		fmt.Println("🏁 Sink channel closed, finished collecting results.")
	}()

	// --- Wait and Summarize ---
	wg.Wait() // Wait for pipeline Run to finish
	fmt.Println("📊 Pipeline execution finished. Waiting for result collection...")
	collectWg.Wait() // Wait for collector to finish (ensures sink is drained)

	fmt.Printf("\n--- Processing Summary (%d results collected) ---\n", len(results))
	processedCount := 0
	filteredCount := 0
	statusCounts := make(map[string]int)
	for _, res := range results {
		statusCounts[res.Status]++
		if res.Status == "Processed" {
			processedCount++
		} else if res.Status == "Filtered/Invalid" {
			filteredCount++
		}
	}

	fmt.Printf("  - Successfully Processed (Priority Actions Taken): %d\n", processedCount)
	fmt.Printf("  - Filtered or Invalid: %d\n", filteredCount)
	fmt.Printf("  - Status Breakdown: %v\n", statusCounts)
	fmt.Printf("  - Total Events Handled: %d\n", len(results))

	if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		fmt.Printf("\n⚠️ Pipeline finished with error: %v\n", runErr)
	}

	fmt.Println("\n🎉 Event Processing Demo Complete!")
}
