package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/synoptiq/go-fluxus"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// initTracer creates and registers a new trace provider with OTLP exporter
func initTracer() (*tracesdk.TracerProvider, error) {
	fmt.Println("🔧 Initializing OpenTelemetry Tracer...")
	otlpEndpoint := os.Getenv("OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "localhost:4317" // Default OTLP gRPC endpoint
	}
	fmt.Printf("   Using OTLP endpoint: %s\n", otlpEndpoint)

	ctx := context.Background()
	traceExporter, err := otlptrace.New(
		ctx,
		otlptracegrpc.NewClient(
			otlptracegrpc.WithEndpoint(otlpEndpoint),
			otlptracegrpc.WithInsecure(), // Use insecure for local demo
		),
	)
	if err != nil {
		return nil, fmt.Errorf("❌ failed to create trace exporter: %w", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(traceExporter),
		tracesdk.WithSampler(tracesdk.AlwaysSample()), // Sample all traces for demo
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("fluxus-tracing-example"),
			attribute.String("environment", "demo"),
		)),
	)

	otel.SetTracerProvider(tp)
	fmt.Println("✅ Tracer initialized and registered globally.")
	return tp, nil
}

// simulateAPICall simulates an API call with some random delay and potential failure
func simulateAPICall(ctx context.Context, apiName string) (map[string]interface{}, error) {
	// Create a span for the API call (will be child of pipeline stage span)
	tracer := otel.Tracer("api-client-simulator")
	_, span := tracer.Start(ctx, fmt.Sprintf("API Call: %s", apiName))
	defer span.End()

	fmt.Printf("      📞 Calling API: %s...\n", apiName)
	delay := 50 + rand.Intn(150) // Reduced delay for faster demo
	time.Sleep(time.Duration(delay) * time.Millisecond)

	span.SetAttributes(
		attribute.String("api.name", apiName),
		attribute.Int("api.latency_ms", delay),
	)

	if rand.Float32() < 0.1 { // 10% failure rate
		err := fmt.Errorf("API %s failed simulation", apiName)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		fmt.Printf("      ❌ API %s failed!\n", apiName)
		return nil, err
	}

	result := map[string]interface{}{
		"api":     apiName,
		"success": true,
		"latency": delay,
		"data":    fmt.Sprintf("Sample data from %s", apiName),
	}
	span.SetStatus(codes.Ok, "Success")
	fmt.Printf("      ✅ API %s succeeded (%d ms).\n", apiName, delay)
	return result, nil
}

// --- API Simulation Functions ---
// CORRECTED: Changed signature to accept Request struct
func fetchUserData(ctx context.Context, _ Request) (map[string]interface{}, error) {
	// In a real scenario, you would use req.UserID here
	return simulateAPICall(ctx, "user-service")
}

// CORRECTED: Changed signature to accept Request struct
func fetchProductData(ctx context.Context, _ Request) (map[string]interface{}, error) {
	// In a real scenario, you would use req.ProductID here
	return simulateAPICall(ctx, "product-service")
}

func fetchPricingData(ctx context.Context, _ string) (map[string]interface{}, error) {
	return simulateAPICall(ctx, "pricing-service")
}
func fetchRecommendations(ctx context.Context, _ string) (map[string]interface{}, error) {
	return simulateAPICall(ctx, "recommendation-service")
}

// Request represents the input to the pipeline
type Request struct {
	UserID    string
	ProductID string
}

// Response represents the output of the pipeline
type Response struct {
	UserData         map[string]interface{}
	ProductData      map[string]interface{}
	PricingData      map[string]interface{}
	Recommendations  map[string]interface{}
	ProcessingTimeMs int64
}

// buildTracedPipeline constructs the example pipeline with tracing enabled
func buildTracedPipeline() *fluxus.Pipeline[Request, Response] {
	fmt.Println("🛠️ Building traced pipeline...")

	// Stage 1: Fetch user and product data in parallel
	// CORRECTED: These conversions now work because the function signatures match
	userDataStage := fluxus.StageFunc[Request, map[string]interface{}](fetchUserData)
	productDataStage := fluxus.StageFunc[Request, map[string]interface{}](fetchProductData)

	// Wrap stages with tracing
	tracedUserDataStage := fluxus.NewTracedStage(
		userDataStage,
		fluxus.WithTracerName[Request, map[string]interface{}]("fetch-user-data"),
		fluxus.WithTracerAttributes[Request, map[string]interface{}](attribute.String("service.type", "user")),
	)
	fmt.Println("   - Wrapped user data stage with tracing.")

	tracedProductDataStage := fluxus.NewTracedStage(
		productDataStage,
		fluxus.WithTracerName[Request, map[string]interface{}]("fetch-product-data"),
		fluxus.WithTracerAttributes[Request, map[string]interface{}](attribute.String("service.type", "product")),
	)
	fmt.Println("   - Wrapped product data stage with tracing.")

	// Fan out for parallel fetching
	dataFetchStages := []fluxus.Stage[Request, map[string]interface{}]{
		tracedUserDataStage,
		tracedProductDataStage,
	}
	dataFanOut := fluxus.NewFanOut(dataFetchStages...)

	// Add tracing to the fan-out operation itself
	tracedDataFanOut := fluxus.NewTracedFanOut(
		dataFanOut,
		"parallel-initial-fetch",
		attribute.String("operation", "data-fetch"),
	)
	fmt.Println("   - Created traced fan-out for initial data fetch.")

	// Stage 2: Process the initial data and make additional API calls
	processDataStage := fluxus.StageFunc[[]map[string]interface{}, Response](func(ctx context.Context, results []map[string]interface{}) (Response, error) {
		fmt.Println("   ⚙️ Entering enrichment stage...")
		if len(results) != 2 {
			return Response{}, fmt.Errorf("expected 2 results from fan-out, got %d", len(results))
		}

		response := Response{UserData: results[0], ProductData: results[1]}

		// --- Get UserID and ProductID from the context or initial request if needed ---
		// For this example, we'll use placeholder IDs as before, but in a real app
		// you might get these from the initial Request passed via context or results.
		productID := "product-xyz" // Simplified for demo
		userID := "user-abc"       // Simplified for demo
		// Example: If the Request struct was passed down via context:
		// if initialRequest, ok := ctx.Value("initialRequest").(Request); ok {
		//     userID = initialRequest.UserID
		//     productID = initialRequest.ProductID
		// }

		// Fetch pricing data (will create its own span via simulateAPICall)
		pricingData, err := fetchPricingData(ctx, productID) // Pass the correct ID
		if err != nil {
			return response, err
		}
		response.PricingData = pricingData

		// Fetch recommendations (will create its own span via simulateAPICall)
		recommendations, err := fetchRecommendations(ctx, userID) // Pass the correct ID
		if err != nil {
			return response, err
		}
		response.Recommendations = recommendations

		// Calculate total simulated latency
		totalLatency := int64(0)
		if l, ok := response.UserData["latency"].(int); ok {
			totalLatency += int64(l)
		}
		if l, ok := response.ProductData["latency"].(int); ok {
			totalLatency += int64(l)
		}
		if l, ok := response.PricingData["latency"].(int); ok {
			totalLatency += int64(l)
		}
		if l, ok := response.Recommendations["latency"].(int); ok {
			totalLatency += int64(l)
		}
		response.ProcessingTimeMs = totalLatency

		fmt.Println("   ✅ Enrichment stage complete.")
		return response, nil
	})

	// Wrap enrichment stage with tracing
	tracedProcessDataStage := fluxus.NewTracedStage(
		processDataStage,
		fluxus.WithTracerName[[]map[string]interface{}, Response]("process-and-enrich-data"),
		fluxus.WithTracerAttributes[[]map[string]interface{}, Response](attribute.String("operation", "enrichment")),
	)
	fmt.Println("   - Wrapped enrichment stage with tracing.")

	// Chain all stages together
	chainedStage := fluxus.Chain(tracedDataFanOut, tracedProcessDataStage)
	fmt.Println("   - Chained fan-out and enrichment stages.")

	// Create a pipeline with the chained stage
	pipeline := fluxus.NewPipeline(chainedStage)
	fmt.Println("✅ Pipeline built successfully.")
	return pipeline
}

func main() {
	fmt.Println("🛰️ Fluxus OpenTelemetry Tracing Demonstration")
	fmt.Println("===========================================")
	fmt.Println("This example shows how to integrate OpenTelemetry tracing into a Fluxus pipeline.")
	fmt.Println("Each stage and simulated API call will generate spans.")

	// Initialize OpenTelemetry tracing
	tp, err := initTracer()
	if err != nil {
		log.Fatalf("❌ Failed to initialize tracer: %v", err)
	}
	// Ensure tracer provider is shut down cleanly on exit
	defer func() {
		fmt.Println("🔌 Shutting down tracer provider...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.Printf("⚠️ Error shutting down tracer provider: %v", err)
		} else {
			fmt.Println("✅ Tracer provider shut down.")
		}
	}()

	// Create a sample request
	request := Request{UserID: "user123", ProductID: "product456"}

	// Build the traced pipeline
	pipeline := buildTracedPipeline()

	// --- Process the request ---
	fmt.Println("\n▶️ Processing request through the pipeline...")
	tracer := otel.Tracer("main-processor") // Get a tracer for the main operation
	ctx, rootSpan := tracer.Start(context.Background(), "HandleProductViewRequest")
	rootSpan.SetAttributes(
		attribute.String("user.id", request.UserID),
		attribute.String("product.id", request.ProductID),
	)

	// Optional: Pass the initial request down via context if needed by later stages
	// ctx = context.WithValue(ctx, "initialRequest", request)

	startTime := time.Now()
	result, err := pipeline.Process(ctx, request) // Pass the context with the root span
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("❌ Pipeline processing failed after %v: %v\n", duration, err)
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, "Pipeline failed")
	} else {
		fmt.Printf("✅ Pipeline processing successful in %v.\n", duration)
		rootSpan.SetAttributes(attribute.Int64("total_simulated_latency_ms", result.ProcessingTimeMs))
		rootSpan.SetStatus(codes.Ok, "Success")
	}
	rootSpan.End() // End the root span

	// --- Output Results ---
	if err == nil {
		fmt.Println("\n📊 Processing Summary:")
		fmt.Printf("   User: %s, Product: %s\n", request.UserID, request.ProductID)
		fmt.Printf("   Total Simulated API Latency: %d ms\n", result.ProcessingTimeMs)
		// You could print more details from the result if needed
	}

	// --- Trace Viewing Instructions ---
	fmt.Println("\n📍 Trace Viewing Instructions:")
	fmt.Println("   Traces have been exported via OTLP.")
	fmt.Println("   Ensure an OTLP collector (like Jaeger, Zipkin, Tempo, etc.) is running")
	fmt.Println("   and accessible at the configured endpoint (default: localhost:4317).")
	fmt.Println("   Example Jaeger setup (Docker):")
	fmt.Println("     docker run -d --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest")
	fmt.Println("   View traces in your backend UI (e.g., http://localhost:16686 for Jaeger).")

	fmt.Println("\nDemo Complete!")
	// Add a small delay to ensure traces are flushed before exit
	time.Sleep(2 * time.Second)
}
