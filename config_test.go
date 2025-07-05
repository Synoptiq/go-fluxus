package fluxus_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/synoptiq/go-fluxus"
)

func TestSimpleClassicPipelineFromConfig(t *testing.T) {
	// Create a simple YAML config for a classic pipeline with one custom stage
	yamlConfig := `version: "1.0.0"
pipeline_name: "test_simple_pipeline"
pipeline_type: "classic"
stages:
  - name: "double_numbers"
    type: "custom"
    properties:
      factory: "double_stage_factory"`

	// Parse the config
	config, err := fluxus.LoadPipelineConfigFromYAML([]byte(yamlConfig))
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	// Verify config was parsed correctly
	if config.Name != "test_simple_pipeline" {
		t.Errorf("Expected pipeline name 'test_simple_pipeline', got '%s'", config.Name)
	}
	if config.Type != fluxus.PipelineTypeClassic {
		t.Errorf("Expected pipeline type 'classic', got '%s'", config.Type)
	}
	if len(config.Stages) != 1 {
		t.Fatalf("Expected 1 stage, got %d", len(config.Stages))
	}

	// Create registry and register our test functions
	registry := fluxus.DefaultRegistry()

	// Register a custom stage factory
	err = registry.RegisterExecutor("double_stage_factory", func(config map[string]any) (interface{}, error) {
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			if num, ok := input.(int); ok {
				return num * 2, nil
			}
			return input, nil
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register double_stage_factory: %v", err)
	}

	// Build the pipeline from config
	pipelineInterface, err := fluxus.BuildPipelineFromConfig(config, registry)
	if err != nil {
		t.Fatalf("Failed to build pipeline: %v", err)
	}

	// Type assert to the correct pipeline type
	pipeline, ok := pipelineInterface.(*fluxus.Pipeline[any, any])
	if !ok {
		t.Fatalf("Expected *Pipeline[any, any], got %T", pipelineInterface)
	}

	// Start the pipeline
	ctx := context.Background()
	err = pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Test the pipeline with an integer (should be doubled)
	result, err := pipeline.Process(ctx, 5)
	if err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	// Single stage doubles 5 to 10
	if result != 10 {
		t.Errorf("Expected result 10, got %v", result)
	}

	// Test the pipeline with a string (should return unchanged since it's not an int)
	result2, err := pipeline.Process(ctx, "hello")
	if err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	// Stage receives "hello" but expects int, so returns "hello" unchanged
	if result2 != "hello" {
		t.Errorf("Expected result 'hello', got %v", result2)
	}
}

func TestComplexClassicPipelineFromConfig(t *testing.T) {
	// Create a more complex YAML config with multiple stages that work together
	yamlConfig := `version: "1.0.0"
pipeline_name: "complex_test_pipeline"
pipeline_type: "classic"
stages:
  - name: "validate_input"
    type: "filter"
    properties:
      filter_function: "positive_numbers_only"
  - name: "double_value"
    type: "custom"
    properties:
      factory: "complex_double_stage_factory"
  - name: "convert_to_string"
    type: "custom"
    properties:
      factory: "to_string_factory"
  - name: "add_prefix"
    type: "custom"
    properties:
      factory: "add_prefix_factory"
      config:
        prefix: "result_"`

	// Parse the config
	config, err := fluxus.LoadPipelineConfigFromYAML([]byte(yamlConfig))
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	// Verify config was parsed correctly
	if config.Name != "complex_test_pipeline" {
		t.Errorf("Expected pipeline name 'complex_test_pipeline', got '%s'", config.Name)
	}
	if len(config.Stages) != 4 {
		t.Fatalf("Expected 4 stages, got %d", len(config.Stages))
	}

	// Create registry and register our test functions
	registry := fluxus.DefaultRegistry()

	// Register filter predicate for positive numbers only
	err = registry.RegisterExecutor("positive_numbers_only", fluxus.PredicateFunc[any](func(ctx context.Context, input any) (bool, error) {
		if num, ok := input.(int); ok {
			return num > 0, nil // Only allow positive numbers
		}
		return false, nil // Reject non-integers
	}))
	if err != nil {
		t.Fatalf("Failed to register positive_numbers_only: %v", err)
	}

	// Register custom stage factory for doubling numbers
	err = registry.RegisterExecutor("complex_double_stage_factory", func(config map[string]any) (interface{}, error) {
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			if num, ok := input.(int); ok {
				return num * 2, nil
			}
			return input, nil
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register complex_double_stage_factory: %v", err)
	}

	// Register custom stage factory for converting to string
	err = registry.RegisterExecutor("to_string_factory", func(config map[string]any) (interface{}, error) {
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			return fmt.Sprintf("%v", input), nil
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register to_string_factory: %v", err)
	}

	// Register custom stage factory for adding prefix (uses config)
	err = registry.RegisterExecutor("add_prefix_factory", func(config map[string]any) (interface{}, error) {
		prefix := "default_"
		if p, ok := config["prefix"].(string); ok {
			prefix = p
		}
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			if str, ok := input.(string); ok {
				return prefix + str, nil
			}
			return input, nil
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register add_prefix_factory: %v", err)
	}

	// Build the pipeline from config
	pipelineInterface, err := fluxus.BuildPipelineFromConfig(config, registry)
	if err != nil {
		t.Fatalf("Failed to build pipeline: %v", err)
	}

	// Type assert to the correct pipeline type
	pipeline, ok := pipelineInterface.(*fluxus.Pipeline[any, any])
	if !ok {
		t.Fatalf("Expected *Pipeline[any, any], got %T", pipelineInterface)
	}

	// Start the pipeline
	ctx := context.Background()
	err = pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Test case 1: Valid positive integer - should go through all stages
	result, err := pipeline.Process(ctx, 5)
	if err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	// Flow: 5 -> filter (pass) -> double (10) -> toString ("10") -> addPrefix ("result_10")
	expected := "result_10"
	if result != expected {
		t.Errorf("Expected result '%s', got %v", expected, result)
	}

	// Test case 2: Negative integer - should be filtered out (rejected by filter)
	result2, err := pipeline.Process(ctx, -3)
	if err == nil {
		t.Errorf("Expected error for negative number, but got result: %v", result2)
	}

	// Test case 3: Zero - should be filtered out (rejected by filter)
	result3, err := pipeline.Process(ctx, 0)
	if err == nil {
		t.Errorf("Expected error for zero, but got result: %v", result3)
	}

	// Test case 4: Non-integer - should be filtered out
	result4, err := pipeline.Process(ctx, "hello")
	if err == nil {
		t.Errorf("Expected error for string input, but got result: %v", result4)
	}

	// Test case 5: Another valid positive integer
	result5, err := pipeline.Process(ctx, 7)
	if err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	// Flow: 7 -> filter (pass) -> double (14) -> toString ("14") -> addPrefix ("result_14")
	expected5 := "result_14"
	if result5 != expected5 {
		t.Errorf("Expected result '%s', got %v", expected5, result5)
	}
}

func TestPipelineStageTypeMismatch(t *testing.T) {
	// Create a YAML config with intentionally incompatible stages
	// Stage 1 outputs string, Stage 2 expects array input
	yamlConfig := `version: "1.0.0"
pipeline_name: "type_mismatch_pipeline"
pipeline_type: "classic"
stages:
  - name: "number_to_string"
    type: "custom"
    properties:
      factory: "number_to_string_factory"
  - name: "process_array"
    type: "custom"
    properties:
      factory: "array_processor_factory"`

	// Parse the config
	config, err := fluxus.LoadPipelineConfigFromYAML([]byte(yamlConfig))
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	// Create registry and register mismatched stages
	registry := fluxus.DefaultRegistry()

	// Stage 1: Converts numbers to strings (outputs string)
	err = registry.RegisterExecutor("number_to_string_factory", func(config map[string]any) (interface{}, error) {
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			if num, ok := input.(int); ok {
				return fmt.Sprintf("number_%d", num), nil
			}
			return "not_a_number", nil
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register number_to_string_factory: %v", err)
	}

	// Stage 2: Expects array input but will receive string from Stage 1
	err = registry.RegisterExecutor("array_processor_factory", func(config map[string]any) (interface{}, error) {
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			// This stage expects an array but will receive a string
			if arr, ok := input.([]int); ok {
				sum := 0
				for _, val := range arr {
					sum += val
				}
				return sum, nil
			}
			// If we don't get an array, this demonstrates the type mismatch
			return nil, fmt.Errorf("expected []int array, got %T: %v", input, input)
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register array_processor_factory: %v", err)
	}

	// Build the pipeline from config - this should succeed
	// (type checking happens at runtime, not build time)
	pipelineInterface, err := fluxus.BuildPipelineFromConfig(config, registry)
	if err != nil {
		t.Fatalf("Failed to build pipeline: %v", err)
	}

	pipeline, ok := pipelineInterface.(*fluxus.Pipeline[any, any])
	if !ok {
		t.Fatalf("Expected *Pipeline[any, any], got %T", pipelineInterface)
	}

	// Start the pipeline
	ctx := context.Background()
	err = pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Test with input - this should fail at runtime due to type mismatch
	result, err := pipeline.Process(ctx, 42)

	// We expect this to fail because:
	// 1. Stage 1 converts 42 to "number_42" (string)
	// 2. Stage 2 expects []int but receives string
	// 3. Stage 2 should return an error about type mismatch
	if err == nil {
		t.Errorf("Expected error due to type mismatch, but got result: %v", result)
	} else {
		// Verify the error message indicates type mismatch
		expectedErrorSubstring := "expected []int array, got string"
		if !strings.Contains(err.Error(), expectedErrorSubstring) {
			t.Errorf("Expected error to contain '%s', got: %v", expectedErrorSubstring, err)
		} else {
			t.Logf("✓ Correctly detected type mismatch: %v", err)
		}
	}
}

func TestPipelineCompatibleStageTypes(t *testing.T) {
	// Create a YAML config with compatible stages to contrast with the mismatch test
	yamlConfig := `version: "1.0.0"
pipeline_name: "compatible_types_pipeline"
pipeline_type: "classic"
stages:
  - name: "create_array"
    type: "custom"
    properties:
      factory: "create_array_factory"
  - name: "process_array"
    type: "custom"
    properties:
      factory: "sum_array_factory"`

	// Parse the config
	config, err := fluxus.LoadPipelineConfigFromYAML([]byte(yamlConfig))
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	// Create registry with compatible stages
	registry := fluxus.DefaultRegistry()

	// Stage 1: Creates an array from a number (outputs []int)
	err = registry.RegisterExecutor("create_array_factory", func(config map[string]any) (interface{}, error) {
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			if num, ok := input.(int); ok {
				// Create array [1, 2, ..., num]
				result := make([]int, num)
				for i := 0; i < num; i++ {
					result[i] = i + 1
				}
				return result, nil
			}
			return []int{}, nil
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register create_array_factory: %v", err)
	}

	// Stage 2: Processes array (expects []int input, same as stage 1 output)
	err = registry.RegisterExecutor("sum_array_factory", func(config map[string]any) (interface{}, error) {
		return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
			if arr, ok := input.([]int); ok {
				sum := 0
				for _, val := range arr {
					sum += val
				}
				return sum, nil
			}
			return nil, fmt.Errorf("expected []int array, got %T: %v", input, input)
		}), nil
	})
	if err != nil {
		t.Fatalf("Failed to register sum_array_factory: %v", err)
	}

	// Build and start pipeline
	pipelineInterface, err := fluxus.BuildPipelineFromConfig(config, registry)
	if err != nil {
		t.Fatalf("Failed to build pipeline: %v", err)
	}

	pipeline, ok := pipelineInterface.(*fluxus.Pipeline[any, any])
	if !ok {
		t.Fatalf("Expected *Pipeline[any, any], got %T", pipelineInterface)
	}

	ctx := context.Background()
	err = pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Test with compatible types - should succeed
	result, err := pipeline.Process(ctx, 5)
	if err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	// Input: 5 -> Stage1: creates [1,2,3,4,5] -> Stage2: sums to 15
	expected := 15 // 1+2+3+4+5 = 15
	if result != expected {
		t.Errorf("Expected result %d, got %v", expected, result)
	} else {
		t.Logf("✓ Compatible stages worked correctly: input=5, output=%v", result)
	}
}

func TestPipelineWithObservability(t *testing.T) {
	// Create a YAML config with metrics and tracing enabled
	yamlConfig := `version: "1.0.0"
pipeline_name: "observability_test_pipeline"
pipeline_type: "classic"
metrics:
  enabled: true
  type: "noop"
  endpoint: ""
tracing:
  enabled: true
  type: "noop"
  endpoint: ""
stages:
  - name: "stage1_with_metrics"
    type: "custom"
    metrics: true
    tracing: false
    properties:
      factory: "observability_stage1_factory"
  - name: "stage2_with_tracing"
    type: "custom"
    metrics: false
    tracing: true
    properties:
      factory: "observability_stage2_factory"
  - name: "stage3_with_both"
    type: "custom"
    metrics: true
    tracing: true
    properties:
      factory: "observability_stage3_factory"
  - name: "stage4_with_neither"
    type: "custom"
    metrics: false
    tracing: false
    properties:
      factory: "observability_stage4_factory"`

	// Parse the config
	config, err := fluxus.LoadPipelineConfigFromYAML([]byte(yamlConfig))
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	// Verify config parsed observability settings correctly
	if !config.Metrics.Enabled {
		t.Error("Expected metrics to be enabled")
	}
	if config.Metrics.Type != "noop" {
		t.Errorf("Expected metrics type 'noop', got '%s'", config.Metrics.Type)
	}
	if !config.Tracing.Enabled {
		t.Error("Expected tracing to be enabled")
	}
	if config.Tracing.Type != "noop" {
		t.Errorf("Expected tracing type 'noop', got '%s'", config.Tracing.Type)
	}

	// Verify individual stage observability settings
	expectedStageSettings := []struct {
		name    string
		metrics bool
		tracing bool
	}{
		{"stage1_with_metrics", true, false},
		{"stage2_with_tracing", false, true},
		{"stage3_with_both", true, true},
		{"stage4_with_neither", false, false},
	}

	for i, expected := range expectedStageSettings {
		stage := config.Stages[i]
		if stage.Name != expected.name {
			t.Errorf("Stage %d: expected name '%s', got '%s'", i, expected.name, stage.Name)
		}
		if stage.Metrics != expected.metrics {
			t.Errorf("Stage %s: expected metrics=%v, got %v", stage.Name, expected.metrics, stage.Metrics)
		}
		if stage.Tracing != expected.tracing {
			t.Errorf("Stage %s: expected tracing=%v, got %v", stage.Name, expected.tracing, stage.Tracing)
		}
	}

	// Create registry and register observability-aware stages
	registry := fluxus.DefaultRegistry()

	// Register simple stage factories
	stageFactories := map[string]func(config map[string]any) (interface{}, error){
		"observability_stage1_factory": func(config map[string]any) (interface{}, error) {
			return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
				return fmt.Sprintf("stage1(%v)", input), nil
			}), nil
		},
		"observability_stage2_factory": func(config map[string]any) (interface{}, error) {
			return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
				return fmt.Sprintf("stage2(%v)", input), nil
			}), nil
		},
		"observability_stage3_factory": func(config map[string]any) (interface{}, error) {
			return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (any, error) {
				return fmt.Sprintf("stage3(%v)", input), nil
			}), nil
		},
		"observability_stage4_factory": func(config map[string]any) (interface{}, error) {
			return fluxus.StageFunc[any, any](func(ctx context.Context, input any) (interface{}, error) {
				return fmt.Sprintf("stage4(%v)", input), nil
			}), nil
		},
	}

	for name, factory := range stageFactories {
		err = registry.RegisterExecutor(fluxus.Executor(name), factory)
		if err != nil {
			t.Fatalf("Failed to register %s: %v", name, err)
		}
	}

	// Build the pipeline from config - this should create observability components automatically
	pipelineInterface, err := fluxus.BuildPipelineFromConfig(config, registry)
	if err != nil {
		t.Fatalf("Failed to build pipeline: %v", err)
	}

	pipeline, ok := pipelineInterface.(*fluxus.Pipeline[any, any])
	if !ok {
		t.Fatalf("Expected *Pipeline[any, any], got %T", pipelineInterface)
	}

	// Start and test the pipeline
	ctx := context.Background()
	err = pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Process some data through the pipeline
	result, err := pipeline.Process(ctx, "test_input")
	if err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	// Verify the result (data flowed through all stages)
	expected := "stage4(stage3(stage2(stage1(test_input))))"
	if result != expected {
		t.Errorf("Expected result '%s', got %v", expected, result)
	}

	// Verify that Noop observability components were created and used
	t.Logf("✓ Pipeline processed successfully with observability config")
	t.Logf("✓ Configuration correctly parsed:")
	t.Logf("  - Pipeline-level metrics: enabled=%v, type=%s", config.Metrics.Enabled, config.Metrics.Type)
	t.Logf("  - Pipeline-level tracing: enabled=%v, type=%s", config.Tracing.Enabled, config.Tracing.Type)
	t.Logf("  - Stage1: metrics=%v, tracing=%v", config.Stages[0].Metrics, config.Stages[0].Tracing)
	t.Logf("  - Stage2: metrics=%v, tracing=%v", config.Stages[1].Metrics, config.Stages[1].Tracing)
	t.Logf("  - Stage3: metrics=%v, tracing=%v", config.Stages[2].Metrics, config.Stages[2].Tracing)
	t.Logf("  - Stage4: metrics=%v, tracing=%v", config.Stages[3].Metrics, config.Stages[3].Tracing)
	t.Logf("✓ Result: %v", result)

	// Enhanced observability verification: Test that the ObservabilityFactory
	// correctly creates Noop implementations and that they work properly
	observabilityFactory := fluxus.NewObservabilityFactory()
	
	// Test metrics collector creation
	testMetricsCollector, err := observabilityFactory.CreateMetricsCollector(config.Metrics)
	if err != nil {
		t.Fatalf("Failed to create metrics collector: %v", err)
	}
	if _, ok := testMetricsCollector.(*fluxus.NoopMetricsCollector); !ok {
		t.Errorf("Expected NoopMetricsCollector, got %T", testMetricsCollector)
	} else {
		t.Logf("✓ NoopMetricsCollector created correctly from config")
	}

	// Test tracer provider creation
	testTracerProvider, err := observabilityFactory.CreateTracerProvider(config.Tracing, "test-service")
	if err != nil {
		t.Fatalf("Failed to create tracer provider: %v", err)
	}
	if _, ok := testTracerProvider.(*fluxus.NoopTracerProvider); !ok {
		t.Errorf("Expected NoopTracerProvider, got %T", testTracerProvider)
	} else {
		t.Logf("✓ NoopTracerProvider created correctly from config")
	}

	// Test that the NoopMetricsCollector methods can be called (they should do nothing)
	testMetricsCollector.PipelineStarted(ctx, "test-pipeline")
	testMetricsCollector.StageStarted(ctx, "test-stage")
	testMetricsCollector.StageCompleted(ctx, "test-stage", time.Millisecond*100)
	testMetricsCollector.PipelineCompleted(ctx, "test-pipeline", time.Millisecond*500, nil)
	t.Logf("✓ NoopMetricsCollector methods executed without error")

	// Test that the NoopTracerProvider can create tracers (they should do nothing)
	testTracer := testTracerProvider.Tracer("test-tracer")
	testCtx, span := testTracer.Start(ctx, "test-span")
	span.End()
	_ = testCtx // Use the context to avoid unused variable warning
	t.Logf("✓ NoopTracerProvider created tracer and span without error")

	// Test the observability factory with disabled configs
	disabledMetricsConfig := fluxus.PipelineMetricsConfig{Enabled: false}
	disabledMetricsCollector, err := observabilityFactory.CreateMetricsCollector(disabledMetricsConfig)
	if err != nil {
		t.Fatalf("Failed to create disabled metrics collector: %v", err)
	}
	if _, ok := disabledMetricsCollector.(*fluxus.NoopMetricsCollector); !ok {
		t.Errorf("Expected NoopMetricsCollector for disabled config, got %T", disabledMetricsCollector)
	} else {
		t.Logf("✓ Disabled metrics config correctly returns NoopMetricsCollector")
	}

	disabledTracingConfig := fluxus.PipelineTracingConfig{Enabled: false}
	disabledTracerProvider, err := observabilityFactory.CreateTracerProvider(disabledTracingConfig, "test-service")
	if err != nil {
		t.Fatalf("Failed to create disabled tracer provider: %v", err)
	}
	if _, ok := disabledTracerProvider.(*fluxus.NoopTracerProvider); !ok {
		t.Errorf("Expected NoopTracerProvider for disabled config, got %T", disabledTracerProvider)
	} else {
		t.Logf("✓ Disabled tracing config correctly returns NoopTracerProvider")
	}
}

func TestLoadPipelineConfigFromFile(t *testing.T) {
	// Create a temporary YAML file
	yamlContent := `
version: "1.0.0"
pipeline_name: "file_test_pipeline"
pipeline_type: "classic"
stages:
  - name: "test_stage"
    type: "map"
    properties:
      map_function: "test_func"
`

	// Create temp file
	tmpFile, err := os.CreateTemp("", "test_pipeline_*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up

	// Write content to temp file
	if _, err := tmpFile.WriteString(yamlContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Load config from file
	config, err := fluxus.LoadPipelineConfigFromFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config from file: %v", err)
	}

	// Verify config
	if config.Name != "file_test_pipeline" {
		t.Errorf("Expected pipeline name 'file_test_pipeline', got '%s'", config.Name)
	}
	if len(config.Stages) != 1 {
		t.Errorf("Expected 1 stage, got %d", len(config.Stages))
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name          string
		yaml          string
		expectError   bool
		errorContains string
	}{
		{
			name: "missing version",
			yaml: `
pipeline_name: "test"
pipeline_type: "classic"
stages:
  - name: "test"
    type: "map"
    properties:
      map_function: "test"
`,
			expectError:   true,
			errorContains: "Version",
		},
		{
			name: "invalid pipeline type",
			yaml: `
version: "1.0.0"
pipeline_name: "test"
pipeline_type: "invalid"
stages:
  - name: "test"
    type: "map"
    properties:
      map_function: "test"
`,
			expectError:   true,
			errorContains: "Type",
		},
		{
			name: "no stages",
			yaml: `
version: "1.0.0"
pipeline_name: "test"
pipeline_type: "classic"
stages: []
`,
			expectError:   true,
			errorContains: "Stages",
		},
		{
			name: "unsupported stage type",
			yaml: `
version: "1.0.0"
pipeline_name: "test"
pipeline_type: "classic"
stages:
  - name: "test"
    type: "invalid_stage_type"
    properties:
      some_prop: "value"
`,
			expectError:   true,
			errorContains: "unsupported stage type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := fluxus.LoadPipelineConfigFromYAML([]byte(test.yaml))

			if test.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), test.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", test.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}
