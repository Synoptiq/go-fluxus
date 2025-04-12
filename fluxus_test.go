package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// TestChain verifies the Chain functionality
func TestChain(t *testing.T) {
	// Create two stages with different input/output types
	stage1 := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
		return len(input), nil
	})

	stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Length: %d", input), nil
	})

	// Chain the stages
	chainedStage := fluxus.Chain(stage1, stage2)

	// Process with the chained stage
	result, err := chainedStage.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Chain failed: %v", err)
	}

	// Verify result
	expected := "Length: 5"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestBasicPipeline verifies a simple linear pipeline works correctly
func TestBasicPipeline(t *testing.T) {
	// Create two stages with different input/output types
	stage1 := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
		return len(input), nil
	})

	stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Length: %d", input), nil
	})

	// Chain the stages and create a pipeline
	chainedStage := fluxus.Chain(stage1, stage2)
	p := fluxus.NewPipeline(chainedStage)

	// Process the pipeline
	result, err := p.Process(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	expected := "Length: 5"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestChainError verifies error handling in a chain
func TestChainError(t *testing.T) {
	// Create two stages, one of which returns an error
	expectedErr := errors.New("test error")
	stage1 := fluxus.StageFunc[string, int](func(_ context.Context, _ string) (int, error) {
		return 0, expectedErr
	})

	stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Length: %d", input), nil
	})

	// Chain the stages
	chainedStage := fluxus.Chain(stage1, stage2)

	// Process with the chained stage
	_, err := chainedStage.Process(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Verify the error is correctly propagated
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to be %v, got %v", expectedErr, err)
	}
}

// TestPipelineWithError verifies error handling in a pipeline
func TestPipelineWithError(t *testing.T) {
	// Create a pipeline with a stage that returns an error
	expectedErr := errors.New("test error")
	p := fluxus.NewPipeline(
		fluxus.StageFunc[string, string](func(_ context.Context, _ string) (string, error) {
			return "", expectedErr
		}),
	)

	// Process the pipeline
	_, err := p.Process(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Verify the error is correctly wrapped
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to be %v, got %v", expectedErr, err)
	}
}

// TestMultipleChaining verifies chaining multiple stages with different types
func TestMultipleChaining(t *testing.T) {
	// Create stages with different input/output types
	stage1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, " "), nil
	})

	stage2 := fluxus.StageFunc[[]string, int](func(_ context.Context, input []string) (int, error) {
		return len(input), nil
	})

	stage3 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Word count: %d", input), nil
	})

	// Chain all three stages
	chainedStage := fluxus.Chain(stage1, fluxus.Chain(stage2, stage3))

	// Process with the chained stage
	result, err := chainedStage.Process(context.Background(), "hello world pipeline test")
	if err != nil {
		t.Fatalf("Multiple chain failed: %v", err)
	}

	// Verify result
	expected := "Word count: 4"
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestCancellation verifies that pipeline respects context cancellation
func TestCancellation(t *testing.T) {
	// Create a stage that checks for cancellation
	stage := fluxus.StageFunc[string, string](func(ctx context.Context, _ string) (string, error) {
		select {
		case <-time.After(500 * time.Millisecond):
			return "Done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	// Create a pipeline with the stage
	p := fluxus.NewPipeline(stage)

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Start processing in a goroutine
	resultCh := make(chan string)
	errCh := make(chan error)
	go func() {
		result, err := p.Process(ctx, "test")
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	// Cancel the context after a short delay
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Wait for result or error
	select {
	case <-resultCh:
		t.Fatal("Expected an error due to cancellation, got success")
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Test timed out")
	}
}

// TestComplexPipeline tests a more complex pipeline with multiple stages and error handling
func TestComplexPipeline(t *testing.T) {
	// Create a complex pipeline that matches the diagram in the request:
	//
	//                       (parallel processing)
	//                      /-> func2 --\
	// input-> func1 -<                  -> func4 -> output
	//                      \-> func3 --/

	// Define the stages
	func1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, " "), nil
	})

	func2 := fluxus.StageFunc[[]string, string](func(_ context.Context, words []string) (string, error) {
		var result []string
		for _, word := range words {
			result = append(result, strings.ToUpper(word))
		}
		return strings.Join(result, "+"), nil
	})

	func3 := fluxus.StageFunc[[]string, string](func(_ context.Context, words []string) (string, error) {
		return fmt.Sprintf("Word count: %d", len(words)), nil
	})

	func4 := fluxus.StageFunc[[]string, string](func(_ context.Context, inputs []string) (string, error) {
		return strings.Join(inputs, " | "), nil
	})

	// Create the parallel part
	parallelStage := fluxus.Parallel[[]string, string, []string](
		[]fluxus.Stage[[]string, string]{func2, func3},
		func(results []string) ([]string, error) {
			return results, nil
		},
	)

	// Create the complete pipeline using Chain
	chainedStage := fluxus.Chain(func1,
		fluxus.Chain(parallelStage, func4))

	p := fluxus.NewPipeline(chainedStage)

	// Process with the pipeline
	input := "hello world pipeline"
	result, err := p.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Complex pipeline failed: %v", err)
	}

	// Verify the result contains outputs from both parallel branches
	if !strings.Contains(result, "HELLO+WORLD+PIPELINE") || !strings.Contains(result, "Word count: 3") {
		t.Errorf("Expected result to contain both parallel outputs, got: %s", result)
	}
}

// BenchmarkSimplePipeline benchmarks a simple pipeline with a single stage
func BenchmarkSimplePipeline(b *testing.B) {
	// Create a simple stage
	stage := fluxus.StageFunc[string, string](func(_ context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Create a pipeline
	p := fluxus.NewPipeline(stage)

	// Prepare input
	input := "hello world"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkChainedPipeline benchmarks a pipeline with multiple chained stages
func BenchmarkChainedPipeline(b *testing.B) {
	// Create stages
	stage1 := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, " "), nil
	})

	stage2 := fluxus.StageFunc[[]string, int](func(_ context.Context, input []string) (int, error) {
		return len(input), nil
	})

	stage3 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("Count: %d", input), nil
	})

	// Chain the stages
	chainedStage := fluxus.Chain(stage1, fluxus.Chain(stage2, stage3))

	// Create a pipeline
	p := fluxus.NewPipeline(chainedStage)

	// Prepare input
	input := "hello world benchmark test"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkParallelProcessing benchmarks a pipeline with parallel processing
func BenchmarkParallelProcessing(b *testing.B) {
	// Create stages for parallel processing
	stages := []fluxus.Stage[int, int]{
		fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input * 2, nil
		}),
		fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input * 3, nil
		}),
		fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input * 4, nil
		}),
	}

	// Create a parallel stage
	parallelStage := fluxus.Parallel(
		stages,
		func(results []int) (int, error) {
			sum := 0
			for _, result := range results {
				sum += result
			}
			return sum, nil
		},
	)

	// Create a pipeline
	p := fluxus.NewPipeline(parallelStage)

	// Prepare input
	input := 42
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkComplexPipeline benchmarks a complex pipeline with multiple stages
func BenchmarkComplexPipeline(b *testing.B) {
	// Create stages for a more complex pipeline
	preprocessStage := fluxus.StageFunc[string, []string](func(_ context.Context, input string) ([]string, error) {
		return strings.Split(input, ","), nil
	})

	// Create parallel stages
	stage1 := fluxus.StageFunc[[]string, []string](func(_ context.Context, input []string) ([]string, error) {
		result := make([]string, len(input))
		for i, s := range input {
			result[i] = strings.ToUpper(s)
		}
		return result, nil
	})

	stage2 := fluxus.StageFunc[[]string, int](func(_ context.Context, input []string) (int, error) {
		totalLen := 0
		for _, s := range input {
			totalLen += len(s)
		}
		return totalLen, nil
	})

	// Create parallel processing
	parallelStages := []fluxus.Stage[[]string, interface{}]{
		// We need to adapt the stages to have a common output type
		fluxus.StageFunc[[]string, interface{}](func(ctx context.Context, input []string) (interface{}, error) {
			return stage1.Process(ctx, input)
		}),
		fluxus.StageFunc[[]string, interface{}](func(ctx context.Context, input []string) (interface{}, error) {
			return stage2.Process(ctx, input)
		}),
	}

	// Create a parallel stage
	parallelStage := fluxus.StageFunc[[]string, []interface{}](
		func(ctx context.Context, input []string) ([]interface{}, error) {
			results := make([]interface{}, len(parallelStages))
			for i, stage := range parallelStages {
				result, err := stage.Process(ctx, input)
				if err != nil {
					return nil, err
				}
				results[i] = result
			}
			return results, nil
		},
	)

	// Final stage to combine results
	finalStage := fluxus.StageFunc[[]interface{}, string](func(_ context.Context, input []interface{}) (string, error) {
		if len(input) != 2 {
			return "", fmt.Errorf("expected 2 inputs, got %d", len(input))
		}

		// Type assertions
		upperStrings, ok1 := input[0].([]string)
		totalLen, ok2 := input[1].(int)

		if !ok1 || !ok2 {
			return "", errors.New("type assertion failed")
		}

		return fmt.Sprintf("Uppercase: %s, Total length: %d", strings.Join(upperStrings, "+"), totalLen), nil
	})

	// Chain all stages
	chainedStage := fluxus.Chain(preprocessStage, fluxus.Chain(parallelStage, finalStage))

	// Create a pipeline
	p := fluxus.NewPipeline(chainedStage)

	// Prepare input
	input := "hello,world,pipeline,benchmark"
	ctx := context.Background()

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Process(ctx, input)
	}
}

// BenchmarkChainVsDirectCalls compares chained stages vs. direct function calls
func BenchmarkChainVsDirectCalls(b *testing.B) {
	// Define the functions we'll use
	lenFunc := func(s string) int {
		return len(s)
	}

	formatFunc := func(n int) string {
		return fmt.Sprintf("Length: %d", n)
	}

	// Direct function calls
	b.Run("DirectCalls", func(b *testing.B) {
		input := "hello world benchmark"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			length := lenFunc(input)
			_ = formatFunc(length)
		}
	})

	// Using Chain
	b.Run("ChainedStages", func(b *testing.B) {
		// Create stages
		stage1 := fluxus.StageFunc[string, int](func(_ context.Context, input string) (int, error) {
			return lenFunc(input), nil
		})

		stage2 := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
			return formatFunc(input), nil
		})

		// Chain the stages
		chainedStage := fluxus.Chain(stage1, stage2)

		// Prepare input
		input := "hello world benchmark"
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = chainedStage.Process(ctx, input)
		}
	})
}

// BenchmarkChainDepth tests how performance scales with chain depth
func BenchmarkChainDepth(b *testing.B) {
	// Test different chain depths
	depths := []int{1, 5, 10, 20, 50}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("ChainDepth-%d", depth), func(b *testing.B) {
			// Create a simple stage that we'll chain multiple times
			simpleStage := fluxus.StageFunc[int, int](func(_ context.Context, input int) (int, error) {
				return input + 1, nil
			})

			// Chain the stage to the specified depth
			var chainedStage fluxus.Stage[int, int] = simpleStage
			for i := 1; i < depth; i++ {
				chainedStage = fluxus.Chain(chainedStage, simpleStage)
			}

			// Create a pipeline
			p := fluxus.NewPipeline(chainedStage)

			// Prepare input
			input := 0
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = p.Process(ctx, input)
			}
		})
	}
}
