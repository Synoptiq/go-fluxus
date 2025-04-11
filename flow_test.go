package fluxus_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/synoptiq/go-fluxus"
)

// TestFilterKeep tests the Filter stage when the predicate returns true.
func TestFilterKeep(t *testing.T) {
	predicate := func(_ context.Context, item int) (bool, error) {
		return item > 5, nil // Keep items greater than 5
	}
	filterStage := fluxus.NewFilter(predicate)

	input := 10
	output, err := filterStage.Process(context.Background(), input)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if output != input {
		t.Errorf("Expected output %d, got %d", input, output)
	}
}

// TestFilterDrop tests the Filter stage when the predicate returns false.
func TestFilterDrop(t *testing.T) {
	predicate := func(_ context.Context, item int) (bool, error) {
		return item > 5, nil // Keep items greater than 5
	}
	filterStage := fluxus.NewFilter(predicate)

	input := 3
	output, err := filterStage.Process(context.Background(), input)

	if !errors.Is(err, fluxus.ErrItemFiltered) {
		t.Fatalf("Expected ErrItemFiltered, got: %v", err)
	}
	// Even though filtered, the original item should be returned along with the error
	if output != input {
		t.Errorf("Expected output %d even on filter, got %d", input, output)
	}
}

// TestFilterPredicateError tests the Filter stage when the predicate returns an error.
func TestFilterPredicateError(t *testing.T) {
	expectedErr := errors.New("predicate failed")
	predicate := func(_ context.Context, item int) (bool, error) {
		if item == 0 {
			return false, expectedErr
		}
		return true, nil
	}
	filterStage := fluxus.NewFilter(predicate)

	input := 0
	_, err := filterStage.Process(context.Background(), input)

	if err == nil {
		t.Fatal("Expected an error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to wrap %v, got %v", expectedErr, err)
	}
	if !strings.Contains(err.Error(), "filter predicate error") {
		t.Errorf("Expected error message to contain 'filter predicate error', got: %s", err.Error())
	}
}

// TestFilterContextCancellation tests the Filter stage with context cancellation.
func TestFilterContextCancellation(t *testing.T) {
	predicate := func(ctx context.Context, _ int) (bool, error) {
		// Simulate some work that respects cancellation
		select {
		case <-time.After(50 * time.Millisecond):
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	filterStage := fluxus.NewFilter(predicate)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	input := 1
	_, err := filterStage.Process(ctx, input)

	if err == nil {
		t.Fatal("Expected a context error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

// TestFilterNilPredicate tests the Filter stage when created with a nil predicate.
func TestFilterNilPredicate(t *testing.T) {
	// Should default to keeping everything
	filterStage := fluxus.NewFilter[int](nil)

	input := 42
	output, err := filterStage.Process(context.Background(), input)

	if err != nil {
		t.Fatalf("Expected no error with nil predicate, got: %v", err)
	}
	if output != input {
		t.Errorf("Expected output %d, got %d", input, output)
	}
}

// TestFilterWithErrorHandler tests the custom error handler.
func TestFilterWithErrorHandler(t *testing.T) {
	predicateErr := errors.New("predicate failed")
	customErr := errors.New("custom handler error")

	predicate := func(_ context.Context, item int) (bool, error) {
		if item == 0 {
			return false, predicateErr // Predicate error
		}
		if item < 0 {
			return false, nil // Filtered out
		}
		return true, nil // Keep
	}

	handler := func(err error) error {
		if errors.Is(err, predicateErr) {
			// Wrap the predicate error
			return fmt.Errorf("%w: %w", customErr, err)
		}
		// Return other errors (like context errors) as is
		return err
	}

	filterStage := fluxus.NewFilter(predicate).WithErrorHandler(handler)

	// --- Test Case 1: Predicate Error ---
	_, err := filterStage.Process(context.Background(), 0)
	if err == nil {
		t.Fatal("Expected an error for predicate failure, got nil")
	}
	if !errors.Is(err, customErr) {
		t.Errorf("Expected error to wrap customErr, got %v", err)
	}
	if !errors.Is(err, predicateErr) {
		t.Errorf("Expected error to wrap predicateErr, got %v", err)
	}

	// --- Test Case 2: Filtered Item (ErrItemFiltered should NOT be handled) ---
	_, err = filterStage.Process(context.Background(), -5)
	if !errors.Is(err, fluxus.ErrItemFiltered) {
		t.Fatalf("Expected ErrItemFiltered, got: %v", err)
	}
	// Verify the custom error is NOT part of the chain for ErrItemFiltered
	if errors.Is(err, customErr) {
		t.Errorf("ErrItemFiltered should not be wrapped by the custom handler")
	}

	// --- Test Case 3: Context Error (Should be handled) ---
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	_, err = filterStage.Process(ctx, 10)
	if err == nil {
		t.Fatal("Expected a context error, got nil")
	}
	// The default handler just returns the context error
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
	// Verify the custom error is NOT part of the chain for context errors (unless handler wraps it)
	if errors.Is(err, customErr) {
		t.Errorf("Context error should not be wrapped by the custom handler in this setup")
	}

	// --- Test Case 4: Success (No error) ---
	_, err = filterStage.Process(context.Background(), 10)
	if err != nil {
		t.Fatalf("Expected no error for successful processing, got: %v", err)
	}
}

// --- Benchmarks ---

// BenchmarkFilterOverhead measures the performance overhead of adding a Filter stage.
func BenchmarkFilterOverhead(b *testing.B) {
	// Simple stage that does minimal work (e.g., string conversion)
	baseStage := fluxus.StageFunc[int, string](func(_ context.Context, input int) (string, error) {
		return fmt.Sprintf("value: %d", input), nil
	})

	// Filter stage that always keeps the item (worst-case overhead for filter logic)
	filterStage := fluxus.NewFilter(func(_ context.Context, _ int) (bool, error) {
		return true, nil
	})

	// Chain the filter and the base stage
	filteredPipelineStage := fluxus.Chain(filterStage, baseStage)

	input := 12345
	ctx := context.Background()

	// Benchmark the base stage directly
	b.Run("BaseStageOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = baseStage.Process(ctx, input)
		}
	})

	// Benchmark the pipeline with the filter stage included
	b.Run("WithFilterStage", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = filteredPipelineStage.Process(ctx, input)
		}
	})
}

// --- Router Tests ---

// Helper stage for router tests
func makeSimpleStage(id string, delay time.Duration, fail bool) fluxus.Stage[string, string] {
	return fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
		select {
		case <-time.After(delay):
			if fail {
				return "", fmt.Errorf("stage %s failed", id)
			}
			return fmt.Sprintf("%s -> %s", input, id), nil
		case <-ctx.Done():
			return "", fmt.Errorf("stage %s cancelled: %w", id, ctx.Err())
		}
	})
}

// TestRouterSingleRoute tests routing to a single selected stage.
func TestRouterSingleRoute(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 0, false)},
		{Name: "B", Stage: makeSimpleStage("B", 0, false)},
	}
	selector := func(_ context.Context, item string) ([]int, error) {
		if item == "route-to-A" {
			return []int{0}, nil // Select route A
		}
		return nil, nil
	}
	router := fluxus.NewRouter(selector, routes...)

	output, err := router.Process(context.Background(), "route-to-A")
	require.NoError(t, err)
	require.Len(t, output, 1)
	assert.Equal(t, "route-to-A -> A", output[0])
}

// TestRouterMultipleRoutes tests routing to multiple selected stages concurrently.
func TestRouterMultipleRoutes(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 10*time.Millisecond, false)},
		{Name: "B", Stage: makeSimpleStage("B", 10*time.Millisecond, false)},
		{Name: "C", Stage: makeSimpleStage("C", 10*time.Millisecond, false)},
	}
	selector := func(_ context.Context, item string) ([]int, error) {
		if item == "route-to-A-C" {
			return []int{0, 2}, nil // Select routes A and C
		}
		return nil, nil
	}
	router := fluxus.NewRouter(selector, routes...)

	start := time.Now()
	output, err := router.Process(context.Background(), "route-to-A-C")
	duration := time.Since(start)

	require.NoError(t, err)
	require.Len(t, output, 2)
	// Order should match the selected indices order
	assert.Equal(t, "route-to-A-C -> A", output[0])
	assert.Equal(t, "route-to-A-C -> C", output[1])

	// Check if it ran concurrently (should take slightly more than 10ms, not 20ms+)
	assert.Less(t, duration, 20*time.Millisecond, "Execution took too long, likely not concurrent: %v", duration)
}

// TestRouterNoRouteMatched tests when the selector returns no matching routes.
func TestRouterNoRouteMatched(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 0, false)},
	}
	selector := func(_ context.Context, _ string) ([]int, error) {
		return nil, nil // No match
	}
	router := fluxus.NewRouter(selector, routes...)

	_, err := router.Process(context.Background(), "input")
	require.Error(t, err)
	assert.ErrorIs(t, err, fluxus.ErrNoRouteMatched, "Expected ErrNoRouteMatched")
}

// TestRouterSelectorError tests when the selector function itself returns an error.
func TestRouterSelectorError(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 0, false)},
	}
	expectedErr := errors.New("selector failed")
	selector := func(_ context.Context, _ string) ([]int, error) {
		return nil, expectedErr
	}
	router := fluxus.NewRouter(selector, routes...)

	_, err := router.Process(context.Background(), "input")
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr, "Expected selector error to be returned")
	assert.Contains(t, err.Error(), "router selectorFunc error")
}

// TestRouterSelectorInvalidIndex tests when the selector returns an out-of-bounds index.
func TestRouterSelectorInvalidIndex(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 0, false)},
	}
	selector := func(_ context.Context, _ string) ([]int, error) {
		return []int{0, 1}, nil // Index 1 is invalid
	}
	router := fluxus.NewRouter(selector, routes...)

	_, err := router.Process(context.Background(), "input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "router selectorFunc returned invalid index 1")
}

// TestRouterDownstreamError tests when one of the selected stages fails.
func TestRouterDownstreamError(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 10*time.Millisecond, false)}, // Succeeds
		{Name: "B", Stage: makeSimpleStage("B", 5*time.Millisecond, true)},   // Fails quickly
		{Name: "C", Stage: makeSimpleStage("C", 15*time.Millisecond, false)}, // Should be cancelled
	}
	selector := func(_ context.Context, _ string) ([]int, error) {
		return []int{0, 1, 2}, nil // Select all
	}
	router := fluxus.NewRouter(selector, routes...)

	_, err := router.Process(context.Background(), "input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stage B failed")
	assert.Contains(t, err.Error(), "router target stage (selected index 1) error") // Check error wrapping
}

// TestRouterContextCancellationDuringSelection tests cancellation before selection.
func TestRouterContextCancellationDuringSelection(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 0, false)},
	}
	selector := func(ctx context.Context, _ string) ([]int, error) {
		select {
		case <-time.After(50 * time.Millisecond):
			return []int{0}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	router := fluxus.NewRouter(selector, routes...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := router.Process(ctx, "input")
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded error")
	// Check if the error comes from the selector or the initial context check
	assert.True(t, strings.Contains(err.Error(), "router selectorFunc error") || errors.Is(err, ctx.Err()))
}

// TestRouterContextCancellationDuringExecution tests cancellation while stages are running.
func TestRouterContextCancellationDuringExecution(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 50*time.Millisecond, false)}, // Takes longer
		{Name: "B", Stage: makeSimpleStage("B", 50*time.Millisecond, false)}, // Takes longer
	}
	selector := func(_ context.Context, _ string) ([]int, error) {
		return []int{0, 1}, nil // Select both
	}
	router := fluxus.NewRouter(selector, routes...)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond) // Timeout shorter than stage duration
	defer cancel()

	_, err := router.Process(ctx, "input")
	require.Error(t, err)
	// The error should indicate cancellation during execution
	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded error")
	// Check that the error indicates a stage was cancelled due to the deadline
	assert.Contains(t, err.Error(), "router target stage")                  // Check for the stage-specific wrapping
	assert.Contains(t, err.Error(), "cancelled: context deadline exceeded") // Check for the cancellation reason
}

// TestRouterWithConcurrencyLimit tests the concurrency setting.
func TestRouterWithConcurrencyLimit(t *testing.T) {
	var runCount atomic.Int32
	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32
	var mu sync.Mutex // To protect maxConcurrent update

	stageFunc := func(id string) fluxus.Stage[string, string] {
		return fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
			runCount.Add(1)
			concurrent := currentConcurrent.Add(1)

			mu.Lock()
			if concurrent > maxConcurrent.Load() {
				maxConcurrent.Store(concurrent)
			}
			mu.Unlock()

			defer currentConcurrent.Add(-1)

			select {
			case <-time.After(20 * time.Millisecond): // Simulate work
				return fmt.Sprintf("%s -> %s", input, id), nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
	}

	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: stageFunc("A")},
		{Name: "B", Stage: stageFunc("B")},
		{Name: "C", Stage: stageFunc("C")},
		{Name: "D", Stage: stageFunc("D")},
	}
	selector := func(_ context.Context, _ string) ([]int, error) {
		return []int{0, 1, 2, 3}, nil // Select all
	}
	router := fluxus.NewRouter(selector, routes...).WithConcurrency(2) // Limit to 2

	start := time.Now()
	output, err := router.Process(context.Background(), "input")
	duration := time.Since(start)

	require.NoError(t, err)
	require.Len(t, output, 4)
	assert.Equal(t, int32(4), runCount.Load(), "All stages should have run")
	assert.Equal(t, int32(2), maxConcurrent.Load(), "Max concurrency should be limited to 2")

	// With 4 tasks and concurrency 2, duration should be roughly 2 * stage duration
	assert.True(t, duration > 35*time.Millisecond && duration < 60*time.Millisecond, "Duration %v not consistent with concurrency limit 2", duration)
}

// TestRouterWithErrorHandler tests the custom error handler.
func TestRouterWithErrorHandler(t *testing.T) {
	selectorErr := errors.New("selector failed")
	stageErr := errors.New("stage B failed") // Predefined error instance
	customErr := errors.New("custom handler error")

	// Helper stage that can return a specific predefined error
	makeStageWithError := func(id string, delay time.Duration, failErr error) fluxus.Stage[string, string] {
		return fluxus.StageFunc[string, string](func(ctx context.Context, input string) (string, error) {
			select {
			case <-time.After(delay):
				if failErr != nil {
					// Return the exact error instance passed in
					return "", failErr
				}
				return fmt.Sprintf("%s -> %s", input, id), nil
			case <-ctx.Done():
				return "", fmt.Errorf("stage %s cancelled: %w", id, ctx.Err())
			}
		})
	}

	routes := []fluxus.Route[string, string]{
		// Use the new helper for stage B to ensure it returns the specific stageErr instance
		{Name: "A", Stage: makeStageWithError("A", 0, nil)},      // Succeeds
		{Name: "B", Stage: makeStageWithError("B", 0, stageErr)}, // Fails with the predefined stageErr
	}

	selector := func(_ context.Context, item string) ([]int, error) {
		if item == "selector-fail" {
			return nil, selectorErr
		}
		if item == "stage-fail" {
			return []int{0, 1}, nil // Select A and B
		}
		if item == "no-match" {
			return nil, nil // No match
		}
		return nil, errors.New("unexpected input")
	}

	handler := func(err error) error {
		// Now errors.Is(err, stageErr) will correctly identify the error instance
		if errors.Is(err, selectorErr) || errors.Is(err, stageErr) {
			return fmt.Errorf("%w: %w", customErr, err) // Wrap specific errors
		}
		// Do not wrap ErrNoRouteMatched or context errors
		return err
	}

	router := fluxus.NewRouter(selector, routes...).WithErrorHandler(handler)

	// --- Test Case 1: Selector Error ---
	_, err := router.Process(context.Background(), "selector-fail")
	require.Error(t, err)
	require.ErrorIs(t, err, customErr, "Error should be wrapped by custom handler")
	require.ErrorIs(t, err, selectorErr, "Original selector error should be present")

	// --- Test Case 2: Downstream Stage Error ---
	_, err = router.Process(context.Background(), "stage-fail")
	require.Error(t, err)
	// These assertions should now pass because the handler correctly identifies stageErr
	require.ErrorIs(t, err, customErr, "Error should be wrapped by custom handler")
	require.ErrorIs(t, err, stageErr, "Original stage error should be present")
	assert.Contains(t, err.Error(), "router target stage", "Error context should be preserved")

	// --- Test Case 3: No Route Matched (Should NOT be handled) ---
	_, err = router.Process(context.Background(), "no-match")
	require.Error(t, err)
	require.ErrorIs(t, err, fluxus.ErrNoRouteMatched, "ErrNoRouteMatched should be returned directly")
	require.NotErrorIs(t, err, customErr, "ErrNoRouteMatched should not be wrapped by the custom handler")

	// --- Test Case 4: Context Error (Should NOT be handled by this specific handler) ---
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = router.Process(ctx, "stage-fail") // Input doesn't matter here
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled, "Context error should be returned directly")
	require.NotErrorIs(t, err, customErr, "Context error should not be wrapped by the custom handler")
}

// TestRouterNilSelector tests the default behavior with a nil selector.
func TestRouterNilSelector(t *testing.T) {
	routes := []fluxus.Route[string, string]{
		{Name: "A", Stage: makeSimpleStage("A", 0, false)},
	}
	router := fluxus.NewRouter[string, string](nil, routes...) // Pass nil selector

	_, err := router.Process(context.Background(), "input")
	require.Error(t, err)
	assert.ErrorIs(t, err, fluxus.ErrNoRouteMatched, "Default nil selector should result in ErrNoRouteMatched")
}

// --- Benchmarks ---

// BenchmarkRouterOverhead measures the overhead of routing vs direct execution.
func BenchmarkRouterOverhead(b *testing.B) {
	baseStage := fluxus.StageFunc[int, string](func(_ context.Context, i int) (string, error) {
		return strconv.Itoa(i * 2), nil
	})

	routes := []fluxus.Route[int, string]{
		{Name: "Double", Stage: baseStage},
	}
	// Selector always picks the first (and only) route
	selector := func(_ context.Context, _ int) ([]int, error) {
		return []int{0}, nil
	}
	router := fluxus.NewRouter(selector, routes...)

	input := 123
	ctx := context.Background()

	b.Run("DirectStage", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = baseStage.Process(ctx, input)
		}
	})

	b.Run("RouterToOneStage", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = router.Process(ctx, input)
		}
	})
}

// BenchmarkRouterMultipleRoutes measures performance routing to multiple concurrent stages.
func BenchmarkRouterMultipleRoutes(b *testing.B) {
	// Stage with a tiny bit of work to make concurrency measurable
	workStage := fluxus.StageFunc[int, string](func(_ context.Context, i int) (string, error) {
		time.Sleep(1 * time.Microsecond) // Simulate tiny work
		return strconv.Itoa(i), nil
	})

	numRoutes := 8
	routes := make([]fluxus.Route[int, string], numRoutes)
	for i := 0; i < numRoutes; i++ {
		routes[i] = fluxus.Route[int, string]{Name: fmt.Sprintf("R%d", i), Stage: workStage}
	}

	// Selector picks all routes
	allIndices := make([]int, numRoutes)
	for i := 0; i < numRoutes; i++ {
		allIndices[i] = i
	}
	selector := func(_ context.Context, _ int) ([]int, error) {
		return allIndices, nil
	}

	input := 456
	ctx := context.Background()

	for _, concurrency := range []int{0, 1, 2, 4, 8} { // 0 = unlimited
		router := fluxus.NewRouter(selector, routes...).WithConcurrency(concurrency)
		concurrencyLabel := "Unlimited"
		if concurrency > 0 {
			concurrencyLabel = strconv.Itoa(concurrency)
		}

		b.Run(fmt.Sprintf("RouterTo%dStages_Concurrency%s", numRoutes, concurrencyLabel), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := router.Process(ctx, input)
				// Add checks to prevent compiler optimizing away the call
				if err != nil {
					b.Fatal(err)
				}
				if len(res) != numRoutes {
					b.Fatalf("Expected %d results, got %d", numRoutes, len(res))
				}
			}
		})
	}
}
