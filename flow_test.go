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
