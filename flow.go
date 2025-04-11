package fluxus

import (
	"context"
	"errors"
	"fmt"
)

// PredicateFunc defines the function signature for filtering items.
// It returns true if the item should be kept, false if it should be dropped.
// It can also return an error, which will be handled by the pipeline's error handler.
type PredicateFunc[T any] func(ctx context.Context, item T) (bool, error)

// ErrItemFiltered is a sentinel error returned by the Filter stage when an item
// is dropped because the predicate function returned false. This allows
// distinguishing filtering actions from actual processing errors if needed,
// for example, by a custom error handler or a Dead Letter Queue mechanism later.
var ErrItemFiltered = errors.New("item filtered out")

// Filter is a stage that conditionally passes items downstream based on a predicate.
// If the predicate returns true, the item continues.
// If the predicate returns false, the stage returns the original item along with ErrItemFiltered.
// If the predicate returns an error, that error is propagated (potentially wrapped).
type Filter[T any] struct {
	predicate  PredicateFunc[T]
	errHandler func(error) error
}

// NewFilter creates a new Filter stage.
// The predicate function determines whether an item should pass through.
func NewFilter[T any](predicate PredicateFunc[T]) *Filter[T] {
	if predicate == nil {
		// Default to keeping everything if no predicate is provided (though usually one is expected)
		predicate = func(_ context.Context, _ T) (bool, error) {
			return true, nil
		}
	}
	return &Filter[T]{
		predicate:  predicate,
		errHandler: func(err error) error { return err }, // Default error handler just returns the error
	}
}

// WithErrorHandler adds a custom error handler to the filter stage.
// This handler is invoked if the predicate function itself returns an error,
// or if the context is cancelled. It does NOT wrap ErrItemFiltered.
func (f *Filter[T]) WithErrorHandler(handler func(error) error) *Filter[T] {
	if handler == nil {
		handler = func(err error) error { return err } // Reset to default if nil
	}
	f.errHandler = handler
	return f
}

// Process implements the Stage interface for Filter.
// It evaluates the predicate for the input item.
func (f *Filter[T]) Process(ctx context.Context, input T) (T, error) {
	// Check context cancellation first
	if ctx.Err() != nil {
		// Use the error handler for context errors
		return input, f.errHandler(ctx.Err())
	}

	// Evaluate the predicate
	keep, err := f.predicate(ctx, input)
	if err != nil {
		// An error occurred during predicate evaluation, use the error handler
		return input, f.errHandler(fmt.Errorf("filter predicate error: %w", err))
	}

	if !keep {
		// Item should be dropped. Return the original item and the specific ErrItemFiltered.
		// The custom errHandler is NOT used for ErrItemFiltered, as it's not an
		// unexpected processing error but an expected outcome of filtering.
		return input, ErrItemFiltered
	}

	// Item should be kept, pass it through with no error.
	return input, nil
}

// Ensure Filter implements the Stage interface.
// We use a concrete type like string for the check.
var _ Stage[string, string] = (*Filter[string])(nil)
