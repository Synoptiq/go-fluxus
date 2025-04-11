package fluxus

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// Route holds a potential downstream stage for a Router.
type Route[I, O any] struct {
	Name  string      // Optional name for identification/metrics
	Stage Stage[I, O] // The actual stage to execute if selected
}

// SelectorFunc defines the function signature for selecting downstream routes by index.
// It returns a slice of indices corresponding to the routes that the item should be sent to.
// An empty slice or nil means the item is dropped (similar to Filter).
type SelectorFunc[I any] func(ctx context.Context, item I) ([]int, error)

// ErrNoRouteMatched is a sentinel error returned by the Router stage when an item
// matches no routes and is effectively dropped. This allows distinguishing
// routing misses from actual processing errors.
var ErrNoRouteMatched = errors.New("item matched no routes")

// Router is a stage that conditionally routes an input item to one or more
// downstream stages based on a selector function.
// It behaves like a conditional FanOut, executing the selected stages concurrently.
// The output is a slice containing the results from all successfully executed routes,
// ordered according to the order of the selected routes.
// If the selector function returns an empty slice or nil, the Router returns ErrNoRouteMatched.
type Router[I, O any] struct {
	routes       []Route[I, O]
	selectorFunc SelectorFunc[I]
	errHandler   func(error) error
	concurrency  int
}

// Helper task struct needs to be defined outside or passed generics if helper is outside
type task[I, O any] struct {
	resultIndex int // Index in the results slice
	stage       Stage[I, O]
}

// NewRouter creates a new Router stage.
// The selectorFunc determines which routes (by index from the 'routes' parameter)
// receive the input item. The routes slice defines all potential downstream stages.
func NewRouter[I, O any](selectorFunc SelectorFunc[I], routes ...Route[I, O]) *Router[I, O] {
	if selectorFunc == nil {
		// Default to dropping everything if no selector is provided
		selectorFunc = func(_ context.Context, _ I) ([]int, error) {
			return nil, nil
		}
	}
	// Defensive copy of routes slice to prevent modification after creation
	routesCopy := make([]Route[I, O], len(routes))
	copy(routesCopy, routes)

	return &Router[I, O]{
		routes:       routesCopy,
		selectorFunc: selectorFunc,
		errHandler:   func(err error) error { return err }, // Default error handler
		concurrency:  0,                                    // Default: run all matched stages concurrently (no limit)
	}
}

// WithErrorHandler adds a custom error handler to the router stage.
// This handles errors from the selectorFunc itself or from the downstream stages.
// It does NOT wrap ErrNoRouteMatched.
func (r *Router[I, O]) WithErrorHandler(handler func(error) error) *Router[I, O] {
	if handler == nil {
		handler = func(err error) error { return err } // Reset to default
	}
	r.errHandler = handler
	return r
}

// WithConcurrency limits the number of concurrent stage executions if multiple routes match.
// A value of 0 or less means no limit (all matched stages run concurrently).
func (r *Router[I, O]) WithConcurrency(n int) *Router[I, O] {
	r.concurrency = n
	return r
}

// runWorker is the core logic executed by each worker goroutine.
func (r *Router[I, O]) runWorker(
	execCtx context.Context, // The cancellable context for this execution run
	cancel context.CancelFunc, // Function to cancel the context on error
	input I, // The input item being processed
	tasks <-chan task[I, O], // Channel to receive tasks from
	results []O, // Slice to store results (must be accessed by index)
	errChan chan<- error, // Channel to send the first error encountered
) {
	for t := range tasks {
		select {
		case <-execCtx.Done():
			// Context was cancelled (likely by another worker finding an error,
			// or external cancellation). Stop processing.
			return
		default:
			// Process the stage for the current task
			result, stageErr := t.stage.Process(execCtx, input)
			if stageErr != nil {
				// --- Error Handling ---
				// Cannot reliably compare stages to find original index.
				// Use resultIndex which refers to the index within the selected stages for this run.
				wrappedErr := fmt.Errorf("router target stage (selected index %d) error: %w", t.resultIndex, stageErr)

				// Send the error non-blockingly and cancel the context
				// to signal other workers.
				select {
				case errChan <- wrappedErr:
					cancel() // Signal other workers to stop
				default:
					// Another error was already sent, or channel is blocked (shouldn't happen with buffer 1)
				}
				return // Stop this worker after encountering an error
			}

			// --- Success ---
			// Store successful result at the correct index in the results slice.
			// This is safe because each worker writes to a unique index.
			results[t.resultIndex] = result
		}
	}
}

// processSelectedStagesConcurrently handles the concurrent execution of the selected stages.
// It uses a worker pool pattern with concurrency control and cancellation.
// It populates the results slice and returns the first error encountered.
func (r *Router[I, O]) processSelectedStagesConcurrently(
	ctx context.Context,
	input I,
	targetStages []Stage[I, O], // The specific stages selected for this input
	results []O, // Pre-allocated slice to store results
) error { // Returns only the error, results are populated directly
	numSelected := len(targetStages)
	if numSelected == 0 {
		return nil // Safe check
	}

	// Determine actual concurrency limit
	concurrency := r.concurrency
	if concurrency <= 0 || concurrency > numSelected {
		concurrency = numSelected
	}

	// Create cancelable context for concurrent execution
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	tasks := make(chan task[I, O], numSelected) // Use the defined task struct
	errChan := make(chan error, 1)              // Buffered channel for the first error

	// Start worker goroutines
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			// Delegate the core loop logic to the helper method
			r.runWorker(execCtx, cancel, input, tasks, results, errChan)
		}()
	}

	// Send tasks to workers, checking for cancellation
	for i := 0; i < numSelected; i++ {
		select {
		case <-execCtx.Done():
			goto finish // Stop sending tasks if context is cancelled
		case tasks <- task[I, O]{resultIndex: i, stage: targetStages[i]}:
			// Task sent
		}
	}

finish:
	close(tasks) // Close tasks channel once all tasks are sent or cancellation occurred

	// Wait for all active workers to finish
	wg.Wait()

	// Check if an error occurred during execution
	select {
	case finalErr := <-errChan:
		// An error occurred in one of the target stages
		return r.errHandler(finalErr) // Apply top-level error handler
	default:
		// No errors reported by workers. Check if the context was cancelled externally.
		// Use execCtx.Err() as it reflects cancellation triggered by workers too.
		if execCtx.Err() != nil && !errors.Is(execCtx.Err(), context.Canceled) {
			// Context was cancelled (e.g., timeout, external cancel), not by an internal error that we already handled.
			// Note: context.Canceled might occur if the *parent* ctx was cancelled AND no worker errored.
			return r.errHandler(fmt.Errorf("router execution cancelled: %w", execCtx.Err()))
		}
		// No errors during execution
		return nil
	}
}

// Process implements the Stage interface for Router.
// It determines the target routes, executes their stages concurrently, and collects results.
func (r *Router[I, O]) Process(ctx context.Context, input I) ([]O, error) {
	// 1. Check context cancellation
	if ctx.Err() != nil {
		return nil, r.errHandler(ctx.Err())
	}

	// 2. Determine target route indices
	selectedIndices, selectorErr := r.selectorFunc(ctx, input)
	if selectorErr != nil {
		return nil, r.errHandler(fmt.Errorf("router selectorFunc error: %w", selectorErr))
	}

	numSelected := len(selectedIndices)
	if numSelected == 0 {
		return nil, ErrNoRouteMatched // No routes matched
	}

	// --- Fast Path for Single Selected Route ---
	if numSelected == 1 {
		index := selectedIndices[0]
		// Validate the single index
		if index < 0 || index >= len(r.routes) {
			invalidIndexErr := fmt.Errorf("router selectorFunc returned invalid index %d (max %d)", index, len(r.routes)-1)
			return nil, r.errHandler(invalidIndexErr)
		}
		// Get the single target stage
		targetStage := r.routes[index].Stage

		// Process directly
		result, err := targetStage.Process(ctx, input)
		if err != nil {
			// Apply error handler, wrapping with context about the stage
			// Note: We use the selected index (0) here as it's the only one.
			wrappedErr := fmt.Errorf("router target stage (selected index 0) error: %w", err)
			return nil, r.errHandler(wrappedErr)
		}
		// Return the single result wrapped in a slice
		return []O{result}, nil
	}

	// --- Default Path for Multiple Selected Routes ---

	// 3. Validate indices and gather target stages
	targetStages := make([]Stage[I, O], numSelected)
	for i, index := range selectedIndices {
		if index < 0 || index >= len(r.routes) {
			invalidIndexErr := fmt.Errorf("router selectorFunc returned invalid index %d (max %d)", index, len(r.routes)-1)
			return nil, r.errHandler(invalidIndexErr) // Use error handler for invalid selector output
		}
		targetStages[i] = r.routes[index].Stage
	}

	// 4. Prepare results slice
	results := make([]O, numSelected) // Pre-allocate results slice

	// 5. Execute concurrently using the internal method
	err := r.processSelectedStagesConcurrently(ctx, input, targetStages, results)
	if err != nil {
		return nil, err // Error already handled by errHandler inside the method
	}

	// 6. Return results on success
	return results, nil
}

// Ensure Router implements the Stage interface.
// Input type I, Output type []O
var _ Stage[string, []string] = (*Router[string, string])(nil)
