package fluxus

import (
	"context"
	"errors"
	"fmt"
)

// ErrPipelineCancelled is returned when a pipeline is cancelled.
var ErrPipelineCancelled = errors.New("pipeline cancelled")

// Stage represents a processing stage in a pipeline.
// It takes an input of type I and produces an output of type O.
// The stage can also return an error if processing fails.
type Stage[I, O any] interface {
	Process(ctx context.Context, input I) (O, error)
}

// StageFunc is a function that implements the Stage interface.
type StageFunc[I, O any] func(ctx context.Context, input I) (O, error)

// Process implements the Stage interface for StageFunc.
func (f StageFunc[I, O]) Process(ctx context.Context, input I) (O, error) {
	return f(ctx, input)
}

// Chain combines two stages where the output of the first becomes input to the second.
// This allows creating pipelines with stages of different input and output types.
func Chain[A, B, C any](first Stage[A, B], second Stage[B, C]) Stage[A, C] {
	return StageFunc[A, C](func(ctx context.Context, input A) (C, error) {
		// Process through the first stage
		intermediate, err := first.Process(ctx, input)
		if err != nil {
			var zero C
			return zero, err
		}

		// Process through the second stage
		return second.Process(ctx, intermediate)
	})
}

// ChainMany combines multiple stages into a single stage.
// The output type of each stage must match the input type of the next stage.
func ChainMany[I, O any](stages ...interface{}) Stage[I, O] {
	if len(stages) == 0 {
		return StageFunc[I, O](func(_ context.Context, _ I) (O, error) {
			var zero O
			return zero, errors.New("cannot chain zero stages")
		})
	}

	// Validate the first stage
	firstStage, ok := stages[0].(Stage[I, any])
	if !ok {
		return StageFunc[I, O](func(_ context.Context, _ I) (O, error) {
			var zero O
			return zero, errors.New("first stage has incorrect input type")
		})
	}

	// For a single stage, check if it's already the right type
	if len(stages) == 1 {
		if directStage, okDirect := stages[0].(Stage[I, O]); okDirect {
			return directStage
		}
		return StageFunc[I, O](func(_ context.Context, _ I) (O, error) {
			var zero O
			return zero, errors.New("single stage has incorrect output type")
		})
	}

	// For multiple stages, we need to build a function that chains them all
	return StageFunc[I, O](func(ctx context.Context, input I) (O, error) {
		var currentInput any = input

		for i, stage := range stages {
			// Skip type checking for the first stage as we already did it
			if i == 0 {
				result, err := firstStage.Process(ctx, input)
				if err != nil {
					var zero O
					return zero, fmt.Errorf("stage %d: %w", i, err)
				}
				currentInput = result
				continue
			}

			// For the last stage, it must have output type O
			if i == len(stages)-1 {
				lastStage, okLast := stage.(Stage[any, O])
				if !okLast {
					var zero O
					return zero, errors.New("last stage has incorrect output type")
				}

				return lastStage.Process(ctx, currentInput)
			}

			// For intermediate stages
			intermediateStage, okIntermediate := stage.(Stage[any, any])
			if !okIntermediate {
				var zero O
				return zero, fmt.Errorf("stage %d has incorrect type", i)
			}

			result, err := intermediateStage.Process(ctx, currentInput)
			if err != nil {
				var zero O
				return zero, fmt.Errorf("stage %d: %w", i, err)
			}

			currentInput = result
		}

		// This should never happen if the stages are validated correctly
		var zero O
		return zero, errors.New("unexpected error in ChainMany")
	})
}

// Pipeline represents a sequence of processing stages.
type Pipeline[I, O any] struct {
	stage      Stage[I, O]
	errHandler func(error) error
}

// NewPipeline creates a new pipeline with a single stage.
func NewPipeline[I, O any](stage Stage[I, O]) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		stage:      stage,
		errHandler: func(err error) error { return err },
	}
}

// WithErrorHandler adds a custom error handler to the pipeline.
func (p *Pipeline[I, O]) WithErrorHandler(handler func(error) error) *Pipeline[I, O] {
	p.errHandler = handler
	return p
}

// Process runs the pipeline on the given input.
func (p *Pipeline[I, O]) Process(ctx context.Context, input I) (O, error) {
	var zero O

	// Check for context cancellation
	if ctx.Err() != nil {
		return zero, p.errHandler(ctx.Err())
	}

	// Process through the stage
	result, err := p.stage.Process(ctx, input)
	if err != nil {
		return zero, p.errHandler(err)
	}

	return result, nil
}
