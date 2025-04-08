package fluxus

import (
	"fmt"
)

// Error types for specific failure scenarios in pipeline processing

// StageError represents an error that occurred in a specific pipeline stage.
type StageError struct {
	// StageName is an optional identifier for the stage where the error occurred
	StageName string
	// StageIndex is the index of the stage in a multi-stage operation (like FanOut)
	StageIndex int
	// OriginalError is the underlying error that occurred
	OriginalError error
}

// Error implements the error interface for StageError.
func (e *StageError) Error() string {
	if e.StageName != "" {
		return fmt.Sprintf("stage %q (index %d): %v", e.StageName, e.StageIndex, e.OriginalError)
	}
	return fmt.Sprintf("stage %d: %v", e.StageIndex, e.OriginalError)
}

// Unwrap returns the underlying error for compatibility with errors.Is and errors.As.
func (e *StageError) Unwrap() error {
	return e.OriginalError
}

// NewStageError creates a new StageError with the provided details.
func NewStageError(stageName string, stageIndex int, err error) *StageError {
	return &StageError{
		StageName:     stageName,
		StageIndex:    stageIndex,
		OriginalError: err,
	}
}

// FanOutError represents an error that occurred during a fan-out operation.
type FanOutError struct {
	// FailedStages maps stage indices to their corresponding errors
	FailedStages map[int]error
}

// Error implements the error interface for FanOutError.
func (e *FanOutError) Error() string {
	if len(e.FailedStages) == 1 {
		for idx, err := range e.FailedStages {
			return fmt.Sprintf("fan-out stage %d failed: %v", idx, err)
		}
	}

	return fmt.Sprintf("%d fan-out stages failed", len(e.FailedStages))
}

// NewFanOutError creates a new FanOutError with the provided failed stages.
func NewFanOutError(failedStages map[int]error) *FanOutError {
	return &FanOutError{
		FailedStages: failedStages,
	}
}

// RetryExhaustedError occurs when all retry attempts have been exhausted without success.
type RetryExhaustedError struct {
	// MaxAttempts is the maximum number of attempts that were made
	MaxAttempts int
	// LastError is the last error that occurred before giving up
	LastError error
}

// Error implements the error interface for RetryExhaustedError.
func (e *RetryExhaustedError) Error() string {
	return fmt.Sprintf("retry exhausted %d attempts: %v", e.MaxAttempts, e.LastError)
}

// Unwrap returns the underlying error for compatibility with errors.Is and errors.As.
func (e *RetryExhaustedError) Unwrap() error {
	return e.LastError
}

// NewRetryExhaustedError creates a new RetryExhaustedError with the provided details.
func NewRetryExhaustedError(maxAttempts int, lastError error) *RetryExhaustedError {
	return &RetryExhaustedError{
		MaxAttempts: maxAttempts,
		LastError:   lastError,
	}
}

// TimeoutError occurs when a stage execution times out.
type TimeoutError struct {
	// StageName is an optional identifier for the stage where the timeout occurred
	StageName string
	// Duration is the timeout duration
	Duration string
	// OriginalError is the underlying timeout error
	OriginalError error
}

// Error implements the error interface for TimeoutError.
func (e *TimeoutError) Error() string {
	if e.StageName != "" {
		return fmt.Sprintf("stage %q timed out after %s: %v", e.StageName, e.Duration, e.OriginalError)
	}
	return fmt.Sprintf("stage timed out after %s: %v", e.Duration, e.OriginalError)
}

// Unwrap returns the underlying error for compatibility with errors.Is and errors.As.
func (e *TimeoutError) Unwrap() error {
	return e.OriginalError
}

// NewTimeoutError creates a new TimeoutError with the provided details.
func NewTimeoutError(stageName string, duration string, err error) *TimeoutError {
	return &TimeoutError{
		StageName:     stageName,
		Duration:      duration,
		OriginalError: err,
	}
}

// BufferError occurs during batch processing operations.
type BufferError struct {
	// BatchIndex is the index of the batch where the error occurred
	BatchIndex int
	// Offset is the offset in the input where the batch starts
	Offset int
	// OriginalError is the underlying error
	OriginalError error
}

// Error implements the error interface for BufferError.
func (e *BufferError) Error() string {
	return fmt.Sprintf("batch processing error at batch %d (offset %d): %v", e.BatchIndex, e.Offset, e.OriginalError)
}

// Unwrap returns the underlying error for compatibility with errors.Is and errors.As.
func (e *BufferError) Unwrap() error {
	return e.OriginalError
}

// NewBufferError creates a new BufferError with the provided details.
func NewBufferError(batchIndex, offset int, err error) *BufferError {
	return &BufferError{
		BatchIndex:    batchIndex,
		Offset:        offset,
		OriginalError: err,
	}
}
