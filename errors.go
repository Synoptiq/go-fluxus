package fluxus

import (
	"bytes"
	"fmt"
)

// Error types for specific failure scenarios in pipeline processing

// --- Pipeline Lifecycle/Configuration Errors ---

// PipelineConfigurationError represents an error during pipeline setup or configuration.
type PipelineConfigurationError struct {
	Reason string
}

func (e *PipelineConfigurationError) Error() string {
	return fmt.Sprintf("pipeline configuration error: %s", e.Reason)
}

// NewPipelineConfigurationError creates a new configuration error.
func NewPipelineConfigurationError(reason string) error {
	return &PipelineConfigurationError{Reason: reason}
}

// PipelineLifecycleError represents an error related to the pipeline's state (Start/Stop).
type PipelineLifecycleError struct {
	Operation string // e.g., "Start", "Stop", "Wait"
	Reason    string
	Err       error // Optional underlying error
}

func (e *PipelineLifecycleError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("pipeline %s failed: %s: %v", e.Operation, e.Reason, e.Err)
	}
	return fmt.Sprintf("pipeline %s failed: %s", e.Operation, e.Reason)
}

func (e *PipelineLifecycleError) Unwrap() error {
	return e.Err
}

// NewPipelineLifecycleError creates a new lifecycle error.
func NewPipelineLifecycleError(operation, reason string, err error) error {
	return &PipelineLifecycleError{Operation: operation, Reason: reason, Err: err}
}

// Predefined Lifecycle/Config Errors (Examples)
var (
	ErrPipelineAlreadyStarted = NewPipelineLifecycleError("Start", "pipeline already started", nil)
	ErrPipelineNotStarted     = NewPipelineLifecycleError("Wait/Stop", "pipeline not started", nil)
	ErrEmptyPipeline          = NewPipelineConfigurationError("cannot build or run an empty pipeline")
	// You could add more specific ones like ErrIncompatibleSourceType, ErrIncompatibleSinkType etc.
	// Or wrap them using the base types above.
)

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

// MultiError holds multiple errors, e.g., from Map or FanOut with CollectErrors.
type MultiError struct {
	Errors []error
}

// NewMultiError creates a MultiError, filtering out nil errors.
func NewMultiError(errs []error) *MultiError {
	filtered := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	if len(filtered) == 0 {
		return nil // Return nil if no actual errors
	}
	return &MultiError{Errors: filtered}
}

// Error implements the error interface.
func (m *MultiError) Error() string {
	if !m.HasErrors() {
		return ""
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d error(s) occurred:", len(m.Errors))
	for i, err := range m.Errors {
		fmt.Fprintf(&buf, "\n\t[%d] %s", i, err)
	}
	return buf.String()
}

// Add appends a non-nil error to the list.
func (m *MultiError) Add(err error) {
	if err != nil {
		m.Errors = append(m.Errors, err)
	}
}

// HasErrors returns true if there are any errors.
func (m *MultiError) HasErrors() bool {
	return m != nil && len(m.Errors) > 0
}

// Unwrap provides compatibility with errors.Is/As by returning the first error.
// You might choose a different behavior depending on your needs.
func (m *MultiError) Unwrap() error {
	if m.HasErrors() {
		return m.Errors[0]
	}

	return nil
}

// MapItemError represents an error that occurred while processing a specific item
// within a Map stage.
type MapItemError struct {
	// ItemIndex is the index of the input item that caused the error
	ItemIndex int
	// OriginalError is the underlying error that occurred
	OriginalError error
}

// Error implements the error interface for MapItemError.
func (e *MapItemError) Error() string {
	return fmt.Sprintf("map stage item %d failed: %v", e.ItemIndex, e.OriginalError)
}

// Unwrap returns the underlying error for compatibility with errors.Is and errors.As.
func (e *MapItemError) Unwrap() error {
	return e.OriginalError
}

// NewMapItemError creates a new MapItemError with the provided details.
func NewMapItemError(itemIndex int, err error) *MapItemError {
	return &MapItemError{
		ItemIndex:     itemIndex,
		OriginalError: err,
	}
}
