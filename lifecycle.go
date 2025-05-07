package fluxus

import "context"

// Initializer defines an interface for stages that require an initialization step
// before they start processing data. This is useful for setting up resources,
// loading initial state, or performing other one-time setup tasks.
//
// For stages used within a StreamPipeline, the pipeline runner should call Setup
// once for each stage instance before any items are processed by that stage.
type Initializer interface {
	Setup(ctx context.Context) error
}

// Closer defines an interface for stages that need to perform cleanup
// operations when they are being shut down. This is useful for releasing resources,
// flushing pending data, or other teardown tasks.
//
// For stages used within a StreamPipeline, the pipeline runner should call Close
// once for each stage instance after all processing for that stage has completed
// or when the pipeline is shutting down.
type Closer interface {
	Close(ctx context.Context) error
}

// Resettable defines an interface for stages that can have their internal state
// reset to an initial or clean condition. This is useful for reusing stage
// instances across different processing batches or in testing scenarios where
// a fresh state is required without full re-initialization.
// The context can be used for cancellation or passing reset-specific parameters.
type Resettable interface {
	Reset(ctx context.Context) error
}

// HealthCheckable defines an interface for stages that can report their
// operational health. This is useful for monitoring and automated systems
// to determine if a stage is functioning correctly.
//
// The HealthStatus method should return nil if the stage is healthy,
// or an error describing the problem if it's unhealthy.
type HealthCheckable interface {
	HealthStatus(ctx context.Context) error
}
