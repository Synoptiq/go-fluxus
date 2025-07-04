package fluxus

import (
	"fmt"

	"github.com/go-playground/validator/v10"
)

const (
	// PipelineVersion is the default version of the pipeline configuration.
	PipelineVersion = "1.0.0"
)

type PipelineType string

const (
	// PipelineTypeClassic represents a classic pipeline type.
	PipelineTypeClassic PipelineType = "classic"
	// PipelineTypeStreaming represents a streaming pipeline type.
	PipelineTypeStreaming PipelineType = "streaming"
)

// PipelineDebugConfig holds the configuration for debugging a pipeline.
type PipelineDebugConfig struct {
	// Enabled indicates whether the pipeline is in debug mode.
	Enabled bool `yaml:"enabled,omitempty"` // Whether the pipeline is in debug mode
	// Pipelines in debug mode will not generate the actual pipeline, but will instead log the configuration and stages
	// during the pipeline building process.
	// This is useful for testing and debugging the pipeline configuration without executing it.
}

// PipelineStreamingConfig holds the configuration for a streaming pipeline.
type PipelineStreamingConfig struct {
	BufferSize  int      `yaml:"buffer_size,omitempty"` // Size of the buffer for the pipeline, used in streaming pipelines
	Concurrency int      `yaml:"concurrency,omitempty"` // Number of concurrent workers for the pipeline
	Logger      Executor `yaml:"logger,omitempty"`      // Logger function to be called for each item in the pipeline
}

// PipelineConfig holds the parsed configuration for a single pipeline.
type PipelineConfig struct {
	// General properties
	Version string `yaml:"version"           validate:"required"` // Version of the pipeline configuration
	// These properties are common to all pipelines, regardless of their type.
	Name    string                `yaml:"pipeline_name"     validate:"required"`                         // Name of the pipeline
	Type    PipelineType          `yaml:"pipeline_type"     validate:"required,oneof=classic streaming"` // Type of the pipeline (classic or streaming)
	Tracing PipelineTracingConfig `yaml:"tracing,omitempty"`                                             // Tracing configuration for the pipeline
	Metrics PipelineMetricsConfig `yaml:"metrics,omitempty"`                                             // Metrics configuration for the pipeline
	Stages  []StageConfig         `yaml:"stages"            validate:"required,min=1"`                   // List of stages in the pipeline

	// Streaming properties
	Streaming PipelineStreamingConfig `yaml:"streaming,omitempty"` // Streaming configuration for the pipeline, used

	// Debug configuration
	Debug PipelineDebugConfig `yaml:"debug,omitempty"` // Debug configuration for the pipeline
}

// Validate checks the pipeline configuration for correctness using struct tags.
func (pc *PipelineConfig) Validate() error {
	validate := validator.New()

	// Validate the top-level PipelineConfig fields
	if err := validate.Struct(pc); err != nil {
		return fmt.Errorf("pipeline configuration validation failed: %w", err)
	}

	// Recursively validate each stage
	for i, stage := range pc.Stages {
		if err := stage.validate(validate); err != nil {
			return fmt.Errorf("validation failed for stage #%d ('%s'): %w", i, stage.Name, err)
		}
	}
	return nil
}

// validate is a helper for recursive validation of a single stage.
func (sc *StageConfig) validate(validate *validator.Validate) error {
	// Validate the StageConfig struct itself (Name, Type, non-nil Properties)
	if err := validate.Struct(sc); err != nil {
		return err
	}
	// Validate the specific properties struct that Properties points to
	if err := validate.Struct(sc.Properties); err != nil {
		return fmt.Errorf("invalid properties: %w", err)
	}

	// Recursively validate any nested stages within wrapper or fan-out stages
	return sc.validateNestedStages(validate)
}

// TracingType represents the type of tracing used in the pipeline.
// It can be used to specify different tracing backends or methods.
// Possible values could include "zipkin", "jaeger", "prometheus" or "noop".
type TracingType string

const (
	// TracingTypeZipkin represents Zipkin tracing.
	TracingTypeZipkin TracingType = "zipkin"
	// TracingTypeJaeger represents Jaeger tracing.
	TracingTypeJaeger TracingType = "jaeger"
	// TracingTypePrometheus represents Prometheus tracing.
	TracingTypePrometheus TracingType = "prometheus"
	// TracingTypeNoop represents no tracing.
	TracingTypeNoop TracingType = "noop"
)

// PipelineTracingConfig holds the configuration for tracing in a pipeline.
type PipelineTracingConfig struct {
	Enabled  bool        `yaml:"enabled"`  // Whether tracing is enabled for the pipeline
	Type     TracingType `yaml:"type"`     // Type of tracing used in the pipeline
	Endpoint string      `yaml:"endpoint"` // Endpoint for the tracing service (e.g., Zipkin or Jaeger)
}

// MetricsType represents the type of metrics used in the pipeline.
// It can be used to specify different metrics backends or methods.
// Possible values could include "mongodb" or "noop".
type MetricsType string

const (
	// MetricsTypeMongoDB represents MongoDB metrics.
	MetricsTypeMongoDB MetricsType = "mongodb"
	// MetricsTypeNoop represents no metrics.
	MetricsTypeNoop MetricsType = "noop"
)

// PipelineMetricsConfig holds the configuration for metrics in a pipeline.
type PipelineMetricsConfig struct {
	Enabled  bool        `yaml:"enabled"`  // Whether metrics are enabled for the pipeline
	Type     MetricsType `yaml:"type"`     // Type of metrics used in the pipeline
	Endpoint string      `yaml:"endpoint"` // Endpoint for the metrics service (e.g., MongoDB URI) mongodb://localhost:27017
}

// Executor represents a callable function that will be executed as a result of an event in the pipeline.
// It can be used to define custom logic that will be executed when the pipeline processes an event or during error handling.
type Executor string

// StageType represents the type of stage in the pipeline.
type StageType string

// StageType constants represent the different types of stages that can be used in a pipeline.
const (
	// StageTypeMap represents a map stage in the pipeline.
	StageTypeMap StageType = "map"
	// StageTypeBuffer represents a buffer stage in the pipeline.
	StageTypeBuffer StageType = "buffer"
	// StageTypeFanOut represents a fan-out stage in the pipeline.
	StageTypeFanOut StageType = "fan_out"
	// StageTypeFanIn represents a fan-in stage in the pipeline.
	StageTypeFanIn StageType = "fan_in"
	// StageTypeMapReduce represents a map-reduce stage in the pipeline.
	StageTypeMapReduce StageType = "map_reduce"

	// StageTypeFilter represents a filter stage in the pipeline.
	StageTypeFilter StageType = "filter"
	// StageTypeRouter represents a router stage in the pipeline.
	StageTypeRouter StageType = "router"
	// StageTypeJoinByKey represents a join by key stage in the pipeline.
	StageTypeJoinByKey StageType = "join_by_key"

	// StageTypeTumblingCountWindow represents a tumbling count window stage in the pipeline.
	StageTypeTumblingCountWindow StageType = "tumbling_count_window"
	// StageTypeTumblingTimeWindow represents a tumbling time window stage in the pipeline.
	StageTypeTumblingTimeWindow StageType = "tumbling_time_window"
	// StageTypeSlidingCountWindow represents a sliding count window stage in the pipeline.
	StageTypeSlidingCountWindow StageType = "sliding_count_window"
	// StageTypeSlidingTimeWindow represents a sliding time window stage in the pipeline.
	StageTypeSlidingTimeWindow StageType = "sliding_time_window"

	// StageTypeCircuitBreaker represents a circuit breaker stage in the pipeline.
	StageTypeCircuitBreaker StageType = "circuit_breaker"
	// StageTypeRetry represents a retry stage in the pipeline.
	StageTypeRetry StageType = "retry"
	// StageTypeTimeout represents a timeout stage in the pipeline.
	StageTypeTimeout StageType = "timeout"
	// StageTypeDeadLetterQueue represents a dead letter queue stage in the pipeline.
	StageTypeDeadLetterQueue StageType = "dead_letter_queue"

	// StageTypeCustom represents a custom stage in the pipeline.
	StageTypeCustom StageType = "custom"
)

// StageConfigurer is an interface implemented by all stage-specific configuration structs.
// This allows for polymorphic unmarshaling of stage properties.
type StageConfigurer interface {
	// IsStageConfigurer is a marker method to make the interface explicit.
	IsStageConfigurer()
}

// StageConfig holds the configuration for a single stage in a pipeline.
type StageConfig struct {
	Name       string          `yaml:"name"       validate:"required"` // Name of the stage
	Type       StageType       `yaml:"type"       validate:"required"` // Type of the stage (e.g , "map", "buffer", "fan_out", etc.)
	Properties StageConfigurer `yaml:"properties" validate:"required"` // Stage-specific properties, unmarshaled based on Type
}

// validateNestedStages handles recursive validation for stages that contain other stages.
func (sc *StageConfig) validateNestedStages(validate *validator.Validate) error {
	switch props := sc.Properties.(type) {
	case *FanOutProperties:
		for i, subStage := range props.Stages {
			if err := subStage.validate(validate); err != nil {
				return fmt.Errorf("fan_out sub-stage #%d ('%s') is invalid: %w", i, subStage.Name, err)
			}
		}
	case *RouterProperties:
		for routeName, subStage := range props.Routes {
			if err := subStage.validate(validate); err != nil {
				return fmt.Errorf("router route '%s' has invalid stage ('%s'): %w", routeName, subStage.Name, err)
			}
		}
	case *CircuitBreakerProperties:
		return props.Stage.validate(validate)
	case *RetryProperties:
		return props.Stage.validate(validate)
	case *TimeoutProperties:
		return props.Stage.validate(validate)
	case *DeadLetterQueueProperties:
		return props.Stage.validate(validate)
	}
	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for StageConfig.
func (sc *StageConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// umarshal based on the type field
	var stageType struct {
		Name string    `yaml:"name"`
		Type StageType `yaml:"type"`
	}

	if err := unmarshal(&stageType); err != nil {
		return err
	}

	sc.Name = stageType.Name
	sc.Type = stageType.Type

	var props StageConfigurer
	switch stageType.Type {
	case StageTypeMap:
		props = &MapProperties{}
	case StageTypeBuffer:
		props = &BufferProperties{}
	case StageTypeFanOut:
		props = &FanOutProperties{}
	case StageTypeFanIn:
		props = &FanInProperties{}
	case StageTypeMapReduce:
		props = &MapReduceProperties{}
	case StageTypeFilter:
		props = &FilterProperties{}
	case StageTypeRouter:
		props = &RouterProperties{}
	case StageTypeJoinByKey:
		props = &JoinByKeyProperties{}
	case StageTypeTumblingCountWindow:
		props = &TumblingCountWindowProperties{}
	case StageTypeTumblingTimeWindow:
		props = &TumblingTimeWindowProperties{}
	case StageTypeSlidingCountWindow:
		props = &SlidingCountWindowProperties{}
	case StageTypeSlidingTimeWindow:
		props = &SlidingTimeWindowProperties{}
	case StageTypeCircuitBreaker:
		props = &CircuitBreakerProperties{}
	case StageTypeRetry:
		props = &RetryProperties{}
	case StageTypeTimeout:
		props = &TimeoutProperties{}
	case StageTypeDeadLetterQueue:
		props = &DeadLetterQueueProperties{}
	case StageTypeCustom:
		props = &CustomProperties{}
	default:
		return fmt.Errorf("unsupported stage type '%s' for stage '%s'", sc.Type, sc.Name)
	}

	// 3. Unmarshal the 'properties' field into the specific struct.
	var propsWrapper struct {
		Properties interface{} `yaml:"properties"`
	}

	propsWrapper.Properties = props // Point to our typed struct

	if err := unmarshal(&propsWrapper); err != nil {
		return fmt.Errorf("failed to unmarshal properties for stage '%s' (type %s): %w", sc.Name, sc.Type, err)
	}

	sc.Properties = props
	return nil
}

// MapProperties holds the configuration for a map stage in a pipeline.
type MapProperties struct {
	MapFunction   Executor `yaml:"map_function"             validate:"required"` // Map function to be called for each item
	Concurrency   int      `yaml:"concurrency,omitempty"`                        // Number of concurrent workers for the map
	CollectErrors bool     `yaml:"collect_errors,omitempty"`                     // Whether to collect errors in the map stage
	ErrorHandler  Executor `yaml:"error_handler,omitempty"`                      // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make MapProperties implement StageConfigurer interface.
func (m *MapProperties) IsStageConfigurer() {}

// BufferProperties holds the configuration for a buffer stage in a pipeline.
type BufferProperties struct {
	BatchSize    int      `yaml:"batch_size"              validate:"gt=0"`     // Size of the buffer batch
	Processor    Executor `yaml:"processor"               validate:"required"` // Processor function to be called for each batch
	ErrorHandler Executor `yaml:"error_handler,omitempty"`                     // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make BufferProperties implement StageConfigurer interface.
func (b *BufferProperties) IsStageConfigurer() {}

// FanOutProperties holds the configuration for a fan-out stage in a pipeline.
type FanOutProperties struct {
	Stages       []StageConfig `yaml:"stages"                  validate:"required,min=1"` // List of stages to fan out to
	Concurrency  int           `yaml:"concurrency,omitempty"`                             // Number of concurrent workers for the fan-out
	ErrorHandler Executor      `yaml:"error_handler,omitempty"`                           // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make FanOutProperties implement StageConfigurer interface.
func (f *FanOutProperties) IsStageConfigurer() {}

// FanInProperties holds the configuration for a fan-in stage in a pipeline.
type FanInProperties struct {
	Aggregator   Executor `yaml:"aggregator"              validate:"required"` // Aggregator function to be called for each item
	ErrorHandler Executor `yaml:"error_handler,omitempty"`                     // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make FanInProperties implement StageConfigurer interface.
func (f *FanInProperties) IsStageConfigurer() {}

// MapReduceProperties holds the configuration for a map-reduce stage in a pipeline.
type MapReduceProperties struct {
	MapperFunction  Executor `yaml:"mapper_function"       validate:"required"` // Mapper function to be called for each item
	ReducerFunction Executor `yaml:"reducer_function"      validate:"required"` // Reducer function to be called for each item
	Parallelism     int      `yaml:"parallelism,omitempty"`                     // Number of concurrent workers for the map-reduce
}

// IsStageConfigurer is a marker method to make MapReduceProperties implement StageConfigurer interface.
func (m *MapReduceProperties) IsStageConfigurer() {}

// FilterProperties holds the configuration for a filter stage in a pipeline.
type FilterProperties struct {
	FilterFunction Executor `yaml:"filter_function"         validate:"required"` // Filter function to be called for each item
	ErrorHandler   Executor `yaml:"error_handler,omitempty"`                     // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make FilterProperties implement StageConfigurer interface.
func (f *FilterProperties) IsStageConfigurer() {}

// RouterProperties holds the configuration for a router stage in a pipeline.
type RouterProperties struct {
	Routes           map[string]StageConfig `yaml:"routes"                  validate:"required,min=1"` // Map of routes to stages
	SelectorFunction Executor               `yaml:"selector_function"       validate:"required"`       // Selector function to determine the route for each item
	ErrorHandler     Executor               `yaml:"error_handler,omitempty"`                           // Error handler function to be called in case of errors
	Concurrency      int                    `yaml:"concurrency,omitempty"`                             // Number of concurrent workers for the router
}

// IsStageConfigurer is a marker method to make RouterProperties implement StageConfigurer interface.
func (r *RouterProperties) IsStageConfigurer() {}

// JoinByKeyProperties holds the configuration for a join by key stage in a pipeline.
type JoinByKeyProperties struct {
	KeyFunction  Executor `yaml:"key_function"            validate:"required"` // Key function to extract the key for joining
	ErrorHandler Executor `yaml:"error_handler,omitempty"`                     // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make JoinByKeyProperties implement StageConfigurer interface.
func (j *JoinByKeyProperties) IsStageConfigurer() {}

// TumblingCountWindowProperties holds the configuration for a tumbling count window stage in a pipeline.
type TumblingCountWindowProperties struct {
	Size int `yaml:"size" validate:"gt=0"` // Size of the tumbling count window
}

// IsStageConfigurer is a marker method to make TumblingCountWindowProperties implement StageConfigurer interface.
func (t *TumblingCountWindowProperties) IsStageConfigurer() {}

// TumblingTimeWindowProperties holds the configuration for a tumbling time window stage in a pipeline.
type TumblingTimeWindowProperties struct {
	Duration int `yaml:"duration" validate:"gt=0"` // Duration of the tumbling time window in milliseconds
}

// IsStageConfigurer is a marker method to make TumblingTimeWindowProperties implement StageConfigurer interface.
func (t *TumblingTimeWindowProperties) IsStageConfigurer() {}

// SlidingCountWindowProperties holds the configuration for a sliding count window stage in a pipeline.
type SlidingCountWindowProperties struct {
	Size  int `yaml:"size"  validate:"gt=0"` // Size of the sliding count window
	Slide int `yaml:"slide" validate:"gt=0"` // Slide interval of the sliding count window
}

// IsStageConfigurer is a marker method to make SlidingCountWindowProperties implement StageConfigurer interface.
func (s *SlidingCountWindowProperties) IsStageConfigurer() {}

// SlidingTimeWindowProperties holds the configuration for a sliding time window stage in a pipeline.
type SlidingTimeWindowProperties struct {
	Duration int `yaml:"duration" validate:"gt=0"` // Duration of the sliding time window in milliseconds
	Slide    int `yaml:"slide"    validate:"gt=0"` // Slide interval of the sliding time window in milliseconds
}

// IsStageConfigurer is a marker method to make SlidingTimeWindowProperties implement StageConfigurer interface.
func (s *SlidingTimeWindowProperties) IsStageConfigurer() {}

// CircuitBreakerProperties holds the configuration for a circuit breaker stage in a pipeline.
type CircuitBreakerProperties struct {
	Stage            StageConfig `yaml:"stage"                       validate:"required"` // The stage to wrap with a circuit breaker
	FailureThreshold int         `yaml:"failure_threshold"           validate:"gt=0"`     // Number of failures before the circuit opens
	ResetTimeout     int         `yaml:"reset_timeout"               validate:"gt=0"`     // Time in milliseconds before the circuit resets
	SuccessThreshold int         `yaml:"success_threshold,omitempty"`                     // Number of successes before the circuit closes
	HalfOpenMax      int         `yaml:"half_open_max,omitempty"`                         // Maximum number of requests allowed in the half-open state
}

// IsStageConfigurer is a marker method to make CircuitBreakerProperties implement StageConfigurer interface.
func (c *CircuitBreakerProperties) IsStageConfigurer() {}

// RetryProperties holds the configuration for a retry stage in a pipeline.
type RetryProperties struct {
	Stage        StageConfig `yaml:"stage"                   validate:"required"` // The stage to wrap with retry logic
	Attempts     int         `yaml:"attempts"                validate:"gt=0"`     // Number of retry attempts
	ShouldRetry  Executor    `yaml:"should_retry,omitempty"`                      // Function to determine if a retry should be attempted
	Backoff      Executor    `yaml:"backoff,omitempty"`                           // Backoff function to be called between retries
	ErrorHandler Executor    `yaml:"error_handler,omitempty"`                     // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make RetryProperties implement StageConfigurer interface.
func (r *RetryProperties) IsStageConfigurer() {}

// TimeoutProperties holds the configuration for a timeout stage in a pipeline.
type TimeoutProperties struct {
	Stage        StageConfig `yaml:"stage"                   validate:"required"` // The stage to wrap with a timeout
	Timeout      int         `yaml:"timeout"                 validate:"gt=0"`     // Timeout duration in milliseconds
	ErrorHandler Executor    `yaml:"error_handler,omitempty"`                     // Error handler function to be called in case of errors
}

// IsStageConfigurer is a marker method to make TimeoutProperties implement StageConfigurer interface.
func (t *TimeoutProperties) IsStageConfigurer() {}

// DeadLetterQueueProperties holds the configuration for a dead letter queue stage in a pipeline.
type DeadLetterQueueProperties struct {
	Stage             StageConfig `yaml:"stage"                         validate:"required"` // The stage to wrap with a dead-letter queue
	Handler           Executor    `yaml:"handler"                       validate:"required"` // Handler function to be called for each item in the dead letter queue
	ShouldDQLFunction Executor    `yaml:"should_dql_function,omitempty"`                     // Function to determine if an item should be sent to the dead letter queue
	ErrorLogger       Executor    `yaml:"error_logger,omitempty"`                            // Error logger function to be called in case of errors
}

// IsStageConfigurer is a marker method to make DeadLetterQueueProperties implement StageConfigurer interface.
func (d *DeadLetterQueueProperties) IsStageConfigurer() {}

// CustomProperties holds the configuration for a custom stage in a pipeline.
type CustomProperties struct {
	Factory Executor       `yaml:"factory"          validate:"required"` // The name of the registered factory function for this custom stage.
	Config  map[string]any `yaml:"config,omitempty"`                     // Custom configuration for the stage, passed to the factory.
}

// IsStageConfigurer is a marker method to make CustomProperties implement StageConfigurer interface.
func (c *CustomProperties) IsStageConfigurer() {}
