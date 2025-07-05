package fluxus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Error messages
const (
	ErrStageBuilderExists = "stage builder already registered for type: %s"
	ErrExecutorExists     = "executor with name '%s' is already registered"
)

// BuildContext holds the observability context that gets passed to stage builders.
// This includes pipeline-level metrics collector and tracing provider that can be
// used to wrap individual stages when they have metrics/tracing enabled.
type BuildContext struct {
	MetricsCollector MetricsCollector // Pipeline-level metrics collector, may be nil
	TracerProvider   TracerProvider   // Pipeline-level tracer provider, may be nil
}

// StageBuilder is a function that constructs a stage from its configuration.
// It receives the complete stage configuration, the registry to look up executors,
// a recursive build function to construct any nested stages, and a build context
// containing pipeline-level observability components.
// The returned stage is an interface{} to accommodate different generic types,
// but it's expected to be a Stage[any, any] when built from config.
type StageBuilder func(stageConfig *StageConfig, registry *Registry, buildNestedStage func(*StageConfig) (interface{}, error), buildContext *BuildContext) (interface{}, error)

// Registry holds registered factory functions for creating stages and user-defined executors.
type Registry struct {
	mu            sync.RWMutex
	stageBuilders map[StageType]StageBuilder
	executors     map[Executor]interface{} // Stores user-defined functions, e.g., Stage[any, any] or PredicateFunc[any]
}

// NewRegistry creates a new, empty registry.
func NewRegistry() *Registry {
	return &Registry{
		stageBuilders: make(map[StageType]StageBuilder),
		executors:     make(map[Executor]interface{}),
	}
}

// RegisterStageBuilder adds a new builder for a specific StageType.
// This is typically used by the framework to register builders for built-in stages.
// Returns an error if a builder is already registered for this type.
func (r *Registry) RegisterStageBuilder(stageType StageType, builder StageBuilder) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.stageBuilders[stageType]; exists {
		return fmt.Errorf(ErrStageBuilderExists, stageType)
	}
	r.stageBuilders[stageType] = builder
	return nil
}

// GetStageBuilder retrieves a builder function by its StageType.
func (r *Registry) GetStageBuilder(stageType StageType) (StageBuilder, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	builder, ok := r.stageBuilders[stageType]
	return builder, ok
}

// RegisterExecutor allows users to register their custom functions (e.g., a StageFunc or PredicateFunc).
// Returns an error if an executor is already registered with this name.
func (r *Registry) RegisterExecutor(name Executor, function interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.executors[name]; exists {
		return fmt.Errorf(ErrExecutorExists, name)
	}
	r.executors[name] = function
	return nil
}

// GetExecutor retrieves a user-defined function by its name.
func (r *Registry) GetExecutor(name Executor) (interface{}, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, ok := r.executors[name]
	return executor, ok
}

// BuildPipelineFromConfig is the main entry point for creating a runnable pipeline
// from a parsed configuration. It validates the config and then builds the stages.
func BuildPipelineFromConfig(config *PipelineConfig, registry *Registry) (interface{}, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pipeline configuration: %w", err)
	}

	switch config.Type {
	case PipelineTypeClassic:
		return buildClassicPipeline(config, registry)
	case PipelineTypeStreaming:
		return buildStreamPipeline(config, registry)
	default:
		return nil, fmt.Errorf("unsupported pipeline type: %s", config.Type)
	}
}

func buildClassicPipeline(config *PipelineConfig, registry *Registry) (*Pipeline[any, any], error) {
	if len(config.Stages) == 0 {
		return nil, errors.New("classic pipeline requires at least one stage")
	}

	// Create BuildContext from pipeline configuration
	buildContext, err := createBuildContext(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create build context: %w", err)
	}

	var stages []interface{}
	for i, stageConfig := range config.Stages {
		builtStage, errBuild := buildStage(&stageConfig, registry, buildContext)
		if errBuild != nil {
			return nil, fmt.Errorf("failed to build stage #%d ('%s'): %w", i, stageConfig.Name, errBuild)
		}
		stages = append(stages, builtStage)
	}

	finalStage := ChainMany[any, any](stages...)
	pipeline := NewPipeline(finalStage)
	// Here you could add logic to configure the pipeline's error handler from the config if needed.

	return pipeline, nil
}

func buildStreamPipeline(config *PipelineConfig, registry *Registry) (interface{}, error) {
	// 1. Configure pipeline-level options
	var streamOpts []StreamPipelineOption
	if config.Streaming.BufferSize > 0 {
		streamOpts = append(streamOpts, WithStreamBufferSize(config.Streaming.BufferSize))
	}
	if config.Streaming.Concurrency > 0 {
		streamOpts = append(streamOpts, WithStreamConcurrency(config.Streaming.Concurrency))
	}
	if config.Name != "" {
		streamOpts = append(streamOpts, WithStreamPipelineName(config.Name))
	}
	if config.Streaming.Logger != "" {
		loggerExec, ok := registry.GetExecutor(config.Streaming.Logger)
		if !ok {
			return nil, fmt.Errorf("streaming logger executor '%s' not found in registry", config.Streaming.Logger)
		}
		logger, ok := loggerExec.(*log.Logger)
		if !ok {
			return nil, fmt.Errorf("executor '%s' is not a valid *log.Logger", config.Streaming.Logger)
		}
		streamOpts = append(streamOpts, WithStreamLogger(logger))
	}

	// 1.5. Create BuildContext from pipeline configuration
	buildContext, err := createBuildContext(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create build context: %w", err)
	}

	// 2. Start the builder. The pipeline will be dynamically typed as any -> any.
	builder := NewStreamPipeline[any](streamOpts...)
	var currentBuilder interface{} = builder

	// 3. Iterate through stages and add them to the builder
	for i, stageConfig := range config.Stages {
		builtStage, errBuild := buildStage(&stageConfig, registry, buildContext)
		if errBuild != nil {
			return nil, fmt.Errorf("failed to build stage #%d ('%s'): %w", i, stageConfig.Name, errBuild)
		}

		switch stage := builtStage.(type) {
		case Stage[any, any]:
			b, ok := currentBuilder.(*StreamPipelineBuilder[any])
			if !ok {
				return nil, fmt.Errorf("internal builder error: expected *StreamPipelineBuilder[any] before stage '%s', but got %T", stageConfig.Name, currentBuilder)
			}
			currentBuilder = AddStage(b, stageConfig.Name, stage)

		case Stage[[]any, map[any][]any]:
			// JoinByKey
			b, ok := currentBuilder.(*StreamPipelineBuilder[[]any])
			if !ok {
				return nil, fmt.Errorf("internal builder error: expected *StreamPipelineBuilder[[]any] before JoinByKey stage '%s', but got %T", stageConfig.Name, currentBuilder)
			}
			currentBuilder = AddStage(b, stageConfig.Name, stage)

		case Stage[any, []any]:
			// FanOut
			b, ok := currentBuilder.(*StreamPipelineBuilder[any])
			if !ok {
				return nil, fmt.Errorf("internal builder error: expected *StreamPipelineBuilder[any] before FanOut stage '%s', but got %T", stageConfig.Name, currentBuilder)
			}
			currentBuilder = AddStage(b, stageConfig.Name, stage)

		case Stage[[]any, any]:
			// FanIn
			b, ok := currentBuilder.(*StreamPipelineBuilder[[]any])
			if !ok {
				return nil, fmt.Errorf("internal builder error: expected *StreamPipelineBuilder[[]any] before FanIn stage '%s', but got %T", stageConfig.Name, currentBuilder)
			}
			currentBuilder = AddStage(b, stageConfig.Name, stage)

		case Stage[[]any, []any]:
			// Buffer, MapReduce
			b, ok := currentBuilder.(*StreamPipelineBuilder[[]any])
			if !ok {
				return nil, fmt.Errorf("internal builder error: expected *StreamPipelineBuilder[[]any] before batch processing stage '%s', but got %T", stageConfig.Name, currentBuilder)
			}
			currentBuilder = AddStage(b, stageConfig.Name, stage)

		case StreamStage[any, []any]:
			// Window stages
			b, ok := currentBuilder.(*StreamPipelineBuilder[any])
			if !ok {
				return nil, fmt.Errorf("internal builder error: expected *StreamPipelineBuilder[any] before windowing stage '%s', but got %T", stageConfig.Name, currentBuilder)
			}
			windowedBuilder := AddStreamStage(b, stageConfig.Name, stage)

			unwrapperStage := StageFunc[[]any, any](func(_ context.Context, input []any) (any, error) {
				return input, nil
			})
			currentBuilder = AddStage(windowedBuilder, stageConfig.Name+"_unwrapper", unwrapperStage)

		default:
			return nil, fmt.Errorf("unsupported stage type for streaming pipeline from config: stage '%s' has type %T", stageConfig.Name, builtStage)
		}
	}

	// 4. Finalize the pipeline
	finalBuilder, ok := currentBuilder.(*StreamPipelineBuilder[any])
	if !ok {
		return nil, fmt.Errorf("internal builder error: final pipeline builder has unexpected type %T", currentBuilder)
	}

	return Finalize[any](finalBuilder)
}

// buildStage is the recursive core builder for a single stage.
func buildStage(stageConfig *StageConfig, registry *Registry, buildContext *BuildContext) (interface{}, error) {
	builder, ok := registry.GetStageBuilder(stageConfig.Type)
	if !ok {
		return nil, fmt.Errorf("no builder registered for stage type '%s'", stageConfig.Type)
	}

	// The recursive call is passed as a closure to the builder function.
	buildNested := func(nestedConfig *StageConfig) (interface{}, error) {
		return buildStage(nestedConfig, registry, buildContext)
	}

	return builder(stageConfig, registry, buildNested, buildContext)
}

// createBuildContext creates a BuildContext from pipeline configuration.
func createBuildContext(config *PipelineConfig) (*BuildContext, error) {
	factory := NewObservabilityFactory()

	// Create metrics collector
	metricsCollector, err := factory.CreateMetricsCollector(config.Metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics collector: %w", err)
	}

	// Create tracer provider
	tracerProvider, err := factory.CreateTracerProvider(config.Tracing, config.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create tracer provider: %w", err)
	}

	return &BuildContext{
		MetricsCollector: metricsCollector,
		TracerProvider:   tracerProvider,
	}, nil
}

// wrapStageWithObservability wraps a stage with pattern-specific metrics and/or tracing if enabled in the stage config.
func wrapStageWithObservability(stage interface{}, stageConfig *StageConfig, buildContext *BuildContext) interface{} {
	// Apply pattern-specific metrics wrapping if enabled
	if stageConfig.Metrics && buildContext.MetricsCollector != nil {
		stage = wrapWithPatternSpecificMetrics(stage, stageConfig.Type, buildContext.MetricsCollector)
	}

	// Apply pattern-specific tracing wrapping if enabled
	if stageConfig.Tracing && buildContext.TracerProvider != nil {
		stage = wrapWithPatternSpecificTracing(stage, stageConfig.Type, buildContext.TracerProvider)
	}

	return stage
}

// wrapWithPatternSpecificMetrics wraps a stage with the appropriate pattern-specific metrics wrapper.
const (
	observabilityTypeMetrics = "metrics"
	observabilityTypeTracing = "tracing"
)

// wrapWithObservability provides a unified wrapper for both metrics and tracing
func wrapWithObservability(
	stage any,
	stageType StageType,
	wrapType string,
	collector MetricsCollector,
	provider TracerProvider,
) any {
	switch stageType {
	case StageTypeMap:
		return wrapMapStage(stage, wrapType, collector, provider)
	case StageTypeBuffer:
		return wrapBufferStage(stage, wrapType, collector, provider)
	case StageTypeFanOut:
		return wrapFanOutStage(stage, wrapType, collector, provider)
	case StageTypeFanIn:
		return wrapFanInStage(stage, wrapType, collector, provider)
	case StageTypeMapReduce:
		return wrapMapReduceStage(stage, wrapType, collector, provider)
	case StageTypeFilter:
		return wrapFilterStage(stage, wrapType, collector, provider)
	case StageTypeRouter:
		return wrapRouterStage(stage, wrapType, collector, provider)
	case StageTypeJoinByKey:
		return wrapJoinByKeyStage(stage, wrapType, collector, provider)
	case StageTypeTumblingCountWindow:
		return wrapTumblingCountWindowStage(stage, wrapType, collector, provider)
	case StageTypeTumblingTimeWindow:
		return wrapTumblingTimeWindowStage(stage, wrapType, collector, provider)
	case StageTypeSlidingCountWindow:
		return wrapSlidingCountWindowStage(stage, wrapType, collector, provider)
	case StageTypeSlidingTimeWindow:
		return wrapSlidingTimeWindowStage(stage, wrapType, collector, provider)
	case StageTypeTimeout:
		return wrapTimeoutStage(stage, wrapType, collector, provider)
	case StageTypeCircuitBreaker:
		return wrapCircuitBreakerStage(stage, wrapType, collector, provider)
	case StageTypeRetry:
		return wrapRetryStage(stage, wrapType, collector, provider)
	case StageTypeDeadLetterQueue:
		return wrapDeadLetterQueueStage(stage, wrapType, collector, provider)
	case StageTypeCustom:
		// Custom stages don't have built-in wrappers, return as-is
		return stage
	}
	return stage
}

func wrapMapStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if mapStage, ok := stage.(*Map[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedMap(mapStage, WithMetricsCollector[[]any, []any](collector))
		}
		return NewTracedMap(mapStage, WithTracerProvider[[]any, []any](provider))
	}
	return stage
}

func wrapBufferStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if bufferStage, ok := stage.(*Buffer[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedBuffer(bufferStage, WithMetricsCollector[[]any, []any](collector))
		}
		return NewTracedBuffer(bufferStage, WithTracerProvider[[]any, []any](provider))
	}
	return stage
}

func wrapFanOutStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if fanOutStage, ok := stage.(*FanOut[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedFanOut(fanOutStage, WithMetricsCollector[any, []any](collector))
		}
		return NewTracedFanOut(fanOutStage, WithTracerProvider[any, []any](provider))
	}
	return stage
}

func wrapFanInStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if fanInStage, ok := stage.(*FanIn[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedFanIn(fanInStage, WithMetricsCollector[[]any, any](collector))
		}
		return NewTracedFanIn(fanInStage, WithTracerProvider[[]any, any](provider))
	}
	return stage
}

func wrapMapReduceStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if mapReduceStage, ok := stage.(*MapReduce[any, any, any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedMapReduce(mapReduceStage, WithMetricsCollector[[]any, []any](collector))
		}
		return NewTracedMapReduce(mapReduceStage, WithTracerProvider[[]any, []any](provider))
	}
	return stage
}

func wrapFilterStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if filterStage, ok := stage.(*Filter[any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedFilter(filterStage, WithMetricsCollector[any, any](collector))
		}
		return NewTracedFilter(filterStage, WithTracerProvider[any, any](provider))
	}
	return stage
}

func wrapRouterStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if routerStage, ok := stage.(*Router[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedRouter(routerStage, WithMetricsCollector[any, []any](collector))
		}
		return NewTracedRouter(routerStage, WithTracerProvider[any, []any](provider))
	}
	return stage
}

func wrapJoinByKeyStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if joinStage, ok := stage.(*JoinByKey[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedJoinByKey(joinStage, WithMetricsCollector[[]any, map[any][]any](collector))
		}
		return NewTracedJoinByKey(joinStage, WithTracerProvider[[]any, map[any][]any](provider))
	}
	return stage
}

func wrapTumblingCountWindowStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if tumblingCountStage, ok := stage.(*TumblingCountWindow[any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedTumblingCountWindow(
				tumblingCountStage,
				WithMetricsStreamCollector[any, []any](collector),
			)
		}
		return NewTracedTumblingCountWindow(tumblingCountStage, WithTracerStreamProvider[any, []any](provider))
	}
	return stage
}

func wrapTumblingTimeWindowStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if tumblingTimeStage, ok := stage.(*TumblingTimeWindow[any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedTumblingTimeWindow(
				tumblingTimeStage,
				WithMetricsStreamCollector[any, []any](collector),
			)
		}
		return NewTracedTumblingTimeWindow(tumblingTimeStage, WithTracerStreamProvider[any, []any](provider))
	}
	return stage
}

func wrapSlidingCountWindowStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if slidingCountStage, ok := stage.(*SlidingCountWindow[any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedSlidingCountWindow(
				slidingCountStage,
				WithMetricsStreamCollector[any, []any](collector),
			)
		}
		return NewTracedSlidingCountWindow(slidingCountStage, WithTracerStreamProvider[any, []any](provider))
	}
	return stage
}

func wrapSlidingTimeWindowStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if slidingTimeStage, ok := stage.(*SlidingTimeWindow[any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedSlidingTimeWindow(
				slidingTimeStage,
				WithMetricsStreamCollector[any, []any](collector),
			)
		}
		return NewTracedSlidingTimeWindow(slidingTimeStage, WithTracerStreamProvider[any, []any](provider))
	}
	return stage
}

func wrapTimeoutStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if timeoutStage, ok := stage.(*Timeout[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedTimeout(timeoutStage, WithMetricsCollector[any, any](collector))
		}
		return NewTracedTimeout(timeoutStage, WithTracerProvider[any, any](provider))
	}
	return stage
}

func wrapCircuitBreakerStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if cbStage, ok := stage.(*CircuitBreaker[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedCircuitBreaker(cbStage, WithMetricsCollector[any, any](collector))
		}
		return NewTracedCircuitBreaker(cbStage, WithTracerProvider[any, any](provider))
	}
	return stage
}

func wrapRetryStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if retryStage, ok := stage.(*Retry[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedRetry(retryStage, WithMetricsCollector[any, any](collector))
		}
		return NewTracedRetry(retryStage, WithTracerProvider[any, any](provider))
	}
	return stage
}

func wrapDeadLetterQueueStage(stage any, wrapType string, collector MetricsCollector, provider TracerProvider) any {
	if dlqStage, ok := stage.(*DeadLetterQueue[any, any]); ok {
		if wrapType == observabilityTypeMetrics {
			return NewMetricatedDeadLetterQueue(dlqStage, WithMetricsCollector[any, any](collector))
		}
		return NewTracedDeadLetterQueue(dlqStage, WithTracerProvider[any, any](provider))
	}
	return stage
}

func wrapWithPatternSpecificMetrics(stage any, stageType StageType, collector MetricsCollector) any {
	return wrapWithObservability(stage, stageType, observabilityTypeMetrics, collector, nil)
}

// wrapWithPatternSpecificTracing wraps a stage with the appropriate pattern-specific tracing wrapper.
func wrapWithPatternSpecificTracing(stage any, stageType StageType, provider TracerProvider) any {
	return wrapWithObservability(stage, stageType, observabilityTypeTracing, nil, provider)
}

// --- Built-in Stage Builders ---

var defaultRegistry *Registry
var once sync.Once

// DefaultRegistry returns a singleton registry pre-populated with builders for all built-in stages.
func DefaultRegistry() *Registry {
	once.Do(func() {
		defaultRegistry = NewRegistry()
		
		// Register all built-in stage builders
		// Since these are built-in types that shouldn't conflict, any error indicates a programming error
		builders := []struct {
			stageType StageType
			builder   StageBuilder
		}{
			{StageTypeMap, mapBuilder},
			{StageTypeBuffer, bufferBuilder},
			{StageTypeFanOut, fanoutBuilder},
			{StageTypeFanIn, faninBuilder},
			{StageTypeMapReduce, mapReduceBuilder},
			{StageTypeFilter, filterBuilder},
			{StageTypeRouter, routerBuilder},
			{StageTypeJoinByKey, joinByKeyBuilder},
			{StageTypeTumblingCountWindow, tumblingCountWindowBuilder},
			{StageTypeTumblingTimeWindow, tumblingTimeWindowBuilder},
			{StageTypeSlidingCountWindow, slidingCountWindowBuilder},
			{StageTypeSlidingTimeWindow, slidingTimeWindowBuilder},
			{StageTypeCircuitBreaker, circuitBreakerBuilder},
			{StageTypeRetry, retryBuilder},
			{StageTypeTimeout, timeoutBuilder},
			{StageTypeDeadLetterQueue, deadLetterQueueBuilder},
			{StageTypeCustom, customBuilder},
		}
		
		for _, b := range builders {
			if err := defaultRegistry.RegisterStageBuilder(b.stageType, b.builder); err != nil {
				// This should never happen for built-in builders, so panic is appropriate
				panic(fmt.Sprintf("Failed to register built-in stage builder for %s: %v", b.stageType, err))
			}
		}
	})
	return defaultRegistry
}

func mapBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	mapProps, ok := stageConfig.Properties.(*MapProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'map': expected *MapProperties, got %T",
			stageConfig.Properties,
		)
	}

	executorFunc, ok := registry.GetExecutor(mapProps.MapFunction)
	if !ok {
		return nil, fmt.Errorf("executor '%s' not found in registry", mapProps.MapFunction)
	}

	// The contract is that registered executors for a Map stage are Stage[any, any].
	mapFunction, ok := executorFunc.(Stage[any, any])
	if !ok {
		return nil, fmt.Errorf("executor '%s' is not a valid Stage[any, any] for a map stage", mapProps.MapFunction)
	}

	// Create and configure the Map stage.
	mapStage := NewMap(mapFunction).
		WithCollectErrors(mapProps.CollectErrors)

	if mapProps.Concurrency > 0 {
		mapStage = mapStage.WithConcurrency(mapProps.Concurrency)
	}

	// Optionally, configure the error handler if provided in the properties.
	if mapProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(mapProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", mapProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			mapStage = mapStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", mapProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(mapStage, stageConfig, buildContext)
	return stage, nil
}

func bufferBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	bufferProps, okBuff := stageConfig.Properties.(*BufferProperties)
	if !okBuff {
		return nil, fmt.Errorf(
			"incorrect properties type for 'buffer': expected *BufferProperties, got %T",
			stageConfig.Properties,
		)
	}

	processorFunc, okProcFnc := registry.GetExecutor(bufferProps.Processor)
	if !okProcFnc {
		return nil, fmt.Errorf("processor '%s' not found in registry", bufferProps.Processor)
	}

	batchSize := bufferProps.BatchSize
	if batchSize <= 0 {
		return nil, fmt.Errorf("invalid batch size for buffer stage: %d", batchSize)
	}

	var bufferStage *Buffer[any, any]

	if processor, okProcFncSig := processorFunc.(func(ctx context.Context, batch []any) ([]any, error)); okProcFncSig {
		bufferStage = NewBuffer(batchSize, processor)
	} else {
		return nil, fmt.Errorf("processor '%s' is not a valid function for Buffer stage", bufferProps.Processor)
	}

	if bufferProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(bufferProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", bufferProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			bufferStage = bufferStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", bufferProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(bufferStage, stageConfig, buildContext)
	return stage, nil
}

func fanoutBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	nestedBuilder func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	fanoutProps, ok := stageConfig.Properties.(*FanOutProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'fanout': expected *FanOutProperties, got %T",
			stageConfig.Properties,
		)
	}

	fanoutConcurrency := fanoutProps.Concurrency
	if fanoutConcurrency < 0 {
		return nil, fmt.Errorf("invalid concurrency for fanout stage: %d", fanoutConcurrency)
	}

	// build nested stages
	var stages []Stage[any, any]
	for i, stageConfig := range fanoutProps.Stages {
		stage, err := nestedBuilder(&stageConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to build nested stage #%d ('%s'): %w", i, stageConfig.Name, err)
		}
		// try casting to Stage[any, any]
		var pipelineStage Stage[any, any]
		if pipelineStage, ok = stage.(Stage[any, any]); !ok {
			return nil, fmt.Errorf("nested stage #%d ('%s') is not a valid Stage[any, any]", i, stageConfig.Name)
		}
		stages = append(stages, pipelineStage)
	}

	var fanoutStage = NewFanOut(stages...).
		WithConcurrency(fanoutConcurrency)

	if fanoutProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(fanoutProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", fanoutProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			fanoutStage = fanoutStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", fanoutProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(fanoutStage, stageConfig, buildContext)
	return stage, nil
}

func faninBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	faninProps, ok := stageConfig.Properties.(*FanInProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'fanin': expected *FanInProperties, got %T",
			stageConfig.Properties,
		)
	}

	faninAggregatorFunc, ok := registry.GetExecutor(faninProps.Aggregator)
	if !ok {
		return nil, fmt.Errorf("aggregator '%s' not found in registry", faninProps.Aggregator)
	}

	var faninStage *FanIn[any, any]
	var aggregator func(inputs []any) (any, error)

	if aggregator, ok = faninAggregatorFunc.(func(inputs []any) (any, error)); !ok {
		return nil, fmt.Errorf("aggregator '%s' is not a valid function for FanIn stage", faninProps.Aggregator)
	}

	faninStage = NewFanIn(aggregator)

	if faninProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(faninProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", faninProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			faninStage = faninStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", faninProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(faninStage, stageConfig, buildContext)
	return stage, nil
}

func mapReduceBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	mapReduceProps, ok := stageConfig.Properties.(*MapReduceProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'map_reduce': expected *MapReduceProperties, got %T",
			stageConfig.Properties,
		)
	}

	mapperFunc, ok := registry.GetExecutor(mapReduceProps.MapperFunction)
	if !ok {
		return nil, fmt.Errorf("mapper '%s' not found in registry", mapReduceProps.MapperFunction)
	}

	reducerFunc, ok := registry.GetExecutor(mapReduceProps.ReducerFunction)
	if !ok {
		return nil, fmt.Errorf("reducer '%s' not found in registry", mapReduceProps.ReducerFunction)
	}

	var mapreduceStage *MapReduce[any, any, any, any]

	var mapper func(ctx context.Context, item any) (KeyValue[any, any], error)
	if mapper, ok = mapperFunc.(func(ctx context.Context, item any) (KeyValue[any, any], error)); !ok {
		return nil, fmt.Errorf("mapper '%s' is not a valid function for MapReduce stage", mapReduceProps.MapperFunction)
	}

	var reducer func(ctx context.Context, input ReduceInput[any, any]) (any, error)
	if reducer, ok = reducerFunc.(func(ctx context.Context, input ReduceInput[any, any]) (any, error)); !ok {
		return nil, fmt.Errorf(
			"reducer '%s' is not a valid function for MapReduce stage",
			mapReduceProps.ReducerFunction,
		)
	}

	mapreduceStage = NewMapReduce(mapper, reducer)

	parallelism := mapReduceProps.Parallelism
	if parallelism <= 0 {
		return nil, fmt.Errorf("invalid parallelism for map-reduce stage: %d", parallelism)
	}

	mapreduceStage = mapreduceStage.WithParallelism(parallelism)

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(mapreduceStage, stageConfig, buildContext)
	return stage, nil
}

func filterBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	filterProps, ok := stageConfig.Properties.(*FilterProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'filter': expected *FilterProperties, got %T",
			stageConfig.Properties,
		)
	}

	predicateFunc, ok := registry.GetExecutor(filterProps.FilterFunction)
	if !ok {
		return nil, fmt.Errorf("predicate '%s' not found in registry", filterProps.FilterFunction)
	}

	var filterStage *Filter[any]
	var predicate func(ctx context.Context, item any) (bool, error)

	if predicate, ok = predicateFunc.(PredicateFunc[any]); !ok {
		return nil, fmt.Errorf(
			"predicate '%s' is not a valid PredicateFunc for Filter stage",
			filterProps.FilterFunction,
		)
	}

	filterStage = NewFilter(predicate)

	if filterProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(filterProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", filterProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			filterStage = filterStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", filterProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(filterStage, stageConfig, buildContext)
	return stage, nil
}

func routerBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	nestedBuilder func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	routerProps, ok := stageConfig.Properties.(*RouterProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'router': expected *RouterProperties, got %T",
			stageConfig.Properties,
		)
	}

	var routes []Route[any, any]

	selectorFunc, ok := registry.GetExecutor(routerProps.SelectorFunction)
	if !ok {
		return nil, fmt.Errorf("selector function '%s' not found in registry", routerProps.SelectorFunction)
	}

	for routeName, subStageConfig := range routerProps.Routes {
		subStage, err := nestedBuilder(&subStageConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to build route '%s': %w", routeName, err)
		}
		stage, okStageSig := subStage.(Stage[any, any])
		if !okStageSig {
			return nil, fmt.Errorf("route '%s' is not a valid Stage[any, any]", routeName)
		}

		routes = append(routes, Route[any, any]{
			Name:  routeName,
			Stage: stage,
		})
	}

	var routerStage *Router[any, any]
	var selector func(ctx context.Context, item any) ([]int, error)

	if selector, ok = selectorFunc.(func(ctx context.Context, item any) ([]int, error)); !ok {
		return nil, fmt.Errorf(
			"selector function '%s' is not a valid function for Router stage",
			routerProps.SelectorFunction,
		)
	}

	routerStage = NewRouter(selector, routes...)

	routerConcurrency := routerProps.Concurrency
	if routerConcurrency < 0 {
		return nil, fmt.Errorf("invalid concurrency for router stage: %d", routerConcurrency)
	}

	routerStage = routerStage.WithConcurrency(routerConcurrency)

	if routerProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(routerProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", routerProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			routerStage = routerStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", routerProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(routerStage, stageConfig, buildContext)
	return stage, nil
}

func joinByKeyBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	joinByKeyProps, ok := stageConfig.Properties.(*JoinByKeyProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'join_by_key': expected *JoinByKeyProperties, got %T",
			stageConfig.Properties,
		)
	}

	keyFunction, ok := registry.GetExecutor(joinByKeyProps.KeyFunction)
	if !ok {
		return nil, fmt.Errorf("key function '%s' not found in registry", joinByKeyProps.KeyFunction)
	}

	var joinStage *JoinByKey[any, any]
	var keyFunc func(ctx context.Context, item any) (any, error)

	if keyFunc, ok = keyFunction.(func(ctx context.Context, item any) (any, error)); !ok {
		return nil, fmt.Errorf(
			"key function '%s' is not a valid function for JoinByKey stage",
			joinByKeyProps.KeyFunction,
		)
	}
	joinStage = NewJoinByKey(keyFunc)

	if joinByKeyProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(joinByKeyProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", joinByKeyProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			joinStage = joinStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", joinByKeyProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(joinStage, stageConfig, buildContext)
	return stage, nil
}

func tumblingCountWindowBuilder(
	stageConfig *StageConfig,
	_ *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	tumblingCountWindowProps, ok := stageConfig.Properties.(*TumblingCountWindowProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'tumbling_count_window': expected *TumblingCountWindowProperties, got %T",
			stageConfig.Properties,
		)
	}

	windowSize := tumblingCountWindowProps.Size
	if windowSize <= 0 {
		return nil, fmt.Errorf("invalid window size for tumbling count window stage: %d", windowSize)
	}

	tumblingStage := NewTumblingCountWindow[any](windowSize)

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(tumblingStage, stageConfig, buildContext)
	return stage, nil
}

func tumblingTimeWindowBuilder(
	stageConfig *StageConfig,
	_ *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	tumblingTimeWindowProps, ok := stageConfig.Properties.(*TumblingTimeWindowProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'tumbling_time_window': expected *TumblingTimeWindowProperties, got %T",
			stageConfig.Properties,
		)
	}

	windowDuration := tumblingTimeWindowProps.Duration
	if windowDuration <= 0 {
		return nil, fmt.Errorf("invalid window duration for tumbling time window stage: %d", windowDuration)
	}

	tumblingStage := NewTumblingTimeWindow[any](time.Duration(windowDuration) * time.Millisecond)

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(tumblingStage, stageConfig, buildContext)
	return stage, nil
}

func slidingCountWindowBuilder(
	stageConfig *StageConfig,
	_ *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	slidingCountWindowProps, ok := stageConfig.Properties.(*SlidingCountWindowProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'sliding_count_window': expected *SlidingCountWindowProperties, got %T",
			stageConfig.Properties,
		)
	}

	windowSize := slidingCountWindowProps.Size
	if windowSize <= 0 {
		return nil, fmt.Errorf("invalid window size for sliding count window stage: %d", windowSize)
	}

	windowSlide := slidingCountWindowProps.Slide
	if windowSlide <= 0 {
		return nil, fmt.Errorf("invalid window slide for sliding count window stage: %d", windowSlide)
	}

	slidingStage := NewSlidingCountWindow[any](windowSize, windowSlide)
	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(slidingStage, stageConfig, buildContext)
	return stage, nil
}

func slidingTimeWindowBuilder(
	stageConfig *StageConfig,
	_ *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	slidingTimeWindowProps, ok := stageConfig.Properties.(*SlidingTimeWindowProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'sliding_time_window': expected *SlidingTimeWindowProperties, got %T",
			stageConfig.Properties,
		)
	}

	windowDuration := slidingTimeWindowProps.Duration
	if windowDuration <= 0 {
		return nil, fmt.Errorf("invalid window duration for sliding time window stage: %d", windowDuration)
	}

	windowSlideDuration := slidingTimeWindowProps.Slide
	if windowSlideDuration <= 0 {
		return nil, fmt.Errorf("invalid window slide for sliding time window stage: %d", windowSlideDuration)
	}

	slidingStage := NewSlidingTimeWindow[any](
		time.Duration(windowDuration)*time.Millisecond,
		time.Duration(windowSlideDuration)*time.Millisecond,
	)
	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(slidingStage, stageConfig, buildContext)
	return stage, nil
}

func circuitBreakerBuilder(
	stageConfig *StageConfig,
	_ *Registry,
	nestedBuilder func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	circuitBreakerProps, ok := stageConfig.Properties.(*CircuitBreakerProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'circuit_breaker': expected *CircuitBreakerProperties, got %T",
			stageConfig.Properties,
		)
	}

	// parse the nested stage configuration
	nestedStage, err := nestedBuilder(&circuitBreakerProps.Stage)
	if err != nil {
		return nil, fmt.Errorf("failed to build nested stage for circuit breaker: %w", err)
	}

	nestedBreakerStage, ok := nestedStage.(Stage[any, any])
	if !ok {
		return nil, errors.New("nested stage for circuit breaker is not a valid Stage[any, any]")
	}

	failureThreshold := circuitBreakerProps.FailureThreshold
	if failureThreshold <= 0 {
		return nil, fmt.Errorf("invalid failure threshold for circuit breaker stage: %d", failureThreshold)
	}

	resetTimeout := circuitBreakerProps.ResetTimeout
	if resetTimeout <= 0 {
		return nil, fmt.Errorf("invalid reset timeout for circuit breaker stage: %d", resetTimeout)
	}

	successThreshold := circuitBreakerProps.SuccessThreshold
	if successThreshold < 0 {
		return nil, fmt.Errorf("invalid success threshold for circuit breaker stage: %d", successThreshold)
	}

	halfOpenMax := circuitBreakerProps.HalfOpenMax
	if halfOpenMax < 0 {
		return nil, fmt.Errorf("invalid half-open max for circuit breaker stage: %d", halfOpenMax)
	}

	var options []CircuitBreakerOption[any, any]

	if successThreshold > 0 {
		options = append(options, WithSuccessThreshold[any, any](successThreshold))
	}

	if halfOpenMax > 0 {
		options = append(options, WithHalfOpenMaxRequests[any, any](halfOpenMax))
	}

	circuitBreakerStage := NewCircuitBreaker(
		nestedBreakerStage,
		failureThreshold,
		time.Duration(resetTimeout)*time.Millisecond,
		options...,
	)

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(circuitBreakerStage, stageConfig, buildContext)
	return stage, nil
}

func retryBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	nestedBuilder func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	retryProps, ok := stageConfig.Properties.(*RetryProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'retry': expected *RetryProperties, got %T",
			stageConfig.Properties,
		)
	}

	nestedStage, err := nestedBuilder(&retryProps.Stage)
	if err != nil {
		return nil, fmt.Errorf("failed to build nested stage for retry: %w", err)
	}

	maxAttempts := retryProps.Attempts
	if maxAttempts <= 0 {
		return nil, fmt.Errorf("invalid max attempts for retry stage: %d", maxAttempts)
	}

	var retryStage *Retry[any, any]

	var nested Stage[any, any]
	if nested, ok = nestedStage.(Stage[any, any]); !ok {
		return nil, errors.New("nested stage for retry is not a valid Stage[any, any]")
	}

	retryStage = NewRetry(nested, maxAttempts)

	if retryProps.Backoff != "" {
		backoffFunc, okBackoffFunc := registry.GetExecutor(retryProps.Backoff)
		if !okBackoffFunc {
			return nil, fmt.Errorf("backoff function '%s' not found in registry", retryProps.Backoff)
		}

		if backoff, okBackoffSig := backoffFunc.(func(attempt int) int); okBackoffSig {
			retryStage = retryStage.WithBackoff(backoff)
		} else {
			return nil, fmt.Errorf("backoff function '%s' is not a valid function for Retry stage", retryProps.Backoff)
		}
	}

	if retryProps.ShouldRetry != "" {
		shouldRetryFunc, okRetryFunc := registry.GetExecutor(retryProps.ShouldRetry)
		if !okRetryFunc {
			return nil, fmt.Errorf("should retry function '%s' not found in registry", retryProps.ShouldRetry)
		}

		if shouldRetry, okRetryFuncSig := shouldRetryFunc.(func(error) bool); okRetryFuncSig {
			retryStage = retryStage.WithShouldRetry(shouldRetry)
		} else {
			return nil, fmt.Errorf("should retry function '%s' is not a valid function for Retry stage", retryProps.ShouldRetry)
		}
	}

	if retryProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(retryProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", retryProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			retryStage = retryStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", retryProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(retryStage, stageConfig, buildContext)
	return stage, nil
}

func timeoutBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	nestedBuilder func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	timeoutProps, ok := stageConfig.Properties.(*TimeoutProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'timeout': expected *TimeoutProperties, got %T",
			stageConfig.Properties,
		)
	}

	nestedStage, err := nestedBuilder(&timeoutProps.Stage)
	if err != nil {
		return nil, fmt.Errorf("failed to build nested stage for timeout: %w", err)
	}

	timeoutDuration := timeoutProps.Timeout
	if timeoutDuration <= 0 {
		return nil, fmt.Errorf("invalid timeout duration for timeout stage: %d", timeoutDuration)
	}

	var timeoutStage *Timeout[any, any]
	var nested Stage[any, any]
	var okStageSig bool

	if nested, okStageSig = nestedStage.(Stage[any, any]); !okStageSig {
		return nil, errors.New("nested stage for timeout is not a valid Stage[any, any]")
	}

	timeoutStage = NewTimeout(nested, time.Duration(timeoutDuration)*time.Millisecond)

	if timeoutProps.ErrorHandler != "" {
		errorHandlerFunc, okErrHandler := registry.GetExecutor(timeoutProps.ErrorHandler)
		if !okErrHandler {
			return nil, fmt.Errorf("error handler executor '%s' not found in registry", timeoutProps.ErrorHandler)
		}

		if errorHandler, okErrHandlerSig := errorHandlerFunc.(func(error) error); okErrHandlerSig {
			timeoutStage = timeoutStage.WithErrorHandler(errorHandler)
		} else {
			return nil, fmt.Errorf("error handler executor '%s' is not a valid ErrorHandler", timeoutProps.ErrorHandler)
		}
	}

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(timeoutStage, stageConfig, buildContext)
	return stage, nil
}

func deadLetterQueueBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	nestedBuilder func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	deadLetterQueueProps, ok := stageConfig.Properties.(*DeadLetterQueueProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'dead_letter_queue': expected *DeadLetterQueueProperties, got %T",
			stageConfig.Properties,
		)
	}

	nestedStage, err := nestedBuilder(&deadLetterQueueProps.Stage)
	if err != nil {
		return nil, fmt.Errorf("failed to build nested stage for dead letter queue: %w", err)
	}

	var deadLetterQueueStage *DeadLetterQueue[any, any]
	var deadLetterOptions []DeadLetterQueueOption[any, any]

	if deadLetterQueueProps.ErrorLogger != "" {
		errorLoggerFunc, okErrLog := registry.GetExecutor(deadLetterQueueProps.ErrorLogger)
		if !okErrLog {
			return nil, fmt.Errorf("error logger executor '%s' not found in registry", deadLetterQueueProps.ErrorLogger)
		}

		if errorLogger, okErrorLogSig := errorLoggerFunc.(func(error)); okErrorLogSig {
			deadLetterOptions = append(deadLetterOptions, WithDLQErrorLogger[any, any](errorLogger))
		} else {
			return nil, fmt.Errorf("error logger executor '%s' is not a valid ErrorLogger", deadLetterQueueProps.ErrorLogger)
		}
	}

	if deadLetterQueueProps.Handler != "" {
		handlerFunc, okHandlerFunc := registry.GetExecutor(deadLetterQueueProps.Handler)
		if !okHandlerFunc {
			return nil, fmt.Errorf("handler executor '%s' not found in registry", deadLetterQueueProps.Handler)
		}

		if handler, okHandlerFuncSig := handlerFunc.(DLQHandler[any]); okHandlerFuncSig {
			deadLetterOptions = append(deadLetterOptions, WithDLQHandler[any, any](handler))
		} else {
			return nil, fmt.Errorf("handler executor '%s' is not a valid function for DeadLetterQueue stage", deadLetterQueueProps.Handler)
		}
	}

	if deadLetterQueueProps.ShouldDQLFunction != "" {
		shouldDLQFunc, okDQLFunc := registry.GetExecutor(deadLetterQueueProps.ShouldDQLFunction)
		if !okDQLFunc {
			return nil, fmt.Errorf(
				"should DLQ function '%s' not found in registry",
				deadLetterQueueProps.ShouldDQLFunction,
			)
		}

		if shouldDLQ, okSholdDQL := shouldDLQFunc.(func(error) bool); okSholdDQL {
			deadLetterOptions = append(deadLetterOptions, WithShouldDLQ[any, any](shouldDLQ))
		} else {
			return nil, fmt.Errorf("should DLQ function '%s' is not a valid function for DeadLetterQueue stage", deadLetterQueueProps.ShouldDQLFunction)
		}
	}

	var nested Stage[any, any]

	if nested, ok = nestedStage.(Stage[any, any]); !ok {
		return nil, errors.New("nested stage for dead letter queue is not a valid Stage[any, any]")
	}

	deadLetterQueueStage = NewDeadLetterQueue(nested, deadLetterOptions...)

	// Apply stage-level observability wrapping if enabled
	stage := wrapStageWithObservability(deadLetterQueueStage, stageConfig, buildContext)
	return stage, nil
}

func customBuilder(
	stageConfig *StageConfig,
	registry *Registry,
	_ func(*StageConfig) (interface{}, error),
	buildContext *BuildContext,
) (interface{}, error) {
	customProps, ok := stageConfig.Properties.(*CustomProperties)
	if !ok {
		return nil, fmt.Errorf(
			"incorrect properties type for 'custom': expected *CustomProperties, got %T",
			stageConfig.Properties,
		)
	}

	if customProps.Factory == "" {
		return nil, errors.New("custom stage properties must specify a factory name")
	}

	// The contract for a custom stage is that the user registers a factory function
	// in the executor registry. This factory takes the 'config' map and returns a stage.
	factoryFunc, ok := registry.GetExecutor(customProps.Factory)
	if !ok {
		return nil, fmt.Errorf("no custom stage factory registered for name '%s'", customProps.Factory)
	}

	// The registered executor must be a factory function that returns a stage.
	// We expect func(config map[string]any) (interface{}, error) to allow maximum flexibility.
	stageFactory, ok := factoryFunc.(func(config map[string]any) (interface{}, error))
	if !ok {
		return nil, fmt.Errorf(
			"registered factory for '%s' is not a valid custom stage factory. Expected func(map[string]any) (interface{}, error), got %T",
			customProps.Factory,
			factoryFunc,
		)
	}

	// Call the factory with the provided config map.
	stage, err := stageFactory(customProps.Config)
	if err != nil {
		return nil, err
	}

	// Apply stage-level observability wrapping if enabled
	wrappedStage := wrapStageWithObservability(stage, stageConfig, buildContext)
	return wrappedStage, nil
}
