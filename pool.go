package fluxus

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PoolStats defines an interface for objects that provide pool statistics
type PoolStats interface {
	// Stats returns statistics about the pool usage
	Stats() map[string]int64

	// Name returns the name of the pool
	Name() string
}

// ObjectPool provides a generic object pool for frequent allocation reduction.
// This implementation includes statistics and optimizations for performance.
type ObjectPool[T any] struct {
	pool         sync.Pool
	name         string
	gets         int64 // Atomic counter for gets
	puts         int64 // Atomic counter for puts
	misses       int64 // Atomic counter for cache misses
	maxCapacity  int   // Optional max capacity to limit memory usage
	currentCount int64 // Current number of objects in the pool
}

// ObjectPoolOption is a function that configures an ObjectPool.
type ObjectPoolOption[T any] func(*ObjectPool[T])

// WithPoolName adds a name to the object pool for debugging and metrics.
func WithPoolName[T any](name string) ObjectPoolOption[T] {
	return func(p *ObjectPool[T]) {
		p.name = name
	}
}

// WithMaxCapacity sets a maximum capacity for the pool to limit memory usage.
func WithMaxCapacity[T any](maxCapacity int) ObjectPoolOption[T] {
	return func(p *ObjectPool[T]) {
		p.maxCapacity = maxCapacity
	}
}

// NewObjectPool creates a new optimized ObjectPool with the given factory function.
func NewObjectPool[T any](factory func() T, options ...ObjectPoolOption[T]) *ObjectPool[T] {
	pool := &ObjectPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return factory()
			},
		},
		name:        "generic_pool",
		maxCapacity: -1, // No limit by default
	}

	// Apply options
	for _, option := range options {
		option(pool)
	}

	return pool
}

// Get retrieves an object from the pool.
func (p *ObjectPool[T]) Get() T {
	atomic.AddInt64(&p.gets, 1)

	// Get an object from the pool
	objInterface := p.pool.Get() // Use a different name to avoid confusion

	// Perform the type assertion and check the result
	obj, ok := objInterface.(T)
	if !ok {
		// This shouldn't happen if the pool is used correctly (i.e., only T types are Put),
		// but handle it defensively. Record a miss and return the zero value for T.
		atomic.AddInt64(&p.misses, 1)
		// Note: currentCount is NOT decremented here because we didn't successfully "get"
		// an object of the correct type from the pool's perspective.
		var zero T
		return zero
	}

	// Decrement count only if we successfully got an object of the correct type
	// Note: This assumes currentCount tracks items *available* in the sync.Pool,
	// which might be slightly inaccurate as sync.Pool's internal count isn't exposed.
	// If currentCount is meant to track items *logically* belonging to the pool,
	// the decrement might happen earlier or be handled differently.
	// However, based on the Put logic, decrementing here seems intended.
	atomic.AddInt64(&p.currentCount, -1)

	return obj // Return the already asserted object
}

// Put returns an object to the pool if there's capacity available.
func (p *ObjectPool[T]) Put(obj T) {
	// Check if we should limit the pool size
	if p.maxCapacity > 0 {
		if count := atomic.LoadInt64(&p.currentCount); count >= int64(p.maxCapacity) {
			// Pool is at capacity, don't put the object back
			// It will be garbage collected
			return
		}
	}

	atomic.AddInt64(&p.puts, 1)
	atomic.AddInt64(&p.currentCount, 1)
	p.pool.Put(obj)
}

// Stats returns statistics about the pool usage.
func (p *ObjectPool[T]) Stats() map[string]int64 {
	gets := atomic.LoadInt64(&p.gets)

	// Avoid division by zero
	hitRatio := int64(0)
	if gets > 0 {
		hitRatio = int64(float64(gets-atomic.LoadInt64(&p.misses)) / float64(gets) * 100)
	}

	return map[string]int64{
		"gets":         gets,
		"puts":         atomic.LoadInt64(&p.puts),
		"misses":       atomic.LoadInt64(&p.misses),
		"current_size": atomic.LoadInt64(&p.currentCount),
		"hit_ratio":    hitRatio,
	}
}

// Name returns the name of the pool
func (p *ObjectPool[T]) Name() string {
	return p.name
}

// Ensure ObjectPool implements PoolStats
var _ PoolStats = (*ObjectPool[string])(nil)

// SlicePool is a specialized pool for slices with optimized handling.
type SlicePool[T any] struct {
	*ObjectPool[[]T]
	initialCapacity int
}

// NewSlicePool creates a new optimized SlicePool.
func NewSlicePool[T any](initialCapacity int, options ...ObjectPoolOption[[]T]) *SlicePool[T] {
	factory := func() []T {
		return make([]T, 0, initialCapacity)
	}

	return &SlicePool[T]{
		ObjectPool:      NewObjectPool(factory, options...),
		initialCapacity: initialCapacity,
	}
}

// Get retrieves a slice from the pool and ensures it has the right characteristics.
func (p *SlicePool[T]) Get() []T {
	// Get a slice from the base pool
	slice := p.ObjectPool.Get()

	// Reset length to 0 but preserve capacity
	if len(slice) > 0 {
		slice = slice[:0]
	}

	return slice
}

// GetWithCapacity gets a slice with at least the specified capacity.
func (p *SlicePool[T]) GetWithCapacity(minCapacity int) []T {
	slice := p.Get()

	// If we need more capacity, allocate a new slice
	if cap(slice) < minCapacity {
		// Don't put the too-small slice back - let GC handle it
		return make([]T, 0, minCapacity)
	}

	return slice
}

// PooledStage is a stage that uses object pooling for its internal operations.
type PooledStage[I, O any] struct {
	// The underlying stage
	stage Stage[I, O]
	// Pools this stage manages
	pools []PoolStats
}

// PooledStageOption is a function that configures a PooledStage.
type PooledStageOption[I, O any] func(*PooledStage[I, O])

// NewPooledStage creates a new pooled stage that wraps another stage.
func NewPooledStage[I, O any](stage Stage[I, O], options ...PooledStageOption[I, O]) *PooledStage[I, O] {
	ps := &PooledStage[I, O]{
		stage: stage,
		pools: make([]PoolStats, 0),
	}

	// Apply options
	for _, option := range options {
		option(ps)
	}

	return ps
}

// RegisterPool adds an object pool to be managed by this stage.
func (s *PooledStage[I, O]) RegisterPool(pool PoolStats) {
	s.pools = append(s.pools, pool)
}

// Process implements the Stage interface for PooledStage.
func (s *PooledStage[I, O]) Process(ctx context.Context, input I) (O, error) {
	// Process the input using the underlying stage
	output, err := s.stage.Process(ctx, input)
	return output, err
}

// GetStats returns statistics about all pools managed by this stage.
func (s *PooledStage[I, O]) GetStats() map[string]map[string]int64 {
	stats := make(map[string]map[string]int64)

	for _, pool := range s.pools {
		poolName := fmt.Sprintf("pool_%s", pool.Name())
		stats[poolName] = pool.Stats()
	}

	return stats
}

// PooledBuffer is an optimized buffer implementation using object pools.
type PooledBuffer[I, O any] struct {
	// Basic buffer configuration
	batchSize  int
	processor  func(ctx context.Context, batch []I) ([]O, error)
	errHandler func(error) error

	// Pools for internal use
	resultPool *SlicePool[O]

	// Stats and metrics
	name             string
	metricsCollector MetricsCollector
	processedBatches int64
	processedItems   int64
}

// PooledBufferOption is a function that configures a PooledBuffer.
type PooledBufferOption[I, O any] func(*PooledBuffer[I, O])

// WithBufferName adds a name to the pooled buffer.
func WithBufferName[I, O any](name string) PooledBufferOption[I, O] {
	return func(b *PooledBuffer[I, O]) {
		b.name = name
	}
}

// WithBufferMetricsCollector adds a metrics collector to the pooled buffer.
func WithBufferMetricsCollector[I, O any](collector MetricsCollector) PooledBufferOption[I, O] {
	return func(b *PooledBuffer[I, O]) {
		b.metricsCollector = collector
	}
}

// WithBufferErrorHandler adds a custom error handler to the pooled buffer.
func WithBufferErrorHandler[I, O any](handler func(error) error) PooledBufferOption[I, O] {
	return func(b *PooledBuffer[I, O]) {
		b.errHandler = handler
	}
}

// NewPooledBuffer creates a new pooled buffer with optimal settings.
func NewPooledBuffer[I, O any](
	batchSize int,
	processor func(ctx context.Context, batch []I) ([]O, error),
	options ...PooledBufferOption[I, O],
) *PooledBuffer[I, O] {
	// Ensure reasonable batch size
	if batchSize <= 0 {
		batchSize = runtime.NumCPU() * 10 // Default to CPU count * 10
	}

	pb := &PooledBuffer[I, O]{
		batchSize:        batchSize,
		processor:        processor,
		errHandler:       func(err error) error { return err },
		resultPool:       NewSlicePool[O](batchSize * 2), // Double for efficiency
		name:             "pooled_buffer",
		metricsCollector: DefaultMetricsCollector,
	}

	// Apply options
	for _, option := range options {
		option(pb)
	}

	return pb
}

// Name returns the name of the pooled buffer.
func (pb *PooledBuffer[I, O]) Name() string {
	return pb.name
}

// Stats returns statistics about the buffer usage.
func (pb *PooledBuffer[I, O]) Stats() map[string]int64 {
	stats := map[string]int64{
		"processed_batches": atomic.LoadInt64(&pb.processedBatches),
		"processed_items":   atomic.LoadInt64(&pb.processedItems),
	}

	// Add pool stats if available
	if poolStats := pb.resultPool.ObjectPool.Stats(); poolStats != nil {
		for k, v := range poolStats {
			stats["result_pool_"+k] = v
		}
	}

	return stats
}

// Process implements the Stage interface for PooledBuffer.
func (pb *PooledBuffer[I, O]) Process(ctx context.Context, inputs []I) ([]O, error) {
	// Track processing start time for metrics
	startTime := time.Now()

	// Handle empty input case
	inputLen := len(inputs)
	if inputLen == 0 {
		return []O{}, nil
	}

	// Calculate expected output capacity
	estimatedOutputSize := (inputLen + pb.batchSize - 1) / pb.batchSize * pb.batchSize

	// Get a results slice from the pool with the right capacity
	allResults := pb.resultPool.GetWithCapacity(estimatedOutputSize)

	// Process inputs in batches
	for i := 0; i < inputLen; i += pb.batchSize {
		// Check for context cancellation
		if ctx.Err() != nil {
			pb.resultPool.Put(allResults) // Return the slice to the pool
			return nil, pb.errHandler(ctx.Err())
		}

		// Get batch bounds
		end := i + pb.batchSize
		if end > inputLen {
			end = inputLen
		}

		// Get the current batch (just a slice of the original - no allocation)
		batch := inputs[i:end]

		// Track batch processing for metrics
		batchStartTime := time.Now()

		// Process the batch
		results, err := pb.processor(ctx, batch)

		if err != nil {
			// Return the results slice to the pool if we're not going to use it
			pb.resultPool.Put(allResults)
			return nil, pb.errHandler(fmt.Errorf("batch processing error at offset %d: %w", i, err))
		}

		// Record batch metrics
		if pb.metricsCollector != nil {
			pb.metricsCollector.BufferBatchProcessed(ctx, len(batch), time.Since(batchStartTime))
		}

		// Update stats
		atomic.AddInt64(&pb.processedBatches, 1)
		atomic.AddInt64(&pb.processedItems, int64(len(batch)))

		// Append batch results to our pooled results slice
		allResults = append(allResults, results...)
	}

	// Create a new slice for the final results - we can't return the pooled slice directly
	finalResults := make([]O, len(allResults))
	copy(finalResults, allResults)

	// Return the buffer to the pool
	pb.resultPool.Put(allResults)

	// Record overall metrics
	if pb.metricsCollector != nil {
		pb.metricsCollector.StageCompleted(ctx, pb.name, time.Since(startTime))
	}

	return finalResults, nil
}

// PreWarmPool creates and returns the specified number of objects to the pool.
// This can help reduce allocation overhead during high-load periods.
func PreWarmPool[T any](pool *ObjectPool[T], count int) {
	objects := make([]T, count)

	// Get objects from the pool (forcing creation)
	for i := 0; i < count; i++ {
		objects[i] = pool.Get()
	}

	// Return them to the pool
	for i := 0; i < count; i++ {
		pool.Put(objects[i])
	}
}

// PreWarmSlicePool creates and returns the specified number of slices to the slice pool.
func PreWarmSlicePool[T any](pool *SlicePool[T], count int) {
	slices := make([][]T, count)

	// Get slices from the pool (forcing creation)
	for i := 0; i < count; i++ {
		slices[i] = pool.Get()
	}

	// Return them to the pool
	for i := 0; i < count; i++ {
		pool.Put(slices[i])
	}
}
