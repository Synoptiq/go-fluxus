package fluxus

import (
	"sync"
)

// StatefulStageHelper provides common utilities for building stateful stages.
// It can be embedded into custom stage structs to provide:
//   - Thread-safe access to an internal key-value store for dynamic state.
//   - A mechanism for one-time initialization with error handling.
//   - Basic mutex locks for any additional custom state managed by the embedding struct.
type StatefulStageHelper struct {
	mu         sync.RWMutex
	setupOnce  sync.Once // Used by DoOnceWithError
	setupError error
	state      map[string]any
}

// NewStatefulStageHelper creates a new, initialized StatefulStageHelper.
// Using this constructor is recommended for clarity, though the helper's methods
// are designed to work safely if it's embedded as a zero value.
func NewStatefulStageHelper() *StatefulStageHelper {
	return &StatefulStageHelper{
		state: make(map[string]any),
	}
}

// Lock acquires a write lock. Callers should ensure Unlock is eventually called,
// typically via `defer s.Unlock()`.
func (s *StatefulStageHelper) Lock() {
	s.mu.Lock()
}

// Unlock releases a write lock.
func (s *StatefulStageHelper) Unlock() {
	s.mu.Unlock()
}

// RLock acquires a read lock. Callers should ensure RUnlock is eventually called,
// typically via `defer s.RUnlock()`.
func (s *StatefulStageHelper) RLock() {
	s.mu.RLock()
}

// RUnlock releases a read lock.
func (s *StatefulStageHelper) RUnlock() {
	s.mu.RUnlock()
}

// DoOnceWithError executes the provided function f exactly once for this
// StatefulStageHelper instance. This is typically used within an Initializer's
// Setup method to perform one-time initialization that might fail.
// If f returns an error during its first execution, that error is stored and
// returned by this and all subsequent calls to DoOnceWithError.
// If f succeeds (returns nil), this and subsequent calls will return nil
// without re-executing f.
func (s *StatefulStageHelper) DoOnceWithError(f func() error) error {
	s.setupOnce.Do(func() {
		// Execute the provided function f() outside of the main s.mu lock
		// to avoid holding s.mu for the potentially long duration of f().
		errResult := f()

		// After f() has completed, acquire the lock to safely update s.setupError.
		s.mu.Lock()
		s.setupError = errResult
		s.mu.Unlock()
	})

	// Consistently read s.setupError under a read lock.
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.setupError // This value is now guaranteed to be the one set by the once.Do block
}

// GetSetupError returns the error captured during the first call to DoOnceWithError, if any.
// Returns nil if DoOnceWithError has not been called, or if it was called and succeeded.
func (s *StatefulStageHelper) GetSetupError() error {
	s.mu.RLock() // s.setupError is now consistently protected by s.mu for reads and writes.
	defer s.mu.RUnlock()
	return s.setupError
}

// SetState stores a value associated with a key in the helper's internal
// thread-safe state map.
func (s *StatefulStageHelper) SetState(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil { // Ensure map is initialized if helper is used as zero-value embedded struct
		s.state = make(map[string]any)
	}
	s.state[key] = value
}

// GetState retrieves a value associated with a key from the helper's internal
// thread-safe state map.
// It returns the value and true if the key exists, otherwise nil and false.
func (s *StatefulStageHelper) GetState(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == nil {
		return nil, false
	}
	val, ok := s.state[key]
	return val, ok
}

// DeleteState removes a key and its associated value from the helper's internal
// thread-safe state map.
func (s *StatefulStageHelper) DeleteState(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return
	}
	delete(s.state, key)
}

// ClearAllState removes all key-value pairs from the helper's internal state map.
// This is useful for stages implementing the Resettable interface.
// Note: This does not reset the DoOnceWithError state (setupError).
func (s *StatefulStageHelper) ClearAllState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Ensures s.state is non-nil afterwards, even if it was nil before.
	s.state = make(map[string]any)
}

// GetAllState returns a shallow copy of the internal state map.
// This allows for iterating over all stored state in a thread-safe manner
// without holding a lock for the duration of the iteration.
func (s *StatefulStageHelper) GetAllState() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == nil {
		return make(map[string]any) // Return empty map, not nil
	}
	copiedState := make(map[string]any, len(s.state))
	for k, v := range s.state {
		copiedState[k] = v
	}
	return copiedState
}
