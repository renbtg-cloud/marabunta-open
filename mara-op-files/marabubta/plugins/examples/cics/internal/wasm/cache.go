// Marabunta - Licensed under the MIT License.
package wasm

import (
	"sync"

	"github.com/tetratelabs/wazero"
)

// ModuleCache provides a thread-safe cache of compiled WASM modules using
// double-checked locking for safe concurrent access (spec C.1).
type ModuleCache struct {
	mu      sync.RWMutex
	modules map[string]wazero.CompiledModule
}

// NewModuleCache creates a new empty module cache.
func NewModuleCache() *ModuleCache {
	return &ModuleCache{
		modules: make(map[string]wazero.CompiledModule),
	}
}

// Get retrieves a compiled module from the cache. Returns nil if not found.
// Uses a read lock for fast concurrent access.
func (mc *ModuleCache) Get(name string) wazero.CompiledModule {
	mc.mu.RLock()
	mod := mc.modules[name]
	mc.mu.RUnlock()
	return mod
}

// Put stores a compiled module in the cache. Uses double-checked locking:
// first checks with read lock, then upgrades to write lock if needed.
func (mc *ModuleCache) Put(name string, mod wazero.CompiledModule) {
	// First check: read lock.
	mc.mu.RLock()
	if _, ok := mc.modules[name]; ok {
		mc.mu.RUnlock()
		return // Already cached.
	}
	mc.mu.RUnlock()

	// Second check: write lock (double-checked locking).
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, ok := mc.modules[name]; ok {
		return // Another goroutine cached it between checks.
	}
	mc.modules[name] = mod
}

// GetOrCompile retrieves a cached module or compiles it if not present.
// The compile function is only called if the module is not in the cache.
// This provides atomic get-or-create semantics with double-checked locking.
func (mc *ModuleCache) GetOrCompile(name string, compileFn func() (wazero.CompiledModule, error)) (wazero.CompiledModule, error) {
	// First check: read lock.
	mc.mu.RLock()
	if mod, ok := mc.modules[name]; ok {
		mc.mu.RUnlock()
		return mod, nil
	}
	mc.mu.RUnlock()

	// Second check: write lock.
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mod, ok := mc.modules[name]; ok {
		return mod, nil // Another goroutine compiled it.
	}

	// Compile the module.
	mod, err := compileFn()
	if err != nil {
		return nil, err
	}

	mc.modules[name] = mod
	return mod, nil
}

// Remove removes a module from the cache.
func (mc *ModuleCache) Remove(name string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.modules, name)
}

// Clear removes all modules from the cache.
func (mc *ModuleCache) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.modules = make(map[string]wazero.CompiledModule)
}

// Size returns the number of cached modules.
func (mc *ModuleCache) Size() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.modules)
}

// Names returns the names of all cached modules.
func (mc *ModuleCache) Names() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	names := make([]string, 0, len(mc.modules))
	for name := range mc.modules {
		names = append(names, name)
	}
	return names
}
