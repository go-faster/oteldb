package xsync

import "sync"

// KeyOnce return function that calls make only once per key.
func KeyOnce[K comparable, V any](do func(K) V) func(K) V {
	state := keyOnceState[K, V]{
		onces: map[K]V{},
		do:    do,
	}
	return state.Do
}

// keyOnceState is map that does call only once per key.
type keyOnceState[K comparable, V any] struct {
	onces    map[K]V
	oncesMux sync.Mutex

	do func(K) V
}

// Do calls [KeyOnce.Make] once per key and returns value.
func (k *keyOnceState[K, V]) Do(key K) V {
	k.oncesMux.Lock()
	defer k.oncesMux.Unlock()

	val, ok := k.onces[key]
	if !ok {
		val = k.do(key)
		k.onces[key] = val
	}
	return val
}
