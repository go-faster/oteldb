package xsync

import "sync"

// Pool is a typed wrapper around [sync.Pool].
type Pool[P *T, T any] struct {
	pool sync.Pool
}

func NewPool[P *T, T any](newFunc func() P) *Pool[P, T] {
	return &Pool[P, T]{
		pool: sync.Pool{
			New: func() any {
				return newFunc()
			},
		},
	}
}

// Put returns P to pool.
func (p *Pool[P, T]) Put(item P) {
	p.pool.Put(item)
}

// Get returns a P from pool.
func (p *Pool[P, T]) Get() P {
	return p.pool.Get().(P)
}

// GetReset gets a P from pool and resets it.
func GetReset[
	P interface {
		// Reset resets item state for re-use.
		Reset()
		*T
	},
	T any,
](p *Pool[P, T]) P {
	item := p.Get()
	item.Reset()
	return item
}
