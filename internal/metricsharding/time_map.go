package metricsharding

import (
	"time"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// TimeMap stores attribute blocks by time.
type TimeMap[K comparable, V any] struct {
	Delta  time.Duration
	shards map[int64]timeShard[K, V]
}

// Add adds a value to the block.
func (m *TimeMap[K, V]) Add(t time.Time, key K, vals V) {
	if m.Delta == 0 {
		m.Delta = time.Hour
	}
	if m.shards == nil {
		m.shards = map[int64]timeShard[K, V]{}
	}
	at := t.UTC().Truncate(m.Delta)

	shardKey := at.UnixNano()
	shard, ok := m.shards[shardKey]
	if !ok {
		shard = timeShard[K, V]{
			at: at,
		}
	}
	shard.Add(key, vals)
	m.shards[shardKey] = shard
}

// Each iterates over each shard.
func (m *TimeMap[K, V]) Each(cb func(time.Time, []V)) {
	for _, shard := range m.shards {
		cb(shard.at, shard.values)
	}
}

type timeShard[K comparable, V any] struct {
	at     time.Time
	dedup  map[K]struct{}
	values []V
}

func (s *timeShard[K, V]) Add(key K, val V) {
	if _, ok := s.dedup[key]; ok {
		return
	}

	if s.dedup == nil {
		s.dedup = map[K]struct{}{}
	}
	s.dedup[key] = struct{}{}
	s.values = append(s.values, val)
}

// AttributesKey is a key for [TimeMap].
type AttributesKey struct {
	Name string
	Hash otelstorage.Hash
}
