package globalmetric

import (
	"context"
	"sync"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Tracker tracks global metrics from multiple tracks.
type Tracker struct {
	mu     sync.Mutex
	tracks []*Track

	virtualTime metric.Int64Counter
	userTime    metric.Int64Counter
}

func (t *Tracker) memoryTrackerUsage() int64 {
	var total int64
	for _, c := range t.tracks {
		c.mu.Lock()
		total += c.memoryTrackerUsage
		c.mu.Unlock()
	}

	return total
}

func (t *Tracker) memoryTrackerPeakUsage() int64 {
	var total int64
	for _, c := range t.tracks {
		c.mu.Lock()
		total += c.memoryTrackerPeakUsage
		c.mu.Unlock()
	}

	return total
}

func (t *Tracker) remove(ctx *Track) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, c := range t.tracks {
		if c == ctx {
			t.tracks = append(t.tracks[:i], t.tracks[i+1:]...)
			return
		}
	}
}

// TrackOption configures a [Track].
type TrackOption func(t *Track)

// WithAttributes sets attributes for the track.
func WithAttributes(attrs ...attribute.KeyValue) TrackOption {
	return func(t *Track) {
		t.attributes = append(t.attributes, attrs...)
	}
}

// Track creates a new [Track] with given options.
func (t *Tracker) Track(ctx context.Context, options ...TrackOption) (context.Context, *Track) {
	track := &Track{
		tracker: t,
	}
	for _, o := range options {
		o(track)
	}

	t.tracks = append(t.tracks, track)

	return ctx, track
}

// Track represents a tracked ClickHouse query or series of queries.
type Track struct {
	attributes []attribute.KeyValue
	tracker    *Tracker

	mu                     sync.Mutex
	memoryTrackerPeakUsage int64
	memoryTrackerUsage     int64
	virtualTime            int64
	userTime               int64
}

func (c *Track) increaseVirtualTime(ctx context.Context, delta int64) {
	c.virtualTime += delta
	c.tracker.virtualTime.Add(ctx, delta, metric.WithAttributes(c.attributes...))
}

func (c *Track) increaseUserTime(ctx context.Context, value int64) {
	c.userTime += value
	c.tracker.userTime.Add(ctx, value, metric.WithAttributes(c.attributes...))
}

const (
	EventMemoryTrackerUsage           = "MemoryTrackerUsage"
	EventMemoryTrackerPeakUsage       = "MemoryTrackerPeakUsage"
	EventOSCPUVirtualTimeMicroseconds = "OSCPUVirtualTimeMicroseconds"
	EventUserTimeMicroseconds         = "UserTimeMicroseconds"
)

func (c *Track) OnProfiles(ctx context.Context, events []ch.ProfileEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, e := range events {
		switch e.Name {
		case EventMemoryTrackerUsage:
			c.memoryTrackerUsage = e.Value
		case EventMemoryTrackerPeakUsage:
			c.memoryTrackerPeakUsage = e.Value
		case EventOSCPUVirtualTimeMicroseconds:
			c.increaseVirtualTime(ctx, e.Value)
		case EventUserTimeMicroseconds:
			c.increaseUserTime(ctx, e.Value)
		default:
			continue
		}
	}
}

func (c *Track) End() {
	c.tracker.remove(c)
}

func NewTracker(meterProvider metric.MeterProvider) (*Tracker, error) {
	meter := meterProvider.Meter("github.com/oteldb/oteldb/internal/globalmetric",
		metric.WithSchemaURL("https://schema.oteldb.tech/metrics/v1"),
		metric.WithInstrumentationVersion("0.1.0"),
	)
	virtualTime, err := meter.Int64Counter("clickhouse.cpu.virtual_time_microseconds",
		metric.WithDescription("Total virtual CPU time used by ClickHouse in microseconds"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "virtual time counter")
	}
	userTime, err := meter.Int64Counter("clickhouse.cpu.user_time_microseconds",
		metric.WithDescription("Total user CPU time used by ClickHouse in microseconds"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "user time counter")
	}
	memoryTrackerUsage, err := meter.Int64ObservableGauge("clickhouse.memory_tracker.usage",
		metric.WithDescription("Current memory tracker usage in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "memory tracker usage gauge")
	}
	memoryTrackerPeakUsage, err := meter.Int64ObservableGauge("clickhouse.memory_tracker.peak_usage",
		metric.WithDescription("Peak memory tracker usage in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "memory tracker peak usage gauge")
	}

	tracker := &Tracker{
		virtualTime: virtualTime,
		userTime:    userTime,
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		observer.ObserveInt64(memoryTrackerUsage, tracker.memoryTrackerUsage())
		observer.ObserveInt64(memoryTrackerPeakUsage, tracker.memoryTrackerPeakUsage())
		return nil
	},
		memoryTrackerUsage,
		memoryTrackerPeakUsage,
	)
	if err != nil {
		return nil, errors.Wrap(err, "register memory tracker callback")
	}

	return tracker, nil
}
