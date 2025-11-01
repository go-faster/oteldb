package globalmetric

import (
	"context"
	"sync"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// tracker tracks global metrics from multiple tracks.
type tracker struct {
	mu     sync.Mutex
	tracks []*track

	virtualTime metric.Int64Counter
	userTime    metric.Int64Counter
}

func (t *tracker) memoryTrackerUsage() int64 {
	var total int64
	for _, c := range t.tracks {
		c.mu.Lock()
		total += c.memoryTrackerUsage
		c.mu.Unlock()
	}

	return total
}

func (t *tracker) memoryTrackerPeakUsage() int64 {
	var total int64
	for _, c := range t.tracks {
		c.mu.Lock()
		total += c.memoryTrackerPeakUsage
		c.mu.Unlock()
	}

	return total
}

func (t *tracker) remove(ctx *track) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, c := range t.tracks {
		if c == ctx {
			t.tracks = append(t.tracks[:i], t.tracks[i+1:]...)
			return
		}
	}
}

// TrackOption configures a [track].
type TrackOption func(t *track)

// WithAttributes sets attributes for the track.
func WithAttributes(attrs ...attribute.KeyValue) TrackOption {
	return func(t *track) {
		t.attributes = append(t.attributes, attrs...)
	}
}

// Track creates a new [track] with given options.
func (t *tracker) Track(ctx context.Context, options ...TrackOption) (context.Context, Track) {
	trk := &track{
		tracker: t,
	}
	for _, o := range options {
		o(trk)
	}

	t.tracks = append(t.tracks, trk)

	return ctx, trk
}

// track represents a tracked ClickHouse query or series of queries.
type track struct {
	attributes []attribute.KeyValue
	tracker    *tracker

	mu                     sync.Mutex
	memoryTrackerPeakUsage int64
	memoryTrackerUsage     int64
	virtualTime            int64
	userTime               int64
}

func (t *track) increaseVirtualTime(ctx context.Context, delta int64) {
	t.virtualTime += delta
	t.tracker.virtualTime.Add(ctx, delta, metric.WithAttributes(t.attributes...))
}

func (t *track) increaseUserTime(ctx context.Context, value int64) {
	t.userTime += value
	t.tracker.userTime.Add(ctx, value, metric.WithAttributes(t.attributes...))
}

const (
	EventMemoryTrackerUsage           = "MemoryTrackerUsage"
	EventMemoryTrackerPeakUsage       = "MemoryTrackerPeakUsage"
	EventOSCPUVirtualTimeMicroseconds = "OSCPUVirtualTimeMicroseconds"
	EventUserTimeMicroseconds         = "UserTimeMicroseconds"
)

func (t *track) OnProfiles(ctx context.Context, events []ch.ProfileEvent) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, e := range events {
		switch e.Name {
		case EventMemoryTrackerUsage:
			t.memoryTrackerUsage = e.Value
		case EventMemoryTrackerPeakUsage:
			t.memoryTrackerPeakUsage = e.Value
		case EventOSCPUVirtualTimeMicroseconds:
			t.increaseVirtualTime(ctx, e.Value)
		case EventUserTimeMicroseconds:
			t.increaseUserTime(ctx, e.Value)
		default:
			continue
		}
	}

	return nil
}

func (t *track) End() {
	t.tracker.remove(t)
}

type Track interface {
	End()
	OnProfiles(ctx context.Context, events []ch.ProfileEvent) error
}

type Tracker interface {
	Track(ctx context.Context, opts ...TrackOption) (context.Context, Track)
}

func NewTracker(meterProvider metric.MeterProvider) (Tracker, error) {
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

	t := &tracker{
		virtualTime: virtualTime,
		userTime:    userTime,
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		observer.ObserveInt64(memoryTrackerUsage, t.memoryTrackerUsage())
		observer.ObserveInt64(memoryTrackerPeakUsage, t.memoryTrackerPeakUsage())
		return nil
	},
		memoryTrackerUsage,
		memoryTrackerPeakUsage,
	)
	if err != nil {
		return nil, errors.Wrap(err, "register memory tracker callback")
	}

	return t, nil
}
