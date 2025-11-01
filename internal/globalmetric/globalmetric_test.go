package globalmetric

import (
	"context"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestTracker(t *testing.T) {
	meterProvider := noop.NewMeterProvider()
	tracker, err := NewTracker(meterProvider)
	require.NoError(t, err)

	ctx := context.Background()
	ctx, track := tracker.Track(ctx,
		WithAttributes(
			attribute.String("hello", "world"),
		),
	)
	track.OnProfiles(ctx, []ch.ProfileEvent{
		{
			Name:  EventMemoryTrackerPeakUsage,
			Value: 10_000,
		},
		{
			Name:  EventMemoryTrackerUsage,
			Value: 5_000,
		},
		{
			Name:  EventOSCPUVirtualTimeMicroseconds,
			Value: 1_000_000,
		},
		{
			Name:  EventUserTimeMicroseconds,
			Value: 2_000_000,
		},
	})

	require.Equal(t, int64(10_000), track.memoryTrackerPeakUsage)
	require.Equal(t, int64(5_000), track.memoryTrackerUsage)
	require.Equal(t, int64(1_000_000), track.virtualTime)
	require.Equal(t, int64(2_000_000), track.userTime)

	require.Equal(t, int64(10_000), tracker.memoryTrackerPeakUsage())

	track.End()

	require.Equal(t, int64(0), tracker.memoryTrackerUsage())
}
