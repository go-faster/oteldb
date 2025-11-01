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
	tr, err := NewTracker(meterProvider)
	require.NoError(t, err)

	ctx := context.Background()
	ctx, trk := tr.Track(ctx,
		WithAttributes(
			attribute.String("hello", "world"),
		),
	)
	trk.OnProfiles(ctx, []ch.ProfileEvent{
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

	tkrPrivate := trk.(*track)
	require.Equal(t, int64(10_000), tkrPrivate.memoryTrackerPeakUsage)
	require.Equal(t, int64(5_000), tkrPrivate.memoryTrackerUsage)
	require.Equal(t, int64(1_000_000), tkrPrivate.virtualTime)
	require.Equal(t, int64(2_000_000), tkrPrivate.userTime)

	trPrivate := tr.(*tracker)
	require.Equal(t, int64(10_000), trPrivate.memoryTrackerPeakUsage())

	trk.End()

	require.Equal(t, int64(0), trPrivate.memoryTrackerUsage())
}
