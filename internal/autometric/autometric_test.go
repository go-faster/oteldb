// Package autometric contains a simple reflect-based OpenTelemetry metric initializer.
package autometric

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestInit(t *testing.T) {
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test-meter")

	var test struct {
		// Ignored fields.
		_ int
		_ metric.Int64Counter
		// Embedded fields.
		fmt.Stringer
		// Private fields.
		private        int
		privateCounter metric.Int64Counter
		// Skip.
		SkipMe  metric.Int64Counter           `autometric:"-"`
		SkipMe2 metric.Int64ObservableCounter `autometric:"-"`

		Int64Counter         metric.Int64Counter
		Int64UpDownCounter   metric.Int64UpDownCounter
		Int64Histogram       metric.Int64Histogram
		Int64Gauge           metric.Int64Gauge
		Float64Counter       metric.Float64Counter
		Float64UpDownCounter metric.Float64UpDownCounter
		Float64Histogram     metric.Float64Histogram
		Float64Gauge         metric.Float64Gauge

		Renamed    metric.Int64Counter     `name:"mega_counter"`
		WithDesc   metric.Int64Counter     `name:"with_desc" description:"foo"`
		WithUnit   metric.Int64Counter     `name:"with_unit" unit:"By"`
		WithBounds metric.Float64Histogram `name:"with_bounds" boundaries:"1,2,5"`
	}
	const prefix = "testmetrics.points."
	require.NoError(t, Init(meter, &test, InitOptions{
		Prefix: prefix,
	}))

	require.Nil(t, test.privateCounter)

	require.NotNil(t, test.Int64Counter)
	test.Int64Counter.Add(ctx, 1)
	require.NotNil(t, test.Int64UpDownCounter)
	test.Int64UpDownCounter.Add(ctx, 1)
	require.NotNil(t, test.Int64Histogram)
	test.Int64Histogram.Record(ctx, 1)
	require.NotNil(t, test.Int64Gauge)
	test.Int64Gauge.Record(ctx, 1)
	require.NotNil(t, test.Float64Counter)
	test.Float64Counter.Add(ctx, 1)
	require.NotNil(t, test.Float64UpDownCounter)
	test.Float64UpDownCounter.Add(ctx, 1)
	require.NotNil(t, test.Float64Histogram)
	test.Float64Histogram.Record(ctx, 1)
	require.NotNil(t, test.Float64Gauge)
	test.Float64Gauge.Record(ctx, 1)

	require.NotNil(t, test.Renamed)
	test.Renamed.Add(ctx, 1)
	require.NotNil(t, test.WithDesc)
	test.WithDesc.Add(ctx, 1)
	require.NotNil(t, test.WithUnit)
	test.WithUnit.Add(ctx, 1)
	require.NotNil(t, test.WithBounds)
	test.WithBounds.Record(ctx, 1)

	require.NoError(t, mp.ForceFlush(ctx))
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &data))

	type MetricInfo struct {
		Name        string
		Description string
		Unit        string
	}
	var infos []MetricInfo
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			infos = append(infos, MetricInfo{
				Name:        metric.Name,
				Description: metric.Description,
				Unit:        metric.Unit,
			})
		}
	}
	require.Equal(t,
		[]MetricInfo{
			{Name: prefix + "int64_counter"},
			{Name: prefix + "int64_up_down_counter"},
			{Name: prefix + "int64_histogram"},
			{Name: prefix + "int64_gauge"},
			{Name: prefix + "float64_counter"},
			{Name: prefix + "float64_up_down_counter"},
			{Name: prefix + "float64_histogram"},
			{Name: prefix + "float64_gauge"},

			{Name: prefix + "mega_counter"},
			{Name: prefix + "with_desc", Description: "foo"},
			{Name: prefix + "with_unit", Unit: "By"},
			{Name: prefix + "with_bounds"},
		},
		infos,
	)
}

func TestInitErrors(t *testing.T) {
	type (
		JustStruct struct{}

		UnexpectedType struct {
			Foo metric.Observable
		}
		UnsupportedInt64Observable struct {
			Observable metric.Int64ObservableCounter
		}
		UnsupportedFloat64Observable struct {
			Observable metric.Float64ObservableCounter
		}

		BoundariesOnNonHistogram struct {
			C metric.Int64Counter `boundaries:"foo"`
		}

		BadBoundaries struct {
			H metric.Float64Histogram `boundaries:"foo"`
		}
		BadBoundaries2 struct {
			H metric.Float64Histogram `boundaries:"foo,"`
		}
	)

	for i, tt := range []struct {
		s   any
		err string
	}{
		{0, "a pointer-to-struct expected, got int"},
		{JustStruct{}, "a pointer-to-struct expected, got autometric.JustStruct"},

		{&UnexpectedType{}, "field (autometric.UnexpectedType).Foo: unexpected type metric.Observable"},
		{&UnsupportedInt64Observable{}, "field (autometric.UnsupportedInt64Observable).Observable: observables are not supported"},
		{&UnsupportedFloat64Observable{}, "field (autometric.UnsupportedFloat64Observable).Observable: observables are not supported"},

		{&BoundariesOnNonHistogram{}, `field (autometric.BoundariesOnNonHistogram).C: boundaries tag should be used only on histogram metrics: got metric.Int64Counter`},
		{&BadBoundaries{}, `field (autometric.BadBoundaries).H: parse boundaries: strconv.ParseFloat: parsing "foo": invalid syntax`},
		{&BadBoundaries2{}, `field (autometric.BadBoundaries2).H: parse boundaries: strconv.ParseFloat: parsing "foo": invalid syntax`},
	} {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			mp := sdkmetric.NewMeterProvider()
			meter := mp.Meter("test-meter")
			require.EqualError(t, Init(meter, tt.s, InitOptions{}), tt.err)
		})
	}
}
