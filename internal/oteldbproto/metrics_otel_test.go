package oteldbproto

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var metricOTELProtoTests = []struct {
	input   func(require.TestingT) pmetric.Metrics
	want    Metrics
	wantErr bool
}{
	{
		func(require.TestingT) pmetric.Metrics {
			m := pmetric.NewMetrics()

			resMetrics := m.ResourceMetrics().AppendEmpty()
			resMetrics.SetSchemaUrl("https://example.com")
			res := resMetrics.Resource()
			res.Attributes().PutStr("resource.attr", "amongus")
			res.SetDroppedAttributesCount(1)

			scopeMetrics := resMetrics.ScopeMetrics().AppendEmpty()
			scopeMetrics.SetSchemaUrl("https://example.com")
			scope := scopeMetrics.Scope()
			scope.Attributes().PutStr("scope.attr", "foo")
			scope.SetName("name")
			scope.SetVersion("version")
			scope.SetDroppedAttributesCount(1)

			{
				gaugeMetric := scopeMetrics.Metrics().AppendEmpty()
				gaugeMetric.SetName("name")
				gaugeMetric.SetDescription("description")
				gaugeMetric.SetUnit("unit")

				gauge := gaugeMetric.SetEmptyGauge()
				point := gauge.DataPoints().AppendEmpty()
				a := point.Attributes()
				a.PutStr("metric.str", "bar")
				a.PutInt("metric.int", 1)
				a.PutDouble("metric.double", 3.14)
				a.PutBool("metric.bool", true)
				a.PutEmptyMap("metric.map").PutStr("metric.map.str", "inside map")
				slice := a.PutEmptySlice("metric.slice")
				slice.AppendEmpty().SetStr("inside slice 1")
				slice.AppendEmpty().SetStr("inside slice 2")
				a.PutEmptyBytes("metric.bytes").Append('a', 'b', 'c')

				point.SetStartTimestamp(1)
				point.SetTimestamp(2)
				point.SetDoubleValue(3.14)
				point.SetFlags(pmetric.DefaultDataPointFlags)
			}
			{
				sumMetric := scopeMetrics.Metrics().AppendEmpty()
				sumMetric.SetName("name")
				sumMetric.SetDescription("description")
				sumMetric.SetUnit("unit")

				sum := sumMetric.SetEmptySum()
				point := sum.DataPoints().AppendEmpty()
				a := point.Attributes()
				a.PutStr("metric.str", "baz")
				slice := a.PutEmptySlice("metric.slice")
				slice.AppendEmpty().SetStr("inside slice 3")
				slice.AppendEmpty().SetStr("inside slice 4")

				point.SetStartTimestamp(3)
				point.SetTimestamp(4)
				point.SetIntValue(5)
				point.SetFlags(pmetric.DefaultDataPointFlags)
			}

			return m
		},
		Metrics{
			Number: NumberPoints{
				MetricHeader: MetricHeader{
					Resource: []int{0, 0},
					Scope:    []int{0, 0},
					Name:     []string{"name", "name"},
					Unit:     []string{"unit", "unit"},
				},
				Attributes: []Attributes{
					{
						Keys: []string{"metric.str", "metric.int", "metric.double", "metric.bool", "metric.map", "metric.slice", "metric.bytes"},
						Values: []Value{
							{kind: 0x1, strval: "bar"},
							{kind: 0x2, val: 0x1},
							{kind: 0x3, val: 0x40091eb851eb851f},
							{kind: 0x4, val: 0x1},
							{kind: 0x5, val: 0x0},
							{kind: 0x6, val: 0x0},
							{kind: 0x7, strval: "abc"},
						},
					}, {
						Keys: []string{"metric.str", "metric.slice"},
						Values: []Value{
							{kind: 0x1, strval: "baz"},
							{kind: 0x6, val: 0x1},
						},
					},
				},
				StartTimeUnixNano: []uint64{0x1, 0x3},
				TimeUnixNano:      []uint64{0x2, 0x4},
				Value: []Value{
					{kind: 0x3, val: 0x40091eb851eb851f},
					{kind: 0x2, val: 0x5},
				},
				Exemplar: [][]Exemplar{
					[]Exemplar(nil),
					[]Exemplar(nil),
				},
				Flags: []pmetric.DataPointFlags{0x0, 0x0},
			},
			Resources: []Resource{
				{
					SchemaURL: "https://example.com",
					Attributes: Attributes{
						Keys: []string{"resource.attr"},
						Values: []Value{
							{kind: 0x1, val: 0x0, strval: "amongus"},
						},
					},
					DroppedAttributesCount: 0x1,
				},
			},
			Scopes: []Scope{
				{
					SchemaURL: "https://example.com",
					Name:      "name",
					Version:   "version",
					Attributes: Attributes{
						Keys: []string{"scope.attr"},
						Values: []Value{
							{kind: 0x1, strval: "foo"},
						},
					},
					DroppedAttributesCount: 0x1,
				},
			},
			Arrays: [][]Value{
				{
					{kind: 0x1, strval: "inside slice 1"},
					{kind: 0x1, strval: "inside slice 2"},
				},
				{
					{kind: 0x1, strval: "inside slice 3"},
					{kind: 0x1, strval: "inside slice 4"},
				},
			},
			Objects: [][]KeyValue{
				{
					{Name: "metric.map.str", Value: Value{kind: 0x1, strval: "inside map"}},
				},
			},
		},
		false,
	},
}

func TestMetricsOpenTelemetryUnmarshal(t *testing.T) {
	for i, tt := range metricOTELProtoTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var p pmetric.ProtoMarshaler
			data, err := p.MarshalMetrics(tt.input(t))
			require.NoError(t, err)

			var got MetricsOpenTelemetry
			err = got.Unmarshal(data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got.Metrics)
		})
	}
}
