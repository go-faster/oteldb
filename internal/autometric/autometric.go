// Package autometric contains a simple reflect-based OpenTelemetry metric initializer.
package autometric

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/metric"
)

var (
	int64CounterType                 = reflect.TypeOf(new(metric.Int64Counter)).Elem()
	int64UpDownCounterType           = reflect.TypeOf(new(metric.Int64UpDownCounter)).Elem()
	int64HistogramType               = reflect.TypeOf(new(metric.Int64Histogram)).Elem()
	int64GaugeType                   = reflect.TypeOf(new(metric.Int64Gauge)).Elem()
	int64ObservableCounterType       = reflect.TypeOf(new(metric.Int64ObservableCounter)).Elem()
	int64ObservableUpDownCounterType = reflect.TypeOf(new(metric.Int64ObservableUpDownCounter)).Elem()
	int64ObservableGaugeType         = reflect.TypeOf(new(metric.Int64ObservableGauge)).Elem()
)

var (
	float64CounterType                 = reflect.TypeOf(new(metric.Float64Counter)).Elem()
	float64UpDownCounterType           = reflect.TypeOf(new(metric.Float64UpDownCounter)).Elem()
	float64HistogramType               = reflect.TypeOf(new(metric.Float64Histogram)).Elem()
	float64GaugeType                   = reflect.TypeOf(new(metric.Float64Gauge)).Elem()
	float64ObservableCounterType       = reflect.TypeOf(new(metric.Float64ObservableCounter)).Elem()
	float64ObservableUpDownCounterType = reflect.TypeOf(new(metric.Float64ObservableUpDownCounter)).Elem()
	float64ObservableGaugeType         = reflect.TypeOf(new(metric.Float64ObservableGauge)).Elem()
)

// InitOptions defines options for [Init].
type InitOptions struct {
	// Prefix defines common prefix for all metrics.
	Prefix string
	// FieldName returns name for given field.
	FieldName func(prefix string, sf reflect.StructField) string
}

func (opts *InitOptions) setDefaults() {
	if opts.FieldName == nil {
		opts.FieldName = fieldName
	}
}

func fieldName(prefix string, sf reflect.StructField) string {
	name := snakeCase(sf.Name)
	if tag, ok := sf.Tag.Lookup("name"); ok {
		name = tag
	}
	return prefix + name
}

// Init initialize metrics in given struct s using given meter.
func Init(m metric.Meter, s any, opts InitOptions) error {
	opts.setDefaults()

	ptr := reflect.ValueOf(s)
	if !isValidPtrStruct(ptr) {
		return errors.Errorf("a pointer-to-struct expected, got %T", s)
	}

	var (
		struct_    = ptr.Elem()
		structType = struct_.Type()
	)
	for i := 0; i < struct_.NumField(); i++ {
		fieldType := structType.Field(i)
		if fieldType.Anonymous || !fieldType.IsExported() {
			continue
		}
		if n, ok := fieldType.Tag.Lookup("autometric"); ok && n == "-" {
			continue
		}

		field := struct_.Field(i)
		if !field.CanSet() {
			continue
		}

		mt, err := makeField(m, fieldType, opts)
		if err != nil {
			return errors.Wrapf(err, "field (%s).%s", structType, fieldType.Name)
		}
		field.Set(reflect.ValueOf(mt))
	}

	return nil
}

func makeField(m metric.Meter, sf reflect.StructField, opts InitOptions) (any, error) {
	var (
		name       = opts.FieldName(opts.Prefix, sf)
		unit       = sf.Tag.Get("unit")
		desc       = sf.Tag.Get("description")
		boundaries []float64
	)
	if b, ok := sf.Tag.Lookup("boundaries"); ok {
		switch ftyp := sf.Type; ftyp {
		case int64HistogramType, float64HistogramType:
		default:
			return nil, errors.Errorf("boundaries tag should be used only on histogram metrics: got %v", ftyp)
		}
		for _, val := range strings.Split(b, ",") {
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, errors.Wrap(err, "parse boundaries")
			}
			boundaries = append(boundaries, f)
		}
	}

	switch ftyp := sf.Type; ftyp {
	case int64CounterType:
		return m.Int64Counter(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
		)
	case int64UpDownCounterType:
		return m.Int64UpDownCounter(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
		)
	case int64HistogramType:
		return m.Int64Histogram(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(boundaries...),
		)
	case int64GaugeType:
		return m.Int64Gauge(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
		)
	case int64ObservableCounterType,
		int64ObservableUpDownCounterType,
		int64ObservableGaugeType:
		return nil, errors.New("observables are not supported")

	case float64CounterType:
		return m.Float64Counter(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
		)
	case float64UpDownCounterType:
		return m.Float64UpDownCounter(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
		)
	case float64HistogramType:
		return m.Float64Histogram(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(boundaries...),
		)
	case float64GaugeType:
		return m.Float64Gauge(name,
			metric.WithUnit(unit),
			metric.WithDescription(desc),
		)
	case float64ObservableCounterType,
		float64ObservableUpDownCounterType,
		float64ObservableGaugeType:
		return nil, errors.New("observables are not supported")
	default:
		return nil, errors.Errorf("unexpected type %v", ftyp)
	}
}

func isValidPtrStruct(ptr reflect.Value) bool {
	return ptr.Kind() == reflect.Pointer &&
		ptr.Elem().Kind() == reflect.Struct
}
