// Package prome2e provides scripts for E2E testing Prometheus API implementation.
package prome2e

import (
	"io"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// BatchSet is a set of batches.
type BatchSet struct {
	Batches []pmetric.Metrics

	Labels map[string]map[string]struct{}
}

// ParseBatchSet parses JSON batches from given reader.
func ParseBatchSet(r io.Reader) (s BatchSet, _ error) {
	d := jx.Decode(r, 4096)
	u := pmetric.JSONUnmarshaler{}

	for d.Next() != jx.Invalid {
		data, err := d.Raw()
		if err != nil {
			return s, errors.Wrap(err, "read line")
		}

		raw, err := u.UnmarshalMetrics(data)
		if err != nil {
			return s, errors.Wrap(err, "parse batch")
		}

		if err := s.addBatch(raw); err != nil {
			return s, errors.Wrap(err, "add batch")
		}
	}
	return s, nil
}

func (s *BatchSet) addBatch(raw pmetric.Metrics) error {
	s.Batches = append(s.Batches, raw)

	resMetrics := raw.ResourceMetrics()
	for i := 0; i < resMetrics.Len(); i++ {
		resLog := resMetrics.At(i)
		res := resLog.Resource()
		s.addLabels(res.Attributes())

		scopeMetrics := resLog.ScopeMetrics()
		for i := 0; i < scopeMetrics.Len(); i++ {
			scopeMetrics := scopeMetrics.At(i)
			scope := scopeMetrics.Scope()
			s.addLabels(scope.Attributes())

			metrics := scopeMetrics.Metrics()
			for i := 0; i < metrics.Len(); i++ {
				metric := metrics.At(i)
				if err := s.addMetric(metric); err != nil {
					return errors.Wrap(err, "add metric")
				}
			}
		}
	}
	return nil
}

func (s *BatchSet) addMetric(metric pmetric.Metric) error {
	switch t := metric.Type(); t {
	case pmetric.MetricTypeGauge:
		points := metric.Gauge().DataPoints()
		for i := 0; i < points.Len(); i++ {
			point := points.At(i)
			s.addLabels(point.Attributes())
		}
	case pmetric.MetricTypeSum:
		points := metric.Sum().DataPoints()
		for i := 0; i < points.Len(); i++ {
			point := points.At(i)
			s.addLabels(point.Attributes())
		}
	case pmetric.MetricTypeHistogram:
	case pmetric.MetricTypeExponentialHistogram:
	case pmetric.MetricTypeSummary:
	case pmetric.MetricTypeEmpty:
	default:
		return errors.Errorf("unexpected type %v", t)
	}
	return nil
}

func (s *BatchSet) addLabels(m pcommon.Map) {
	m.Range(func(k string, v pcommon.Value) bool {
		switch t := v.Type(); t {
		case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		default:
			s.addLabel(k, v.AsString())
		}
		return true
	})
}

func (s *BatchSet) addLabel(label, val string) {
	if s.Labels == nil {
		s.Labels = map[string]map[string]struct{}{}
	}
	m := s.Labels[label]
	if m == nil {
		m = map[string]struct{}{}
		s.Labels[label] = m
	}
	m[val] = struct{}{}
}
