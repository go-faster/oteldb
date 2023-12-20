// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheusremotewrite

import (
	"errors"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// FromTimeSeries converts TimeSeries to OTLP metrics.
func FromTimeSeries(tss []prompb.TimeSeries, settings Settings) (pmetric.Metrics, error) {
	pms := pmetric.NewMetrics()
	for _, ts := range tss {
		empty := pms.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		pm := pmetric.NewMetric()
		metricName, err := finalName(ts.Labels)
		if err != nil {
			return pms, err
		}
		pm.SetName(metricName)
		settings.Logger.Debug("Metric name", zap.String("metric_name", pm.Name()))

		s1, s2 := metricSuffixes(metricName)
		metricsType := ""
		if s1 != "" || s2 != "" {
			lastSuffixInMetricName := s2
			if IsValidSuffix(lastSuffixInMetricName) {
				metricsType = lastSuffixInMetricName
				if s2 != "" {
					secondSuffixInMetricName := s1
					if IsValidUnit(secondSuffixInMetricName) {
						pm.SetUnit(secondSuffixInMetricName)
					}
				}
			} else if IsValidUnit(lastSuffixInMetricName) {
				pm.SetUnit(lastSuffixInMetricName)
			}
		}
		for _, s := range ts.Samples {
			ppoint := pmetric.NewNumberDataPoint()
			ppoint.SetDoubleValue(s.Value)
			ppoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, s.Timestamp*int64(time.Millisecond))))
			if ppoint.Timestamp().AsTime().Before(time.Now().Add(-time.Duration(settings.TimeThreshold) * time.Hour)) {
				settings.Logger.Debug("Metric older than the threshold",
					zap.String("metric name", pm.Name()),
					zap.Time("metric_timestamp", ppoint.Timestamp().AsTime()),
				)
				continue
			}
			for _, l := range ts.Labels {
				labelName := l.Name
				if l.Name == nameStr {
					continue
				}
				ppoint.Attributes().PutStr(labelName, l.Value)
			}
			if IsValidCumulativeSuffix(metricsType) {
				pm.SetEmptySum()
				pm.Sum().SetIsMonotonic(true)
				pm.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				ppoint.CopyTo(pm.Sum().DataPoints().AppendEmpty())
			} else {
				pm.SetEmptyGauge()
				ppoint.CopyTo(pm.Gauge().DataPoints().AppendEmpty())
			}
		}
		pm.MoveTo(empty)
	}
	return pms, nil
}

func finalName(labels []prompb.Label) (ret string, err error) {
	for _, label := range labels {
		if label.Name == nameStr {
			return label.Value, nil
		}
	}
	return "", errors.New("label name not found")
}
