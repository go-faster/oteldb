// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promhandler

import (
	"math"
	"strconv"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/go-faster/oteldb/internal/promapi"
)

// / https://github.com/prometheus/prometheus/blob/e9b94515caa4c0d7a0e31f722a1534948ebad838/web/api/v1/api.go#L783-L794
var (
	// MinTime is the default timestamp used for the begin of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()

	// MaxTime is the default timestamp used for the end of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = MinTime.Format(time.RFC3339Nano)
	maxTimeFormatted = MaxTime.Format(time.RFC3339Nano)
)

func parseOptTimestamp(t promapi.OptPrometheusTimestamp, or time.Time) (time.Time, error) {
	v, ok := t.Get()
	if !ok {
		return or, nil
	}
	return parseTimestamp(v)
}

func parseTimestamp[S ~string](raw S) (time.Time, error) {
	// https://github.com/prometheus/prometheus/blob/e9b94515caa4c0d7a0e31f722a1534948ebad838/web/api/v1/api.go#L1790-L1811
	s := string(raw)

	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	switch s {
	case minTimeFormatted:
		return MinTime, nil
	case maxTimeFormatted:
		return MaxTime, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseStep[S ~string](raw S) (time.Duration, error) {
	d, err := parseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, errors.New(`zero or negative query resolution step widths are not accepted`)
	}
	return d, nil
}

func parseDuration[S ~string](raw S) (time.Duration, error) {
	if seconds, err := strconv.ParseFloat(string(raw), 64); err == nil {
		return time.Duration(seconds * float64(time.Second)), nil
	}
	if d, err := model.ParseDuration(string(raw)); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("invalid duration %q", raw)
}

func parseQueryOpts(
	deltaParam, statsParam promapi.OptString,
	defaultDelta time.Duration,
) (_ promql.QueryOpts, err error) {
	delta := defaultDelta
	if rawDelta, ok := deltaParam.Get(); ok {
		delta, err = parseDuration(rawDelta)
		if err != nil {
			return nil, validationErr("parse lookback delta", err)
		}
	}
	return promql.NewPrometheusQueryOpts(
		statsParam.Or("") == "all",
		delta,
	), nil
}

func parseLabelMatchers(matchers []string) ([][]*labels.Matcher, error) {
	var matcherSets [][]*labels.Matcher
	for _, s := range matchers {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, err
		}
		matcherSets = append(matcherSets, matchers)
	}

OUTER:
	for _, ms := range matcherSets {
		for _, lm := range ms {
			if lm != nil && !lm.Matches("") {
				continue OUTER
			}
		}
		return nil, errors.New("match[] must contain at least one non-empty matcher")
	}
	return matcherSets, nil
}
