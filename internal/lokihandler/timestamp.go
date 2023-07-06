package lokihandler

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

func parseTimeRange(
	now time.Time,
	startParam lokiapi.OptLokiTime,
	endParam lokiapi.OptLokiTime,
	sinceParam lokiapi.OptPrometheusDuration,
) (start, end time.Time, err error) {
	since := 6 * time.Hour
	if v, ok := sinceParam.Get(); ok {
		d, err := model.ParseDuration(string(v))
		if err != nil {
			return start, end, errors.Wrap(err, "parse since")
		}
		since = time.Duration(d)
	}

	endValue := endParam.Or("")
	end, err = parseTimestamp(endValue, now)
	if err != nil {
		return start, end, errors.Wrapf(err, "parse end %q", endValue)
	}

	// endOrNow is used to apply a default for the start time or an offset if 'since' is provided.
	// we want to use the 'end' time so long as it's not in the future as this should provide
	// a more intuitive experience when end time is in the future.
	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}

	startValue := startParam.Or("")
	start, err = parseTimestamp(startValue, endOrNow.Add(-since))
	if err != nil {
		return start, end, errors.Wrapf(err, "parse start %q", startValue)
	}
	return start, end, nil
}

func parseTimestamp(lt lokiapi.LokiTime, def time.Time) (time.Time, error) {
	value := string(lt)
	if value == "" {
		return def, nil
	}

	if strings.Contains(value, ".") {
		if t, err := strconv.ParseFloat(value, 64); err == nil {
			s, ns := math.Modf(t)
			ns = math.Round(ns*1000) / 1000
			return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
		}
	}
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		if ts, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return ts, nil
		}
		return time.Time{}, err
	}
	if len(value) <= 10 {
		return time.Unix(nanos, 0), nil
	}
	return time.Unix(0, nanos), nil
}

func parseStep(param lokiapi.OptPrometheusDuration, start, end time.Time) (time.Duration, error) {
	v, ok := param.Get()
	if !ok {
		return defaultStep(start, end), nil
	}
	return parseDuration(v)
}

func defaultStep(start, end time.Time) time.Duration {
	seconds := math.Max(
		math.Floor(end.Sub(start).Seconds()/250),
		1,
	)
	return time.Duration(seconds) * time.Second
}

func parseDuration(param lokiapi.PrometheusDuration) (time.Duration, error) {
	value := string(param)
	if !strings.ContainsAny(value, "smhdwy") {
		f, err := strconv.ParseFloat(value, 64)
		if err == nil {
			d := f * float64(time.Second)
			return time.Duration(d), nil
		}
	}
	md, err := model.ParseDuration(value)
	return time.Duration(md), err
}
