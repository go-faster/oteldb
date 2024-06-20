package logqlbench

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

func (p *LogQLBenchmark) sendAndRecord(ctx context.Context, q Query) (rerr error) {
	return p.tracker.Track(ctx, q, p.send)
}

func toLokiTimestamp(t time.Time) (opt lokiapi.OptLokiTime) {
	if t.IsZero() {
		return opt
	}
	s := strconv.FormatInt(t.UnixNano(), 10)
	opt.SetTo(lokiapi.LokiTime(s))
	return opt
}

func toLokiDuration(t time.Duration) (opt lokiapi.OptPrometheusDuration) {
	opt.SetTo(lokiapi.PrometheusDuration(t.String()))
	return opt
}

func (p *LogQLBenchmark) send(ctx context.Context, q Query) error {
	ctx, cancel := context.WithTimeout(ctx, p.RequestTimeout)
	defer cancel()

	isEmpty := func(data lokiapi.QueryResponseData) bool {
		switch typ := data.Type; typ {
		case lokiapi.StreamsResultQueryResponseData:
			streams := data.StreamsResult
			return len(streams.Result) == 0
		case lokiapi.ScalarResultQueryResponseData:
			return false
		case lokiapi.VectorResultQueryResponseData:
			vector := data.VectorResult
			return len(vector.Result) == 0
		case lokiapi.MatrixResultQueryResponseData:
			matrix := data.MatrixResult
			return len(matrix.Result) == 0
		default:
			return true
		}
	}

	const limit = 1000
	switch q.Type {
	case "instant":
		queryInfo := fmt.Sprintf("instant %q (start: %s, limit: %d)",
			q.Query,
			q.Start, limit,
		)

		resp, err := p.client.Query(ctx, lokiapi.QueryParams{
			Query: q.Query,
			Time:  toLokiTimestamp(q.Start),
			Limit: lokiapi.NewOptInt(limit),
		})
		if err != nil {
			return errors.Wrapf(err, "send %s", queryInfo)
		}

		if isEmpty(resp.Data) && !p.AllowEmpty {
			return errors.Errorf("unexpected empty data: %s", queryInfo)
		}
		return nil
	case "range":
		queryInfo := fmt.Sprintf("range %q (start: %s, end: %s, step: %s, limit: %d)",
			q.Query,
			q.Start, q.End, q.Step,
			limit,
		)

		resp, err := p.client.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: q.Query,
			Start: toLokiTimestamp(q.Start),
			End:   toLokiTimestamp(q.End),
			Step:  toLokiDuration(q.Step),
			Limit: lokiapi.NewOptInt(limit),
		})
		if err != nil {
			return errors.Wrapf(err, "send %s", queryInfo)
		}

		if isEmpty(resp.Data) && !p.AllowEmpty {
			return errors.Errorf("unexpected empty data: %s", queryInfo)
		}
		return nil
	case "series":
		queryInfo := fmt.Sprintf("series %v (start: %s, end: %s)",
			q.Match,
			q.Start, q.End,
		)

		resp, err := p.client.Series(ctx, lokiapi.SeriesParams{
			Start: toLokiTimestamp(q.Start),
			End:   toLokiTimestamp(q.End),
			Match: q.Match,
		})
		if err != nil {
			return errors.Wrapf(err, "send %s", queryInfo)
		}

		if len(resp.Data) == 0 && !p.AllowEmpty {
			return errors.Errorf("unexpected empty data: %s", queryInfo)
		}
		return nil
	default:
		return errors.Errorf("unknown query type %q", q.Type)
	}
}
