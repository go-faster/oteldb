package lokicompliance

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/golang/snappy"
	"github.com/grafana/loki/pkg/push"
	ht "github.com/ogen-go/ogen/http"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

// GenerateOptions defines options for [LogGenerator.Generate].
type GenerateOptions struct {
	Start, End time.Time
	Step       time.Duration
	Lines      int
	Streams    []string
	Client     ht.Client
}

func (opts *GenerateOptions) setDefaults() {
	switch s, e := opts.Start, opts.End; {
	case s.IsZero() && e.IsZero():
		opts.End = time.Now()
		fallthrough
	case s.IsZero():
		opts.Start = opts.End.Add(-time.Minute)
	case e.IsZero():
		opts.End = opts.Start.Add(time.Minute)
	}
	if opts.Step == 0 {
		opts.Step = time.Second
	}
	if opts.Lines == 0 {
		opts.Lines = 1
	}
	if len(opts.Streams) == 0 {
		opts.Streams = []string{"log.json"}
	}
	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}
}

// GenerateLogs generates logs and sends them to targets
func GenerateLogs(ctx context.Context, targets []string, opts GenerateOptions) error {
	opts.setDefaults()

	streams := map[string]push.Stream{}
	for ts := opts.Start; ts.Before(opts.End); ts = ts.Add(opts.Step) {
		rt := ts
		for i := 0; i < opts.Lines; i++ {
			rt = rt.Add(100 * time.Microsecond)

			stream := randomElement(opts.Streams)
			s, ok := streams[stream]
			if !ok {
				s = push.Stream{
					Labels: makeLabels(
						labelPair{Name: "filename", Value: stream},
						labelPair{Name: "job", Value: "varlogs"},
					),
				}
			}
			line := getLine(rt)
			s.Entries = append(s.Entries, push.Entry{
				Timestamp: rt,
				Line:      line,
			})
			streams[stream] = s
		}
	}

	req := push.PushRequest{
		Streams: maps.Values(streams),
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	for _, target := range targets {
		target := target
		grp.Go(func() error {
			ctx := grpCtx
			if err := sendLogs(ctx, opts.Client, target, req); err != nil {
				return errors.Wrapf(err, "target %q", target)
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}

	return nil
}

type labelPair struct {
	Name, Value string
}

func makeLabels(labels ...labelPair) string {
	var sb strings.Builder
	sb.WriteByte('{')

	slices.SortFunc(labels, func(a, b labelPair) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i, p := range labels {
		if i != 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s=%q", p.Name, p.Value)
	}

	sb.WriteByte('}')
	return sb.String()
}

func sendLogs(ctx context.Context, client ht.Client, target string, body push.PushRequest) error {
	data, err := body.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshal")
	}
	compressed := snappy.Encode(nil, data)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(compressed))
	if err != nil {
		return errors.Wrap(err, "create request")
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send")
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if code := resp.StatusCode; code >= 400 {
		var errbody strings.Builder
		if _, err := io.Copy(&errbody, resp.Body); err != nil {
			return err
		}
		return errors.Errorf("status code %d: %q", code, &errbody)
	}

	return nil
}

func getLine(ts time.Time) string {
	e := jx.GetEncoder()
	defer jx.PutEncoder(e)

	e.ObjStart()
	e.Field("ts", func(e *jx.Encoder) {
		e.Str(ts.String())
	})
	e.Field("method", func(e *jx.Encoder) {
		method := randomElement([]string{
			http.MethodGet,
			http.MethodHead,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		})
		e.Str(method)
	})
	e.Field("status", func(e *jx.Encoder) {
		status := randomElement([]int{
			http.StatusOK,
			http.StatusCreated,
			http.StatusNoContent,

			http.StatusBadRequest,
			http.StatusUnauthorized,
			http.StatusForbidden,
			http.StatusNotFound,
			http.StatusMethodNotAllowed,
			http.StatusNotAcceptable,
			http.StatusUnsupportedMediaType,

			http.StatusInternalServerError,
			http.StatusNotImplemented,
		})
		e.Int(status)
	})
	e.Field("bytes", func(e *jx.Encoder) {
		size := rand.Intn(1024*1024*1024) + 100 // #nosec: G404
		e.Int(size)
	})
	e.ObjEnd()

	return e.String()
}

// GenerateResult is log generation result.
type GenerateResult struct {
	StartMarker string
	EndMarker   string
}

func randomElement[S ~[]T, T any](s S) (r T) {
	if len(s) == 0 {
		return r
	}
	return s[rand.Intn(len(s))] // #nosec: G404
}
