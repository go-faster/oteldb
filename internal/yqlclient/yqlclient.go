// Package yqlclient provides YTSaurus YQL client.
package yqlclient

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	"github.com/ogen-go/ogen/ogenerrors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/yqlclient/ytqueryapi"
)

// Client is a YQL client.
type Client struct {
	client *ytqueryapi.Client
	tracer trace.Tracer
}

// ClientOptions is a Client creation options.
type ClientOptions struct {
	// Token to use. If empty, authentication would not be used.
	Token string

	// Client to use. Defaults to http.DefaultClient.
	Client *http.Client

	// TracerProvider is a tracer provider. Defaults to otel.GetTracerProvider.
	TracerProvider trace.TracerProvider
	// MeterProvider is a meter provider. Defaults to otel.GetMeterProvider.
	MeterProvider metric.MeterProvider
}

func (opts *ClientOptions) setDefaults() {
	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
}

type securitySource struct {
	Token string
}

func (s *securitySource) YTToken(context.Context, string) (t ytqueryapi.YTToken, _ error) {
	if s.Token == "" {
		return t, ogenerrors.ErrSkipClientSecurity
	}
	t.APIKey = "OAuth " + s.Token
	return t, nil
}

// NewClient creates new Client.
func NewClient(proxyURL string, opts ClientOptions) (*Client, error) {
	opts.setDefaults()

	client, err := ytqueryapi.NewClient(
		proxyURL,
		&securitySource{Token: opts.Token},
		ytqueryapi.WithClient(opts.Client),
		ytqueryapi.WithTracerProvider(opts.TracerProvider),
		ytqueryapi.WithMeterProvider(opts.MeterProvider),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create ogen client")
	}

	return &Client{
		client: client,
		tracer: otel.Tracer("yqlclient"),
	}, nil
}

// RawClient returns raw client.
func (c *Client) RawClient() *ytqueryapi.Client {
	return c.client
}

// ExecuteQueryParams sets ExecuteQuery parameters.
type ExecuteQueryParams struct {
	// PollInterval is a query result polling interval. Defaults to 1s.
	PollInterval time.Duration

	// AbortTimeout sets timeout for aborting query. Defaults to 10s.
	AbortTimeout time.Duration

	// Engine to run query. Defaults to YQL.
	Engine ytqueryapi.Engine
}

func (p *ExecuteQueryParams) setDefaults() {
	if p.PollInterval == 0 {
		p.PollInterval = time.Second
	}
	if p.AbortTimeout == 0 {
		p.AbortTimeout = 10 * time.Second
	}
	if p.Engine == "" {
		p.Engine = ytqueryapi.EngineYql
	}
}

func (c *Client) abortQuery(queryID ytqueryapi.QueryID, timeout time.Duration) error {
	abortCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := c.client.AbortQuery(abortCtx, ytqueryapi.AbortQueryParams{QueryID: queryID}); err != nil {
		return errors.Wrapf(err, "abort query %s", queryID)
	}
	return nil
}

// ExecuteQuery starts query and waits for query completion.
//
// Caller may abort the query by canceling the context.
func (c *Client) ExecuteQuery(ctx context.Context, q string, params ExecuteQueryParams) (queryID ytqueryapi.QueryID, rerr error) {
	params.setDefaults()

	ctx, span := c.tracer.Start(ctx, "ExecuteQuery", trace.WithAttributes(
		attribute.String("engine", string(params.Engine)),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	started, err := c.client.StartQuery(ctx, ytqueryapi.StartQueryParams{
		Query:  q,
		Engine: params.Engine,
	})
	if err != nil {
		return queryID, errors.Wrap(err, "start query")
	}
	queryID = started.QueryID
	span.SetAttributes(attribute.String("yt.query_id", string(queryID)))
	span.AddEvent("QueryStarted")

	t := time.NewTicker(params.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			span.AddEvent("QueryCanceled")
			return queryID, multierr.Append(
				ctx.Err(),
				c.abortQuery(queryID, params.AbortTimeout),
			)
		case <-t.C:
			status, err := c.client.GetQuery(ctx, ytqueryapi.GetQueryParams{QueryID: queryID})
			if err != nil {
				return queryID, errors.Wrapf(err, "get query %s status", queryID)
			}

			switch status.State {
			case ytqueryapi.OperationStateAborted:
				span.AddEvent("QueryAborted")
				return queryID, errors.Wrapf(err, "query %s aborted", queryID)
			case ytqueryapi.OperationStateFailed:
				e := status.Error.Value
				span.AddEvent("QueryFailed", trace.WithAttributes(
					attribute.Int("yt.error_code", e.Code),
					attribute.String("yt.error_message", e.Message),
				))

				return queryID, &Error{Err: e}
			case ytqueryapi.OperationStateCompleted:
				span.AddEvent("QueryCompleted", trace.WithAttributes(
					attribute.Int("yt.result_count", status.ResultCount.Or(0)),
				))
				return queryID, nil
			}
		}
	}
}

// YQLQuery makes an YQL query.
func YQLQuery[T any](ctx context.Context, c *Client, q string) (iterators.Iterator[T], error) {
	queryID, err := c.ExecuteQuery(ctx, q, ExecuteQueryParams{
		Engine: ytqueryapi.EngineYql,
	})
	if err != nil {
		return nil, errors.Wrap(err, "execute query")
	}

	iter, err := ReadResult[T](ctx, c, queryID)
	if err != nil {
		return nil, errors.Wrap(err, "read result")
	}
	return iter, nil
}
