package otelbench

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"github.com/google/go-github/v58/github"
	"github.com/ogen-go/ogen/ogenerrors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/oauth2"

	"github.com/go-faster/oteldb/internal/otelbench/ent"
	"github.com/go-faster/oteldb/internal/otelbotapi"
)

// NewHandler initializes and returns a new Handler.
func NewHandler(db *ent.Client, m *app.Metrics) *Handler {
	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport,
			otelhttp.WithTracerProvider(m.TracerProvider()),
			otelhttp.WithMeterProvider(m.MeterProvider()),
			otelhttp.WithPropagators(m.TextMapPropagator()),
		),
	}
	return &Handler{
		http: httpClient,
		db:   db,
	}
}

// Handler handles otelbench api.
type Handler struct {
	db   *ent.Client
	http *http.Client
}

var (
	_ otelbotapi.Handler         = (*Handler)(nil)
	_ otelbotapi.SecurityHandler = (*Handler)(nil)
)

type githubClient struct{}

func (h *Handler) clientWithToken(ctx context.Context, token string) *github.Client {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, h.http)
	tc := oauth2.NewClient(ctx, ts)
	return github.NewClient(tc)
}

func (h Handler) Ping(ctx context.Context) error {
	zctx.From(ctx).Debug("Ping")
	return nil
}

func (h Handler) GetStatus(ctx context.Context) (*otelbotapi.GetStatusOK, error) {
	// Check github client propagation.
	client, ok := ctx.Value(githubClient{}).(*github.Client)
	if !ok {
		return nil, fmt.Errorf("no github client")
	}
	if _, _, err := client.Repositories.Get(ctx, "go-faster", "oteldb"); err != nil {
		return nil, errors.Wrap(err, "github check")
	}
	// Check database.
	if _, err := h.db.Organization.Query().Count(ctx); err != nil {
		return nil, errors.Wrap(err, "database check")
	}
	return &otelbotapi.GetStatusOK{
		Status: fmt.Sprintf("gh=ok db=ok"),
	}, nil
}

func (h Handler) NewError(ctx context.Context, err error) *otelbotapi.ErrorStatusCode {
	if ent.IsNotFound(err) {
		// Generic not found handler.
		return NewError(ctx, Error{
			Message:    err.Error(),
			StatusCode: http.StatusNotFound,
		})
	}
	if ent.IsConstraintError(err) {
		// Generic constraint error handler.
		return NewError(ctx, Error{
			Message:    err.Error(),
			StatusCode: http.StatusUnprocessableEntity,
		})
	}
	if ent.IsValidationError(err) {
		// Generic validation error handler.
		return NewError(ctx, Error{
			Message:    err.Error(),
			StatusCode: http.StatusBadRequest,
		})
	}
	if errors.Is(err, ogenerrors.ErrSecurityRequirementIsNotSatisfied) {
		// Generic not authenticated handler.
		return NewError(ctx, Error{
			Message:    err.Error(),
			StatusCode: http.StatusUnauthorized,
		})
	}
	return NewError(ctx, Error{
		// Some internal error.
		Message: err.Error(),
	})
}

func (h Handler) HandleTokenAuth(ctx context.Context, _ string, t otelbotapi.TokenAuth) (context.Context, error) {
	ctx = context.WithValue(ctx, githubClient{}, h.clientWithToken(ctx, t.APIKey))
	return ctx, nil
}

type Error struct {
	StatusCode int
	Message    string
}

// NewError creates new structured error response. All fields are optional.
func NewError(ctx context.Context, err Error) *otelbotapi.ErrorStatusCode {
	if err.Message == "" {
		err.Message = "Internal server error"
	}
	if err.StatusCode == 0 {
		err.StatusCode = 500
	}
	e := otelbotapi.Error{
		ErrorMessage: err.Message,
	}
	zctx.From(ctx).Error("NewError",
		zap.Int("status", err.StatusCode),
		zap.String("message", err.Message),
	)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		e.TraceID.SetTo(otelbotapi.TraceID(sc.TraceID().String()))
		e.SpanID.SetTo(otelbotapi.SpanID(sc.SpanID().String()))
	}
	return &otelbotapi.ErrorStatusCode{
		StatusCode: err.StatusCode,
		Response:   e,
	}
}
