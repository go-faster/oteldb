package otelbench

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/ogen-go/ogen/ogenerrors"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/otelbench/ent"
	"github.com/go-faster/oteldb/internal/otelbotapi"
)

// NewHandler initializes and returns a new Handler.
func NewHandler(db *ent.Client) *Handler {
	return &Handler{
		db: db,
	}
}

// Handler handles otelbench api.
type Handler struct {
	db *ent.Client
}

var (
	_ otelbotapi.Handler         = (*Handler)(nil)
	_ otelbotapi.SecurityHandler = (*Handler)(nil)
)

type tokCtx struct{}

func (h Handler) Ping(ctx context.Context) error {
	zctx.From(ctx).Debug("Ping")
	return nil
}

func (h Handler) GetStatus(ctx context.Context) (*otelbotapi.GetStatusOK, error) {
	tok, ok := ctx.Value(tokCtx{}).(string)
	if !ok || tok == "" {
		return nil, fmt.Errorf("no github token")
	}
	count, err := h.db.Organization.Query().Count(ctx)
	if err != nil {
		return nil, err
	}
	return &otelbotapi.GetStatusOK{
		Status: fmt.Sprintf("org: %d", count),
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
	ctx = context.WithValue(ctx, tokCtx{}, t.APIKey)
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
