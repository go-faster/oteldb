package yqlclient

import (
	"fmt"

	"github.com/go-faster/oteldb/internal/yqlclient/ytqueryapi"
)

// Error is a wrapper for API error.
type Error struct {
	Err ytqueryapi.Error
}

// Error implements error.
func (e *Error) Error() string {
	return fmt.Sprintf("code %d: %s", e.Err.Code, e.Err.Message)
}
