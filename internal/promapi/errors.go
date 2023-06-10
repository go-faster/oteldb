package promapi

import "net/http"

// FailToCode converts FailErrorType to http status code.
func FailToCode(f FailErrorType) int {
	switch f {
	case FailErrorTypeBadData:
		return http.StatusBadRequest
	case FailErrorTypeExecution:
		return http.StatusUnprocessableEntity
	case FailErrorTypeCanceled, FailErrorTypeTimeout:
		return http.StatusServiceUnavailable
	case FailErrorTypeInternal:
		return http.StatusInternalServerError
	case FailErrorTypeNotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
