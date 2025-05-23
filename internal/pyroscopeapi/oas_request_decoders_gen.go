// Code generated by ogen, DO NOT EDIT.

package pyroscopeapi

import (
	"mime"
	"net/http"

	"github.com/go-faster/errors"

	ht "github.com/ogen-go/ogen/http"
	"github.com/ogen-go/ogen/validate"
)

func (s *Server) decodeIngestRequest(r *http.Request) (
	req *IngestReqWithContentType,
	close func() error,
	rerr error,
) {
	var closers []func() error
	close = func() error {
		var merr error
		// Close in reverse order, to match defer behavior.
		for i := len(closers) - 1; i >= 0; i-- {
			c := closers[i]
			merr = errors.Join(merr, c())
		}
		return merr
	}
	defer func() {
		if rerr != nil {
			rerr = errors.Join(rerr, close())
		}
	}()
	ct, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return req, close, errors.Wrap(err, "parse media type")
	}
	switch {
	case ht.MatchContentType("*/*", ct):
		reader := r.Body
		request := IngestReq{Data: reader}
		wrapped := IngestReqWithContentType{
			ContentType: ct,
			Content:     request,
		}
		return &wrapped, close, nil
	default:
		return req, close, validate.InvalidContentType(ct)
	}
}
