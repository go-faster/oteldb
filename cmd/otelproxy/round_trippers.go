package main

import (
	"net/http"
	"path"
	"strings"
)

// TransportMiddleware wraps http.RoundTripper.
type TransportMiddleware func(http.RoundTripper) http.RoundTripper

var _ http.RoundTripper = (*pyroscopeTransport)(nil)

// pyroscopeTransport overrides Content-Type for some endpoints.
//
// See https://github.com/grafana/pyroscope/pull/1969.
type pyroscopeTransport struct {
	next http.RoundTripper
}

func newPyroscopeTransport(next http.RoundTripper) http.RoundTripper {
	return &pyroscopeTransport{next: next}
}

func (t *pyroscopeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}

	resp, err := next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if last := path.Base(req.URL.Path); last == "labels" || last == "label-values" {
		resp.Header.Set("Content-Type", "application/json")
	}
	return resp, nil
}

var _ http.RoundTripper = (*tempoTransport)(nil)

// tempoTransport sets Accept for some endpoints.
//
// FIXME(tdakkota): probably, we need to add an Accept header.
type tempoTransport struct {
	next http.RoundTripper
}

func newTempoTransport(next http.RoundTripper) http.RoundTripper {
	return &tempoTransport{next: next}
}

func (t *tempoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}

	if strings.Contains(req.URL.Path, "api/traces/") {
		if req.Header.Get("Accept") == "" {
			req.Header.Set("Accept", "application/protobuf")
		}
	}

	resp, err := next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	return resp, nil
}
