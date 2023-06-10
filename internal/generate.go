// Package internal contains go:generate annotations.
package internal

//go:generate go run github.com/ogen-go/ogen/cmd/ogen -generate-tests -v --target tempoapi --package tempoapi ../_oas/tempo.yml
//go:generate go run github.com/ogen-go/ogen/cmd/ogen -generate-tests -v --target lokiapi --package lokiapi ../_oas/loki.yml
//go:generate go run github.com/ogen-go/ogen/cmd/ogen -generate-tests -v --target promapi --package promapi ../_oas/prometheus.yml
