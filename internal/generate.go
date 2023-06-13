// Package internal contains go:generate annotations.
package internal

//go:generate go run github.com/ogen-go/ogen/cmd/ogen -generate-tests -v --convenient-errors --target tempoapi     --package tempoapi     ../_oas/tempo.yml
//go:generate go run github.com/ogen-go/ogen/cmd/ogen -generate-tests -v --convenient-errors --target lokiapi      --package lokiapi      ../_oas/loki.yml
//go:generate go run github.com/ogen-go/ogen/cmd/ogen -generate-tests -v --convenient-errors --target promapi      --package promapi      ../_oas/prometheus.yml
//go:generate go run github.com/ogen-go/ogen/cmd/ogen -generate-tests -v --convenient-errors --target pyroscopeapi --package pyroscopeapi ../_oas/pyroscope.yml
