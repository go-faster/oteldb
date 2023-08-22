package ytstorage

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/yqlclient"
)

// YQLQuerier implements Querier based on YQL.
type YQLQuerier struct {
	client      *yqlclient.Client
	tables      Tables
	clusterName string
	tracer      trace.Tracer
}

// YQLQuerierOptions is YTQLQuerier's options.
type YQLQuerierOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// ClusterName sets cluster name to use.
	ClusterName string
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
}

func (opts *YQLQuerierOptions) setDefaults() {
	if opts.Tables == (Tables{}) {
		opts.Tables = defaultTables
	}
	if opts.ClusterName == "" {
		opts.ClusterName = "ytdemo"
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = otel.GetMeterProvider()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
}

// NewYQLQuerier creates new YQLQuerier.
func NewYQLQuerier(client *yqlclient.Client, opts YQLQuerierOptions) (*YQLQuerier, error) {
	opts.setDefaults()

	return &YQLQuerier{
		client:      client,
		tables:      opts.Tables,
		clusterName: opts.ClusterName,
		tracer:      opts.TracerProvider.Tracer("ytstorage.YQLQuerier"),
	}, nil
}
