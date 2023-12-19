// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package prometheusremotewritereceiver implements Prometheus Remote Write API for OTLP collector.
package prometheusremotewritereceiver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/otelreceiver/prometheusremotewrite"
)

const receiverFormat = "protobuf"

// Receiver is a Prometheus remote write receiver.
type Receiver struct {
	params       receiver.CreateSettings
	host         component.Host
	nextConsumer consumer.Metrics

	mu         sync.Mutex
	startOnce  sync.Once
	stopOnce   sync.Once
	shutdownWG sync.WaitGroup

	server        *http.Server
	config        *Config
	timeThreshold *int64
	logger        *zap.Logger
	obsrecv       *receiverhelper.ObsReport
}

// NewReceiver creates new [Start].
func NewReceiver(params receiver.CreateSettings, config *Config, mconsumer consumer.Metrics) (*Receiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             params.ID,
		Transport:              "http",
		ReceiverCreateSettings: params,
	})
	zr := &Receiver{
		params:        params,
		nextConsumer:  mconsumer,
		config:        config,
		logger:        params.Logger,
		obsrecv:       obsrecv,
		timeThreshold: &config.TimeThreshold,
	}
	return zr, err
}

// Start implements [component.Component].
func (rec *Receiver) Start(_ context.Context, host component.Host) error {
	if host == nil {
		return errors.New("nil host")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	err := component.ErrNilNextConsumer
	rec.startOnce.Do(func() {
		err = nil
		rec.host = host
		rec.server, err = rec.config.HTTPServerSettings.ToServer(host, rec.params.TelemetrySettings, rec,
			confighttp.WithDecoder("snappy", snappyDecoder),
		)
		var listener net.Listener
		listener, err = rec.config.HTTPServerSettings.ToListener()
		if err != nil {
			return
		}
		rec.shutdownWG.Add(1)
		go func() {
			defer rec.shutdownWG.Done()
			if errHTTP := rec.server.Serve(listener); errHTTP != http.ErrServerClosed {
				host.ReportFatalError(errHTTP)
			}
		}()
	})
	return err
}

func snappyDecoder(body io.ReadCloser) (io.ReadCloser, error) {
	compressed := getBuf()
	defer putBuf(compressed)

	if _, err := io.Copy(compressed, body); err != nil {
		return nil, err
	}

	decompressed, err := snappy.Decode(nil, compressed.Bytes())
	if err != nil {
		return nil, err
	}

	return &closerReader{
		data:   decompressed,
		Reader: *bytes.NewReader(decompressed),
	}, nil
}

func decodeRequest(r io.Reader) (*prompb.WriteRequest, error) {
	var data []byte
	switch r := r.(type) {
	case *closerReader:
		// Do not make an unnecessary copy of data.
		data = r.data
	default:
		var err error
		data, err = io.ReadAll(r)
		if err != nil {
			return nil, err
		}
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func (rec *Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := rec.obsrecv.StartMetricsOp(r.Context())
	req, err := decodeRequest(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pms, err := prometheusremotewrite.FromTimeSeries(req.Timeseries, prometheusremotewrite.Settings{
		TimeThreshold: *rec.timeThreshold,
		Logger:        *rec.logger,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metricCount := pms.ResourceMetrics().Len()
	dataPointCount := pms.DataPointCount()
	if metricCount != 0 {
		err = rec.nextConsumer.ConsumeMetrics(ctx, pms)
	}
	rec.obsrecv.EndMetricsOp(ctx, receiverFormat, dataPointCount, err)
	w.WriteHeader(http.StatusAccepted)
}

// Shutdown implements [component.Component].
func (rec *Receiver) Shutdown(context.Context) error {
	err := component.ErrNilNextConsumer
	rec.stopOnce.Do(func() {
		err = rec.server.Close()
		rec.shutdownWG.Wait()
	})
	return err
}
