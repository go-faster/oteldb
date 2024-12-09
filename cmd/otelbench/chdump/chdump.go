// Package chdump provides utilities to read ClickHouse data dumps created by oteldb.
package chdump

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
)

// ReadOptions defines options for [Read].
type ReadOptions struct {
	OnTraces func(*Spans) error
	// OnMetrics func(*Metrics) error
	OnLogs func(*Logs) error
}

// Read reads dump from given tar file.
func Read(tarFile string, opts ReadOptions) error {
	ar, err := os.Open(tarFile)
	if err != nil {
		return errors.Wrap(err, "open archive")
	}
	defer func() {
		_ = ar.Close()
	}()

	tr := tar.NewReader(ar)
	for {
		h, err := tr.Next()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		}

		name := path.Clean(h.Name)
		table := strings.TrimSuffix(name, ".bin.zstd")
		switch table {
		case "logs":
			if err := readBlock(tr, NewLogs(), opts.OnLogs); err != nil {
				return errors.Wrapf(err, "read %q", h.Name)
			}
		case "traces_spans":
			if err := readBlock(tr, NewSpans(), opts.OnTraces); err != nil {
				return errors.Wrapf(err, "read %q", h.Name)
			}
		case "metrics_points":
		case "metrics_exp_histograms":
		}
	}
}

const protoVersion = 51902

func readBlock[
	TableColumns interface {
		Reset()
		Result() proto.Results
	},
](
	r io.Reader,
	cols TableColumns,
	cb func(TableColumns) error,
) error {
	if cb == nil {
		return nil
	}

	zr, err := zstd.NewReader(r)
	if err != nil {
		return errors.Wrap(err, "open zstd dump")
	}
	defer zr.Close()

	var (
		chr   = proto.NewReader(zr)
		block = &proto.Block{}

		blockIdx int
		results  = cols.Result()
	)
	for {
		cols.Reset()

		if err := block.DecodeBlock(chr, protoVersion, results); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.Wrapf(err, "decode block %d", blockIdx)
		}

		if err := cb(cols); err != nil {
			return errors.Wrap(err, "callback")
		}
		blockIdx++
	}
}
