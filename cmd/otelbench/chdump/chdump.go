// Package chdump provides utilities to read ClickHouse data dumps created by oteldb.
package chdump

import (
	"archive/tar"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
)

// ReadOptions defines options for [Read].
type ReadOptions struct {
	OnTraces        func(*Spans) error
	OnPoints        func(*Points) error
	OnExpHistograms func(*ExpHistograms) error
	OnLogs          func(*Logs) error
}

// ReadTar reads dump from given tar file.
func ReadTar(r io.Reader, opts ReadOptions) error {
	tr := tar.NewReader(r)
	iter := &tarIter{tr: tr}
	return readDump(iter, opts)
}

// ReadDir reads dump from unpacked directory.
func ReadDir(dir string, opts ReadOptions) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	iter := &dirIter{dir: dir, entries: entries}
	return readDump(iter, opts)
}

// readDump reads dump from given tar file.
func readDump(iter dumpIter, opts ReadOptions) error {
	for {
		name, file, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.Wrap(err, "next")
		}
		if err := func() error {
			defer func() {
				_ = file.Close()
			}()

			table := strings.TrimSuffix(name, ".bin.zstd")
			switch table {
			case "traces_spans":
				return readBlock(file, NewSpans(), opts.OnTraces)
			case "metrics_points":
				return readBlock(file, NewPoints(), opts.OnPoints)
			case "metrics_exp_histograms":
				return readBlock(file, NewExpHistograms(), opts.OnExpHistograms)
			case "logs":
				return readBlock(file, NewLogs(), opts.OnLogs)
			}

			return nil
		}(); err != nil {
			return errors.Wrapf(err, "read %q", name)
		}
	}
}

type dumpIter interface {
	Next() (string, io.ReadCloser, error)
}

type tarIter struct {
	tr *tar.Reader
}

func (t *tarIter) Next() (string, io.ReadCloser, error) {
	h, err := t.tr.Next()
	if err != nil {
		return "", nil, err
	}
	name := path.Clean(h.Name)
	return name, io.NopCloser(t.tr), nil
}

type dirIter struct {
	dir     string
	entries []fs.DirEntry
	idx     int
}

func (d *dirIter) Next() (string, io.ReadCloser, error) {
	for {
		if d.idx >= len(d.entries) {
			return "", nil, io.EOF
		}

		entry := d.entries[d.idx]
		if entry.IsDir() {
			d.idx++
			continue
		}

		f, err := os.Open(filepath.Join(d.dir, entry.Name()))
		if err != nil {
			return "", nil, err
		}
		name := filepath.Clean(entry.Name())

		d.idx++
		return name, f, nil
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
