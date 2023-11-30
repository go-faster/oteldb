package lokie2e_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"sigs.k8s.io/yaml"

	"github.com/go-faster/oteldb/integration/lokie2e"
)

func appendAttributes(target pcommon.Map, attrs []attribute.KeyValue) {
	for _, attr := range attrs {
		k := string(attr.Key)
		switch attr.Value.Type() {
		case attribute.BOOL:
			target.PutBool(k, attr.Value.AsBool())
		case attribute.INT64:
			target.PutInt(k, attr.Value.AsInt64())
		case attribute.FLOAT64:
			target.PutDouble(k, attr.Value.AsFloat64())
		case attribute.STRING:
			target.PutStr(k, attr.Value.AsString())
		}
	}
}

type httpLog struct {
	Time     time.Time
	Method   string
	Status   int
	Bytes    int
	Protocol string
	User     string
	URL      string
	IP       string
	Ref      string
}

func (l httpLog) Append(s *lokie2e.BatchSet) error {
	var (
		ld = plog.NewLogs()
		rl = ld.ResourceLogs().AppendEmpty()
	)
	appendAttributes(rl.Resource().Attributes(), []attribute.KeyValue{
		semconv.ServiceName("testService"),
		semconv.ServiceVersion("testVersion"),
		semconv.ServiceNamespace("testNamespace"),
	})
	rl.SetSchemaUrl(semconv.SchemaURL)
	il := rl.ScopeLogs().AppendEmpty()
	{
		sc := il.Scope()
		sc.SetName("name")
		sc.SetVersion("version")
		sc.Attributes().PutStr("oteldb.name", "testDB")
		sc.SetDroppedAttributesCount(1)
	}
	il.SetSchemaUrl(semconv.SchemaURL)
	lg := il.LogRecords().AppendEmpty()
	lg.Body().SetStr(fmt.Sprintf("%s %s %d %d - 0.000 ms", l.Method, l.URL, l.Status, l.Bytes))
	lg.SetTimestamp(pcommon.NewTimestampFromTime(l.Time))
	lg.SetObservedTimestamp(pcommon.NewTimestampFromTime(l.Time))
	appendAttributes(lg.Attributes(), []attribute.KeyValue{
		semconv.HTTPMethod(l.Method),
		semconv.HTTPStatusCode(l.Status),
		semconv.ClientAddress(l.IP),
		attribute.String("protocol", l.Protocol),
	})
	lg.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	if err := s.Append(ld); err != nil {
		return err
	}
	return nil
}

func generateLogs(now time.Time) (s lokie2e.BatchSet, err error) {
	type httpLogBatch struct {
		Method   string
		Status   int
		Count    int
		IP       string
		Protocol string
	}
	var lines []httpLog
	for _, b := range []httpLogBatch{
		{Method: "GET", Status: 200, Count: 11, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "GET", Status: 200, Count: 10, IP: "200.1.1.1", Protocol: "HTTP/1.1"},
		{Method: "DELETE", Status: 200, Count: 20, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "POST", Status: 200, Count: 21, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "PATCH", Status: 200, Count: 19, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "HEAD", Status: 200, Count: 15, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "HEAD", Status: 200, Count: 4, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "HEAD", Status: 200, Count: 1, IP: "236.7.233.166", Protocol: "HTTP/2.0"},
		{Method: "HEAD", Status: 500, Count: 2, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "PUT", Status: 200, Count: 20, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
	} {
		for i := 0; i < b.Count; i++ {
			now = now.Add(time.Millisecond * 120)
			lines = append(lines, httpLog{
				Time:     now,
				Method:   b.Method,
				Status:   b.Status,
				Bytes:    250,
				Protocol: b.Protocol,
				IP:       b.IP,
				URL:      "/api/v1/series",
				Ref:      "https://api.go-faster.org",
			})
		}
	}

	for _, l := range lines {
		if err := l.Append(&s); err != nil {
			return s, err
		}
	}
	return s, nil
}

func TestGenerateLogs(t *testing.T) {
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now)
	require.NoError(t, err)
	logEncoder := plog.JSONMarshaler{}
	var out bytes.Buffer
	for _, b := range set.Batches {
		data, err := logEncoder.MarshalLogs(b)
		require.NoError(t, err)
		outData, err := yaml.JSONToYAML(data)
		require.NoError(t, err)
		out.WriteString("---\n")
		out.Write(outData)
	}

	gold.Str(t, out.String(), "logs.yml")
}
