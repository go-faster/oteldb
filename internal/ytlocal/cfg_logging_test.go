package ytlocal

import "testing"

func TestLogging(t *testing.T) {
	for _, tc := range []struct {
		Name   string
		Writer LoggingWriter
	}{
		{
			Name: "stderr",
			Writer: LoggingWriter{
				WriterType: LogWriterTypeStderr,
				Format:     LogFormatJSON,
			},
		},
		{
			Name: "debug",
			Writer: LoggingWriter{
				WriterType: LogWriterTypeFile,
				Format:     LogFormatPlainText,
				FileName:   "debug.log",
			},
		},
		{
			Name: "error",
			Writer: LoggingWriter{
				WriterType: LogWriterTypeFile,
				Format:     LogFormatYSON,
				FileName:   "error.log",
			},
		},
		{
			Name: "trace",
			Writer: LoggingWriter{
				WriterType: LogWriterTypeFile,
				Format:     LogFormatJSON,
				FileName:   "trace.log",
			},
		},
	} {
		encode(t, "logging-"+tc.Name, Logging{
			Writers: map[string]LoggingWriter{
				tc.Name: {
					WriterType: LogWriterTypeStderr,
					Format:     LogFormatJSON,
				},
			},
			Rules: []LoggingRule{
				{
					MinLevel: LogLevelDebug,
					Writers:  []string{"debug"},
				},
				{
					MinLevel: LogLevelInfo,
					Writers:  []string{"stderr"},
				},
				{
					MinLevel: LogLevelError,
					Writers:  []string{"error"},
				},
				{
					MinLevel: LogLevelTrace,
					Writers:  []string{"trace"},
				},
			},
		})
	}
}
