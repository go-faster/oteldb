package ytlocal

import "testing"

func TestLogging(t *testing.T) {
	encode(t, "logging", Logging{
		Writers: map[string]LoggingWriter{
			"stderr": {
				WriterType: LogWriterTypeStderr,
				Format:     LogFormatJSON,
			},
			"debug": {
				WriterType: LogWriterTypeFile,
				Format:     LogFormatPlainText,
				FileName:   "debug.log",
			},
			"error": {
				WriterType: LogWriterTypeFile,
				Format:     LogFormatYSON,
				FileName:   "error.log",
			},
			"trace": {
				WriterType: LogWriterTypeFile,
				Format:     LogFormatJSON,
				FileName:   "trace.log",
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
