package ytlocal

import "testing"

func TestLogging(t *testing.T) {
	encode(t, "logging", Logging{
		Writers: map[string]LoggingWriter{
			"stderr": {
				Format:     LogFormatJSON,
				WriterType: LogWriterTypeStderr,
			},
		},
		Rules: []LoggingRule{
			{
				MinLevel: LogLevelError,
				Writers:  []string{"stderr"},
			},
		},
	})
}
