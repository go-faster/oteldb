package ytlocal

// LogLevel string describes possible logging levels.
type LogLevel string

const (
	LogLevelTrace   LogLevel = "trace"
	LogLevelDebug   LogLevel = "debug"
	LogLevelInfo    LogLevel = "info"
	LogLevenWarning LogLevel = "warning"
	LogLevelError   LogLevel = "error"
)

// LogWriterType string describes types of possible log writers.
type LogWriterType string

// Possible log writer types.
const (
	LogWriterTypeFile   LogWriterType = "file"
	LogWriterTypeStderr LogWriterType = "stderr"
)

// LoggingRule configures logging.
type LoggingRule struct {
	ExcludeCategories []string `yson:"exclude_categories,omitempty"`
	IncludeCategories []string `yson:"include_categories,omitempty"`
	MinLevel          LogLevel `yson:"min_level,omitempty"`
	Writers           []string `yson:"writers,omitempty"`
}

// LogFormat is a string describing possible log formats.
type LogFormat string

// Possible log formats.
const (
	LogFormatPlainText LogFormat = "plain_text"
	LogFormatJSON      LogFormat = "json"
	LogFormatYSON      LogFormat = "yson"
)

// LoggingWriter configures logging writer.
type LoggingWriter struct {
	WriterType LogWriterType `yson:"type,omitempty"`
	Format     LogFormat     `yson:"format,omitempty"`
	FileName   string        `yson:"file_name,omitempty"`
}

// Logging config.
type Logging struct {
	AbortOnAlert           bool                     `yson:"abort_on_alert,omitempty"`
	CompressionThreadCount int                      `yson:"compression_thread_count,omitempty"`
	Writers                map[string]LoggingWriter `yson:"writers,omitempty"`
	Rules                  []LoggingRule            `yson:"rules,omitempty"`
}
