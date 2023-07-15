package logqlmetric

// UnsupportedError is an error that reports unsupported expressions.
type UnsupportedError struct {
	Msg string
}

// Error implements error.
func (e *UnsupportedError) Error() string {
	return e.Msg
}
