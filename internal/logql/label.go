package logql

import "github.com/go-faster/errors"

// Label is a LogQL identifier.
type Label string

const (
	// ErrorLabel is a specific label for LogQL errors.
	ErrorLabel = "__error__"
	// ErrorDetailsLabel is a specific label for LogQL error details.
	ErrorDetailsLabel = "__error_details__"
)

// IsValidLabel validates label name.
func IsValidLabel(s string, allowDot bool) error {
	if s == "" {
		return errors.New("label name cannot be empty")
	}
	if r := s[0]; !(isLetter(rune(r)) || r == '_') {
		return errors.Errorf("invalid label name character %q at 0", r)
	}
	for i, r := range s {
		if isLetter(r) ||
			(r >= '0' && r <= '9') ||
			r == '_' ||
			(allowDot && r == '.') {
			continue
		}
		return errors.Errorf("invalid label name character %q at %d", r, i)
	}
	return nil
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}
