package oteldbproto

func appendZero[S ~[]E, E any](s *S) int {
	var zero E
	*s = append(*s, zero)
	return len(*s) - 1
}
