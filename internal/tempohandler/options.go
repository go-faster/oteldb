package tempohandler

// TempoAPIOptions describes [TempoAPI] options.
type TempoAPIOptions struct {
	// EnableAutocompleteQuery whether if handler should parse
	// the `q` parameter in tag requests
	//
	// See https://grafana.com/docs/tempo/latest/api_docs/#filtered-tag-values.
	EnableAutocompleteQuery bool
}

func (opts *TempoAPIOptions) setDefaults() {}
