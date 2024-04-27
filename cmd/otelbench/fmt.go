package main

import (
	"strings"

	"github.com/dustin/go-humanize"
)

func compactBytes(v int) string {
	s := humanize.Bytes(uint64(v))
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func fmtInt(v int) string {
	s := humanize.SIWithDigits(float64(v), 2, "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}
