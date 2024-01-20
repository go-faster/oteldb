package otelbench

import (
	"context"
	"os"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/otelbotapi"
)

type envSecuritySource struct{}

func (e envSecuritySource) TokenAuth(_ context.Context, _ string) (otelbotapi.TokenAuth, error) {
	v := os.Getenv("GITHUB_TOKEN")
	if v == "" {
		return otelbotapi.TokenAuth{}, errors.New("GITHUB_TOKEN is empty")
	}
	return otelbotapi.TokenAuth{APIKey: v}, nil
}

var _ otelbotapi.SecuritySource = envSecuritySource{}

func EnvSecuritySource() otelbotapi.SecuritySource {
	return envSecuritySource{}
}
