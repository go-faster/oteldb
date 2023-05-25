package ytstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_hexUUID_String(t *testing.T) {
	const hex = "0561aba782cf95ea524660dcace86773"
	tests := []struct {
		id   string
		want string
	}{
		{uuid.MustParse(hex).String(), hex},
		{hex, hex},
		{strings.ToUpper(hex), hex},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			id := uuid.MustParse(tt.id)
			require.Equal(t, tt.want, hexUUID(id).String())
		})
	}
}
