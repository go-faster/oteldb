package faker

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModel(t *testing.T) {
	m := modelFromConfig(Config{
		Rand:  rand.New(rand.NewSource(42)),
		Nodes: 10,
		RPS:   1000,
		Services: Services{
			API: API{
				Replicas: 2,
			},
			Backend: Backend{
				Replicas: 3,
			},
			DB: DB{
				Replicas: 3,
			},
			Cache: Cache{
				Replicas: 1,
			},
			Frontend: Frontend{
				Replicas: 3,
			},
		},
	})
	assert.Equal(t, 10, len(m.cluster.servers))
	assert.Equal(t, 1000, m.rps)
	assert.Equal(t, 3, len(m.frontends))

	var services int
	for _, s := range m.cluster.servers {
		services += len(s.services)
	}
	assert.Equal(t, 9, services)
}
