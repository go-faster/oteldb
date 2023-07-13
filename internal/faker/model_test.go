package faker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModel(t *testing.T) {
	m := modelFromConfig(Config{
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
	assert.Equal(t, 12, len(m.cluster.servers[0].services))
}
