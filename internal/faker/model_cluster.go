package faker

import (
	"math/rand"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type cluster struct {
	name    string
	servers []*server
}

func (c *cluster) Attributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.K8SClusterName(c.name),
	}
}

func (c *cluster) getRandomServer(source *rand.Rand) *server {
	return c.servers[source.Intn(len(c.servers))]
}

func (c *cluster) addServer(s *server) {
	c.servers = append(c.servers, s)
}
