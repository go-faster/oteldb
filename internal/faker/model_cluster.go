package faker

import (
	"math/rand"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type cluster struct {
	name    string
	servers []*server
	rand    *rand.Rand
}

func (c *cluster) Attributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.K8SClusterName(c.name),
	}
}

func (c *cluster) getRandomServer() *server {
	return c.servers[c.rand.Intn(len(c.servers))]
}

func (c *cluster) addServer(s *server) {
	c.servers = append(c.servers, s)
}
