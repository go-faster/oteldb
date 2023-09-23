package faker

import (
	"net/netip"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type service interface {
	routerHandler
	Name() string
}

// Server represents a physical Server, typically a node in cluster.
//
// Metrics:
//   - [ ] Host metrics, like node exporter
//
// Traces: N/A
//
// Logs: N/A
type server struct {
	name     string     // hostname
	ip       netip.Addr // address
	id       int        // unique id
	services []service
}

func (s *server) Attributes() []attribute.KeyValue {
	hostID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(s.name))
	return []attribute.KeyValue{
		semconv.OSTypeLinux,
		semconv.HostName(s.name),
		semconv.OSDescription("Ubuntu 22.04.3 LTS (Jammy Jellyfish) (Linux server 6.2.0-32-generic #32~22.04.1-Ubuntu SMP PREEMPT_DYNAMIC Fri Aug 18 10:40:13 UTC 2 x86_64)"),
		semconv.HostID(hostID.String()),
		attribute.Int("faker.server.id", s.id),
	}
}

func (s *server) addService(service service) {
	s.services = append(s.services, service)
}
