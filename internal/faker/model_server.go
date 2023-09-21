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
		semconv.HostID(hostID.String()),
		attribute.Int("faker.server.id", s.id),
	}
}

func (s *server) addService(service service) {
	s.services = append(s.services, service)
}
