package faker

import (
	"context"
	"math/rand"
	"net/netip"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type service interface {
	routerHandler
	Name() string
}

// Server represents a physical Server, typically a node in cluster.
//
// Metrics:
//   - [ ] System metrics
//
// Traces: N/A
//
// Logs: N/A
type server struct {
	name     string     // hostname
	ip       netip.Addr // address
	id       int        // unique id
	services []service
	meter    metric.Meter
	rnd      *rand.Rand
	cpus     int

	cpuUtilization metric.Float64ObservableGauge
	memUtilization metric.Float64ObservableGauge
}

func (s *server) Metrics() (err error) {
	// https://opentelemetry.io/docs/specs/otel/metrics/semantic_conventions/system-metrics/
	if s.cpuUtilization, err = s.meter.Float64ObservableGauge("system.cpu.utilization",
		metric.WithUnit("1"),
		metric.WithDescription("Difference in system.cpu.time since the last measurement, divided by the elapsed time and number of CPUs/"),
	); err != nil {
		return err
	}
	if _, err := s.meter.RegisterCallback(s.Observe, s.cpuUtilization); err != nil {
		return err
	}
	if s.memUtilization, err = s.meter.Float64ObservableGauge("system.memory.utilization",
		metric.WithUnit("1"),
	); err != nil {
		return err
	}

	return nil
}

// Observe is metric callback.
func (s *server) Observe(_ context.Context, observer metric.Observer) error {
	for i := 0; i < s.cpus; i++ {
		for _, state := range []string{
			"idle",
			"system",
			"user",
		} {
			observer.ObserveFloat64(s.cpuUtilization, s.rnd.Float64(), metric.WithAttributes(
				attribute.String("state", state),
				attribute.Int("cpu", i+1), // CPU number (0..n)
			))
		}
	}
	for _, state := range []string{
		"used",
		"free",
	} {
		observer.ObserveFloat64(s.memUtilization, s.rnd.Float64(), metric.WithAttributes(
			attribute.String("state", state),
		))
	}
	return nil
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
