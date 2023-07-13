package faker

import (
	"net/netip"
	"strconv"
)

type cluster struct {
	name    string
	servers []server
}

func (c *cluster) addServer(s server) {
	c.servers = append(c.servers, s)
}

type service interface {
	Name() string
}

type server struct {
	name     string     // hostname
	ip       netip.Addr // address
	services []service
}

func (s *server) addService(service service) {
	s.services = append(s.services, service)
}

type model struct {
	rps     int
	cluster cluster
}

type apiService struct {
	replica int
}

func (s apiService) Name() string { return "api" }

type dbService struct {
	replica int
}

func (s dbService) Name() string { return "db" }

type cacheService struct {
	replica int
}

func (s cacheService) Name() string { return "cache" }

type backendService struct {
	replica int
}

func (s backendService) Name() string { return "backend" }

type frontendService struct {
	replica int
}

func (s frontendService) Name() string { return "frontend" }

func modelFromConfig(c Config) model {
	m := model{
		rps: c.RPS,
	}
	m.cluster.name = "msk1"
	for i := 0; i < c.Nodes; i++ {
		s := server{
			name: "node-" + strconv.Itoa(i),
			ip:   netip.MustParseAddr("192.168.0." + strconv.Itoa(i)),
		}
		for j := 0; j < c.Services.API.Replicas; j++ {
			s.addService(apiService{
				replica: j,
			})
		}
		for j := 0; j < c.Services.Backend.Replicas; j++ {
			s.addService(backendService{
				replica: j,
			})
		}
		for j := 0; j < c.Services.DB.Replicas; j++ {
			s.addService(dbService{
				replica: j,
			})
		}
		for j := 0; j < c.Services.Cache.Replicas; j++ {
			s.addService(cacheService{
				replica: j,
			})
		}
		for j := 0; j < c.Services.Frontend.Replicas; j++ {
			s.addService(frontendService{
				replica: j,
			})
		}
		m.cluster.addServer(s)
	}
	return m
}
