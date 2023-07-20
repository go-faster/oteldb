package faker

import (
	"math/rand"
	"net/netip"
	"strconv"
)

type cluster struct {
	name    string
	servers []server
}

func (c *cluster) getRandomServer(source *rand.Rand) int {
	return source.Intn(len(c.servers))
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
	id       int        // unique id
	services []service
}

func (s *server) addService(service service) {
	s.services = append(s.services, service)
}

type model struct {
	rps       int
	cluster   cluster
	frontends []frontendService
	rand      *rand.Rand
}

type apiService struct {
	id   int
	ip   netip.Addr
	port int
}

func (s apiService) Name() string { return "api" }

type dbService struct {
	id   int
	ip   netip.Addr
	port int
}

func (s dbService) Name() string { return "db" }

type cacheService struct {
	id   int
	ip   netip.Addr
	port int
}

func (s cacheService) Name() string { return "cache" }

type backendService struct {
	id   int
	ip   netip.Addr
	port int
}

func (s backendService) Name() string { return "backend" }

type frontendService struct {
	id int
	ip netip.Addr
}

func (s frontendService) Name() string { return "frontend" }

func modelFromConfig(c Config) model {
	m := model{
		rps:  c.RPS,
		rand: c.Rand,
	}
	m.cluster.name = "msk1"

	// Generate clients.
	residentialPool := newIPAllocator(netip.MustParseAddr("95.24.0.0"))
	for i := 0; i < c.Services.Frontend.Replicas; i++ {
		m.frontends = append(m.frontends, frontendService{
			id: i,
			ip: residentialPool.Next(),
		})
	}

	// Pool of external IP addresses.
	serverPool := newIPAllocator(netip.MustParseAddr("103.21.244.0"))
	for i := 0; i < c.Nodes; i++ {
		m.cluster.addServer(server{
			name: "node-" + strconv.Itoa(i),
			ip:   serverPool.Next(),
			id:   i,
		})
	}

	// Distribute HTTP API.
	for i := 0; i < c.Services.API.Replicas; i++ {
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		// Using note IP address as being exposed on node 80 port.
		s := apiService{
			id:   i,
			ip:   m.cluster.servers[j].ip,
			port: 80,
		}
		m.cluster.servers[j].addService(s)
	}

	// Distribute internal services.
	pool := newIPAllocator(netip.MustParseAddr("10.43.0.0"))

	// Distribute Backend.
	for i := 0; i < c.Services.Backend.Replicas; i++ {
		s := backendService{
			id:   i,
			ip:   pool.Next(),
			port: 8080,
		}
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		m.cluster.servers[j].addService(s)
	}

	// Distribute DB.
	for i := 0; i < c.Services.DB.Replicas; i++ {
		s := dbService{
			id:   i,
			ip:   pool.Next(),
			port: 5432,
		}
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		m.cluster.servers[j].addService(s)
	}

	// Distribute Cache.
	for i := 0; i < c.Services.Cache.Replicas; i++ {
		s := cacheService{
			id:   i,
			ip:   pool.Next(),
			port: 6379,
		}
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		m.cluster.servers[j].addService(s)
	}

	return m
}
