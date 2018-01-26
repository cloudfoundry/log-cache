package logcache

import (
	"hash/crc64"
	"io/ioutil"
	"log"
	"net"

	"google.golang.org/grpc"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/egress"
	"code.cloudfoundry.org/log-cache/internal/ingress"
	"code.cloudfoundry.org/log-cache/internal/store"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	log        *log.Logger
	lis        net.Listener
	serverOpts []grpc.ServerOption
	metrics    Metrics

	storeSize    int
	maxPerSource int
	proxy        *store.ProxyStore

	// Cluster Properties
	addr     string
	dialOpts []grpc.DialOption

	// nodeAddrs are the addresses of all the nodes (including the current
	// node). The index corresponds with the nodeIndex. It defaults to a
	// single bogus address so the node will not attempt to route data
	// externally and instead will store all of it.
	nodeAddrs []string
	nodeIndex int
}

// NewLogCache creates a new LogCache.
func New(opts ...LogCacheOption) *LogCache {
	cache := &LogCache{
		log:          log.New(ioutil.Discard, "", 0),
		metrics:      nopMetrics{},
		storeSize:    10000000,
		maxPerSource: 100000,

		// Defaults to a single entry. The default does not route data.
		nodeAddrs: []string{"bogus-address"},
		nodeIndex: 0,
		addr:      ":8080",
	}

	for _, o := range opts {
		o(cache)
	}

	return cache
}

// LogCacheOption configures a LogCache.
type LogCacheOption func(*LogCache)

// WithLogger returns a LogCacheOption that configures the logger used for
// the LogCache. Defaults to silent logger.
func WithLogger(l *log.Logger) LogCacheOption {
	return func(c *LogCache) {
		c.log = l
	}
}

// WithStoreSize returns a LogCacheOption that configures the store's
// memory size as number of envelopes. Defaults to 1000000 envelopes.
func WithStoreSize(size int) LogCacheOption {
	return func(c *LogCache) {
		c.storeSize = size
	}
}

// WithMaxPerSource returns a LogCacheOption that configures the store's
// memory size as number of envelopes for a specific sourceID. Defaults to
// 100000 envelopes.
func WithMaxPerSource(size int) LogCacheOption {
	return func(c *LogCache) {
		c.maxPerSource = size
	}
}

// WithAddr configures the address to listen for gRPC requests. It defaults to
// :8080.
func WithAddr(addr string) LogCacheOption {
	return func(c *LogCache) {
		c.addr = addr
	}
}

// WithServerOpts configures the gRPC server options. It defaults to an
// empty list
func WithServerOpts(opts ...grpc.ServerOption) LogCacheOption {
	return func(c *LogCache) {
		c.serverOpts = opts
	}
}

// WithClustered enables the LogCache to route data to peer nodes. It hashes
// each envelope by SourceId and routes data that does not belong on the node
// to the correct node. NodeAddrs is a slice of node addresses where the slice
// index corresponds to the NodeIndex. The current node's address is included.
// The default is standalone mode where the LogCache will store all the data
// and forward none of it.
func WithClustered(nodeIndex int, nodeAddrs []string, opts ...grpc.DialOption) LogCacheOption {
	return func(c *LogCache) {
		c.nodeIndex = nodeIndex
		c.nodeAddrs = nodeAddrs
		c.dialOpts = opts
	}
}

// Metrics registers Counter and Gauge metrics.
type Metrics interface {
	// NewCounter returns a function to increment for the given metric.
	NewCounter(name string) func(delta uint64)

	// NewGauge returns a function to set the value for the given metric.
	NewGauge(name string) func(value float64)
}

// WithMetrics returns a LogCacheOption that configures the metrics for the
// LogCache. It will add metrics to the given map.
func WithMetrics(m Metrics) LogCacheOption {
	return func(c *LogCache) {
		c.metrics = m
	}
}

// nopMetrics are the default metrics.
type nopMetrics struct{}

func (m nopMetrics) NewCounter(name string) func(uint64) {
	return func(uint64) {}
}

func (m nopMetrics) NewGauge(name string) func(float64) {
	return func(float64) {}
}

// Start starts the LogCache. It has an internal go-routine that it creates
// and therefore does not block.
func (c *LogCache) Start() {
	store := store.NewStore(c.storeSize, c.maxPerSource, c.metrics)
	c.setupRouting(store)
}

func (c *LogCache) setupRouting(s *store.Store) {
	tableECMA := crc64.MakeTable(crc64.ECMA)
	hasher := func(s string) uint64 {
		return crc64.Checksum([]byte(s), tableECMA)
	}

	lookup := ingress.NewStaticLookup(len(c.nodeAddrs), hasher)
	ps := ingress.NewPubsub(lookup.Lookup)

	// gRPC
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	c.lis = lis
	c.log.Printf("listening on %s...", c.Addr())

	egressClients := make(map[int]logcache.EgressClient)

	// Register peers and current node
	for i, addr := range c.nodeAddrs {
		if i == c.nodeIndex {
			ps.Subscribe(i, func(e *loggregator_v2.Envelope) {
				s.Put(e, e.GetSourceId())
			})
			continue
		}

		writer := egress.NewPeerWriter(addr, c.dialOpts...)
		ps.Subscribe(i, writer.Write)
		egressClients[i] = writer
	}

	c.proxy = store.NewProxyStore(s, c.nodeIndex, egressClients, lookup.Lookup)

	go func() {
		peerReader := ingress.NewPeerReader(ps.Publish, c.proxy)
		srv := grpc.NewServer(c.serverOpts...)
		logcache.RegisterIngressServer(srv, peerReader)
		logcache.RegisterEgressServer(srv, peerReader)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC ingress server: %s", err)
		}
	}()
}

// SourceIDs returns all source ids in the cache
func (c *LogCache) SourceIDs() []string {
	localOnly := false
	return c.proxy.Meta(localOnly)
}

// Addr returns the address that the LogCache is listening on. This is only
// valid after Start has been invoked.
func (c *LogCache) Addr() string {
	return c.lis.Addr().String()
}
