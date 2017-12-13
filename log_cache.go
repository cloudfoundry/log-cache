package logcache

import (
	"context"
	"expvar"
	"hash/crc64"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"google.golang.org/grpc"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/egress"
	"code.cloudfoundry.org/log-cache/internal/ingress"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"code.cloudfoundry.org/log-cache/internal/rpc/logcache"
	"code.cloudfoundry.org/log-cache/internal/store"
	"code.cloudfoundry.org/log-cache/internal/web"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	connector  StreamConnector
	egressAddr string
	log        *log.Logger
	egressLis  net.Listener
	ingressLis net.Listener
	metricMap  MetricMap

	storeSize    int
	maxPerSource int

	// Cluster Properties
	clusterGrpc ClusterGrpc

	// nodeAddrs are the addresses of all the nodes (including the current
	// node). The index corresponds with the nodeIndex. It defaults to a
	// single bogus address so the node will not attempt to route data
	// externally and instead will store all of it.
	nodeAddrs []string
	nodeIndex int
}

// StreamConnector reads envelopes from the the logs provider.
type StreamConnector interface {
	// Stream creates a EnvelopeStream for the given request.
	Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream
}

// NewLogCache creates a new LogCache.
func New(c StreamConnector, opts ...LogCacheOption) *LogCache {
	cache := &LogCache{
		connector:    c,
		egressAddr:   ":8080",
		log:          log.New(ioutil.Discard, "", 0),
		storeSize:    10000000,
		maxPerSource: 100000,

		// Defaults to a single entry. The default does not route data.
		nodeAddrs: []string{"bogus-address"},
		nodeIndex: 0,
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

// WithEgressAddr returns a LogCacheOption that configures the LogCache's
// egress address. It defaults to ":8080".
func WithEgressAddr(addr string) LogCacheOption {
	return func(c *LogCache) {
		c.egressAddr = addr
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

// ClusterGrpc configures the gRPC for cluster communication.
type ClusterGrpc struct {
	// Addr is the address to bind to for incoming messages. Defaults to :0.
	Addr string

	// DialOptions are used to configure dialers to write to peers.
	DialOptions []grpc.DialOption
}

// WithClustered enables the LogCache to route data to peer nodes. It hashes
// each envelope by SourceId and routes data that does not belong on the node
// to the correct node. NodeAddrs is a slice of node addresses where the slice
// index corresponds to the NodeIndex. The current node's address is included.
// The default is standalone mode where the LogCache will store all the data
// and forward none of it.
func WithClustered(nodeIndex int, nodeAddrs []string, g ClusterGrpc) LogCacheOption {
	return func(c *LogCache) {
		c.nodeIndex = nodeIndex
		c.nodeAddrs = nodeAddrs
		c.clusterGrpc = g
	}
}

// MetricMap mirrors expvar.Map.
type MetricMap interface {
	// Set is implemented by Map.Set()
	Set(key string, av expvar.Var)
}

// WithMetrics returns a LogCacheOption that configures the metrics for the
// LogCache. It will add metrics to the given map.
func WithMetrics(m MetricMap) LogCacheOption {
	return func(c *LogCache) {
		c.metricMap = m
	}
}

// Start starts the LogCache. It has an internal go-routine that it creates
// and therefore does not block.
func (c *LogCache) Start() {
	lis, err := net.Listen("tcp", c.egressAddr)
	if err != nil {
		c.log.Fatalf("failed to listen on addr %s: %s", c.egressAddr, err)
	}
	c.egressLis = lis
	c.log.Printf("listening on %s...", lis.Addr().String())

	metrics := metrics.New(c.metricMap)
	store := store.NewStore(c.storeSize, c.maxPerSource, metrics)

	envelopeDestination, proxy := c.setupRouting(store)

	go func() {
		router := web.NewRouter(proxy.Get, metrics)
		server := &http.Server{Handler: router}
		server.Serve(lis)
	}()

	go func() {
		rx := c.connector.Stream(context.Background(), &loggregator_v2.EgressBatchRequest{
			ShardId: "log-cache",
		})
		es := ingress.NewEnvelopeStream(ingress.Stream(rx), envelopeDestination, metrics)
		es.Start()
	}()
}

func (c *LogCache) setupRouting(s *store.Store) (func(batch []*loggregator_v2.Envelope), *store.ProxyStore) {
	tableECMA := crc64.MakeTable(crc64.ECMA)
	hasher := func(s string) uint64 {
		return crc64.Checksum([]byte(s), tableECMA)
	}

	lookup := ingress.NewStaticLookup(len(c.nodeAddrs), hasher)
	ps := ingress.NewPubsub(lookup.Lookup)

	envelopeDestination := func(batch []*loggregator_v2.Envelope) {
		for _, e := range batch {
			ps.Publish(e)
		}
	}

	// gRPC
	ingressLis, err := net.Listen("tcp", c.clusterGrpc.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	c.ingressLis = ingressLis

	go func() {
		peerReader := ingress.NewPeerReader(s)
		srv := grpc.NewServer()
		logcache.RegisterIngressServer(srv, peerReader)
		logcache.RegisterEgressServer(srv, peerReader)
		if err := srv.Serve(ingressLis); err != nil {
			log.Fatalf("failed to serve gRPC ingress server: %s", err)
		}
	}()

	egressClients := make(map[int]logcache.EgressClient)

	// Register peers and current node
	for i, addr := range c.nodeAddrs {
		if i == c.nodeIndex {
			ps.Subscribe(i, s.Put)
			continue
		}

		writer := egress.NewPeerWriter(addr, c.clusterGrpc.DialOptions...)
		ps.Subscribe(i, writer.Write)
		egressClients[i] = writer
	}

	proxy := store.NewProxyStore(s.Get, c.nodeIndex, egressClients, lookup.Lookup)

	return envelopeDestination, proxy
}

// EgressAddr returns the address that the LogCache is listening on. This is
// only valid after Start has been invoked.
func (c *LogCache) EgressAddr() string {
	return c.egressLis.Addr().String()
}

// IngressAddr returns the address that the LogCache is listening on for
// incoming envelopes. This is only valid after Start has been invoked with
// clustered mode.
func (c *LogCache) IngressAddr() string {
	return c.ingressLis.Addr().String()
}
