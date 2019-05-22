package cache

import (
	"hash/crc64"
	"io/ioutil"
	"log"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache/store"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"code.cloudfoundry.org/log-cache/internal/promql"
	"code.cloudfoundry.org/log-cache/internal/promql/data_reader"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"code.cloudfoundry.org/log-cache/pkg/client"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	log *log.Logger

	lis    net.Listener
	server *grpc.Server

	serverOpts []grpc.ServerOption
	metrics    metrics.Initializer
	closing    int64

	maxPerSource       int
	memoryLimitPercent float64
	queryTimeout       time.Duration

	// Cluster Properties
	addr     string
	dialOpts []grpc.DialOption
	extAddr  string

	// nodeAddrs are the addresses of all the nodes (including the current
	// node). The index corresponds with the nodeIndex. It defaults to a
	// single bogus address so the node will not attempt to route data
	// externally and instead will store all of it.
	nodeAddrs []string
	nodeIndex int

	store *store.Store
}

// NewLogCache creates a new LogCache.
func New(opts ...LogCacheOption) *LogCache {
	cache := &LogCache{
		log:                log.New(ioutil.Discard, "", 0),
		metrics:            metrics.NullMetrics{},
		maxPerSource:       100000,
		memoryLimitPercent: 50,
		queryTimeout:       10 * time.Second,

		addr:     ":8080",
		dialOpts: []grpc.DialOption{grpc.WithInsecure()},
	}

	for _, o := range opts {
		o(cache)
	}

	if len(cache.nodeAddrs) == 0 {
		cache.nodeAddrs = []string{cache.addr}
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
// empty list.
func WithServerOpts(opts ...grpc.ServerOption) LogCacheOption {
	return func(c *LogCache) {
		c.serverOpts = opts
	}
}

// WithMemoryLimit sets the percentage of total system memory to use for the
// cache. If exceeded, the cache will prune. Default is 50%.
func WithMemoryLimit(memoryPercent float64) LogCacheOption {
	return func(c *LogCache) {
		c.memoryLimitPercent = memoryPercent
	}
}

// WithQueryTimeout sets the maximum allowed runtime of a single PromQL query.
// The default is 10s. If you increase this limit, make sure to keep in mind
// that memory usage will increase with longer durations.
func WithQueryTimeout(queryTimeout time.Duration) LogCacheOption {
	return func(c *LogCache) {
		c.queryTimeout = queryTimeout
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

// WithExternalAddr returns a LogCacheOption that sets
// address the scheduler will refer to the given node as. This is required
// when the set address won't match what the scheduler will refer to the node
// as (e.g. :0). Defaults to the resulting address from the listener.
func WithExternalAddr(addr string) LogCacheOption {
	return func(c *LogCache) {
		c.extAddr = addr
	}
}

// WithMetrics returns a LogCacheOption that configures the metrics for the
// LogCache. It will add metrics to the given map.
func WithMetrics(m metrics.Initializer) LogCacheOption {
	return func(c *LogCache) {
		c.metrics = m
	}
}

// Start starts the LogCache. It has an internal go-routine that it creates
// and therefore does not block.
func (c *LogCache) Start() {
	p := store.NewPruneConsultant(2, c.memoryLimitPercent, NewMemoryAnalyzer(c.metrics))
	c.store = store.NewStore(c.maxPerSource, p, c.metrics)
	c.setupRouting(c.store)
}

func (c *LogCache) Put(env *loggregator_v2.Envelope, sourceId string) {
	c.store.Put(env, sourceId)
}

// Close will shutdown the gRPC server
func (c *LogCache) Close() error {
	atomic.AddInt64(&c.closing, 1)
	c.server.GracefulStop()
	return nil
}

func (c *LogCache) setupRouting(s *store.Store) {
	tableECMA := crc64.MakeTable(crc64.ECMA)
	hasher := func(s string) uint64 {
		return crc64.Checksum([]byte(s), tableECMA)
	}

	// gRPC
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	c.lis = lis
	c.log.Printf("listening on %s...", c.Addr())

	if c.extAddr == "" {
		c.extAddr = c.lis.Addr().String()
	}

	lookup := routing.NewRoutingTable(c.nodeAddrs, hasher)
	orchestratorAgent := routing.NewOrchestratorAgent(lookup)

	var (
		ingressClients []logcache_v1.IngressClient
		egressClients  []logcache_v1.EgressClient
		localIdx       int
	)

	lcr := routing.NewLocalStoreReader(s)

	// Register peers and current node
	for i, addr := range c.nodeAddrs {
		if i != c.nodeIndex {
			conn, err := grpc.Dial(addr, c.dialOpts...)
			if err != nil {
				log.Printf("failed to dial %s: %s", addr, err)
				continue
			}

			bw := routing.NewBatchedIngressClient(
				100,
				250*time.Millisecond,
				logcache_v1.NewIngressClient(conn),
				c.metrics.NewPerNodeCounter("ingress_dropped", i),
				c.log,
			)

			ingressClients = append(ingressClients, bw)
			egressClients = append(egressClients, logcache_v1.NewEgressClient(conn))

			continue
		}

		localIdx = i
		ingressClients = append(ingressClients, routing.IngressClientFunc(func(ctx context.Context, r *logcache_v1.SendRequest, opts ...grpc.CallOption) (*logcache_v1.SendResponse, error) {
			for _, e := range r.GetEnvelopes().GetBatch() {
				s.Put(e, e.GetSourceId())
			}

			return &logcache_v1.SendResponse{}, nil
		}))
		egressClients = append(egressClients, lcr)
	}

	ingressReverseProxy := routing.NewIngressReverseProxy(lookup.Lookup, ingressClients, localIdx, c.log)
	egressReverseProxy := routing.NewEgressReverseProxy(lookup.Lookup, egressClients, localIdx, c.log)

	promQL := promql.New(
		data_reader.NewWalkingDataReader(
			client.NewClient(c.Addr(), client.WithViaGRPC(c.dialOpts...)).Read,
		),
		c.metrics,
		c.log,
		c.queryTimeout,
	)
	c.server = grpc.NewServer(c.serverOpts...)

	go func() {
		logcache_v1.RegisterIngressServer(c.server, ingressReverseProxy)
		logcache_v1.RegisterEgressServer(c.server, egressReverseProxy)
		logcache_v1.RegisterOrchestrationServer(c.server, orchestratorAgent)
		logcache_v1.RegisterPromQLQuerierServer(c.server, promQL)
		if err := c.server.Serve(lis); err != nil && atomic.LoadInt64(&c.closing) == 0 {
			c.log.Fatalf("failed to serve gRPC ingress server: %s %#v", err, err)
		}
	}()
}

// Addr returns the address that the LogCache is listening on. This is only
// valid after Start has been invoked.
func (c *LogCache) Addr() string {
	return c.lis.Addr().String()
}
