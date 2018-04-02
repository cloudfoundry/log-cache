package logcache

import (
	"encoding/json"
	"hash/crc64"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"code.cloudfoundry.org/log-cache/internal/store"
	"code.cloudfoundry.org/log-cache/internal/store/replication"
	replication_v1 "code.cloudfoundry.org/log-cache/internal/store/replication/api"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	log *log.Logger

	lis    net.Listener
	server *grpc.Server

	serverOpts []grpc.ServerOption
	metrics    Metrics
	closing    int64

	maxPerSource int
	min          int

	// Cluster Properties
	addr     string
	dialOpts []grpc.DialOption
	serverID string

	// nodeAddrs are the addresses of all the nodes (including the current
	// node). The index corresponds with the nodeIndex. It defaults to a
	// single bogus address so the node will not attempt to route data
	// externally and instead will store all of it.
	nodeAddrs []string
	nodeIndex int

	mu    sync.Mutex
	rafts map[logcache_v1.Range]*replication.PeerManager
}

// NewLogCache creates a new LogCache.
func New(opts ...LogCacheOption) *LogCache {
	cache := &LogCache{
		log:          log.New(ioutil.Discard, "", 0),
		metrics:      nopMetrics{},
		maxPerSource: 100000,
		min:          1000,

		addr:     ":8080",
		dialOpts: []grpc.DialOption{grpc.WithInsecure()},
		rafts:    make(map[logcache_v1.Range]*replication.PeerManager),
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
// empty list
func WithServerOpts(opts ...grpc.ServerOption) LogCacheOption {
	return func(c *LogCache) {
		c.serverOpts = opts
	}
}

// WithMinimumSize sets the lower bound for pruning. It will not prune once
// this size is reached. Defaults to 1000.
func WithMinimumSize(min int) LogCacheOption {
	return func(c *LogCache) {
		c.min = min
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

// WithServerID returns a LogCacheOption that sets ID the scheduler will refer
// to the given node as. The scheduler speaks in addresses. This is required
// when the set address won't match what the scheduler will refer to the node
// as (e.g. :0). Defaults to the resulting address from the listener.
func WithServerID(addr string) LogCacheOption {
	return func(c *LogCache) {
		c.serverID = addr
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
	c.setupRouting()
}

// Close will shutdown the gRPC server
func (c *LogCache) Close() error {
	atomic.AddInt64(&c.closing, 1)

	c.mu.Lock()
	for _, r := range c.rafts {
		r.Close()
	}
	c.mu.Unlock()

	c.server.Stop()
	return nil
}

func (c *LogCache) setupRouting() {
	// gRPC
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	c.lis = lis
	c.log.Printf("listening on %s...", c.Addr())

	if c.serverID == "" {
		c.serverID = c.lis.Addr().String()
	}

	tableECMA := crc64.MakeTable(crc64.ECMA)
	hasher := func(s string) uint64 {
		return crc64.Checksum([]byte(s), tableECMA)
	}

	lookup := routing.NewRoutingTable(c.nodeAddrs, hasher)
	raftTransport := replication.NewGRPCTransport(c.serverID, c.log, c.dialOpts...)

	multiMetrics := metrics.NewMultiMetrics(c.metrics, map[string]func([]float64) float64{
		"StoreSize": func(fs []float64) float64 {
			// max
			var max float64
			for _, f := range fs {
				if f > max {
					max = f
				}
			}

			return max
		},
	})

	p := store.NewPruneConsultant(2, 70, NewMemoryAnalyzer(c.metrics))
	s := store.NewMultiStore(func(r logcache_v1.Range) store.SubStore {
		metrics := multiMetrics.SubMetrics()
		subStore := store.NewStore(c.maxPerSource, c.min, p, metrics)
		raftID, err := json.Marshal(r)
		if err != nil {
			c.log.Panic(err)
		}

		nodeIndices := lookup.RangeLookup(r)
		var servers []raft.Server
		var rangeAddrs []string
		for _, idx := range nodeIndices {
			servers = append(servers, raft.Server{
				ID:       raft.ServerID(c.nodeAddrs[idx]),
				Address:  raft.ServerAddress(c.nodeAddrs[idx]),
				Suffrage: raft.Voter,
			})
			rangeAddrs = append(rangeAddrs, c.nodeAddrs[idx])
		}

		envelopeCache := replication.NewEnvelopeCache(10000, c.metrics)

		raft := c.newRaftCluster(
			c.serverID,
			servers,
			string(raftID),
			subStore,
			raftTransport,
			envelopeCache,
		)

		c.mu.Lock()
		peerManager := replication.NewPeerManager(raft, rangeAddrs)
		c.rafts[r] = peerManager
		c.mu.Unlock()

		replStore := replication.NewStore(
			subStore,
			raft,
			envelopeCache,
			func() {
				// On Close
				c.mu.Lock()
				defer c.mu.Unlock()
				delete(c.rafts, r)
				metrics.Close()
			},
			c.log,
		)

		return replication.NewAsyncStore(replStore, c.log)
	}, hasher)

	orch := routing.NewOrchestrator(routing.RangeSetterFunc(func(in *logcache_v1.SetRangesRequest) {
		lookup.SetRanges(context.Background(), in)

		r, ok := in.Ranges[c.serverID]
		if ok {
			s.SetRanges(r.Ranges)
		}

		c.mu.Lock()
		for r, pm := range c.rafts {
			var rangeAddrs []string
			for _, idx := range lookup.RangeLookup(r) {
				rangeAddrs = append(rangeAddrs, c.nodeAddrs[idx])
			}

			pm.SetPeers(rangeAddrs)
		}
		c.mu.Unlock()
	}))

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
	c.server = grpc.NewServer(c.serverOpts...)

	go func() {
		logcache_v1.RegisterIngressServer(c.server, ingressReverseProxy)
		logcache_v1.RegisterEgressServer(c.server, egressReverseProxy)
		logcache_v1.RegisterOrchestrationServer(c.server, orch)
		replication_v1.RegisterTransportServer(c.server, raftTransport)
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

func (c *LogCache) newRaftCluster(
	serverID string,
	peers []raft.Server,
	raftID string,
	s *store.Store,
	t *replication.GRPCTransport,
	envelopeCache *replication.EnvelopeCache,
) *raft.Raft {

	inmem := raft.NewInmemStore()
	// inmemSnap := raft.NewInmemSnapshotStore()
	onDemandSnap := replication.NewOnDemandSnapshotStore()
	netTransport := raft.NewNetworkTransport(
		t.StreamLayerFor(raftID),
		100,
		5*time.Second,
		ioutil.Discard,
	)

	cfg := &raft.Config{
		ProtocolVersion:    raft.ProtocolVersionMax,
		LocalID:            raft.ServerID(serverID),
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      500 * time.Millisecond,
		MaxAppendEntries:   64,
		SnapshotInterval:   10 * time.Second,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		LogOutput:          ioutil.Discard,
		TrailingLogs:       10240,
	}

	r, err := raft.NewRaft(
		cfg,
		replication.NewFSMAdapter(s, envelopeCache),
		inmem,
		inmem,
		// inmemSnap,
		onDemandSnap,
		netTransport,
	)

	if err != nil {
		c.log.Panic(err)
	}

	r.BootstrapCluster(raft.Configuration{Servers: peers})

	return r
}
