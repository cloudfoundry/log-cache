package logcache

import (
	"hash/crc64"
	"io/ioutil"
	"log"
	"net"
	"time"

	"code.cloudfoundry.org/go-log-cache"
	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache/internal/groups"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"code.cloudfoundry.org/log-cache/internal/store"
	"google.golang.org/grpc"
)

// ShardGroupReader gathers data for several source IDs to allow a consumer to read
// from many different source IDs much like it would read from one.
type ShardGroupReader struct {
	addr    string
	extAddr string

	lis     net.Listener
	log     *log.Logger
	client  *logcache.Client
	metrics Metrics

	nodeAddrs []string
	nodeIndex int

	serverOpts []grpc.ServerOption
	dialOpts   []grpc.DialOption

	maxPerSource       int
	memoryLimitPercent float64
}

// NewGroupReader creates a new ShardGroupReader. NodeAddrs has the hostport of
// every node in the cluster (including its own). The nodeIndex is the address
// of the current node.
func NewGroupReader(logCacheAddr string, nodeAddrs []string, nodeIndex int, opts ...GroupReaderOption) *ShardGroupReader {
	// Copy nodeAddrs to ensure we can manipulate without fear
	na := make([]string, len(nodeAddrs))
	copy(na, nodeAddrs)

	r := &ShardGroupReader{
		addr:     nodeAddrs[nodeIndex],
		log:      log.New(ioutil.Discard, "", 0),
		metrics:  nopMetrics{},
		dialOpts: []grpc.DialOption{grpc.WithInsecure()},

		nodeAddrs:          na,
		nodeIndex:          nodeIndex,
		maxPerSource:       1000,
		memoryLimitPercent: 50,
	}

	for _, o := range opts {
		o(r)
	}

	r.client = logcache.NewClient(logCacheAddr, logcache.WithViaGRPC(r.dialOpts...))

	return r
}

// GroupReaderOption configures a ShardGroupReader.
type GroupReaderOption func(*ShardGroupReader)

// WithGroupReaderLogger returns a GroupReaderOption that configures the
// logger used for the ShardGroupReader. Defaults to silent logger.
func WithGroupReaderLogger(l *log.Logger) GroupReaderOption {
	return func(r *ShardGroupReader) {
		r.log = l
	}
}

// WithGroupReaderMetrics returns a GroupReaderOption that configures the
// metrics for the ShardGroupReader. It will add metrics to the given map.
func WithGroupReaderMetrics(m Metrics) GroupReaderOption {
	return func(r *ShardGroupReader) {
		r.metrics = m
	}
}

// WithGroupReaderServerOpts returns a GroupReaderOption that sets
// grpc.ServerOptions for the ShardGroupReader server.
func WithGroupReaderServerOpts(opts ...grpc.ServerOption) GroupReaderOption {
	return func(g *ShardGroupReader) {
		g.serverOpts = opts
	}
}

// WithGroupReaderDialOpts returns a GroupReaderOption that sets
// grpc.DialOptions for the ShardGroupReader client.
func WithGroupReaderDialOpts(opts ...grpc.DialOption) GroupReaderOption {
	return func(g *ShardGroupReader) {
		g.dialOpts = opts
	}
}

// WithGroupReaderExternalAddr returns a GroupReaderOption that sets
// address the scheduler will refer to the given node as. This is required
// when the set address won't match what the scheduler will refer to the node
// as (e.g. :0). Defaults to the resulting address from the listener.
func WithGroupReaderExternalAddr(addr string) GroupReaderOption {
	return func(g *ShardGroupReader) {
		g.extAddr = addr
	}
}

// WithGroupReaderMaxPerSource returns a GroupReaderOption that configures the
// store's memory size as number of envelopes for a specific sourceID.
// Defaults to 1000 envelopes.
func WithGroupReaderMaxPerSource(size int) GroupReaderOption {
	return func(g *ShardGroupReader) {
		g.maxPerSource = size
	}
}

// WithMemoryLimit sets the percentage of total system memory to use for the
// cache. If exceeded, the cache will prune. Default is 50%.
func WithGroupReaderMemoryLimit(memoryPercent float64) GroupReaderOption {
	return func(g *ShardGroupReader) {
		g.memoryLimitPercent = memoryPercent
	}
}

// Start starts servicing for group requests. It does not block.
func (g *ShardGroupReader) Start() {
	lis, err := net.Listen("tcp", g.addr)
	if err != nil {
		g.log.Fatalf("failed to listen: %v", err)
	}
	g.lis = lis

	if g.extAddr == "" {
		g.extAddr = lis.Addr().String()
	}

	// Ensure that everything that will receive the list of addresses, looks
	// for the external address of this node and not the condensed address
	// (e.g., :0).
	g.nodeAddrs[g.nodeIndex] = g.extAddr

	go func() {
		tableECMA := crc64.MakeTable(crc64.ECMA)
		hasher := func(s string) uint64 {
			return crc64.Checksum([]byte(s), tableECMA)
		}

		p := store.NewPruneConsultant(2, g.memoryLimitPercent, NewMemoryAnalyzer(g.metrics))
		s := groups.NewStorage(g.maxPerSource, g.client.Read, time.Second, p, g.metrics, g.log)

		m := groups.NewManager(s, time.Minute)
		lookup := routing.NewRoutingTable(g.nodeAddrs, hasher)
		orch := routing.NewOrchestrator(lookup)

		srv := grpc.NewServer(g.serverOpts...)

		rp := g.reverseProxy(lookup, m)

		rpc.RegisterShardGroupReaderServer(srv, rp)
		rpc.RegisterOrchestrationServer(srv, orch)
		if err := srv.Serve(lis); err != nil {
			g.log.Fatalf("failed to serve: %v", err)
		}
	}()
}

// Addr returns the address of the ShardGroupReader. Start must be invoked first.
func (g *ShardGroupReader) Addr() string {
	return g.lis.Addr().String()
}

func (g *ShardGroupReader) reverseProxy(lookup groups.Lookup, m *groups.Manager) rpc.ShardGroupReaderServer {
	var gs []rpc.ShardGroupReaderClient
	for i, a := range g.nodeAddrs {
		if i == g.nodeIndex {
			gs = append(gs, m)
			continue
		}

		conn, err := grpc.Dial(a, g.dialOpts...)
		if err != nil {
			log.Fatalf("failed to dial %s: %s", a, err)
		}
		gs = append(gs, rpc.NewShardGroupReaderClient(conn))
	}

	return groups.NewRPCReverseProxy(gs, g.nodeIndex, lookup, g.log)
}
