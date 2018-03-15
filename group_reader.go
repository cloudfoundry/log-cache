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

// GroupReader gathers data for several source IDs to allow a consumer to read
// from many different source IDs much like it would read from one.
type GroupReader struct {
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
}

// NewGroupReader creates a new GroupReader. NodeAddrs has the hostport of
// every node in the cluster (including its own). The nodeIndex is the address
// of the current node.
func NewGroupReader(logCacheAddr string, nodeAddrs []string, nodeIndex int, opts ...GroupReaderOption) *GroupReader {
	// Copy nodeAddrs to ensure we can manipulate without fear
	na := make([]string, len(nodeAddrs))
	copy(na, nodeAddrs)

	r := &GroupReader{
		addr:     nodeAddrs[nodeIndex],
		log:      log.New(ioutil.Discard, "", 0),
		metrics:  nopMetrics{},
		dialOpts: []grpc.DialOption{grpc.WithInsecure()},

		nodeAddrs: na,
		nodeIndex: nodeIndex,
	}

	for _, o := range opts {
		o(r)
	}

	r.client = logcache.NewClient(logCacheAddr, logcache.WithViaGRPC(r.dialOpts...))

	return r
}

// GroupReaderOption configures a GroupReader.
type GroupReaderOption func(*GroupReader)

// WithGroupReaderLogger returns a GroupReaderOption that configures the
// logger used for the GroupReader. Defaults to silent logger.
func WithGroupReaderLogger(l *log.Logger) GroupReaderOption {
	return func(r *GroupReader) {
		r.log = l
	}
}

// WithGroupReaderMetrics returns a GroupReaderOption that configures the
// metrics for the GroupReader. It will add metrics to the given map.
func WithGroupReaderMetrics(m Metrics) GroupReaderOption {
	return func(r *GroupReader) {
		r.metrics = m
	}
}

// WithGroupReaderServerOpts returns a GroupReaderOption that sets
// grpc.ServerOptions for the GroupReader server.
func WithGroupReaderServerOpts(opts ...grpc.ServerOption) GroupReaderOption {
	return func(g *GroupReader) {
		g.serverOpts = opts
	}
}

// WithGroupReaderDialOpts returns a GroupReaderOption that sets
// grpc.DialOptions for the GroupReader client.
func WithGroupReaderDialOpts(opts ...grpc.DialOption) GroupReaderOption {
	return func(g *GroupReader) {
		g.dialOpts = opts
	}
}

// WithGroupReaderExternalAddr returns a GroupReaderOption that sets
// address the scheduler will refer to the given node as. This is required
// when the set address won't match what the scheduler will refer to the node
// as (e.g. :0). Defaults to the resulting address from the listener.
func WithGroupReaderExternalAddr(addr string) GroupReaderOption {
	return func(g *GroupReader) {
		g.extAddr = addr
	}
}

// Start starts servicing for group requests. It does not block.
func (g *GroupReader) Start() {
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

		p := store.NewPruneConsultant(2, 70, NewMemoryAnalyzer(g.metrics))
		s := groups.NewStorage(g.client.Read, time.Second, p, g.metrics, g.log)

		m := groups.NewManager(s, time.Minute)
		lookup := routing.NewRoutingTable(g.nodeAddrs, hasher)
		orch := routing.NewOrchestrator(lookup)

		srv := grpc.NewServer(g.serverOpts...)

		rp := g.reverseProxy(lookup, m)

		rpc.RegisterGroupReaderServer(srv, rp)
		rpc.RegisterOrchestrationServer(srv, orch)
		if err := srv.Serve(lis); err != nil {
			g.log.Fatalf("failed to serve: %v", err)
		}
	}()
}

// Addr returns the address of the GroupReader. Start must be invoked first.
func (g *GroupReader) Addr() string {
	return g.lis.Addr().String()
}

func (g *GroupReader) reverseProxy(lookup groups.Lookup, m *groups.Manager) rpc.GroupReaderServer {
	var gs []rpc.GroupReaderClient
	for i, a := range g.nodeAddrs {
		if i == g.nodeIndex {
			gs = append(gs, m)
			continue
		}

		conn, err := grpc.Dial(a, g.dialOpts...)
		if err != nil {
			log.Fatalf("failed to dial %s: %s", a, err)
		}
		gs = append(gs, rpc.NewGroupReaderClient(conn))
	}

	return groups.NewRPCReverseProxy(gs, g.nodeIndex, lookup, g.log)
}
