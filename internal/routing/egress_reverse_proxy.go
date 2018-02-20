package routing

import (
	"context"
	"log"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
)

// EgressReverseProxy is a reverse proxy for Egress requests.
type EgressReverseProxy struct {
	clients  []rpc.EgressClient
	l        Lookup
	localIdx int
	log      *log.Logger
}

// NewEgressReverseProxy returns a new EgressReverseProxy. LocalIdx is
// required to know where to find the local node for meta lookups.
func NewEgressReverseProxy(
	l Lookup,
	clients []rpc.EgressClient,
	localIdx int,
	log *log.Logger,
) *EgressReverseProxy {
	return &EgressReverseProxy{
		l:        l,
		clients:  clients,
		localIdx: localIdx,
		log:      log,
	}
}

// Read will either read from the local node or remote nodes.
func (e *EgressReverseProxy) Read(ctx context.Context, in *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	return e.clients[e.l(in.GetSourceId())].Read(ctx, in)
}

// Meta will gather meta from the local store and remote nodes.
func (e *EgressReverseProxy) Meta(ctx context.Context, in *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	if in.LocalOnly {
		return e.clients[e.localIdx].Meta(ctx, in)
	}

	// Each remote should only fetch their local meta data.
	req := &rpc.MetaRequest{
		LocalOnly: true,
	}

	result := &rpc.MetaResponse{
		Meta: make(map[string]*rpc.MetaInfo),
	}

	for _, c := range e.clients {
		resp, err := c.Meta(context.Background(), req)
		if err != nil {
			// TODO: Metric
			e.log.Printf("failed to read meta data from remote node: %s", err)
			return nil, err
		}

		for sourceID, mi := range resp.Meta {
			result.Meta[sourceID] = mi
		}
	}

	return result, nil
}
