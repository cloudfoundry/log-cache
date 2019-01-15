package routing

import (
	"context"
	"log"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"
)

// IngressReverseProxy is a reverse proxy for Ingress requests.
type IngressReverseProxy struct {
	clients  []rpc.IngressClient
	localIdx int
	l        Lookup
	log      *log.Logger
}

// Lookup is used to find which Clients a source ID should be routed to.
type Lookup func(sourceID string) []int

// NewIngressReverseProxy returns a new IngressReverseProxy.
func NewIngressReverseProxy(
	l Lookup,
	clients []rpc.IngressClient,
	localIdx int,
	log *log.Logger,
) *IngressReverseProxy {

	return &IngressReverseProxy{
		clients:  clients,
		localIdx: localIdx,
		l:        l,
		log:      log,
	}
}

// Send will send to either the local node or the correct remote node
// according to its source ID.
func (p *IngressReverseProxy) Send(ctx context.Context, r *rpc.SendRequest) (*rpc.SendResponse, error) {
	if r.LocalOnly {
		return p.clients[p.localIdx].Send(ctx, r)
	}

	envelopesByNode := make(map[int][]*loggregator_v2.Envelope)

	for _, e := range r.Envelopes.Batch {
		for _, idx := range p.l(e.GetSourceId()) {
			envelopesByNode[idx] = append(envelopesByNode[idx], e)
		}
	}

	for idx, envelopes := range envelopesByNode {
		_, err := p.clients[idx].Send(ctx, &rpc.SendRequest{
			LocalOnly: true,
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: envelopes,
			},
		})

		if err != nil {
			p.log.Printf("ingress reverse proxy: failed to write to client: %s", err)
			continue
		}
	}

	return &rpc.SendResponse{}, nil
}

// IngressClientFunc transforms a function into an IngressClient.
type IngressClientFunc func(ctx context.Context, r *rpc.SendRequest, opts ...grpc.CallOption) (*rpc.SendResponse, error)

// Send implements an IngressClient.
func (f IngressClientFunc) Send(ctx context.Context, r *rpc.SendRequest, opts ...grpc.CallOption) (*rpc.SendResponse, error) {
	return f(ctx, r, opts...)
}
