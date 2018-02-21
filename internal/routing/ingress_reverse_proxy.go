package routing

import (
	"context"
	"log"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"google.golang.org/grpc"
)

// IngressReverseProxy is a reverse proxy for Ingress requests.
type IngressReverseProxy struct {
	clients []rpc.IngressClient
	l       Lookup
	log     *log.Logger
}

// Lookup is used to find which Client a source ID should be routed to.
type Lookup func(sourceID string) int

// NewIngressReverseProxy returns a new IngressReverseProxy.
func NewIngressReverseProxy(l Lookup, clients []rpc.IngressClient, log *log.Logger) *IngressReverseProxy {
	return &IngressReverseProxy{
		clients: clients,
		l:       l,
		log:     log,
	}
}

// Send will send to either the local node or the correct remote node
// according to its source ID.
func (i *IngressReverseProxy) Send(ctx context.Context, r *rpc.SendRequest) (*rpc.SendResponse, error) {
	for _, e := range r.Envelopes.Batch {
		_, err := i.clients[i.l(e.GetSourceId())].Send(ctx, &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{e},
			},
		})

		if err != nil {
			i.log.Printf("failed to write to client: %s", err)
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
