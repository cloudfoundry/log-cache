package egress

import (
	"log"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/rpc/logcache"
	"google.golang.org/grpc"
)

type PeerWriter struct {
	addr string
	c    logcache.IngressClient
}

func NewPeerWriter(addr string, opts ...grpc.DialOption) *PeerWriter {
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Panicf("failed to dial %s: %s", addr, err)
	}

	return &PeerWriter{
		addr: addr,
		c:    logcache.NewIngressClient(conn),
	}
}

func (w *PeerWriter) Write(e *loggregator_v2.Envelope) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	_, err := w.c.Send(ctx, &logcache.SendRequest{
		Envelopes: &loggregator_v2.EnvelopeBatch{[]*loggregator_v2.Envelope{e}},
	})

	if err != nil {
		log.Printf("failed to write envelope to %s: %s", w.addr, err)
	}
}
