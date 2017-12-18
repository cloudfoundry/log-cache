package egress

import (
	"log"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-batching"
	"code.cloudfoundry.org/go-diodes"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/rpc/logcache"
	"google.golang.org/grpc"
)

// PeerWriter writes envelopes to other LogCache nodes.
type PeerWriter struct {
	addr   string
	c      logcache.IngressClient
	egress logcache.EgressClient

	diode *diodes.OneToOne
}

// NewPeerWriter creates a new PeerWriter.
func NewPeerWriter(addr string, opts ...grpc.DialOption) *PeerWriter {
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Panicf("failed to dial %s: %s", addr, err)
	}

	w := &PeerWriter{
		addr:   addr,
		c:      logcache.NewIngressClient(conn),
		egress: logcache.NewEgressClient(conn),
		diode: diodes.NewOneToOne(10000, diodes.AlertFunc(func(missed int) {
			log.Printf("Dropped %d envelopes for %s", missed, addr)
		})),
	}

	go func() {
		batcher := batching.NewBatcher(100, 250*time.Millisecond, batching.WriterFunc(w.write))
		for {
			e, ok := w.diode.TryNext()
			if !ok {
				batcher.Flush()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			batcher.Write((*loggregator_v2.Envelope)(e))
		}
	}()

	return w
}

// Write collects envelopes and writes batches.
func (w *PeerWriter) Write(e *loggregator_v2.Envelope) {
	w.diode.Set(diodes.GenericDataType(e))
}

// Read reads from the LogCache peer.
func (w *PeerWriter) Read(ctx context.Context, in *logcache.ReadRequest, opts ...grpc.CallOption) (*logcache.ReadResponse, error) {
	return w.egress.Read(ctx, in, opts...)
}

func (w *PeerWriter) write(b []interface{}) {
	var e []*loggregator_v2.Envelope
	for _, i := range b {
		e = append(e, i.(*loggregator_v2.Envelope))
	}

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := w.c.Send(ctx, &logcache.SendRequest{
		Envelopes: &loggregator_v2.EnvelopeBatch{e},
	})

	if err != nil {
		log.Printf("failed to write envelope to %s: %s", w.addr, err)
	}
}
