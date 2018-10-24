package routing

import (
	"log"
	"time"

	batching "code.cloudfoundry.org/go-batching"
	diodes "code.cloudfoundry.org/go-diodes"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// BatchedIngressClient batches envelopes before sending it. Each invocation
// to Send is async.
type BatchedIngressClient struct {
	c rpc.IngressClient

	buffer   *diodes.OneToOne
	size     int
	interval time.Duration
	log      *log.Logger
}

// Metrics registers new Counter metrics.
type Metrics interface {
	// NewCounter returns a func that can be used to increment a counter
	// metric.
	NewCounter(name string) func(delta uint64)
}

// NewBatchedIngressClient returns a new BatchedIngressClient.
func NewBatchedIngressClient(
	size int,
	interval time.Duration,
	c rpc.IngressClient,
	m Metrics,
	log *log.Logger,
) *BatchedIngressClient {
	dropped := m.NewCounter("Dropped")
	b := &BatchedIngressClient{
		c:        c,
		size:     size,
		interval: interval,
		log:      log,

		buffer: diodes.NewOneToOne(10000, diodes.AlertFunc(func(missed int) {
			log.Printf("Dropped %d envelopes", missed)
			dropped(uint64(missed))
		})),
	}

	go b.start()

	return b
}

// Send batches envelopes before shipping them to the client.
func (b *BatchedIngressClient) Send(ctx context.Context, in *rpc.SendRequest, opts ...grpc.CallOption) (*rpc.SendResponse, error) {
	for i := range in.GetEnvelopes().GetBatch() {
		b.buffer.Set(diodes.GenericDataType(in.Envelopes.Batch[i]))
	}

	return &rpc.SendResponse{}, nil
}

func (b *BatchedIngressClient) start() {
	batcher := batching.NewBatcher(b.size, b.interval, batching.WriterFunc(b.write))
	for {
		e, ok := b.buffer.TryNext()
		if !ok {
			batcher.Flush()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		batcher.Write((*loggregator_v2.Envelope)(e))
	}
}

func (b *BatchedIngressClient) write(batch []interface{}) {
	var e []*loggregator_v2.Envelope
	for _, i := range batch {
		e = append(e, i.(*loggregator_v2.Envelope))
	}

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := b.c.Send(ctx, &rpc.SendRequest{
		LocalOnly: true,
		Envelopes: &loggregator_v2.EnvelopeBatch{Batch: e},
	})

	if err != nil {
		b.log.Printf("failed to write envelope: %s", err)
	}
}
