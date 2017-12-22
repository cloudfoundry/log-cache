package logcache

import (
	"io/ioutil"
	"log"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Nozzle reads envelopes and writes them to LogCache.
type Nozzle struct {
	log     *log.Logger
	s       StreamConnector
	metrics Metrics

	// LogCache
	addr string
	opts []grpc.DialOption
}

// StreamConnector reads envelopes from the the logs provider.
type StreamConnector interface {
	// Stream creates a EnvelopeStream for the given request.
	Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream
}

// NewNozzle creates a new Nozzle.
func NewNozzle(c StreamConnector, logCacheAddr string, opts ...NozzleOption) *Nozzle {
	n := &Nozzle{
		s:       c,
		addr:    logCacheAddr,
		opts:    []grpc.DialOption{grpc.WithInsecure()},
		log:     log.New(ioutil.Discard, "", 0),
		metrics: nopMetrics{},
	}

	for _, o := range opts {
		o(n)
	}

	return n
}

// NozzleOption configures a Nozzle.
type NozzleOption func(*Nozzle)

// WithNozzleLogger returns a NozzleOption that configures a nozzle's logger.
// It defaults to silent logging.
func WithNozzleLogger(l *log.Logger) NozzleOption {
	return func(n *Nozzle) {
		n.log = l
	}
}

// WithNozzleMetrics returns a NozzleOption that configures the metrics for the
// Nozzle. It will add metrics to the given map.
func WithNozzleMetrics(m Metrics) NozzleOption {
	return func(n *Nozzle) {
		n.metrics = m
	}
}

// WithNozzleDialOpts returns a NozzleOption that configures the dial options
// for dialing the LogCache. It defaults to grpc.WithInsecure().
func WithNozzleDialOpts(opts ...grpc.DialOption) NozzleOption {
	return func(n *Nozzle) {
		n.opts = opts
	}
}

// Start starts reading envelopes from the logs provider and writes them to
// LogCache. It blocks indefinately.
func (n *Nozzle) Start() {
	rx := n.s.Stream(context.Background(), n.buildBatchReq())

	conn, err := grpc.Dial(n.addr, n.opts...)
	if err != nil {
		log.Fatalf("failed to dial %s: %s", n.addr, err)
	}
	client := logcache.NewIngressClient(conn)

	ingressInc := n.metrics.NewCounter("Ingress")
	egressInc := n.metrics.NewCounter("Egress")
	errInc := n.metrics.NewCounter("Err")

	for {
		batch := rx()
		ingressInc(uint64(len(batch)))

		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.Send(ctx, &logcache.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: batch,
			},
		})

		if err != nil {
			log.Printf("failed to write envelopes: %s", err)
			errInc(1)
			continue
		}

		egressInc(uint64(len(batch)))
	}
}

func (n *Nozzle) buildBatchReq() *loggregator_v2.EgressBatchRequest {
	return &loggregator_v2.EgressBatchRequest{
		ShardId:          "log-cache",
		UsePreferredTags: true,
		Selectors: []*loggregator_v2.Selector{
			{
				Message: &loggregator_v2.Selector_Log{
					Log: &loggregator_v2.LogSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Gauge{
					Gauge: &loggregator_v2.GaugeSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Counter{
					Counter: &loggregator_v2.CounterSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Timer{
					Timer: &loggregator_v2.TimerSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Event{
					Event: &loggregator_v2.EventSelector{},
				},
			},
		},
	}
}
