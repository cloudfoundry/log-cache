package logcache

import (
	"io/ioutil"
	"log"
	"sync"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	orchestrator "code.cloudfoundry.org/go-orchestrator"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Scheduler manages the routes of the Log Cache nodes.
type Scheduler struct {
	log      *log.Logger
	metrics  Metrics
	interval time.Duration
	count    int
	orch     *orchestrator.Orchestrator
	dialOpts []grpc.DialOption

	clients []clientInfo
}

// NewScheduler returns a new Scheduler. Addrs are the addresses of the Cache
// nodes.
func NewScheduler(addrs []string, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		log:      log.New(ioutil.Discard, "", 0),
		metrics:  nopMetrics{},
		interval: time.Minute,
		count:    100,
		dialOpts: []grpc.DialOption{grpc.WithInsecure()},
	}

	for _, o := range opts {
		o(s)
	}

	s.orch = orchestrator.New(&comm{
		log: s.log,
	})

	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, s.dialOpts...)
		if err != nil {
			s.log.Panic(err)
		}
		s.clients = append(s.clients, clientInfo{l: rpc.NewOrchestrationClient(conn), addr: addr})
	}

	return s
}

// SchedulerOption configures a Scheduler.
type SchedulerOption func(*Scheduler)

// WithSchedulerLogger returns a SchedulerOption that configures the logger
// used for the Scheduler. Defaults to silent logger.
func WithSchedulerLogger(l *log.Logger) SchedulerOption {
	return func(s *Scheduler) {
		s.log = l
	}
}

// WithSchedulerMetrics returns a SchedulerOption that configures the metrics
// for the Scheduler. It will add metrics to the given map.
func WithSchedulerMetrics(m Metrics) SchedulerOption {
	return func(s *Scheduler) {
		s.metrics = m
	}
}

// WithSchedulerInterval returns a SchedulerOption that configures the
// interval for terms to take place. It defaults to a minute.
func WithSchedulerInterval(interval time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.interval = interval
	}
}

// WithSchedulerCount returns a SchedulerOption that configures the
// number of ranges to manage. Defaults to 100.
func WithSchedulerCount(count int) SchedulerOption {
	return func(s *Scheduler) {
		s.count = count
	}
}

// WithSchedulerDialOpts are the gRPC options used to dial peer Log Cache
// nodes. It defaults to WithInsecure().
func WithSchedulerDialOpts(opts ...grpc.DialOption) SchedulerOption {
	return func(s *Scheduler) {
		s.dialOpts = opts
	}
}

// Start starts the scheduler. It does not block.
func (s *Scheduler) Start() {
	for _, lc := range s.clients {
		s.orch.AddWorker(lc)
	}

	maxHash := uint64(18446744073709551615)
	x := maxHash / uint64(s.count)
	var start uint64

	for i := 0; i < s.count-1; i++ {
		s.orch.AddTask(rpc.Range{
			Start: start,
			End:   start + x,
		},
			orchestrator.WithTaskInstances(5),
		)
		start += x + 1
	}

	s.orch.AddTask(rpc.Range{
		Start: start,
		End:   maxHash,
	},
		orchestrator.WithTaskInstances(5),
	)

	go func() {
		for range time.Tick(s.interval) {
			// Apply changes
			s.orch.NextTerm(context.Background())

			// Run again before setting remote tables to allow the
			// orchestrator to go and query for updates.
			s.orch.NextTerm(context.Background())
			s.setRemoteTables(s.convertWorkerState(s.orch.LastActual()))
		}
	}()
}

func (s *Scheduler) setRemoteTables(m map[string]*rpc.Ranges) {
	req := &rpc.SetRangesRequest{
		Ranges: m,
	}

	for _, lc := range s.clients {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := lc.l.SetRanges(ctx, req); err != nil {
			s.log.Printf("failed to set remote table: %s", err)
			continue
		}
	}
}

func (s *Scheduler) convertWorkerState(ws []orchestrator.WorkerState) map[string]*rpc.Ranges {
	m := make(map[string]*rpc.Ranges)
	for _, w := range ws {
		var ranges []*rpc.Range
		for _, t := range w.Tasks {
			rr := t.(rpc.Range)
			ranges = append(ranges, &rr)
		}

		m[w.Name.(clientInfo).addr] = &rpc.Ranges{
			Ranges: ranges,
		}
	}

	return m
}

type clientInfo struct {
	l    rpc.OrchestrationClient
	addr string
}

type comm struct {
	mu   sync.Mutex
	term uint64
	log  *log.Logger
}

// List implements orchestrator.Communicator.
func (c *comm) List(ctx context.Context, worker interface{}) ([]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lc := worker.(clientInfo)
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	resp, err := lc.l.ListRanges(ctx, &rpc.ListRangesRequest{})
	if err != nil {
		c.log.Printf("failed to list ranges from %s: %s", lc.addr, err)
		return nil, err
	}

	var results []interface{}
	for _, r := range resp.Ranges {
		if c.term <= r.Term {
			c.term = r.Term + 1
		}

		// The orch algorithm only knows about term 0, therefore we need to
		// ditch the term value.
		r.Term = 0
		results = append(results, *r)
	}

	return results, nil
}

// Add implements orchestrator.Communicator.
func (c *comm) Add(ctx context.Context, worker interface{}, task interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lc := worker.(clientInfo)
	r := task.(rpc.Range)
	r.Term = c.term
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	_, err := lc.l.AddRange(ctx, &rpc.AddRangeRequest{
		Range: &r,
	})

	if err != nil {
		c.log.Printf("failed to add range to %s: %s", lc.addr, err)
		return err
	}

	return nil
}

// Remvoe implements orchestrator.Communicator.
func (c *comm) Remove(ctx context.Context, worker interface{}, task interface{}) error {
	// NOP
	return nil
}
