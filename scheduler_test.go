package logcache_test

import (
	"net"
	"sync"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var _ = Describe("Scheduler", func() {
	var (
		s *logcache.Scheduler

		logCacheSpy1 *spyOrchestration
		logCacheSpy2 *spyOrchestration

		groupSpy1 *spyOrchestration
		groupSpy2 *spyOrchestration
	)

	BeforeEach(func() {
		logCacheSpy1 = startSpyOrchestration()
		logCacheSpy2 = startSpyOrchestration()
		groupSpy1 = startSpyOrchestration()
		groupSpy2 = startSpyOrchestration()

		s = logcache.NewScheduler(
			[]string{
				logCacheSpy1.lis.Addr().String(),
				logCacheSpy2.lis.Addr().String(),
			},
			[]string{
				groupSpy1.lis.Addr().String(),
				groupSpy2.lis.Addr().String(),
			},
			logcache.WithSchedulerInterval(time.Millisecond),
			logcache.WithSchedulerCount(7),
		)
	})

	Describe("Log Cache Ranges", func() {
		It("schedules the ranges evenly across the nodes", func() {
			s.Start()
			Eventually(logCacheSpy1.ReqCount, 2).Should(BeNumerically(">=", 50))
			Eventually(logCacheSpy2.ReqCount, 2).Should(BeNumerically(">=", 50))

			reqs := append(logCacheSpy1.AddReqs(), logCacheSpy2.AddReqs()...)

			count := 7

			maxHash := uint64(18446744073709551615)
			x := maxHash / uint64(count)
			var start uint64

			for i := 0; i < count; i++ {
				if i == count-1 {
					Expect(reqs).To(ContainElement(&rpc.Range{
						Start: start,
						End:   maxHash,
					}))
					break
				}
				Expect(reqs).To(ContainElement(&rpc.Range{
					Start: start,
					End:   start + x,
				}))

				start += x + 1
			}
		})

		It("reads the term from the cluster to set the next term", func() {
			logCacheSpy1.listRanges = []*rpc.Range{
				{
					Term: 99,
				},
			}

			logCacheSpy2.listRanges = []*rpc.Range{
				{
					Term: 100,
				},
			}

			s.Start()

			Eventually(logCacheSpy1.ReqCount).ShouldNot(BeZero())
			Expect(logCacheSpy1.AddReqs()[0].Term).To(Equal(uint64(101)))
		})

		It("sets the range table after listing all the nodes", func() {
			s.Start()

			Eventually(logCacheSpy1.SetCount).ShouldNot(BeZero())
			Eventually(logCacheSpy2.SetCount).ShouldNot(BeZero())

			Expect(logCacheSpy1.SetReqs()[0].Ranges).To(HaveLen(2))
			Expect(logCacheSpy2.SetReqs()[0].Ranges).To(HaveLen(2))
		})
	})

	Describe("Group Ranges", func() {
		It("schedules the ranges evenly across the nodes", func() {
			s.Start()
			Eventually(groupSpy1.ReqCount, 2).Should(BeNumerically(">=", 50))
			Eventually(groupSpy2.ReqCount, 2).Should(BeNumerically(">=", 50))

			reqs := append(groupSpy1.AddReqs(), groupSpy2.AddReqs()...)

			count := 7

			maxHash := uint64(18446744073709551615)
			x := maxHash / uint64(count)
			var start uint64

			for i := 0; i < count; i++ {
				if i == count-1 {
					Expect(reqs).To(ContainElement(&rpc.Range{
						Start: start,
						End:   maxHash,
					}))
					break
				}
				Expect(reqs).To(ContainElement(&rpc.Range{
					Start: start,
					End:   start + x,
				}))

				start += x + 1
			}
		})

		It("reads the term from the cluster to set the next term", func() {
			groupSpy1.listRanges = []*rpc.Range{
				{
					Term: 99,
				},
			}

			groupSpy2.listRanges = []*rpc.Range{
				{
					Term: 100,
				},
			}

			s.Start()

			Eventually(groupSpy1.ReqCount).ShouldNot(BeZero())
			Expect(groupSpy1.AddReqs()[0].Term).To(Equal(uint64(101)))
		})

		It("sets the range table after listing all the nodes", func() {
			s.Start()

			Eventually(groupSpy1.SetCount).ShouldNot(BeZero())
			Eventually(groupSpy2.SetCount).ShouldNot(BeZero())

			Expect(groupSpy1.SetReqs()[0].Ranges).To(HaveLen(2))
			Expect(groupSpy2.SetReqs()[0].Ranges).To(HaveLen(2))
		})
	})
})

type spyOrchestration struct {
	mu      sync.Mutex
	lis     net.Listener
	addReqs []*rpc.Range
	addErr  error

	listReqs   []*rpc.ListRangesRequest
	listRanges []*rpc.Range
	listErr    error

	setReqs []*rpc.SetRangesRequest
}

func startSpyOrchestration() *spyOrchestration {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	s := &spyOrchestration{
		lis: lis,
	}

	go func() {
		srv := grpc.NewServer()
		rpc.RegisterOrchestrationServer(srv, s)
		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}()

	return s
}

func (s *spyOrchestration) AddRange(ctx context.Context, r *rpc.AddRangeRequest) (*rpc.AddRangeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.addReqs = append(s.addReqs, r.Range)
	return &rpc.AddRangeResponse{}, s.addErr
}

func (s *spyOrchestration) AddReqs() []*rpc.Range {
	s.mu.Lock()
	defer s.mu.Unlock()

	addReqs := make([]*rpc.Range, len(s.addReqs))
	copy(addReqs, s.addReqs)
	return addReqs
}

func (s *spyOrchestration) ReqCount() int {
	return len(s.AddReqs())
}

func (s *spyOrchestration) ListRanges(ctx context.Context, r *rpc.ListRangesRequest) (*rpc.ListRangesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.listReqs = append(s.listReqs, r)
	return &rpc.ListRangesResponse{
		Ranges: s.listRanges,
	}, s.listErr
}

func (s *spyOrchestration) SetRanges(ctx context.Context, r *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setReqs = append(s.setReqs, r)
	return &rpc.SetRangesResponse{}, nil
}

func (s *spyOrchestration) SetReqs() []*rpc.SetRangesRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	setReqs := make([]*rpc.SetRangesRequest, len(s.setReqs))
	copy(setReqs, s.setReqs)
	return setReqs
}

func (s *spyOrchestration) SetCount() int {
	return len(s.SetReqs())
}
