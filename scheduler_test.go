package logcache_test

import (
	"net"
	"sync"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
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

		leadershipSpy *spyLeadership
	)

	BeforeEach(func() {
		logCacheSpy1 = startSpyOrchestration()
		logCacheSpy2 = startSpyOrchestration()
		groupSpy1 = startSpyOrchestration()
		groupSpy2 = startSpyOrchestration()
		leadershipSpy = newSpyLeadership()
		leadershipSpy.result = true

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
			logcache.WithSchedulerReplicationFactor(2),
			logcache.WithSchedulerLeadership(leadershipSpy.IsLeader),
		)
	})

	DescribeTable("schedules the ranges evenly across the nodes", func(spyF1, spyF2 func() *spyOrchestration) {
		spy1 := spyF1()
		spy2 := spyF2()
		count := 7

		maxHash := uint64(18446744073709551615)
		x := maxHash / uint64(count)
		var start uint64

		// Populate spy1 with all the ranges. The scheduler should leave spy1
		// alone.
		for i := 0; i < count; i++ {
			if i == count-1 {
				spy1.listRanges = append(spy1.listRanges, &rpc.Range{
					Start: start,
					End:   maxHash,
				})

				break
			}

			spy1.listRanges = append(spy1.listRanges, &rpc.Range{
				Start: start,
				End:   start + x,
			})

			start += x + 1
		}

		s.Start()
		Eventually(spy2.ReqCount, 2).Should(BeNumerically(">=", 50))

		m := make(map[rpc.Range]int)

		for _, r := range spy2.AddReqs() {
			m[*r]++
		}

		start = 0
		for i := 0; i < count; i++ {
			if i == count-1 {
				Expect(m).To(HaveKey(rpc.Range{
					Start: start,
					End:   maxHash,
				}))
				break
			}

			Expect(m).To(HaveKey(rpc.Range{
				Start: start,
				End:   start + x,
			}))

			start += x + 1
		}

		Expect(spy1.AddReqs()).To(BeEmpty())
		Expect(spy1.RemoveReqs()).To(BeEmpty())
	},
		// Why are these functions? Go is eager and therefore if we passed the
		// spy in directly, the BeforeEach would not have a chance to
		// initialze them and therefore they would just be nil.
		Entry("LogCache Ranges",
			func() *spyOrchestration { return logCacheSpy1 },
			func() *spyOrchestration { return logCacheSpy2 },
		),
		Entry("GroupReader Ranges",
			func() *spyOrchestration { return groupSpy1 },
			func() *spyOrchestration { return groupSpy2 },
		),
	)

	Describe("Log Cache Ranges", func() {
		It("sets the range table after listing all the nodes", func() {
			s.Start()

			Eventually(logCacheSpy1.SetCount).ShouldNot(BeZero())
			Eventually(logCacheSpy2.SetCount).ShouldNot(BeZero())

			Expect(logCacheSpy1.SetReqs()[0].Ranges).To(HaveLen(2))
			Expect(logCacheSpy2.SetReqs()[0].Ranges).To(HaveLen(2))
		})

		It("rebalances ranges", func() {
			count := 7

			maxHash := uint64(18446744073709551615)
			x := maxHash / uint64(count)
			var start uint64

			for i := 0; i < count; i++ {
				if i == count-1 {
					logCacheSpy1.listRanges = append(logCacheSpy1.listRanges, &rpc.Range{
						Start: start,
						End:   maxHash,
					})

					break
				}

				logCacheSpy1.listRanges = append(logCacheSpy1.listRanges, &rpc.Range{
					Start: start,
					End:   start + x,
				})

				start += x + 1
			}

			s := logcache.NewScheduler(
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

			s.Start()

			Eventually(func() int {
				return len(logCacheSpy1.RemoveReqs())
			}).Should(BeNumerically(">=", 3))

			Eventually(func() int {
				return len(logCacheSpy2.AddReqs())
			}).Should(BeNumerically(">=", 3))
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

		It("sets the range table after listing all the nodes", func() {
			s.Start()

			Eventually(groupSpy1.SetCount).ShouldNot(BeZero())
			Eventually(groupSpy2.SetCount).ShouldNot(BeZero())

			Expect(groupSpy1.SetReqs()[0].Ranges).To(HaveLen(2))
			Expect(groupSpy2.SetReqs()[0].Ranges).To(HaveLen(2))
		})
	})

	Describe("leader and follower", func() {
		It("does not schedule until it is the leader", func() {
			leadershipSpy.result = false
			s.Start()

			Consistently(groupSpy1.SetCount).Should(BeZero())
			Consistently(groupSpy2.SetCount).Should(BeZero())

			Consistently(groupSpy1.AddReqs).Should(BeEmpty())
			Consistently(groupSpy2.AddReqs).Should(BeEmpty())

			Consistently(groupSpy1.RemoveReqs).Should(BeEmpty())
			Consistently(groupSpy2.RemoveReqs).Should(BeEmpty())

			leadershipSpy.result = true
			Eventually(groupSpy1.SetCount).ShouldNot(BeZero())
			Eventually(groupSpy2.SetCount).ShouldNot(BeZero())

			Consistently(groupSpy1.AddReqs).ShouldNot(BeEmpty())
			Consistently(groupSpy2.AddReqs).ShouldNot(BeEmpty())
		})
	})
})

type spyOrchestration struct {
	mu      sync.Mutex
	lis     net.Listener
	addReqs []*rpc.Range
	addErr  error

	removeReqs []*rpc.Range
	removeErr  error

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

func (s *spyOrchestration) RemoveRange(ctx context.Context, r *rpc.RemoveRangeRequest) (*rpc.RemoveRangeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removeReqs = append(s.removeReqs, r.Range)

	return &rpc.RemoveRangeResponse{}, s.removeErr
}

func (s *spyOrchestration) RemoveReqs() []*rpc.Range {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.removeReqs
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

type spyLeadership struct {
	result bool
}

func newSpyLeadership() *spyLeadership {
	return &spyLeadership{}
}

func (s *spyLeadership) IsLeader() bool {
	return s.result
}
