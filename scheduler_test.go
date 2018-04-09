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
		leadershipSpy = newSpyLeadership(true)

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
		Eventually(spy2.reqCount, 5).Should(BeNumerically(">=", 50))

		m := make(map[rpc.Range]int)

		for _, r := range spy2.addReqs() {
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

		Expect(spy1.addReqs()).To(BeEmpty())
		Expect(spy1.removeReqs()).To(BeEmpty())
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

			Eventually(logCacheSpy1.setCount).ShouldNot(BeZero())
			Eventually(logCacheSpy2.setCount).ShouldNot(BeZero())

			Expect(logCacheSpy1.setReqs()[0].Ranges).To(HaveLen(2))
			Expect(logCacheSpy2.setReqs()[0].Ranges).To(HaveLen(2))
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
				return len(logCacheSpy1.removeReqs())
			}).Should(BeNumerically(">=", 3))

			Eventually(func() int {
				return len(logCacheSpy2.addReqs())
			}).Should(BeNumerically(">=", 3))
		})
	})

	Describe("Group Ranges", func() {
		It("schedules the ranges evenly across the nodes", func() {
			s.Start()
			Eventually(groupSpy1.reqCount, 2).Should(BeNumerically(">=", 50))
			Eventually(groupSpy2.reqCount, 2).Should(BeNumerically(">=", 50))

			reqs := append(groupSpy1.addReqs(), groupSpy2.addReqs()...)

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

			Eventually(groupSpy1.setCount).ShouldNot(BeZero())
			Eventually(groupSpy2.setCount).ShouldNot(BeZero())

			Expect(groupSpy1.setReqs()[0].Ranges).To(HaveLen(2))
			Expect(groupSpy2.setReqs()[0].Ranges).To(HaveLen(2))
		})
	})

	Describe("leader and follower", func() {
		It("does not schedule until it is the leader", func() {
			leadershipSpy.setResult(false)
			s.Start()

			Consistently(groupSpy1.setCount).Should(BeZero())
			Consistently(groupSpy2.setCount).Should(BeZero())

			Consistently(groupSpy1.addReqs).Should(BeEmpty())
			Consistently(groupSpy2.addReqs).Should(BeEmpty())

			Consistently(groupSpy1.removeReqs).Should(BeEmpty())
			Consistently(groupSpy2.removeReqs).Should(BeEmpty())

			leadershipSpy.setResult(true)
			Eventually(groupSpy1.setCount).ShouldNot(BeZero())
			Eventually(groupSpy2.setCount).ShouldNot(BeZero())

			Consistently(groupSpy1.addReqs).ShouldNot(BeEmpty())
			Consistently(groupSpy2.addReqs).ShouldNot(BeEmpty())
		})
	})
})

type spyOrchestration struct {
	mu       sync.Mutex
	lis      net.Listener
	addReqs_ []*rpc.Range
	addErr   error

	removeReqs_ []*rpc.Range
	removeErr   error

	listReqs   []*rpc.ListRangesRequest
	listRanges []*rpc.Range
	listErr    error

	setReqs_ []*rpc.SetRangesRequest
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

	s.addReqs_ = append(s.addReqs_, r.Range)
	return &rpc.AddRangeResponse{}, s.addErr
}

func (s *spyOrchestration) RemoveRange(ctx context.Context, r *rpc.RemoveRangeRequest) (*rpc.RemoveRangeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removeReqs_ = append(s.removeReqs_, r.Range)

	return &rpc.RemoveRangeResponse{}, s.removeErr
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

	s.setReqs_ = append(s.setReqs_, r)
	return &rpc.SetRangesResponse{}, nil
}

func (s *spyOrchestration) addReqs() []*rpc.Range {
	s.mu.Lock()
	defer s.mu.Unlock()

	addReqs := make([]*rpc.Range, len(s.addReqs_))
	copy(addReqs, s.addReqs_)
	return addReqs
}

func (s *spyOrchestration) reqCount() int {
	return len(s.addReqs())
}

func (s *spyOrchestration) removeReqs() []*rpc.Range {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.removeReqs_
}

func (s *spyOrchestration) setReqs() []*rpc.SetRangesRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	setReqs := make([]*rpc.SetRangesRequest, len(s.setReqs_))
	copy(setReqs, s.setReqs_)
	return setReqs
}

func (s *spyOrchestration) setCount() int {
	return len(s.setReqs())
}

type spyLeadership struct {
	mu     sync.Mutex
	result bool
}

func newSpyLeadership(result bool) *spyLeadership {
	return &spyLeadership{
		result: result,
	}
}

func (s *spyLeadership) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (s *spyLeadership) setResult(b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result = b
}
