package groups_test

import (
	"context"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/groups"
	"code.cloudfoundry.org/log-cache/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Manager", func() {
	var (
		m              *groups.Manager
		spyDataStorage *spyDataStorage
	)

	BeforeEach(func() {
		spyDataStorage = newSpyDataStorage()
		m = groups.NewManager(spyDataStorage, time.Hour)
	})

	It("keeps track of source IDs for groups", func() {
		r, err := m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())
		Expect(spyDataStorage.addNames).To(ContainElement("a"))

		// Add sourceID 1 twice to ensure it is only reported once
		r, err = m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())

		r, err = m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())

		r, err = m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "b",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())
		Expect(spyDataStorage.addNames).To(ContainElement("b"))

		resp, err := m.Group(context.Background(), &logcache_v1.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.SourceIds).To(ConsistOf("1", "2"))

		rr, err := m.RemoveFromGroup(context.Background(), &logcache_v1.RemoveFromGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rr).ToNot(BeNil())
		Expect(spyDataStorage.removes).To(ConsistOf("1"))
		Expect(spyDataStorage.removeNames).To(ContainElement("a"))

		resp, err = m.Group(context.Background(), &logcache_v1.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.SourceIds).To(ConsistOf("2"))

		rr, err = m.RemoveFromGroup(context.Background(), &logcache_v1.RemoveFromGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rr).ToNot(BeNil())

		Expect(m.ListGroups()).To(ConsistOf("b"))
	})

	It("keeps track of requester IDs for a group", func() {
		_, err := m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.Read(context.Background(), &logcache_v1.GroupReadRequest{
			Name:        "a",
			RequesterId: 1,
		})
		Expect(err).ToNot(HaveOccurred())

		// Do RequestId 1 twice to ensure it is only reported once.
		_, err = m.Read(context.Background(), &logcache_v1.GroupReadRequest{
			Name:        "a",
			RequesterId: 1,
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.Read(context.Background(), &logcache_v1.GroupReadRequest{
			Name:        "a",
			RequesterId: 2,
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := m.Group(context.Background(), &logcache_v1.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.RequesterIds).To(ConsistOf(uint64(1), uint64(2)))

		Expect(spyDataStorage.addReqNames).To(ContainElement("a"))
		Expect(spyDataStorage.addReqIDs).To(ConsistOf(uint64(1), (uint64(2))))

		Expect(spyDataStorage.getRequestIDs).To(ContainElement(uint64(1)))
		Expect(spyDataStorage.getRequestIDs).To(ContainElement(uint64(2)))
	})

	It("expires source IDs from group", func() {
		// Shadow m to protect against race conditions
		m := groups.NewManager(spyDataStorage, 10*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			defer GinkgoRecover()
			for range time.Tick(100 * time.Microsecond) {
				_, err := m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
					Name:     "a",
					SourceId: "1",
				})
				Expect(err).ToNot(HaveOccurred())

				if ctx.Err() != nil {
					return
				}
			}
		}()

		go func() {
			defer GinkgoRecover()
			for range time.Tick(100 * time.Microsecond) {
				_, err := m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
					Name:     "a",
					SourceId: "2",
				})
				Expect(err).ToNot(HaveOccurred())
			}
		}()

		f := func() int {
			r, err := m.Group(context.Background(), &logcache_v1.GroupRequest{Name: "a"})
			Expect(err).ToNot(HaveOccurred())
			return len(r.SourceIds)
		}

		Eventually(f).Should(Equal(2))
		cancel()
		Eventually(f).Should(Equal(1))
		Consistently(f).Should(Equal(1))
	})

	It("expires requester IDs after a given time", func() {
		// Shadow m to protect against race conditions
		m := groups.NewManager(spyDataStorage, 10*time.Millisecond)

		go func() {
			for range time.Tick(time.Microsecond) {
				_, err := m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
					Name:     "a",
					SourceId: "1",
				})
				Expect(err).ToNot(HaveOccurred())
			}
		}()

		f := func() error {
			_, err := m.Read(context.Background(), &logcache_v1.GroupReadRequest{
				Name:        "a",
				RequesterId: 1,
			})
			return err
		}
		Eventually(f).ShouldNot(HaveOccurred())

		ff := func() []uint64 {
			_, err := m.Read(context.Background(), &logcache_v1.GroupReadRequest{
				Name:        "a",
				RequesterId: 2,
			})
			Expect(err).ToNot(HaveOccurred())

			resp, err := m.Group(context.Background(), &logcache_v1.GroupRequest{
				Name: "a",
			})
			Expect(err).ToNot(HaveOccurred())
			return resp.RequesterIds
		}
		Eventually(ff, "1s", "100us").Should(ConsistOf(uint64(2)))

		Expect(spyDataStorage.removeReqNames).To(ConsistOf("a"))
		Expect(spyDataStorage.removeReqIDs).To(ConsistOf(uint64(1)))
	})

	It("reads from a known group", func() {
		spyDataStorage.getResult = []*loggregator_v2.Envelope{
			{Timestamp: 1},
			{Timestamp: 2},
		}

		_, err := m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := m.Read(context.Background(), &logcache_v1.GroupReadRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.Envelopes.Batch).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 1},
			&loggregator_v2.Envelope{Timestamp: 2},
		))

		Expect(spyDataStorage.adds).To(ConsistOf("1", "2"))
	})

	It("defaults startTime to 0, endTime to now, envelopeType to nil and limit to 100", func() {
		m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})

		m.Read(context.Background(), &logcache_v1.GroupReadRequest{
			Name: "a",
		})

		Expect(spyDataStorage.getStarts).To(ContainElement(int64(0)))
		Expect(spyDataStorage.getEnds).To(ContainElement(BeNumerically("~", time.Now().UnixNano(), 3*time.Second)))
		Expect(spyDataStorage.getLimits).To(ContainElement(100))
		Expect(spyDataStorage.getEnvelopeTypes).To(ContainElement(BeNil()))
	})

	It("returns an error for unknown groups", func() {
		_, err := m.Read(context.Background(), &logcache_v1.GroupReadRequest{
			Name: "unknown-name",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.NotFound))
	})

	It("rejects empty group names and source IDs or either that are too long", func() {
		_, err := m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "",
			SourceId: "1",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     strings.Repeat("x", 129),
			SourceId: "1",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: "",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
			Name:     "a",
			SourceId: strings.Repeat("x", 129),
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("survives the race detector", func() {
		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(3)
		go func(m *groups.Manager) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.AddToGroup(context.Background(), &logcache_v1.AddToGroupRequest{
					Name:     "a",
					SourceId: "1",
				})
			}
		}(m)

		go func(m *groups.Manager) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.Read(context.Background(), &logcache_v1.GroupReadRequest{
					Name: "a",
				})
			}
		}(m)

		go func(m *groups.Manager) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.ListGroups()
			}
		}(m)

		for i := 0; i < 100; i++ {
			m.RemoveFromGroup(context.Background(), &logcache_v1.RemoveFromGroupRequest{
				Name:     "a",
				SourceId: "1",
			})
		}
	})
})

type spyDataStorage struct {
	adds     []string
	addNames []string

	removes     []string
	removeNames []string

	addReqNames []string
	addReqIDs   []uint64

	removeReqNames []string
	removeReqIDs   []uint64

	getNames         []string
	getStarts        []int64
	getEnds          []int64
	getLimits        []int
	getEnvelopeTypes []store.EnvelopeType
	getDescending    []bool
	getRequestIDs    []uint64
	getResult        []*loggregator_v2.Envelope
}

func newSpyDataStorage() *spyDataStorage {
	return &spyDataStorage{}
}

func (s *spyDataStorage) Get(
	name string,
	start time.Time,
	end time.Time,
	envelopeType store.EnvelopeType,
	limit int,
	descending bool,
	requesterID uint64,
) []*loggregator_v2.Envelope {
	s.getNames = append(s.getNames, name)
	s.getStarts = append(s.getStarts, start.UnixNano())
	s.getEnds = append(s.getEnds, end.UnixNano())
	s.getLimits = append(s.getLimits, limit)
	s.getEnvelopeTypes = append(s.getEnvelopeTypes, envelopeType)
	s.getDescending = append(s.getDescending, descending)
	s.getRequestIDs = append(s.getRequestIDs, requesterID)

	return s.getResult
}

func (s *spyDataStorage) Add(name, sourceID string) {
	s.addNames = append(s.addNames, name)
	s.adds = append(s.adds, sourceID)
}

func (s *spyDataStorage) AddRequester(name string, requesterID uint64) {
	s.addReqNames = append(s.addReqNames, name)
	s.addReqIDs = append(s.addReqIDs, requesterID)
}

func (s *spyDataStorage) Remove(name, sourceID string) {
	s.removeNames = append(s.removeNames, name)
	s.removes = append(s.removes, sourceID)
}

func (s *spyDataStorage) RemoveRequester(name string, requesterID uint64) {
	s.removeReqNames = append(s.removeReqNames, name)
	s.removeReqIDs = append(s.removeReqIDs, requesterID)
}
