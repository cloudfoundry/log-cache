package groups_test

import (
	"context"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/groups"
	"code.cloudfoundry.org/log-cache/internal/store"

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
		m = groups.NewManager(spyDataStorage)
	})

	It("keeps track of source IDs for groups", func() {
		r, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())
		Expect(spyDataStorage.addNames).To(ContainElement("a"))

		r, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())

		r, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "b",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())
		Expect(spyDataStorage.addNames).To(ContainElement("b"))

		resp, err := m.Group(context.Background(), &logcache.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.SourceIds).To(ConsistOf("1", "2"))

		rr, err := m.RemoveFromGroup(context.Background(), &logcache.RemoveFromGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rr).ToNot(BeNil())
		Expect(spyDataStorage.removes).To(ConsistOf("1"))
		Expect(spyDataStorage.removeNames).To(ContainElement("a"))

		resp, err = m.Group(context.Background(), &logcache.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.SourceIds).To(ConsistOf("2"))

		rr, err = m.RemoveFromGroup(context.Background(), &logcache.RemoveFromGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rr).ToNot(BeNil())
	})

	It("reads from a known group", func() {
		spyDataStorage.getResult = []*loggregator_v2.Envelope{
			{Timestamp: 1},
			{Timestamp: 2},
		}

		_, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := m.Read(context.Background(), &logcache.GroupReadRequest{
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
		m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})

		m.Read(context.Background(), &logcache.GroupReadRequest{
			Name: "a",
		})

		Expect(spyDataStorage.getStarts).To(ContainElement(int64(0)))
		Expect(spyDataStorage.getEnds).To(ContainElement(BeNumerically("~", time.Now().UnixNano(), 3*time.Second)))
		Expect(spyDataStorage.getLimits).To(ContainElement(100))
		Expect(spyDataStorage.getEnvelopeTypes).To(ContainElement(BeNil()))
	})

	It("returns an error for unknown groups", func() {
		_, err := m.Read(context.Background(), &logcache.GroupReadRequest{
			Name: "unknown-name",
		})
		Expect(err).To(HaveOccurred())
	})

	It("survives the race detector", func() {
		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(2)
		go func(m *groups.Manager) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
					Name:     "a",
					SourceId: "1",
				})
			}
		}(m)

		go func(m *groups.Manager) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.Read(context.Background(), &logcache.GroupReadRequest{
					Name: "a",
				})
			}
		}(m)

		for i := 0; i < 100; i++ {
			m.RemoveFromGroup(context.Background(), &logcache.RemoveFromGroupRequest{
				Name:     "a",
				SourceId: "1",
			})
		}
	})
})

type spyDataStorage struct {
	adds        []string
	addNames    []string
	removes     []string
	removeNames []string

	getNames         []string
	getStarts        []int64
	getEnds          []int64
	getLimits        []int
	getEnvelopeTypes []store.EnvelopeType
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
) []*loggregator_v2.Envelope {
	s.getNames = append(s.getNames, name)
	s.getStarts = append(s.getStarts, start.UnixNano())
	s.getEnds = append(s.getEnds, end.UnixNano())
	s.getLimits = append(s.getLimits, limit)
	s.getEnvelopeTypes = append(s.getEnvelopeTypes, envelopeType)

	return s.getResult
}

func (s *spyDataStorage) Add(name, sourceID string) {
	s.addNames = append(s.addNames, name)
	s.adds = append(s.adds, sourceID)
}

func (s *spyDataStorage) Remove(name, sourceID string) {
	s.removeNames = append(s.removeNames, name)
	s.removes = append(s.removes, sourceID)
}
