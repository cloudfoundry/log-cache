package groups_test

import (
	"context"
	"sync"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/groups"

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
		m = groups.NewManager(func() groups.DataStorage { return spyDataStorage })
	})

	It("keeps track of source IDs for groups", func() {
		r, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())

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

		resp, err = m.Group(context.Background(), &logcache.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.SourceIds).To(ConsistOf("2"))

		Expect(spyDataStorage.closed).To(Equal(0))
		rr, err = m.RemoveFromGroup(context.Background(), &logcache.RemoveFromGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rr).ToNot(BeNil())
		Expect(spyDataStorage.closed).To(Equal(1))
	})

	It("reads from a known group", func() {
		spyDataStorage.result = []*loggregator_v2.Envelope{
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
	adds    []string
	removes []string
	result  []*loggregator_v2.Envelope
	closed  int
}

func newSpyDataStorage() *spyDataStorage {
	return &spyDataStorage{}
}

func (s *spyDataStorage) Next() []*loggregator_v2.Envelope {
	return s.result
}

func (s *spyDataStorage) Add(sourceID string) {
	s.adds = append(s.adds, sourceID)
}

func (s *spyDataStorage) Remove(sourceID string) {
	s.removes = append(s.removes, sourceID)
}

func (s *spyDataStorage) Close() error {
	s.closed++
	return nil
}
