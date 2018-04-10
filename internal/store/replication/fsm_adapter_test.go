package replication_test

import (
	"bytes"
	"io/ioutil"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store/replication"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Assert that LogStore implements raft.FSM
var _ raft.FSM = &replication.FSMAdapter{}

var _ = Describe("FSMAdapter", func() {
	var (
		spySubStore        *spySubStore
		spyMarshallerCache *spyMarshallerCache
		a                  *replication.FSMAdapter
	)

	BeforeEach(func() {
		spySubStore = newSpySubStore()
		spyMarshallerCache = newSpyMarshallerCache()

		a = replication.NewFSMAdapter(spySubStore, spyMarshallerCache)
	})

	It("writes data from the cache to the store", func() {
		e := &loggregator_v2.Envelope{Timestamp: 99, SourceId: "a"}
		data, err := proto.Marshal(e)
		Expect(err).ToNot(HaveOccurred())

		spyMarshallerCache.getResult = e

		a.Apply(&raft.Log{Data: data})

		Expect(spySubStore.putEnvelopes).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 99, SourceId: "a"},
		))

		Expect(spyMarshallerCache.getBytes[0]).To(Equal(data))

		Expect(spySubStore.putIndices).To(ConsistOf("a"))
	})

	It("writes marshalled data when cache misses", func() {
		e := &loggregator_v2.Envelope{Timestamp: 99, SourceId: "a"}
		data, err := proto.Marshal(e)
		Expect(err).ToNot(HaveOccurred())

		spyMarshallerCache.getResult = nil
		a.Apply(&raft.Log{Data: data})

		Expect(spySubStore.putEnvelopes).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 99, SourceId: "a"},
		))

		Expect(spyMarshallerCache.getBytes[0]).To(Equal(data))

		Expect(spySubStore.putIndices).To(ConsistOf("a"))
	})

	It("ignores data that is not an envelope", func() {
		a.Apply(&raft.Log{Data: []byte("some-data")})
		Expect(spySubStore.putEnvelopes).To(BeEmpty())
	})

	It("builds snapshots that will restore data to the SubStore", func() {
		spySubStore.metaResults = map[string]logcache_v1.MetaInfo{
			"a": logcache_v1.MetaInfo{},
			"b": logcache_v1.MetaInfo{},
		}

		spySubStore.getResults = [][]*loggregator_v2.Envelope{
			{
				{Timestamp: 1},
				{Timestamp: 2},
			},
			{
				{Timestamp: 4},
				{Timestamp: 6},
			},
			{},
		}

		snapshot, err := a.Snapshot()
		Expect(err).ToNot(HaveOccurred())

		spySnapshotSink := newSpySnapshotSink()
		Expect(snapshot.Persist(spySnapshotSink)).To(Succeed())

		// Ensure the algorithm walked
		Expect(spySubStore.getStarts).To(HaveLen(4))
		Expect(spySubStore.getStarts[0].UnixNano()).To(Equal(int64(time.Unix(0, 0).UnixNano())))
		Expect(spySubStore.getStarts[1].UnixNano()).To(Equal(int64(time.Unix(0, 3).UnixNano())))
		Expect(spySubStore.getStarts[2].UnixNano()).To(Equal(int64(time.Unix(0, 7).UnixNano())))

		Expect(spySubStore.getStarts[0].UnixNano()).To(Equal(int64(time.Unix(0, 0).UnixNano())))

		Expect(spySubStore.getIndices).To(And(ContainElement("a"), ContainElement("b")))

		Expect(
			a.Restore(ioutil.NopCloser(bytes.NewReader(spySnapshotSink.data))),
		).To(Succeed())

		Expect(spySubStore.putEnvelopes).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 1},
			&loggregator_v2.Envelope{Timestamp: 2},
			&loggregator_v2.Envelope{Timestamp: 4},
			&loggregator_v2.Envelope{Timestamp: 6},
		))
	})
})

type spySnapshotSink struct {
	data        []byte
	writeErr    error
	closeCalled bool
}

func newSpySnapshotSink() *spySnapshotSink {
	return &spySnapshotSink{}
}

func (s *spySnapshotSink) Write(p []byte) (n int, err error) {
	s.data = append(s.data, p...)
	return len(p), s.writeErr
}

func (s *spySnapshotSink) Close() error {
	s.closeCalled = true
	return nil
}

func (s *spySnapshotSink) ID() string {
	panic("not implemented")
}

func (s *spySnapshotSink) Cancel() error {
	panic("not implemented")
}
