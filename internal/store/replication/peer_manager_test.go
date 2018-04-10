package replication_test

import (
	"time"

	"code.cloudfoundry.org/log-cache/internal/store/replication"

	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PeerManager", func() {
	var (
		spyRaft *spyRaft
		m       *replication.PeerManager
	)

	BeforeEach(func() {
		spyRaft = newSpyRaft()
		m = replication.NewPeerManager(spyRaft, []string{"a", "b", "c"})
	})

	It("shutsdown the raft server on close", func() {
		m.Close()
		Expect(spyRaft.shutdownCalled).To(BeTrue())
	})

	It("adds and removes obsolete peers", func() {
		m.SetPeers([]string{"b", "c", "d"})

		Expect(spyRaft.addIDs).To(ConsistOf(raft.ServerID("d")))
		Expect(spyRaft.addAddrs).To(ConsistOf(raft.ServerAddress("d")))
		Expect(spyRaft.addPrevIndices).To(ConsistOf(uint64(0)))
		Expect(spyRaft.addTimeouts).To(ConsistOf(time.Second))

		Expect(spyRaft.removeIDs).To(ConsistOf(raft.ServerID("a")))
		Expect(spyRaft.removePrevIndices).To(ConsistOf(uint64(0)))
		Expect(spyRaft.removeTimeouts).To(ConsistOf(time.Second))
	})
})

type spyRaft struct {
	shutdownCalled bool

	addIDs         []raft.ServerID
	addAddrs       []raft.ServerAddress
	addPrevIndices []uint64
	addTimeouts    []time.Duration

	removeIDs         []raft.ServerID
	removePrevIndices []uint64
	removeTimeouts    []time.Duration
}

func newSpyRaft() *spyRaft {
	return &spyRaft{}
}

func (s *spyRaft) Shutdown() raft.Future {
	s.shutdownCalled = true
	return nil
}

func (s *spyRaft) AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	s.addIDs = append(s.addIDs, id)
	s.addAddrs = append(s.addAddrs, address)
	s.addPrevIndices = append(s.addPrevIndices, prevIndex)
	s.addTimeouts = append(s.addTimeouts, timeout)
	return nil
}

func (s *spyRaft) RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	s.removeIDs = append(s.addIDs, id)
	s.removePrevIndices = append(s.addPrevIndices, prevIndex)
	s.removeTimeouts = append(s.addTimeouts, timeout)
	return nil
}
