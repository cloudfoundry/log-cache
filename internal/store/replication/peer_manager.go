package replication

import (
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type PeerManager struct {
	mu    sync.Mutex
	r     Raft
	peers []string
}

type Raft interface {
	Shutdown() raft.Future
	AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture
}

func NewPeerManager(r Raft, initialPeers []string) *PeerManager {
	return &PeerManager{
		r:     r,
		peers: initialPeers,
	}
}

func (m *PeerManager) SetPeers(peers []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer func() {
		m.peers = peers
	}()

	for _, p := range m.peers {
		if !m.findPeer(p, peers) {
			m.r.RemoveServer(raft.ServerID(p), 0, time.Second)
		}
	}

	for _, p := range peers {
		if !m.findPeer(p, m.peers) {
			m.r.AddVoter(raft.ServerID(p), raft.ServerAddress(p), 0, time.Second)
		}
	}
}

func (m *PeerManager) findPeer(p string, peers []string) bool {
	for _, pp := range peers {
		if p == pp {
			return true
		}
	}

	return false
}

func (m *PeerManager) Close() error {
	m.r.Shutdown()
	return nil
}
