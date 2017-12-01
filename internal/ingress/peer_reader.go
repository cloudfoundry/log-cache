package ingress

import (
	"code.cloudfoundry.org/log-cache/internal/rpc/logcache"
	"golang.org/x/net/context"
)

// PeerReader reads envelopes from peers. It implements
// logcache.IngressServer.
type PeerReader struct {
	s Store
}

// NewPeerReader creates and returns a new PeerReader.
func NewPeerReader(s Store) *PeerReader {
	return &PeerReader{
		s: s,
	}
}

// Send takes in data from the peer and submits it to the store.
func (r *PeerReader) Send(ctx context.Context, req *logcache.SendRequest) (*logcache.SendResponse, error) {
	r.s(req.Envelopes.Batch)
	return &logcache.SendResponse{}, nil
}
