package ingress

import (
	"fmt"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"golang.org/x/net/context"
)

// PeerReader reads envelopes from peers. It implements
// rpc.IngressServer.
type PeerReader struct {
	put   Putter
	proxy LogCacheProxy
}

// Putter writes envelopes to the store.
type Putter func(*loggregator_v2.Envelope)

// LogCacheProxy proxies to the log cache for getting envelopes or Log Cache
// Metadata.
type LogCacheProxy interface {
	// Gets envelopes from a local or remote Log Cache.
	Get(
		sourceID string,
		start time.Time,
		end time.Time,
		envelopeType store.EnvelopeType,
		limit int,
		descending bool,
	) []*loggregator_v2.Envelope

	// Meta gets the metadata from Log Cache instances in the cluster.
	Meta(localOnly bool) map[string]store.MetaInfo
}

// NewPeerReader creates and returns a new PeerReader.
func NewPeerReader(p Putter, lcp LogCacheProxy) *PeerReader {
	return &PeerReader{
		put:   p,
		proxy: lcp,
	}
}

// Send takes in data from the peer and submits it to the store.
func (r *PeerReader) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	for _, e := range req.Envelopes.Batch {
		r.put(e)
	}
	return &rpc.SendResponse{}, nil
}

// Read returns data from the store.
func (r *PeerReader) Read(ctx context.Context, req *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	if req.EndTime != 0 && req.StartTime > req.EndTime {
		return nil, fmt.Errorf("StartTime (%d) must be before EndTime (%d)", req.StartTime, req.EndTime)
	}

	if req.Limit > 1000 {
		return nil, fmt.Errorf("Limit (%d) must be 1000 or less", req.Limit)
	}

	if req.EndTime == 0 {
		req.EndTime = time.Now().UnixNano()
	}

	if req.Limit == 0 {
		req.Limit = 100
	}

	envs := r.proxy.Get(
		req.SourceId,
		time.Unix(0, req.StartTime),
		time.Unix(0, req.EndTime),
		r.convertEnvelopeType(req.EnvelopeType),
		int(req.Limit),
		req.Descending,
	)
	resp := &rpc.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: envs,
		},
	}

	return resp, nil
}

func (r *PeerReader) Meta(ctx context.Context, req *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	sourceIds := r.proxy.Meta(req.LocalOnly)

	metaInfo := make(map[string]*rpc.MetaInfo)
	for sourceId, m := range sourceIds {
		metaInfo[sourceId] = &rpc.MetaInfo{
			Count:           int64(m.Count),
			Expired:         int64(m.Expired),
			NewestTimestamp: m.Newest.UnixNano(),
			OldestTimestamp: m.Oldest.UnixNano(),
		}
	}

	return &rpc.MetaResponse{
		Meta: metaInfo,
	}, nil
}

func (r *PeerReader) convertEnvelopeType(t rpc.EnvelopeTypes) store.EnvelopeType {
	switch t {
	case rpc.EnvelopeTypes_LOG:
		return &loggregator_v2.Log{}
	case rpc.EnvelopeTypes_COUNTER:
		return &loggregator_v2.Counter{}
	case rpc.EnvelopeTypes_GAUGE:
		return &loggregator_v2.Gauge{}
	case rpc.EnvelopeTypes_TIMER:
		return &loggregator_v2.Timer{}
	case rpc.EnvelopeTypes_EVENT:
		return &loggregator_v2.Event{}
	default:
		return nil
	}
}
