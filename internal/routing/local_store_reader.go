package routing

import (
	"fmt"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// LocalStoreReader accesses a store via gRPC calls. It handles converting the
// requests into a form that the store understands for reading.
type LocalStoreReader struct {
	s StoreReader
}

// StoreReader proxies to the log cache for getting envelopes or Log Cache
// Metadata.
type StoreReader interface {
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
	Meta() map[string]store.MetaInfo
}

// NewLocalStoreReader creates and returns a new LocalStoreReader.
func NewLocalStoreReader(s StoreReader) *LocalStoreReader {
	return &LocalStoreReader{
		s: s,
	}
}

// Read returns data from the store.
func (r *LocalStoreReader) Read(ctx context.Context, req *rpc.ReadRequest, opts ...grpc.CallOption) (*rpc.ReadResponse, error) {
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

	envs := r.s.Get(
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

func (r *LocalStoreReader) Meta(ctx context.Context, req *rpc.MetaRequest, opts ...grpc.CallOption) (*rpc.MetaResponse, error) {
	sourceIds := r.s.Meta()

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

func (r *LocalStoreReader) convertEnvelopeType(t rpc.EnvelopeTypes) store.EnvelopeType {
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
