package routing

import (
	"fmt"
	"regexp"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
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
		envelopeTypes []logcache_v1.EnvelopeType,
		nameFilter *regexp.Regexp,
		limit int,
		descending bool,
	) []*loggregator_v2.Envelope

	// Meta gets the metadata from Log Cache instances in the cluster.
	Meta() map[string]logcache_v1.MetaInfo
}

// NewLocalStoreReader creates and returns a new LocalStoreReader.
func NewLocalStoreReader(s StoreReader) *LocalStoreReader {
	return &LocalStoreReader{
		s: s,
	}
}

// Read returns data from the store.
func (r *LocalStoreReader) Read(ctx context.Context, req *logcache_v1.ReadRequest, opts ...grpc.CallOption) (*logcache_v1.ReadResponse, error) {
	if req.EndTime != 0 && req.StartTime > req.EndTime {
		return nil, fmt.Errorf("StartTime (%d) must be before EndTime (%d)", req.StartTime, req.EndTime)
	}

	if req.Limit > 1000 {
		return nil, fmt.Errorf("Limit (%d) must be 1000 or less", req.Limit)
	}

	if req.Limit < 0 {
		return nil, fmt.Errorf("Limit (%d) must be greater than zero", req.Limit)
	}

	if req.EndTime == 0 {
		req.EndTime = time.Now().UnixNano()
	}

	if req.Limit == 0 {
		req.Limit = 100
	}

	var nameFilter *regexp.Regexp
	var err error
	if req.NameFilter != "" {
		nameFilter, err = regexp.Compile(req.NameFilter)
		if err != nil {
			return nil, fmt.Errorf("Name filter must be a valid regular expression: %s", err)
		}
	}

	var envelopeTypes []logcache_v1.EnvelopeType
	for _, e := range req.GetEnvelopeTypes() {
		if e != logcache_v1.EnvelopeType_ANY {
			envelopeTypes = append(envelopeTypes, e)
		}
	}
	envs := r.s.Get(
		req.SourceId,
		time.Unix(0, req.StartTime),
		time.Unix(0, req.EndTime),
		envelopeTypes,
		nameFilter,
		int(req.Limit),
		req.Descending,
	)
	resp := &logcache_v1.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: envs,
		},
	}

	return resp, nil
}

func (r *LocalStoreReader) Meta(ctx context.Context, req *logcache_v1.MetaRequest, opts ...grpc.CallOption) (*logcache_v1.MetaResponse, error) {
	sourceIds := r.s.Meta()

	metaInfo := make(map[string]*logcache_v1.MetaInfo)
	for sourceId, m := range sourceIds {
		// Shadow m so that the range function does not mess with the
		// instance.
		m := m
		metaInfo[sourceId] = &m
	}

	return &logcache_v1.MetaResponse{
		Meta: metaInfo,
	}, nil
}
