package store

import (
	"log"
	"time"

	"golang.org/x/net/context"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// ProxyStore finds what store has the desired data to read from.
type ProxyStore struct {
	local   LocalStore
	remotes RemoteNodes
	lookup  Lookup
	index   int
}

// LocalStore can return envelopes or Log Cache Metadata.
type LocalStore interface {
	// Gets envelopes from a local Log Cache.
	Get(
		sourceID string,
		start time.Time,
		end time.Time,
		envelopeType EnvelopeType,
		limit int,
		descending bool,
	) []*loggregator_v2.Envelope

	// Meta gets local metadata.
	Meta() map[string]MetaInfo
}

// NewProxyStore creates and returns a ProxyStore.
func NewProxyStore(local LocalStore, index int, remotes RemoteNodes, lookup Lookup) *ProxyStore {
	return &ProxyStore{
		local:   local,
		remotes: remotes,
		lookup:  lookup,
		index:   index,
	}
}

// RemoteNodes are used to reach out to other LogCache nodes for data. They
// are indexed based on their index that corresponds to SourceID lookups.
type RemoteNodes map[int]rpc.EgressClient

// Lookup is used to determine what LogCache a sourceID is stored.
type Lookup func(sourceID string) int

// Get looks at the sourceID and either reads from the local store or proxies
// the request to the correct node.
func (s *ProxyStore) Get(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeType EnvelopeType,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	idx := s.lookup(sourceID)
	if s.index == idx {
		// Local
		return s.local.Get(sourceID, start, end, envelopeType, limit, descending)
	}

	// Remote
	remote, ok := s.remotes[idx]
	if !ok {
		log.Panicf("Something went poorly. The map and lookup method are out of sync")
	}

	resp, err := remote.Read(context.Background(), &rpc.ReadRequest{
		SourceId:     sourceID,
		StartTime:    start.UnixNano(),
		EndTime:      end.UnixNano(),
		EnvelopeType: s.convertEnvelopeType(envelopeType),
		Limit:        int64(limit),
		Descending:   descending,
	})
	if err != nil {
		log.Printf("failed to read from peer: %s", err)
		return nil
	}

	return resp.Envelopes.Batch
}

// Meta reads the LogCache Metadata from all log-cache peers
func (p *ProxyStore) Meta(localOnly bool) map[string]MetaInfo {
	meta := p.local.Meta()
	if localOnly {
		return meta
	}

	for _, r := range p.remotes {
		for k, v := range p.metaFromRemote(r) {
			meta[k] = v
		}
	}

	return meta
}

func (p *ProxyStore) metaFromRemote(remote rpc.EgressClient) map[string]MetaInfo {
	if remote == nil {
		return nil
	}

	req := &rpc.MetaRequest{
		LocalOnly: true,
	}

	meta := make(map[string]MetaInfo)
	remoteMeta, err := remote.Meta(context.Background(), req)
	if err != nil {
		return nil
	}

	for id, m := range remoteMeta.Meta {
		meta[id] = MetaInfo{
			Count:   int(m.Count),
			Expired: int(m.Expired),
			Newest:  time.Unix(0, m.NewestTimestamp),
			Oldest:  time.Unix(0, m.OldestTimestamp),
		}
	}

	return meta
}

func (s *ProxyStore) convertEnvelopeType(t EnvelopeType) rpc.EnvelopeTypes {
	switch t.(type) {
	case *loggregator_v2.Log:
		return rpc.EnvelopeTypes_LOG
	case *loggregator_v2.Counter:
		return rpc.EnvelopeTypes_COUNTER
	case *loggregator_v2.Gauge:
		return rpc.EnvelopeTypes_GAUGE
	case *loggregator_v2.Timer:
		return rpc.EnvelopeTypes_TIMER
	case *loggregator_v2.Event:
		return rpc.EnvelopeTypes_EVENT
	default:
		return rpc.EnvelopeTypes_ANY
	}
}
