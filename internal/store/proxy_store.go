package store

import (
	"log"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
)

// ProxyStore finds what store has the desired data to read from.
type ProxyStore struct {
	local   Getter
	remotes RemoteNodes
	lookup  Lookup
	index   int
}

// NewProxyStore creates and returns a ProxyStore.
func NewProxyStore(local Getter, index int, remotes RemoteNodes, lookup Lookup) *ProxyStore {
	return &ProxyStore{
		local:   local,
		remotes: remotes,
		lookup:  lookup,
		index:   index,
	}
}

// Getter gets data from the local store.
type Getter func(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeType EnvelopeType,
	limit int,
) []*loggregator_v2.Envelope

// RemoteNodes are used to reach out to other LogCache nodes for data. They
// are indexed based on their index that corresponds to SourceID lookups.
type RemoteNodes map[int]logcache.EgressClient

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
) []*loggregator_v2.Envelope {
	idx := s.lookup(sourceID)
	if s.index == idx {
		// Local
		return s.local(sourceID, start, end, envelopeType, limit)
	}

	// Remote
	remote, ok := s.remotes[idx]
	if !ok {
		log.Panicf("Something went poorly. The map and lookup method are out of sync")
	}

	resp, err := remote.Read(context.Background(), &logcache.ReadRequest{
		SourceId:     sourceID,
		StartTime:    start.UnixNano(),
		EndTime:      end.UnixNano(),
		EnvelopeType: s.convertEnvelopeType(envelopeType),
		Limit:        int64(limit),
	})
	if err != nil {
		log.Printf("failed to read from peer: %s", err)
		return nil
	}

	return resp.Envelopes.Batch
}

func (s *ProxyStore) convertEnvelopeType(t EnvelopeType) logcache.EnvelopeTypes {
	switch t.(type) {
	case *loggregator_v2.Log:
		return logcache.EnvelopeTypes_LOG
	case *loggregator_v2.Counter:
		return logcache.EnvelopeTypes_COUNTER
	case *loggregator_v2.Gauge:
		return logcache.EnvelopeTypes_GAUGE
	case *loggregator_v2.Timer:
		return logcache.EnvelopeTypes_TIMER
	case *loggregator_v2.Event:
		return logcache.EnvelopeTypes_EVENT
	default:
		return logcache.EnvelopeTypes_ANY
	}
}
