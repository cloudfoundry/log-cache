package app

import (
	"context"
	"log"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	connector StreamConnector
}

// StreamConnector reads envelopes from the the logs provider.
type StreamConnector interface {
	// Stream creates a EnvelopeStream for the given request.
	Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream
}

// LogCacheOption configures a LogCache.
type LogCacheOption func(*LogCache)

// NewLogCache creates a new LogCache.
func NewLogCache(c StreamConnector, opts ...LogCacheOption) *LogCache {
	return &LogCache{
		connector: c,
	}
}

// Start starts the LogCache.
func (c *LogCache) Start() {
	rx := c.connector.Stream(context.Background(), &loggregator_v2.EgressBatchRequest{})
	for {
		for _, e := range rx() {
			log.Printf("Read envelope: %+v", e)
		}
	}
}
