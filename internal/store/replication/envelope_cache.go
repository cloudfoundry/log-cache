package replication

import (
	"hash/crc64"
	"sync/atomic"
	"unsafe"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

type EnvelopeCache struct {
	cache          []unsafe.Pointer
	tableECMA      *crc64.Table
	incCacheMissed func(delta uint64)
	incCacheHit    func(delta uint64)
}

// Metrics registers Counter and Gauge metrics.
type Metrics interface {
	// NewCounter returns a function to increment for the given metric.
	NewCounter(name string) func(delta uint64)
}

func NewEnvelopeCache(size int, m Metrics) *EnvelopeCache {
	return &EnvelopeCache{
		cache:          make([]unsafe.Pointer, size),
		tableECMA:      crc64.MakeTable(crc64.ECMA),
		incCacheMissed: m.NewCounter("EnvelopeCacheMissed"),
		incCacheHit:    m.NewCounter("EnvelopeCacheHit"),
	}
}

type keyValue struct {
	key      string
	envelope *loggregator_v2.Envelope
}

func (c *EnvelopeCache) Put(key []byte, value *loggregator_v2.Envelope) {
	index := c.hash(key)
	kv := &keyValue{
		key:      string(key),
		envelope: value,
	}

	atomic.StorePointer(&c.cache[index], unsafe.Pointer(kv))
}

func (c *EnvelopeCache) Get(key []byte) *loggregator_v2.Envelope {
	index := c.hash(key)

	u := atomic.LoadPointer(&c.cache[index])
	if u == nil {
		c.incCacheMissed(1)
		return nil
	}

	value := (*keyValue)(u)

	if value.key != string(key) {
		c.incCacheMissed(1)
		return nil
	}

	c.incCacheHit(1)
	return value.envelope
}

func (c *EnvelopeCache) hash(b []byte) int {
	hash := crc64.Checksum(b, c.tableECMA)
	return int(hash % uint64(len(c.cache)))
}
