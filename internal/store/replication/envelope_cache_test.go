package replication_test

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store/replication"

	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EnvelopeCache", func() {
	var (
		spyMetrics *spyMetrics
		c          *replication.EnvelopeCache
	)

	BeforeEach(func() {
		spyMetrics = newSpyMetrics()
		c = replication.NewEnvelopeCache(5, spyMetrics)
	})

	It("returns nil when there's nothing in the cahce", func() {
		Expect(c.Get([]byte{})).To(BeNil())
		Expect(spyMetrics.values).To(HaveKeyWithValue("EnvelopeCacheMissed", 1.0))
	})

	It("returns a stored envelope", func() {
		e := &loggregator_v2.Envelope{
			SourceId: "source",
		}
		e2 := &loggregator_v2.Envelope{
			SourceId: "source2",
		}

		eBytes, err := proto.Marshal(e)
		Expect(err).ToNot(HaveOccurred())
		c.Put(eBytes, e)

		e2Bytes, err := proto.Marshal(e2)
		Expect(err).ToNot(HaveOccurred())
		c.Put(e2Bytes, e2)

		Expect(c.Get(eBytes)).To(Equal(e))
		Expect(c.Get(e2Bytes)).To(Equal(e2))
		Expect(spyMetrics.values).ToNot(HaveKey("EnvelopeCacheMissed"))
	})

	It("replaces things in the cache", func() {
		c = replication.NewEnvelopeCache(1, spyMetrics)

		e := &loggregator_v2.Envelope{SourceId: "source"}
		e2 := &loggregator_v2.Envelope{SourceId: "source2"}

		eBytes, err := proto.Marshal(e)
		Expect(err).ToNot(HaveOccurred())

		e2Bytes, err := proto.Marshal(e2)
		Expect(err).ToNot(HaveOccurred())

		c.Put(eBytes, e)
		c.Put(e2Bytes, e2)

		Expect(c.Get(eBytes)).To(BeNil())
		Expect(c.Get(e2Bytes)).To(Equal(e2))
		Expect(spyMetrics.values).To(HaveKeyWithValue("EnvelopeCacheMissed", 1.0))
	})

	It("doesn't fail when run with race", func() {
		e := &loggregator_v2.Envelope{SourceId: "source"}

		eBytes, err := proto.Marshal(e)
		Expect(err).ToNot(HaveOccurred())

		go func() {
			for i := 0; i < 100; i++ {
				c.Put(eBytes, e)
			}
		}()

		for i := 0; i < 100; i++ {
			c.Get(eBytes)
		}
	})
})

type spyMetrics struct {
	values map[string]float64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		values: make(map[string]float64),
	}
}

func (s *spyMetrics) NewCounter(name string) func(delta uint64) {
	return func(d uint64) {
		s.values[name] += float64(d)
	}
}
