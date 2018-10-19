package data_reader_test

import (
	"context"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/promql/data_reader"
	"code.cloudfoundry.org/log-cache/pkg/client"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("WalkingDataReader", func() {
	var (
		spyLogCache *spyLogCache
		r           *data_reader.WalkingDataReader
	)

	BeforeEach(func() {
		spyLogCache = newSpyLogCache()
		r = data_reader.NewWalkingDataReader(spyLogCache.Read)
	})

	It("returns the error from the context", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := r.Read(ctx, &logcache_v1.ReadRequest{})
		Expect(err).To(HaveOccurred())
	})
})

type spyLogCache struct {
	results []*loggregator_v2.Envelope
	err     error
}

func newSpyLogCache() *spyLogCache {
	return &spyLogCache{}
}

func (s *spyLogCache) Read(
	ctx context.Context,
	sourceID string,
	start time.Time,
	opts ...client.ReadOption,
) ([]*loggregator_v2.Envelope, error) {
	return s.results, s.err
}
