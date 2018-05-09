package promql_test

import (
	"context"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/promql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EnvelopesDataReader", func() {
	var (
		r promql.DataReader
	)

	BeforeEach(func() {
		r = promql.NewEnvelopesDataReader([]*loggregator_v2.Envelope{
			{SourceId: "a", Timestamp: 1},
			{SourceId: "a", Timestamp: 2},
			{SourceId: "b", Timestamp: 3},
		})
	})

	It("only returns data for the given source ID", func() {
		result, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(result.GetEnvelopes().GetBatch()).To(ConsistOf(
			&loggregator_v2.Envelope{SourceId: "a", Timestamp: 1},
			&loggregator_v2.Envelope{SourceId: "a", Timestamp: 2},
		))
	})
})
