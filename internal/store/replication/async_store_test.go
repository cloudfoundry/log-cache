package replication_test

import (
	"log"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store/replication"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AsyncStore", func() {
	var (
		spySubStore *spySubStore
		s           *replication.AsyncStore
	)

	BeforeEach(func() {
		spySubStore = newSpySubStore()
		s = replication.NewAsyncStore(spySubStore, log.New(GinkgoWriter, "", 0))
	})

	It("eventually writes the data to the store's put", func() {
		s.Put(&loggregator_v2.Envelope{Timestamp: 3}, "some-index")

		Eventually(spySubStore.PutEnvelopes).Should(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 3},
		))

		Eventually(spySubStore.PutIndices).Should(ConsistOf(
			"some-index",
		))
	})
})
