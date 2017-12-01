package ingress_test

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/ingress"
	"code.cloudfoundry.org/log-cache/internal/rpc/logcache"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PeerReader", func() {
	var (
		r         *ingress.PeerReader
		envelopes []*loggregator_v2.Envelope
	)

	BeforeEach(func() {
		r = ingress.NewPeerReader(func(e []*loggregator_v2.Envelope) {
			envelopes = e
		})
	})

	It("writes the envelope to the store", func() {
		resp, err := r.Send(context.Background(), &logcache.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{Timestamp: 1},
					{Timestamp: 2},
				},
			},
		})

		Expect(resp).ToNot(BeNil())
		Expect(err).ToNot(HaveOccurred())
		Expect(envelopes).To(HaveLen(2))
		Expect(envelopes[0].Timestamp).To(Equal(int64(1)))
		Expect(envelopes[1].Timestamp).To(Equal(int64(2)))
	})
})
