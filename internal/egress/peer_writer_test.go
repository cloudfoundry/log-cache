package egress_test

import (
	"log"
	"net"
	"sync"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/egress"
	"code.cloudfoundry.org/log-cache/internal/rpc/logcache"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PeerWriter", func() {
	var (
		spyLogCache *spyLogCache
		w           *egress.PeerWriter
	)

	BeforeEach(func() {
		spyLogCache = newSpyLogCache()
		addr := spyLogCache.start()

		w = egress.NewPeerWriter(addr, grpc.WithInsecure())
	})

	It("writes data to the peer LogCache", func() {
		w.Write(&loggregator_v2.Envelope{Timestamp: 1})

		Eventually(spyLogCache.getEnvelopes).Should(HaveLen(1))
		Expect(spyLogCache.getEnvelopes()[0].Timestamp).To(Equal(int64(1)))
	})
})

type spyLogCache struct {
	mu        sync.Mutex
	envelopes []*loggregator_v2.Envelope
}

func newSpyLogCache() *spyLogCache {
	return &spyLogCache{}
}

func (s *spyLogCache) start() string {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	srv := grpc.NewServer()
	logcache.RegisterIngressServer(srv, s)
	go srv.Serve(lis)

	return lis.Addr().String()
}

func (s *spyLogCache) getEnvelopes() []*loggregator_v2.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]*loggregator_v2.Envelope, len(s.envelopes))
	copy(r, s.envelopes)
	return r
}

func (s *spyLogCache) Send(ctx context.Context, r *logcache.SendRequest) (*logcache.SendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range r.Envelopes.Batch {
		s.envelopes = append(s.envelopes, e)
	}

	return &logcache.SendResponse{}, nil
}
