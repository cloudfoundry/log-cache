package app_test

import (
	"context"
	"sync"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/app"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogCache", func() {
	It("connects to a logs provider server", func() {
		streamConnector := newSpyStreamConnector()
		cache := app.NewLogCache(streamConnector)

		go cache.Start()

		Eventually(streamConnector.requests).Should(HaveLen(1))
		Eventually(streamConnector.called).ShouldNot(BeZero())
	})
})

type spyStreamConnector struct {
	mu        sync.Mutex
	requests_ []*loggregator_v2.EgressBatchRequest
	called_   int
}

func newSpyStreamConnector() *spyStreamConnector {
	return &spyStreamConnector{}
}

func (s *spyStreamConnector) Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests_ = append(s.requests_, req)

	return func() []*loggregator_v2.Envelope {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.called_++
		return nil
	}
}

func (s *spyStreamConnector) called() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.called_
}

func (s *spyStreamConnector) requests() []*loggregator_v2.EgressBatchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqs := make([]*loggregator_v2.EgressBatchRequest, len(s.requests_))
	copy(reqs, s.requests_)

	return reqs
}
