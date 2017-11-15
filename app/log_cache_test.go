package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/app"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogCache", func() {
	It("connects and reads from a logs provider server", func() {
		streamConnector := newSpyStreamConnector()
		addEnvelope(1, "some-source-id", streamConnector)
		addEnvelope(2, "some-source-id", streamConnector)
		addEnvelope(3, "some-source-id", streamConnector)

		cache := app.NewLogCache(streamConnector)

		cache.Start()

		Eventually(streamConnector.requests).Should(HaveLen(1))
		Eventually(streamConnector.envelopes).Should(HaveLen(0))
	})

	It("returns data filtered by source ID", func() {
		streamConnector := newSpyStreamConnector()
		addEnvelope(1, "app-a", streamConnector)
		addEnvelope(2, "app-b", streamConnector)
		addEnvelope(3, "app-a", streamConnector)

		cache := app.NewLogCache(
			streamConnector,
			app.WithEgressAddr("localhost:0"),
		)

		cache.Start()

		var (
			resp *http.Response
			data []byte
		)
		f := func() error {
			var err error
			URL := fmt.Sprintf("http://%s/app-a", cache.EgressAddr())
			resp, err = http.Get(URL)
			if err != nil {
				return err
			}

			data, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			var jsonData map[string]interface{}
			if err := json.Unmarshal(data, &jsonData); err != nil {
				return err
			}

			if len(jsonData["envelopes"].([]interface{})) != 2 {
				return fmt.Errorf("expected 2 but actual %d", len(jsonData["envelopes"].([]interface{})))
			}

			return nil
		}
		Eventually(f).Should(BeNil())

		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(data).To(MatchJSON(`{
			"envelopes": [
				{
					"timestamp": "1",
					"sourceId": "app-a"
				},
				{
					"timestamp": "3",
					"sourceId": "app-a"
				}
			]
		}`))
	})
})

func addEnvelope(timestamp int64, sourceID string, c *spyStreamConnector) {
	c.envelopes <- []*loggregator_v2.Envelope{
		{
			Timestamp: timestamp,
			SourceId:  sourceID,
		},
	}
}

type spyStreamConnector struct {
	mu        sync.Mutex
	requests_ []*loggregator_v2.EgressBatchRequest
	envelopes chan []*loggregator_v2.Envelope
}

func newSpyStreamConnector() *spyStreamConnector {
	return &spyStreamConnector{
		envelopes: make(chan []*loggregator_v2.Envelope, 100),
	}
}

func (s *spyStreamConnector) Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests_ = append(s.requests_, req)

	return func() []*loggregator_v2.Envelope {
		select {
		case e := <-s.envelopes:
			return e
		default:
			return nil
		}
	}
}

func (s *spyStreamConnector) requests() []*loggregator_v2.EgressBatchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqs := make([]*loggregator_v2.EgressBatchRequest, len(s.requests_))
	copy(reqs, s.requests_)

	return reqs
}
