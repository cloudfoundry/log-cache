package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"

	"google.golang.org/grpc"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/app"
	"code.cloudfoundry.org/log-cache/internal/egress"
	"code.cloudfoundry.org/log-cache/internal/rpc/logcache"

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

	It("returns data filtered by source ID in 1 node cluster", func() {
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

	It("routes data to peers", func() {
		peer := newSpyLogCache()
		peerAddr := peer.start()

		streamConnector := newSpyStreamConnector()
		// source-0 hashes to 7700738999732113484 (route to node 0)
		addEnvelope(1, "source-0", streamConnector)

		// source-1 hashes to 15704273932878139171 (route to node 1)
		addEnvelope(2, "source-1", streamConnector)
		addEnvelope(3, "source-1", streamConnector)

		cache := app.NewLogCache(
			streamConnector,
			app.WithEgressAddr("localhost:0"),
			app.WithClustered(0, []string{"my-addr", peerAddr}, app.ClusterGrpc{
				DialOptions: []grpc.DialOption{grpc.WithInsecure()},
			}),
		)

		cache.Start()
		Eventually(peer.getEnvelopes).Should(HaveLen(2))
		Expect(peer.getEnvelopes()[0].Timestamp).To(Equal(int64(2)))
		Expect(peer.getEnvelopes()[1].Timestamp).To(Equal(int64(3)))
	})

	It("accepts data from peers", func() {
		streamConnector := newSpyStreamConnector()
		cache := app.NewLogCache(
			streamConnector,
			app.WithEgressAddr("localhost:0"),
			app.WithClustered(0, []string{"my-addr", "other-addr"}, app.ClusterGrpc{
				DialOptions: []grpc.DialOption{grpc.WithInsecure()},
			}),
		)
		cache.Start()

		peerWriter := egress.NewPeerWriter(
			cache.IngressAddr(),
			grpc.WithInsecure(),
		)

		// source-0 hashes to 7700738999732113484 (route to node 0)
		peerWriter.Write(&loggregator_v2.Envelope{SourceId: "source-0", Timestamp: 1})

		var (
			resp *http.Response
			data []byte
		)
		f := func() error {
			var err error
			URL := fmt.Sprintf("http://%s/source-0", cache.EgressAddr())
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

			if len(jsonData["envelopes"].([]interface{})) != 1 {
				return fmt.Errorf("expected 1 but actual %d", len(jsonData["envelopes"].([]interface{})))
			}

			return nil
		}
		Eventually(f).Should(BeNil())

		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(data).To(MatchJSON(`{
			"envelopes": [
				{
					"timestamp": "1",
					"sourceId": "source-0"
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
