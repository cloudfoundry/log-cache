package logcache_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache"
	rpc "code.cloudfoundry.org/log-cache/rpc/logcache_v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogCache", func() {
	var (
		tlsConfig *tls.Config
		peer      *spyLogCache
		cache     *logcache.LogCache
		oc        rpc.OrchestrationClient

		spyMetrics *spyMetrics
	)

	BeforeEach(func() {
		var err error
		tlsConfig, err = newTLSConfig(
			Cert("log-cache-ca.crt"),
			Cert("log-cache.crt"),
			Cert("log-cache.key"),
			"log-cache",
		)
		Expect(err).ToNot(HaveOccurred())

		peer = newSpyLogCache(tlsConfig)
		peerAddr := peer.start()
		spyMetrics = newSpyMetrics()

		cache = logcache.New(
			logcache.WithAddr("127.0.0.1:0"),
			logcache.WithClustered(0, []string{"my-addr", peerAddr},
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithMetrics(spyMetrics),
			logcache.WithServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
		)
		cache.Start()

		conn, err := grpc.Dial(
			cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())

		oc = rpc.NewOrchestrationClient(conn)

		_, err = oc.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				cache.Addr(): {
					Ranges: []*rpc.Range{
						{
							Start: 0,
							End:   9223372036854775807,
						},
					},
				},
				peerAddr: {
					Ranges: []*rpc.Range{
						{
							Start: 9223372036854775808,
							End:   math.MaxUint64,
						},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		cache.Close()
	})

	It("returns tail of data filtered by source ID", func() {
		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			// source-0 hashes to 7700738999732113484 (route to node 0)
			{Timestamp: 1, SourceId: "source-0"},
			// source-1 hashes to 15704273932878139171 (route to node 1)
			{Timestamp: 2, SourceId: "source-1"},
			{Timestamp: 3, SourceId: "source-0"},
			{Timestamp: 4, SourceId: "source-0"},
		})

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewEgressClient(conn)

		var es []*loggregator_v2.Envelope
		f := func() error {
			resp, err := client.Read(context.Background(), &rpc.ReadRequest{
				SourceId:   "source-0",
				Descending: true,
				Limit:      2,
			})
			if err != nil {
				return err
			}

			if len(resp.Envelopes.Batch) != 2 {
				return errors.New("expected 2 envelopes")
			}

			es = resp.Envelopes.Batch
			return nil
		}
		Eventually(f).Should(BeNil())

		Expect(es[0].Timestamp).To(Equal(int64(4)))
		Expect(es[0].SourceId).To(Equal("source-0"))
		Expect(es[1].Timestamp).To(Equal(int64(3)))
		Expect(es[1].SourceId).To(Equal("source-0"))

		Eventually(spyMetrics.getter("Ingress")).Should(Equal(uint64(3)))
		Eventually(spyMetrics.getter("Egress")).Should(Equal(uint64(2)))
	})

	It("queries data via PromQL Instant Queries", func() {
		now := time.Now()
		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			// source-0 hashes to 7700738999732113484 (route to node 0)
			{
				Timestamp: now.Add(-2 * time.Second).UnixNano(),
				SourceId:  "source-0",
				Message: &loggregator_v2.Envelope_Counter{
					Counter: &loggregator_v2.Counter{
						Name:  "metric",
						Total: 99,
					},
				},
			},
		})

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewPromQLQuerierClient(conn)

		f := func() error {
			resp, err := client.InstantQuery(context.Background(), &rpc.PromQL_InstantQueryRequest{
				Query: `metric{source_id="source-0"}`,
				Time:  formatTimeWithDecimalMillis(now),
			})
			if err != nil {
				return err
			}

			if len(resp.GetVector().GetSamples()) != 1 {
				return errors.New("expected 1 samples")
			}

			return nil
		}
		Eventually(f).Should(BeNil())
	})

	It("queries data via PromQL Range Queries", func() {
		now := time.Now()
		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			// source-0 hashes to 7700738999732113484 (route to node 0)
			{
				Timestamp: now.Add(-2 * time.Second).UnixNano(),
				SourceId:  "source-0",
				Message: &loggregator_v2.Envelope_Counter{
					Counter: &loggregator_v2.Counter{
						Name:  "metric",
						Total: 99,
					},
				},
			},
		})

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewPromQLQuerierClient(conn)

		f := func() error {
			resp, err := client.RangeQuery(context.Background(), &rpc.PromQL_RangeQueryRequest{
				Query: `metric{source_id="source-0"}`,
				Start: formatTimeWithDecimalMillis(now.Add(-time.Minute)),
				End:   formatTimeWithDecimalMillis(now),
				Step:  "1m",
			})
			if err != nil {
				return err
			}

			Expect(len(resp.GetMatrix().GetSeries())).To(Equal(1))
			Expect(len(resp.GetMatrix().GetSeries()[0].GetPoints())).To(Equal(1))

			return nil
		}
		Eventually(f).Should(BeNil())
	})

	It("uses the routes from the scheduler", func() {
		_, err := oc.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				cache.Addr(): {
					Ranges: []*rpc.Range{
						{
							Start: 0,
							End:   math.MaxUint64,
						},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			{Timestamp: 1, SourceId: "source-0"},
			{Timestamp: 2, SourceId: "source-1"},
			{Timestamp: 3, SourceId: "source-0"},
			{Timestamp: 4, SourceId: "source-0"},
		})

		Eventually(spyMetrics.getter("Ingress")).Should(Equal(uint64(4)))
	})

	It("routes envelopes to peers", func() {
		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			// source-0 hashes to 7700738999732113484 (route to node 0)
			{Timestamp: 1, SourceId: "source-0"},
			// source-1 hashes to 15704273932878139171 (route to node 1)
			{Timestamp: 2, SourceId: "source-1"},
			{Timestamp: 3, SourceId: "source-1"},
		})

		Eventually(peer.getEnvelopes).Should(HaveLen(2))
		Expect(peer.getEnvelopes()[0].Timestamp).To(Equal(int64(2)))
		Expect(peer.getEnvelopes()[1].Timestamp).To(Equal(int64(3)))
		Expect(peer.getLocalOnlyValues()).ToNot(ContainElement(false))
	})

	It("accepts envelopes from peers", func() {
		// source-0 hashes to 7700738999732113484 (route to node 0)
		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			{SourceId: "source-0", Timestamp: 1},
		})

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewEgressClient(conn)

		var es []*loggregator_v2.Envelope
		f := func() error {
			resp, err := client.Read(context.Background(), &rpc.ReadRequest{
				SourceId: "source-0",
			})
			if err != nil {
				return err
			}

			if len(resp.Envelopes.Batch) != 1 {
				return errors.New("expected 1 envelopes")
			}

			es = resp.Envelopes.Batch
			return nil
		}
		Eventually(f).Should(BeNil())

		Expect(es[0].Timestamp).To(Equal(int64(1)))
		Expect(es[0].SourceId).To(Equal("source-0"))
	})

	It("routes query requests to peers", func() {
		peer.readEnvelopes["source-1"] = func() []*loggregator_v2.Envelope {
			return []*loggregator_v2.Envelope{
				{Timestamp: 99},
				{Timestamp: 101},
			}
		}

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewEgressClient(conn)

		// source-1 hashes to 15704273932878139171 (route to node 1)
		resp, err := client.Read(context.Background(), &rpc.ReadRequest{
			SourceId:      "source-1",
			StartTime:     99,
			EndTime:       101,
			EnvelopeTypes: []rpc.EnvelopeType{rpc.EnvelopeType_LOG},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Envelopes.Batch).To(HaveLen(2))

		Eventually(peer.getReadRequests).Should(HaveLen(1))
		req := peer.getReadRequests()[0]
		Expect(req.SourceId).To(Equal("source-1"))
		Expect(req.StartTime).To(Equal(int64(99)))
		Expect(req.EndTime).To(Equal(int64(101)))
		Expect(req.EnvelopeTypes).To(ConsistOf(rpc.EnvelopeType_LOG))
	})

	It("returns all meta information", func() {
		peer.metaResponses = map[string]*rpc.MetaInfo{
			"source-1": {
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			},
		}

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		ingressClient := rpc.NewIngressClient(conn)
		egressClient := rpc.NewEgressClient(conn)

		sendRequest := &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "source-0"},
				},
			},
		}

		ingressClient.Send(context.Background(), sendRequest)
		Eventually(func() map[string]*rpc.MetaInfo {
			resp, err := egressClient.Meta(context.Background(), &rpc.MetaRequest{})
			if err != nil {
				return nil
			}

			return resp.Meta
		}).Should(And(
			HaveKeyWithValue("source-0", &rpc.MetaInfo{
				Count: 1,
			}),
			HaveKeyWithValue("source-1", &rpc.MetaInfo{
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			}),
		))
	})
})

func writeEnvelopes(addr string, es []*loggregator_v2.Envelope) {
	tlsConfig, err := newTLSConfig(
		Cert("log-cache-ca.crt"),
		Cert("log-cache.crt"),
		Cert("log-cache.key"),
		"log-cache",
	)
	if err != nil {
		panic(err)
	}
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	)
	if err != nil {
		panic(err)
	}

	client := rpc.NewIngressClient(conn)
	var envelopes []*loggregator_v2.Envelope
	for _, e := range es {
		envelopes = append(envelopes, &loggregator_v2.Envelope{
			Timestamp: e.Timestamp,
			SourceId:  e.SourceId,
			Message:   e.Message,
		})
	}

	_, err = client.Send(context.Background(), &rpc.SendRequest{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: envelopes,
		},
	})
	if err != nil {
		panic(err)
	}
}

type spyLogCache struct {
	mu                 sync.Mutex
	localOnlyValues    []bool
	envelopes          []*loggregator_v2.Envelope
	readRequests       []*rpc.ReadRequest
	queryRequests      []*rpc.PromQL_InstantQueryRequest
	queryError         error
	rangeQueryRequests []*rpc.PromQL_RangeQueryRequest
	readEnvelopes      map[string]func() []*loggregator_v2.Envelope
	metaResponses      map[string]*rpc.MetaInfo
	tlsConfig          *tls.Config
	value              float64
}

func (s *spyLogCache) SetValue(value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = value
}

func newSpyLogCache(tlsConfig *tls.Config) *spyLogCache {
	return &spyLogCache{
		readEnvelopes: make(map[string]func() []*loggregator_v2.Envelope),
		tlsConfig:     tlsConfig,
		value:         101,
	}
}

func (s *spyLogCache) start() string {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(s.tlsConfig)),
	)
	rpc.RegisterIngressServer(srv, s)
	rpc.RegisterEgressServer(srv, s)
	rpc.RegisterPromQLQuerierServer(srv, s)
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

func (s *spyLogCache) getLocalOnlyValues() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]bool, len(s.localOnlyValues))
	copy(r, s.localOnlyValues)
	return r
}

func (s *spyLogCache) getReadRequests() []*rpc.ReadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]*rpc.ReadRequest, len(s.readRequests))
	copy(r, s.readRequests)
	return r
}

func (s *spyLogCache) Send(ctx context.Context, r *rpc.SendRequest) (*rpc.SendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.localOnlyValues = append(s.localOnlyValues, r.LocalOnly)

	for _, e := range r.Envelopes.Batch {
		s.envelopes = append(s.envelopes, e)
	}

	return &rpc.SendResponse{}, nil
}

func (s *spyLogCache) Read(ctx context.Context, r *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readRequests = append(s.readRequests, r)

	b := s.readEnvelopes[r.GetSourceId()]

	var batch []*loggregator_v2.Envelope
	if b != nil {
		batch = b()
	}

	return &rpc.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	}, nil
}

func (s *spyLogCache) Meta(ctx context.Context, r *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	return &rpc.MetaResponse{
		Meta: s.metaResponses,
	}, nil
}

func (s *spyLogCache) InstantQuery(ctx context.Context, r *rpc.PromQL_InstantQueryRequest) (*rpc.PromQL_InstantQueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.queryRequests = append(s.queryRequests, r)

	return &rpc.PromQL_InstantQueryResult{
		Result: &rpc.PromQL_InstantQueryResult_Scalar{
			Scalar: &rpc.PromQL_Scalar{
				Time:  "99.000",
				Value: s.value,
			},
		},
	}, s.queryError
}

func (s *spyLogCache) getQueryRequests() []*rpc.PromQL_InstantQueryRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.PromQL_InstantQueryRequest, len(s.queryRequests))
	copy(r, s.queryRequests)

	return r
}

func (s *spyLogCache) RangeQuery(ctx context.Context, r *rpc.PromQL_RangeQueryRequest) (*rpc.PromQL_RangeQueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rangeQueryRequests = append(s.rangeQueryRequests, r)

	return &rpc.PromQL_RangeQueryResult{
		Result: &rpc.PromQL_RangeQueryResult_Matrix{
			Matrix: &rpc.PromQL_Matrix{
				Series: []*rpc.PromQL_Series{
					{
						Metric: map[string]string{
							"__name__": "test",
						},
						Points: []*rpc.PromQL_Point{
							{
								Time:  "99.000",
								Value: s.value,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (s *spyLogCache) getRangeQueryRequests() []*rpc.PromQL_RangeQueryRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.PromQL_RangeQueryRequest, len(s.rangeQueryRequests))
	copy(r, s.rangeQueryRequests)

	return r
}

func newTLSConfig(caPath, certPath, keyPath, cn string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		ServerName:         cn,
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: false,
	}

	caCertBytes, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCertBytes); !ok {
		return nil, errors.New("cannot parse ca cert")
	}

	tlsConfig.RootCAs = caCertPool

	return tlsConfig, nil
}

func formatTimeWithDecimalMillis(t time.Time) string {
	return fmt.Sprintf("%.3f", float64(t.UnixNano())/1e9)
}

func parseTimeWithDecimalMillis(t string) time.Time {
	decimalTime, err := strconv.ParseFloat(t, 64)
	Expect(err).ToNot(HaveOccurred())
	return time.Unix(0, int64(decimalTime*1e9))
}
