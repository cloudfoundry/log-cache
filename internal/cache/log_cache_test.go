package cache_test

import (
	"code.cloudfoundry.org/go-loggregator/metrics/testhelpers"
	"context"
	"crypto/tls"
	"errors"
	"io/ioutil"
	"log"
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	. "code.cloudfoundry.org/log-cache/internal/cache"
	sharedtls "code.cloudfoundry.org/log-cache/internal/tls"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"

	"code.cloudfoundry.org/log-cache/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogCache", func() {
	var (
		tlsConfig *tls.Config
		peer      *testing.SpyLogCache
		cache     *LogCache
		oc        rpc.OrchestrationClient

		spyMetrics *testhelpers.SpyMetricsRegistry
	)

	BeforeEach(func() {
		var err error
		tlsConfig, err = sharedtls.NewMutualTLSConfig(
			testing.Cert("log-cache-ca.crt"),
			testing.Cert("log-cache.crt"),
			testing.Cert("log-cache.key"),
			"log-cache",
		)
		Expect(err).ToNot(HaveOccurred())

		peer = testing.NewSpyLogCache(tlsConfig)
		peerAddr := peer.Start()
		spyMetrics = testhelpers.NewMetricsRegistry()

		cache = New(
			spyMetrics,
			log.New(ioutil.Discard, "", 0),
			WithAddr("127.0.0.1:0"),
			WithClustered(0, []string{"my-addr", peerAddr},
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			WithServerOpts(
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

	Describe("TLS security", func() {
		DescribeTable("allows only supported TLS versions", func(clientTLSVersion int, serverAllows bool) {
			clientTlsConfig := tlsConfig.Clone()
			clientTlsConfig.MaxVersion = uint16(clientTLSVersion)
			clientTlsConfig.CipherSuites = []uint16{tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384}

			insecureConn, err := grpc.Dial(
				cache.Addr(),
				grpc.WithTransportCredentials(
					credentials.NewTLS(clientTlsConfig),
				),
			)
			Expect(err).NotTo(HaveOccurred())

			insecureClient := rpc.NewEgressClient(insecureConn)
			_, err = insecureClient.Meta(context.Background(), &rpc.MetaRequest{})

			if serverAllows {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
			}
		},

			Entry("unsupported SSL 3.0", tls.VersionSSL30, false),
			Entry("unsupported TLS 1.0", tls.VersionTLS10, false),
			Entry("unsupported TLS 1.1", tls.VersionTLS11, false),
			Entry("supported TLS 1.2", tls.VersionTLS12, true),
		)

		DescribeTable("allows only supported cipher suites", func(clientCipherSuite uint16, serverAllows bool) {
			clientTlsConfig := tlsConfig.Clone()
			clientTlsConfig.MaxVersion = tls.VersionTLS12
			clientTlsConfig.CipherSuites = []uint16{clientCipherSuite}

			insecureConn, err := grpc.Dial(
				cache.Addr(),
				grpc.WithTransportCredentials(
					credentials.NewTLS(clientTlsConfig),
				),
			)
			Expect(err).NotTo(HaveOccurred())

			insecureClient := rpc.NewEgressClient(insecureConn)
			_, err = insecureClient.Meta(context.Background(), &rpc.MetaRequest{})

			if serverAllows {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
			}
		},

			Entry("unsupported cipher RSA_WITH_3DES_EDE_CBC_SHA", tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA, false),
			Entry("unsupported cipher ECDHE_RSA_WITH_3DES_EDE_CBC_SHA", tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA, false),
			Entry("unsupported cipher RSA_WITH_RC4_128_SHA", tls.TLS_RSA_WITH_RC4_128_SHA, false),
			Entry("unsupported cipher RSA_WITH_AES_128_CBC_SHA256", tls.TLS_RSA_WITH_AES_128_CBC_SHA256, false),
			Entry("unsupported cipher ECDHE_ECDSA_WITH_CHACHA20_POLY1305", tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305, false),
			Entry("unsupported cipher ECDHE_ECDSA_WITH_RC4_128_SHA", tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA, false),
			Entry("unsupported cipher ECDHE_ECDSA_WITH_AES_128_CBC_SHA", tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, false),
			Entry("unsupported cipher ECDHE_ECDSA_WITH_AES_256_CBC_SHA", tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, false),
			Entry("unsupported cipher ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, false),
			Entry("unsupported cipher ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, false),
			Entry("unsupported cipher ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, false),
			Entry("unsupported cipher ECDHE_RSA_WITH_RC4_128_SHA", tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA, false),
			Entry("unsupported cipher ECDHE_RSA_WITH_AES_128_CBC_SHA256", tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, false),
			Entry("unsupported cipher ECDHE_RSA_WITH_AES_128_CBC_SHA", tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, false),
			Entry("unsupported cipher ECDHE_RSA_WITH_AES_256_CBC_SHA", tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, false),
			Entry("unsupported cipher ECDHE_RSA_WITH_CHACHA20_POLY1305", tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305, false),
			Entry("unsupported cipher RSA_WITH_AES_128_CBC_SHA", tls.TLS_RSA_WITH_AES_128_CBC_SHA, false),
			Entry("unsupported cipher RSA_WITH_AES_128_GCM_SHA256", tls.TLS_RSA_WITH_AES_128_GCM_SHA256, false),
			Entry("unsupported cipher RSA_WITH_AES_256_CBC_SHA", tls.TLS_RSA_WITH_AES_256_CBC_SHA, false),
			Entry("unsupported cipher RSA_WITH_AES_256_GCM_SHA384", tls.TLS_RSA_WITH_AES_256_GCM_SHA384, false),

			Entry("supported cipher ECDHE_RSA_WITH_AES_128_GCM_SHA256", tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, true),
			Entry("supported cipher ECDHE_RSA_WITH_AES_256_GCM_SHA384", tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, true),
		)
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

		Eventually(func() float64 {
			return spyMetrics.GetMetricValue("log_cache_ingress", nil)
		}).Should(Equal(3.0))

		Eventually(func() float64 {
			return spyMetrics.GetMetricValue("log_cache_egress", nil)
		}).Should(Equal(2.0))
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
				Time:  testing.FormatTimeWithDecimalMillis(now),
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
				Start: testing.FormatTimeWithDecimalMillis(now.Add(-time.Minute)),
				End:   testing.FormatTimeWithDecimalMillis(now),
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

		Eventually(func() float64 {
			return spyMetrics.GetMetricValue("log_cache_ingress", nil)
		}).Should(Equal(4.0))
	})

	It("routes envelopes to peers", func() {
		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			// source-0 hashes to 7700738999732113484 (route to node 0)
			{Timestamp: 1, SourceId: "source-0"},
			// source-1 hashes to 15704273932878139171 (route to node 1)
			{Timestamp: 2, SourceId: "source-1"},
			{Timestamp: 3, SourceId: "source-1"},
		})

		Eventually(peer.GetEnvelopes).Should(HaveLen(2))
		Expect(peer.GetEnvelopes()[0].Timestamp).To(Equal(int64(2)))
		Expect(peer.GetEnvelopes()[1].Timestamp).To(Equal(int64(3)))
		Expect(peer.GetLocalOnlyValues()).ToNot(ContainElement(false))
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
		peer.ReadEnvelopes["source-1"] = func() []*loggregator_v2.Envelope {
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

		Eventually(peer.GetReadRequests).Should(HaveLen(1))
		req := peer.GetReadRequests()[0]
		Expect(req.SourceId).To(Equal("source-1"))
		Expect(req.StartTime).To(Equal(int64(99)))
		Expect(req.EndTime).To(Equal(int64(101)))
		Expect(req.EnvelopeTypes).To(ConsistOf(rpc.EnvelopeType_LOG))
	})

	It("returns all meta information", func() {
		peer.MetaResponses = map[string]*rpc.MetaInfo{
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
	tlsConfig, err := testing.NewTLSConfig(
		testing.Cert("log-cache-ca.crt"),
		testing.Cert("log-cache.crt"),
		testing.Cert("log-cache.key"),
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
