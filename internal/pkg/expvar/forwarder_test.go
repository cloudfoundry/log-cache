package expvar_test

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	. "code.cloudfoundry.org/log-cache/internal/pkg/expvar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/log-cache/internal/pkg/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("ExpvarForwarder", func() {
	var (
		r *ExpvarForwarder

		addr      string
		server1   *httptest.Server
		server2   *httptest.Server
		server    *httptest.Server
		logCache  *testing.SpyLogCache
		tlsConfig *tls.Config
		sbuffer   *gbytes.Buffer
	)

	Context("Normal gauges and counters", func() {
		BeforeEach(func() {
			var err error
			tlsConfig, err = testing.NewTLSConfig(
				testing.Cert("log-cache-ca.crt"),
				testing.Cert("log-cache.crt"),
				testing.Cert("log-cache.key"),
				"log-cache",
			)
			Expect(err).ToNot(HaveOccurred())

			server1 = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`
			{
				"LogCache": {
					"CachePeriod": 68644,
					"Egress": 999,
					"Expired": 0,
					"Ingress": 633
				}
			}`))
			}))

			server2 = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`
			{
				"LogCache": {
					"Egress": 999,
					"Ingress": 633
				}
			}`))
			}))

			logCache = testing.NewSpyLogCache(tlsConfig)
			addr = logCache.Start()

			sbuffer = gbytes.NewBuffer()

			r = NewExpvarForwarder(addr,
				WithExpvarInterval(time.Millisecond),
				WithExpvarStructuredLogger(log.New(sbuffer, "", 0)),
				WithExpvarDefaultSourceId("log-cache"),
				AddExpvarGaugeTemplate(
					server1.URL,
					"CachePeriod",
					"mS",
					"",
					"{{.LogCache.CachePeriod}}",
					map[string]string{"a": "some-value"},
				),
				AddExpvarCounterTemplate(
					server2.URL,
					"Egress",
					"log-cache-nozzle",
					"{{.LogCache.Egress}}",
					map[string]string{"a": "some-value"},
				),

				WithExpvarDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
			)

			go r.Start()
		})

		It("writes the expvar metrics to LogCache", func() {
			Eventually(func() int {
				return len(logCache.GetEnvelopes())
			}).Should(BeNumerically(">=", 2))

			var e *loggregator_v2.Envelope

			// Find counter
			for _, ee := range logCache.GetEnvelopes() {
				if ee.GetCounter() == nil {
					continue
				}

				e = ee
			}

			Expect(e).ToNot(BeNil())
			Expect(e.SourceId).To(Equal("log-cache-nozzle"))
			Expect(e.Timestamp).ToNot(BeZero())
			Expect(e.GetCounter().Name).To(Equal("Egress"))
			Expect(e.GetCounter().Total).To(Equal(uint64(999)))
			Expect(e.Tags).To(Equal(map[string]string{"a": "some-value"}))

			e = nil
			// Find gauge
			for _, ee := range logCache.GetEnvelopes() {
				if ee.GetGauge() == nil {
					continue
				}

				e = ee
			}

			Expect(e).ToNot(BeNil())
			Expect(e.SourceId).To(Equal("log-cache"))
			Expect(e.Timestamp).ToNot(BeZero())
			Expect(e.GetGauge().Metrics).To(HaveLen(1))
			Expect(e.GetGauge().Metrics["CachePeriod"].Value).To(Equal(68644.0))
			Expect(e.GetGauge().Metrics["CachePeriod"].Unit).To(Equal("mS"))
			Expect(e.Tags).To(Equal(map[string]string{"a": "some-value"}))
		})

		It("writes correct timestamps to LogCache", func() {
			Eventually(func() int {
				return len(logCache.GetEnvelopes())
			}).Should(BeNumerically(">=", 4))

			var counterEnvelopes []*loggregator_v2.Envelope

			// Find counters
			for _, ee := range logCache.GetEnvelopes() {
				if ee.GetCounter() == nil {
					continue
				}

				counterEnvelopes = append(counterEnvelopes, ee)
			}

			Expect(counterEnvelopes[0].Timestamp).ToNot(Equal(counterEnvelopes[1].Timestamp))
		})

		It("writes the expvar counters to the Structured Logger", func() {
			Eventually(sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"Egress","value":999,"source_id":"log-cache-nozzle","type":"counter"}`))
		})

		It("writes the expvar gauges to the Structured Logger", func() {
			Eventually(sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"CachePeriod","value":68644.000000,"source_id":"log-cache","type":"gauge"}`))
		})

		It("panics if a counter or gauge template is invalid", func() {
			Expect(func() {
				NewExpvarForwarder(addr,
					AddExpvarCounterTemplate(
						server1.URL, "some-name", "a", "{{invalid", nil,
					),
				)
			}).To(Panic())

			Expect(func() {
				NewExpvarForwarder(addr,
					AddExpvarGaugeTemplate(
						server1.URL, "some-name", "", "a", "{{invalid", nil,
					),
				)
			}).To(Panic())
		})
	})

	Context("Map gauges", func() {
		BeforeEach(func() {
			var err error
			tlsConfig, err = testing.NewTLSConfig(
				testing.Cert("log-cache-ca.crt"),
				testing.Cert("log-cache.crt"),
				testing.Cert("log-cache.key"),
				"log-cache",
			)
			Expect(err).ToNot(HaveOccurred())

			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{
				"LogCache": {
					"WorkerState": {
						"10.0.0.1:8080": 1,
						"10.0.0.2:8080": 2,
						"10.0.0.3:8080": 3
						}
					}
				}`))
			}))

			logCache = testing.NewSpyLogCache(tlsConfig)
			addr = logCache.Start()

			sbuffer = gbytes.NewBuffer()

			r = NewExpvarForwarder(addr,
				WithExpvarInterval(time.Millisecond),
				WithExpvarStructuredLogger(log.New(sbuffer, "", 0)),
				AddExpvarMapTemplate(
					server.URL,
					"WorkerState",
					"log-cache-scheduler",
					"{{.LogCache.WorkerState | jsonMap}}",
					map[string]string{"a": "some-value"},
				),

				WithExpvarDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
			)

			go r.Start()
		})

		It("writes the expvar map to LogCache as gauges", func() {
			Eventually(func() int {
				return len(logCache.GetEnvelopes())
			}).Should(BeNumerically(">=", 3))

			var e *loggregator_v2.Envelope
			e = nil
			pass := 1.0

			for _, ee := range logCache.GetEnvelopes() {
				if ee.GetGauge() == nil {
					continue
				}
				if ee.Tags["addr"] != fmt.Sprintf("10.0.0.%f:8080", pass) {
					continue
				}

				e = ee
				pass++

				Expect(e.GetGauge().Metrics["WorkerState"].Value).To(Equal(pass))
				// TODO - assert on the other meaningful attributes of e
			}
		})

	})

	Context("Version metrics", func() {
		It("writes the injected version to LogCache as separate gauges", func() {
			tc := setup("1.2.3-dev.4")

			Eventually(func() int {
				return len(tc.logCache.GetEnvelopes())
			}).Should(BeNumerically(">", 0))

			firstEnvelope := tc.logCache.GetEnvelopes()[0]
			Expect(firstEnvelope.SourceId).To(Equal("log-cache"))
			Expect(firstEnvelope.GetGauge().Metrics["version-major"].Value).To(Equal(1.0))
			Expect(firstEnvelope.GetGauge().Metrics["version-minor"].Value).To(Equal(2.0))
			Expect(firstEnvelope.GetGauge().Metrics["version-patch"].Value).To(Equal(3.0))
			Expect(firstEnvelope.GetGauge().Metrics["version-pre"].Value).To(Equal(4.0))
		})

		Context("when the version does not have a pre portion", func() {
			It("writes the injected version to LogCache as separate gauges", func() {
				tc := setup("1.2.3")

				Eventually(func() int {
					return len(tc.logCache.GetEnvelopes())
				}).Should(BeNumerically(">", 0))

				firstEnvelope := tc.logCache.GetEnvelopes()[0]
				Expect(firstEnvelope.SourceId).To(Equal("log-cache"))
				Expect(firstEnvelope.GetGauge().Metrics["version-major"].Value).To(Equal(1.0))
				Expect(firstEnvelope.GetGauge().Metrics["version-minor"].Value).To(Equal(2.0))
				Expect(firstEnvelope.GetGauge().Metrics["version-patch"].Value).To(Equal(3.0))
				Expect(firstEnvelope.GetGauge().Metrics["version-pre"].Value).To(Equal(0.0))
			})
		})

		It("writes the version gauges to the Structured Logger", func() {
			tc := setup("1.2.3-dev.4")
			Eventually(tc.sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"Version","value":"1.2.3-dev.4","source_id":"log-cache","type":"gauge"}`))
		})
	})
})

type testContext struct {
	logCache *testing.SpyLogCache
	sbuffer  *gbytes.Buffer
}

func setup(version string) testContext {
	var err error
	tlsConfig, err := testing.NewTLSConfig(
		testing.Cert("log-cache-ca.crt"),
		testing.Cert("log-cache.crt"),
		testing.Cert("log-cache.key"),
		"log-cache",
	)
	Expect(err).ToNot(HaveOccurred())

	logCache := testing.NewSpyLogCache(tlsConfig)
	addr := logCache.Start()
	sbuffer := gbytes.NewBuffer()

	expvarForwarder := NewExpvarForwarder(addr,
		WithExpvarInterval(time.Millisecond),
		WithExpvarStructuredLogger(log.New(sbuffer, "", 0)),
		WithExpvarVersion(version),
		WithExpvarDefaultSourceId("log-cache"),
		WithExpvarDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
	)

	go expvarForwarder.Start()

	return testContext{
		logCache: logCache,
		sbuffer:  sbuffer,
	}
}
