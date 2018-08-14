package logcache_test

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("ExpvarForwarder", func() {
	var (
		r *logcache.ExpvarForwarder

		addr      string
		server1   *httptest.Server
		server2   *httptest.Server
		server    *httptest.Server
		logCache  *spyLogCache
		tlsConfig *tls.Config
		sbuffer   *gbytes.Buffer
	)

	Context("Normal gauges and counters", func() {
		BeforeEach(func() {
			var err error
			tlsConfig, err = newTLSConfig(
				Cert("log-cache-ca.crt"),
				Cert("log-cache.crt"),
				Cert("log-cache.key"),
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

			logCache = newSpyLogCache(tlsConfig)
			addr = logCache.start()

			sbuffer = gbytes.NewBuffer()

			r = logcache.NewExpvarForwarder(addr,
				logcache.WithExpvarInterval(time.Millisecond),
				logcache.WithExpvarStructuredLogger(log.New(sbuffer, "", 0)),
				logcache.AddExpvarGaugeTemplate(
					server1.URL,
					"CachePeriod",
					"mS",
					"log-cache",
					"{{.LogCache.CachePeriod}}",
					map[string]string{"a": "some-value"},
				),
				logcache.AddExpvarCounterTemplate(
					server2.URL,
					"Egress",
					"log-cache-nozzle",
					"{{.LogCache.Egress}}",
					map[string]string{"a": "some-value"},
				),

				logcache.WithExpvarDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
			)

			go r.Start()
		})

		It("writes the expvar metrics to LogCache", func() {
			Eventually(func() int {
				return len(logCache.getEnvelopes())
			}).Should(BeNumerically(">=", 2))

			var e *loggregator_v2.Envelope

			// Find counter
			for _, ee := range logCache.getEnvelopes() {
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
			for _, ee := range logCache.getEnvelopes() {
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

		It("writes the expvar counters to the Structured Logger", func() {
			Eventually(sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"Egress","value":999,"source_id":"log-cache-nozzle","type":"counter"}`))
		})

		It("writes the expvar gauges to the Structured Logger", func() {
			Eventually(sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"CachePeriod","value":68644.000000,"source_id":"log-cache","type":"gauge"}`))
		})

		It("panics if there is not a counter or gauge configured", func() {
			Expect(func() {
				logcache.NewExpvarForwarder(addr)
			}).To(Panic())
		})

		It("panics if a counter or gauge template is invalid", func() {
			Expect(func() {
				logcache.NewExpvarForwarder(addr,
					logcache.AddExpvarCounterTemplate(
						server1.URL, "some-name", "a", "{{invalid", nil,
					),
				)
			}).To(Panic())

			Expect(func() {
				logcache.NewExpvarForwarder(addr,
					logcache.AddExpvarGaugeTemplate(
						server1.URL, "some-name", "", "a", "{{invalid", nil,
					),
				)
			}).To(Panic())
		})
	})

	Context("Map gauges", func() {
		BeforeEach(func() {
			var err error
			tlsConfig, err = newTLSConfig(
				Cert("log-cache-ca.crt"),
				Cert("log-cache.crt"),
				Cert("log-cache.key"),
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

			logCache = newSpyLogCache(tlsConfig)
			addr = logCache.start()

			sbuffer = gbytes.NewBuffer()

			r = logcache.NewExpvarForwarder(addr,
				logcache.WithExpvarInterval(time.Millisecond),
				logcache.WithExpvarStructuredLogger(log.New(sbuffer, "", 0)),
				logcache.AddExpvarMapTemplate(
					server.URL,
					"WorkerState",
					"log-cache-scheduler",
					"{{.LogCache.WorkerState | jsonMap}}",
					map[string]string{"a": "some-value"},
				),

				logcache.WithExpvarDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
			)

			go r.Start()
		})

		It("writes the expvar map to LogCache as gauges", func() {
			Eventually(func() int {
				return len(logCache.getEnvelopes())
			}).Should(BeNumerically(">=", 3))

			var e *loggregator_v2.Envelope
			e = nil
			pass := 1.0

			for _, ee := range logCache.getEnvelopes() {
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
})
