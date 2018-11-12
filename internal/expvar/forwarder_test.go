package expvar_test

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	. "code.cloudfoundry.org/log-cache/internal/expvar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/log-cache/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("ExpvarForwarder", func() {
	var (
		tc *testContext
	)

	Context("Normal gauges and counters", func() {
		BeforeEach(func() {
			tc = setup()
			ts1 := tc.newServer(`"CachePeriod":68644,"Egress":999,"Expired":0,"Ingress":633`)
			ts2 := tc.newServer(`"Egress":999,"Ingress":633`)

			ts1.newGaugeTemplate(
				"CachePeriod",
				"ms",
				"",
				map[string]string{"a": "some-value"},
			)
			ts2.newCounterTemplate(
				"Egress",
				"log-cache-nozzle",
				map[string]string{"a": "some-value"},
			)

			tc.start()
		})

		It("writes the expvar metrics to LogCache", func() {
			Eventually(func() int {
				return len(tc.agent.GetEnvelopes())
			}).Should(BeNumerically(">=", 2))

			envelope := tc.findCounter()
			Expect(envelope).ToNot(BeNil())
			Expect(envelope.SourceId).To(Equal("log-cache-nozzle"))
			Expect(envelope.Timestamp).ToNot(BeZero())
			Expect(envelope.GetCounter().Name).To(Equal("Egress"))
			Expect(envelope.GetCounter().Total).To(Equal(uint64(999)))
			Expect(envelope.Tags).To(Equal(map[string]string{"a": "some-value"}))

			envelope = tc.findGauge()
			Expect(envelope).ToNot(BeNil())
			Expect(envelope.SourceId).To(Equal("log-cache"))
			Expect(envelope.Timestamp).ToNot(BeZero())
			Expect(envelope.GetGauge().Metrics).To(HaveLen(1))
			Expect(envelope.GetGauge().Metrics["CachePeriod"].Value).To(Equal(68644.0))
			Expect(envelope.GetGauge().Metrics["CachePeriod"].Unit).To(Equal("ms"))
			Expect(envelope.Tags).To(Equal(map[string]string{"a": "some-value"}))
		})

		It("writes correct timestamps to LogCache", func() {
			Eventually(func() int {
				return len(tc.agent.GetEnvelopes())
			}).Should(BeNumerically(">=", 4))

			counterEnvelopes := tc.findCounters()
			Expect(counterEnvelopes[0].Timestamp).ToNot(Equal(counterEnvelopes[1].Timestamp))
		})

		It("writes the expvar counters to the Structured Logger", func() {
			Eventually(tc.sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"Egress","value":999,"source_id":"log-cache-nozzle","type":"counter"}`))
		})

		It("writes the expvar gauges to the Structured Logger", func() {
			Eventually(tc.sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"CachePeriod","value":68644.000000,"source_id":"log-cache","type":"gauge"}`))
		})

		It("panics if a counter or gauge template is invalid", func() {
			Expect(func() {
				NewExpvarForwarder("localhost:1234",
					AddExpvarCounterTemplate(
						"http://localhost:9999", "some-name", "a", "{{invalid", nil,
					),
				)
			}).To(Panic())

			Expect(func() {
				NewExpvarForwarder("localhost:1234",
					AddExpvarGaugeTemplate(
						"http://localhost:9999", "some-name", "", "a", "{{invalid", nil,
					),
				)
			}).To(Panic())
		})
	})

	Context("Map gauges", func() {
		It("writes the expvar map to LogCache as gauges", func() {
			tc := setup()
			ts := tc.newServer(`"WorkerState":{"10.0.0.1:8080":1,"10.0.0.2:8080":2,"10.0.0.3:8080":3}`)
			ts.newMapTemplate(
				"WorkerState",
				"log-cache-scheduler",
				map[string]string{"a": "some-value"},
			)
			tc.start()

			Eventually(func() int {
				return len(tc.agent.GetEnvelopes())
			}).Should(BeNumerically(">=", 3))

			pass := 1.0
			for _, envelope := range tc.findGauges() {
				if envelope.Tags["addr"] != fmt.Sprintf("10.0.0.%f:8080", pass) {
					continue
				}

				pass++

				Expect(envelope.GetGauge().Metrics["WorkerState"].Value).To(Equal(pass))
			}
		})
	})

	Context("Version metrics", func() {
		It("writes the injected version to LogCache as separate gauges", func() {
			tc := setup()
			tc.withVersion("1.2.3-dev.4")
			tc.start()

			Eventually(func() int {
				return len(tc.agent.GetEnvelopes())
			}).Should(BeNumerically(">", 0))

			firstEnvelope := tc.agent.GetEnvelopes()[0]
			Expect(firstEnvelope.SourceId).To(Equal("log-cache"))
			Expect(firstEnvelope.GetGauge().Metrics["version-major"].Value).To(Equal(1.0))
			Expect(firstEnvelope.GetGauge().Metrics["version-minor"].Value).To(Equal(2.0))
			Expect(firstEnvelope.GetGauge().Metrics["version-patch"].Value).To(Equal(3.0))
			Expect(firstEnvelope.GetGauge().Metrics["version-pre"].Value).To(Equal(4.0))
		})

		Context("when the version does not have a pre portion", func() {
			It("writes the injected version to LogCache as separate gauges", func() {
				tc := setup()
				tc.withVersion("1.2.3")
				tc.start()

				Eventually(func() int {
					return len(tc.agent.GetEnvelopes())
				}).Should(BeNumerically(">", 0))

				firstEnvelope := tc.agent.GetEnvelopes()[0]
				Expect(firstEnvelope.SourceId).To(Equal("log-cache"))
				Expect(firstEnvelope.GetGauge().Metrics["version-major"].Value).To(Equal(1.0))
				Expect(firstEnvelope.GetGauge().Metrics["version-minor"].Value).To(Equal(2.0))
				Expect(firstEnvelope.GetGauge().Metrics["version-patch"].Value).To(Equal(3.0))
				Expect(firstEnvelope.GetGauge().Metrics["version-pre"].Value).To(Equal(0.0))
			})
		})

		It("writes the version gauges to the Structured Logger", func() {
			tc := setup()
			tc.withVersion("1.2.3-dev.4")
			tc.start()

			Eventually(tc.sbuffer).Should(gbytes.Say(`{"timestamp":[0-9]+,"name":"Version","value":"1.2.3-dev.4","source_id":"log-cache","type":"gauge"}`))
		})
	})
})

type testContext struct {
	agent           *testing.SpyAgent
	sbuffer         *gbytes.Buffer
	server          *httptest.Server
	expvarForwarder *ExpvarForwarder
}

type testServer struct {
	parentContext *testContext
	server        *httptest.Server
}

func setup() *testContext {
	var err error
	tlsConfig, err := testing.NewTLSConfig(
		testing.Cert("log-cache-ca.crt"),
		testing.Cert("log-cache.crt"),
		testing.Cert("log-cache.key"),
		"log-cache",
	)
	Expect(err).ToNot(HaveOccurred())

	agent := testing.NewSpyAgent(tlsConfig)
	agentAddr := agent.Start()
	sbuffer := gbytes.NewBuffer()

	expvarForwarder := NewExpvarForwarder(agentAddr,
		WithExpvarInterval(time.Millisecond),
		WithExpvarStructuredLogger(log.New(sbuffer, "", 0)),
		WithExpvarDefaultSourceId("log-cache"),
		WithAgentDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
	)

	return &testContext{
		agent:           agent,
		sbuffer:         sbuffer,
		expvarForwarder: expvarForwarder,
	}
}

func (tc *testContext) withVersion(version string) {
	WithExpvarVersion(version)(tc.expvarForwarder)
}

func (tc *testContext) start() {
	go tc.expvarForwarder.Start()
}

func (tc *testContext) newServer(response string) *testServer {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf(`{"LogCache": {%s}}`, response)))
	}))

	return &testServer{
		parentContext: tc,
		server:        server,
	}
}

func (tc *testContext) findCounters() []*loggregator_v2.Envelope {
	var envelopes []*loggregator_v2.Envelope

	for _, ee := range tc.agent.GetEnvelopes() {
		if ee.GetCounter() == nil {
			continue
		}

		envelopes = append(envelopes, ee)
	}

	return envelopes
}

func (tc *testContext) findGauges() []*loggregator_v2.Envelope {
	var envelopes []*loggregator_v2.Envelope

	for _, ee := range tc.agent.GetEnvelopes() {
		if ee.GetGauge() == nil {
			continue
		}

		envelopes = append(envelopes, ee)
	}

	return envelopes
}

func (tc *testContext) findCounter() *loggregator_v2.Envelope {
	return tc.findCounters()[0]
}

func (tc *testContext) findGauge() *loggregator_v2.Envelope {
	return tc.findGauges()[0]
}

func (ts *testServer) newMapTemplate(key, sourceId string, tags map[string]string) {
	AddExpvarMapTemplate(
		ts.server.URL,
		key,
		sourceId,
		fmt.Sprintf("{{.LogCache.%s | jsonMap}}", key),
		tags,
	)(ts.parentContext.expvarForwarder)
}

func (ts *testServer) newGaugeTemplate(key, unit, sourceId string, tags map[string]string) {
	AddExpvarGaugeTemplate(
		ts.server.URL,
		key,
		unit,
		sourceId,
		fmt.Sprintf("{{.LogCache.%s}}", key),
		tags,
	)(ts.parentContext.expvarForwarder)
}

func (ts *testServer) newCounterTemplate(key, sourceId string, tags map[string]string) {
	AddExpvarCounterTemplate(
		ts.server.URL,
		key,
		sourceId,
		fmt.Sprintf("{{.LogCache.%s}}", key),
		tags,
	)(ts.parentContext.expvarForwarder)
}
