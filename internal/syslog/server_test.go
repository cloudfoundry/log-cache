package syslog_test

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/syslog"
	"code.cloudfoundry.org/log-cache/internal/testing"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"code.cloudfoundry.org/tlsconfig"
	"context"
	"crypto/tls"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

var _ = Describe("Syslog", func() {
	var (
		server     *syslog.Server
		spyMetrics *testing.SpyMetrics
		logCache   *spyLogCacheClient
		loggr      *log.Logger
	)

	BeforeEach(func() {
		spyMetrics = testing.NewSpyMetrics()
		logCache = newSpyLogCacheClient()
		loggr = log.New(GinkgoWriter, "", log.LstdFlags)

		server = syslog.NewServer(
			loggr,
			logCache,
			spyMetrics,
			testing.Cert("log-cache.crt"),
			testing.Cert("log-cache.key"),
			syslog.WithServerPort(0),
		)

		go server.Start()
		for server.Addr() == "" {
			continue //Wait for the server to start
		}
	})

	AfterEach(func() {
		server.Stop()
	})

	It("counts incoming messages", func() {
		tlsConfig := buildClientTLSConfig(tls.VersionTLS12, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
		conn, err := tlsClientConnection(server.Addr(), tlsConfig)
		Expect(err).ToNot(HaveOccurred())

		_, err = fmt.Fprint(conn, "89 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [APP/2] - - just a test\n")
		Expect(err).ToNot(HaveOccurred())

		_, err = fmt.Fprint(conn, "89 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [APP/2] - - just a test\n")
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() float64 {
			return spyMetrics.Get("ingress")
		}).Should(Equal(2.0))
	})

	It("sends log messages to log cache", func() {
		tlsConfig := buildClientTLSConfig(tls.VersionTLS12, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
		conn, err := tlsClientConnection(server.Addr(), tlsConfig)
		Expect(err).ToNot(HaveOccurred())

		_, err = fmt.Fprint(conn, "89 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [APP/2] - - just a test\n")
		Expect(err).ToNot(HaveOccurred())

		Eventually(logCache.envelopes).Should(ContainElement(
			&loggregator_v2.Envelope{
				Tags: map[string]string{
					"source_type": "APP",
				},
				InstanceId: "2",
				Timestamp:  12345000,
				SourceId:   "test-app-id",
				Message: &loggregator_v2.Envelope_Log{
					Log: &loggregator_v2.Log{
						Payload: []byte("just a test"),
						Type:    loggregator_v2.Log_OUT,
					},
				},
			},
		))
	})

	It("sends counter messages to log cache", func() {
		tlsConfig := buildClientTLSConfig(tls.VersionTLS12, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
		conn, err := tlsClientConnection(server.Addr(), tlsConfig)
		Expect(err).ToNot(HaveOccurred())

		_, err = fmt.Fprint(conn, "129 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [1] - [counter@47450 name=\"some-counter\" total=\"99\" delta=\"1\"] \n")
		Expect(err).ToNot(HaveOccurred())

		Eventually(logCache.envelopes).Should(ContainElement(
			&loggregator_v2.Envelope{
				InstanceId: "1",
				Timestamp:  12345000,
				SourceId:   "test-app-id",
				Message: &loggregator_v2.Envelope_Counter{
					Counter: &loggregator_v2.Counter{
						Name:  "some-counter",
						Delta: 1,
						Total: 99,
					},
				},
			},

		))
	})

	It("sends gauge messages to log cache", func() {
		tlsConfig := buildClientTLSConfig(tls.VersionTLS12, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
		conn, err := tlsClientConnection(server.Addr(), tlsConfig)
		Expect(err).ToNot(HaveOccurred())

		_, err = fmt.Fprint(conn, "128 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [1] - [gauge@47450 name=\"cpu\" value=\"0.23\" unit=\"percentage\"] \n")
		Expect(err).ToNot(HaveOccurred())

		Eventually(logCache.envelopes).Should(ContainElement(
			&loggregator_v2.Envelope{
				InstanceId: "1",
				Timestamp:  12345000,
				SourceId:   "test-app-id",
				Message: &loggregator_v2.Envelope_Gauge{
					Gauge: &loggregator_v2.Gauge{
						Metrics: map[string]*loggregator_v2.GaugeValue{
							"cpu": {Unit: "percentage", Value: 0.23},
						},
					},
				},
			},
		))
	})

	It("increments invalid message metric when there is an invalid syslog message", func() {
		tlsConfig := buildClientTLSConfig(tls.VersionTLS12, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
		conn, err := tlsClientConnection(server.Addr(), tlsConfig)
		Expect(err).ToNot(HaveOccurred())

		lengthIsWrong := "126 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [1] - [gauge@47450 name=\"cpu\" value=\"0.23\" unit=\"percentage\"] \n"
		_, err = fmt.Fprint(conn, lengthIsWrong)
		Expect(err).ToNot(HaveOccurred())

		_, err = fmt.Fprint(conn, "128 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [1] - [gauge@47450 name=\"cpu\" value=\"0.23\" unit=\"percentage\"] \n")
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() float64 {
			return spyMetrics.Get("invalid_ingress")
		}).Should(Equal(1.0))

		Eventually(func() float64 {
			return spyMetrics.Get("ingress")
		}).Should(Equal(1.0))
	})

	DescribeTable("increments invalid ingress on invalid envelope data", func(rfc5424Log string) {
		tlsConfig := buildClientTLSConfig(tls.VersionTLS12, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
		conn, err := tlsClientConnection(server.Addr(), tlsConfig)
		Expect(err).ToNot(HaveOccurred())

		_, err = fmt.Fprint(conn, rfc5424Log)
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() float64 {
			return spyMetrics.Get("invalid_ingress")
		}).Should(Equal(1.0))
	},
		Entry("Counter - invalid delta", fmt.Sprintf(counterFormat, 129, "99", "d")),
		Entry("Counter - invalid total", fmt.Sprintf(counterFormat, 129, "dd", "9")),
		Entry("Gauge - no unit provided", fmt.Sprintf(gaugeFormat, 128, "0.23", "blah=\"percentage\"")),
		Entry("Gauge - invalid value", fmt.Sprintf(gaugeFormat, 128, "dddd", "unit=\"percentage\"")),
	)

	Describe("TLS security", func() {
		DescribeTable("allows only supported TLS versions", func(clientTLSVersion int, serverShouldAllow bool) {
			tlsConfig := buildClientTLSConfig(uint16(clientTLSVersion), tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256)
			_, err := tlsClientConnection(server.Addr(), tlsConfig)

			if serverShouldAllow {
				Expect(err).ToNot(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
			}
		},
			Entry("unsupported SSL 3.0", tls.VersionSSL30, false),
			Entry("unsupported TLS 1.0", tls.VersionTLS10, false),
			Entry("unsupported TLS 1.1", tls.VersionTLS11, false),
			Entry("supported TLS 1.2", tls.VersionTLS12, true),
		)

		DescribeTable("allows only supported TLS versions", func(cipherSuite uint16, serverShouldAllow bool) {
			tlsConfig := buildClientTLSConfig(tls.VersionTLS12, cipherSuite)
			_, err := tlsClientConnection(server.Addr(), tlsConfig)

			if serverShouldAllow {
				Expect(err).ToNot(HaveOccurred())
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
})

const counterFormat = "%d <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [1] - [counter@47450 name=\"some-counter\" total=\"%s\" delta=\"%s\"] \n"
const gaugeFormat = "%d <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [1] - [gauge@47450 name=\"cpu\" value=\"%s\" %s] \n"

func newSpyLogCacheClient() *spyLogCacheClient {
	return &spyLogCacheClient{}
}

type spyLogCacheClient struct {
	sync.Mutex
	envs      []*loggregator_v2.Envelope
	sendError error
}

func (s *spyLogCacheClient) Send(ctx context.Context, in *logcache_v1.SendRequest, opts ...grpc.CallOption) (*logcache_v1.SendResponse, error) {
	s.Lock()
	defer s.Unlock()

	if s.sendError != nil {
		return nil, s.sendError
	}

	s.envs = append(s.envs, in.Envelopes.Batch...)
	return &logcache_v1.SendResponse{}, nil
}

func (s *spyLogCacheClient) envelopes() []*loggregator_v2.Envelope {
	s.Lock()
	defer s.Unlock()

	return s.envs
}

func buildClientTLSConfig(maxVersion, cipherSuite uint16) *tls.Config {
	tlsConf, err := tlsconfig.Build(
		tlsconfig.WithIdentityFromFile(
			testing.Cert("log-cache.crt"),
			testing.Cert("log-cache.key"),
		),
	).Client(tlsconfig.WithAuthorityFromFile(testing.Cert("log-cache-ca.crt")))
	Expect(err).ToNot(HaveOccurred())

	tlsConf.MaxVersion = uint16(maxVersion)
	tlsConf.CipherSuites = []uint16{cipherSuite}
	tlsConf.InsecureSkipVerify = true

	return tlsConf
}

func tlsClientConnection(addr string, tlsConf *tls.Config) (*tls.Conn, error) {
	dialer := &net.Dialer{}
	dialer.Timeout = time.Second
	return tls.DialWithDialer(dialer, "tcp", addr, tlsConf)
}
