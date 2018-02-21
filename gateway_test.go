package logcache_test

import (
	"fmt"
	"net/http"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gateway", func() {
	var (
		spyLogCache    *spyLogCache
		spyGroupReader *spyGroupReader

		gw *logcache.Gateway
	)

	BeforeEach(func() {
		tlsConfig, err := newTLSConfig(
			Cert("log-cache-ca.crt"),
			Cert("log-cache.crt"),
			Cert("log-cache.key"),
			"log-cache",
		)
		Expect(err).ToNot(HaveOccurred())

		spyLogCache = newSpyLogCache(tlsConfig)
		logCacheAddr := spyLogCache.start()

		spyGroupReader = newSpyGroupReader(tlsConfig)
		groupReaderAddr := spyGroupReader.start()

		gw = logcache.NewGateway(
			logCacheAddr,
			groupReaderAddr,
			"127.0.0.1:0",
			logcache.WithGatewayLogCacheDialOpts(
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithGatewayGroupReaderDialOpts(
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
		)
		gw.Start()
	})

	It("upgrades HTTP requests for LogCache into gRPC requests", func() {
		path := "v1/read/some-source-id?start_time=99&end_time=101&limit=103&envelope_type=LOG"
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		resp, err := http.Get(URL)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyLogCache.getReadRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].SourceId).To(Equal("some-source-id"))
		Expect(reqs[0].StartTime).To(Equal(int64(99)))
		Expect(reqs[0].EndTime).To(Equal(int64(101)))
		Expect(reqs[0].Limit).To(Equal(int64(103)))
		Expect(reqs[0].EnvelopeType).To(Equal(rpc.EnvelopeTypes_LOG))
	})

	It("upgrades HTTP requests for GroupReader into gRPC requests", func() {
		path := "v1/group/some-name?start_time=99&end_time=101&limit=103&envelope_type=LOG"
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		resp, err := http.Get(URL)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyGroupReader.getReadRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].Name).To(Equal("some-name"))
		Expect(reqs[0].StartTime).To(Equal(int64(99)))
		Expect(reqs[0].EndTime).To(Equal(int64(101)))
		Expect(reqs[0].Limit).To(Equal(int64(103)))
		Expect(reqs[0].EnvelopeType).To(Equal(rpc.EnvelopeTypes_LOG))
	})
})
