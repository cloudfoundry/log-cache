package logcache_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gateway", func() {
	var (
		spyLogCache         *spyLogCache
		spyShardGroupReader *spyShardGroupReader

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

		spyShardGroupReader = newSpyGroupReader(tlsConfig)
		groupReaderAddr := spyShardGroupReader.start()

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

	DescribeTable("upgrades HTTP requests for LogCache into gRPC requests", func(pathSourceID, expectedSourceID string) {
		path := fmt.Sprintf("v1/read/%s?start_time=99&end_time=101&limit=103&envelope_types=LOG&envelope_types=GAUGE", pathSourceID)
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		resp, err := http.Get(URL)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyLogCache.getReadRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].SourceId).To(Equal(expectedSourceID))
		Expect(reqs[0].StartTime).To(Equal(int64(99)))
		Expect(reqs[0].EndTime).To(Equal(int64(101)))
		Expect(reqs[0].Limit).To(Equal(int64(103)))
		Expect(reqs[0].EnvelopeTypes).To(ConsistOf(rpc.EnvelopeType_LOG, rpc.EnvelopeType_GAUGE))
	},
		Entry("URL encoded", "some-source%2Fid", "some-source/id"),
		Entry("with slash", "some-source/id", "some-source/id"),
		Entry("with dash", "some-source-id", "some-source-id"),
	)

	It("upgrades HTTP requests for ShardGroupReader into gRPC requests", func() {
		path := "v1/experimental/shard_group/some-name?start_time=99&end_time=101&limit=103&envelope_types=LOG"
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		resp, err := http.Get(URL)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyShardGroupReader.getReadRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].Name).To(Equal("some-name"))
		Expect(reqs[0].StartTime).To(Equal(int64(99)))
		Expect(reqs[0].EndTime).To(Equal(int64(101)))
		Expect(reqs[0].Limit).To(Equal(int64(103)))
		Expect(reqs[0].EnvelopeTypes).To(ConsistOf(rpc.EnvelopeType_LOG))
	})

	It("upgrades HTTP requests for ShardGroupReader PUTs into gRPC requests", func() {
		path := "v1/experimental/shard_group/some-name"
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("PUT", URL, strings.NewReader(`{
			"sourceIds": ["some-source/id"]
		}`))

		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyShardGroupReader.AddRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].Name).To(Equal("some-name"))
		Expect(reqs[0].GetSubGroup().GetSourceIds()).To(ConsistOf("some-source/id"))
	})

	It("upgrades HTTP requests for instant queries via PromQLQuerier GETs into gRPC requests", func() {
		path := `v1/promql?query=metric{source_id="some-id"}&time=1234`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyLogCache.getQueryRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].Query).To(Equal(`metric{source_id="some-id"}`))
		Expect(reqs[0].Time).To(Equal(int64(1234)))
	})

	It("upgrades HTTP requests for range queries via PromQLQuerier GETs into gRPC requests", func() {
		path := `v1/promql_range?query=metric{source_id="some-id"}&start=1234&end=5678&step=30s`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyLogCache.getRangeQueryRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].Query).To(Equal(`metric{source_id="some-id"}`))
		Expect(reqs[0].Start).To(Equal(int64(1234)))
		Expect(reqs[0].End).To(Equal(int64(5678)))
		Expect(reqs[0].Step).To(Equal("30s"))
	})

	It("outputs json with zero-value points", func() {
		path := `v1/promql?query=metric{source_id="some-id"}&time=1234`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		spyLogCache.SetValue(0)

		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, _ := ioutil.ReadAll(resp.Body)
		Expect(string(body)).To(Equal(`{"scalar":{"time":"99","value":0}}`))
	})
})
