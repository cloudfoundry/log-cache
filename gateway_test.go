package logcache_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"code.cloudfoundry.org/log-cache"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gateway", func() {
	var (
		spyLogCache *spyLogCache
		gw          *logcache.Gateway
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

		gw = logcache.NewGateway(
			logCacheAddr,
			"127.0.0.1:0",
			logcache.WithGatewayLogCacheDialOpts(
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithGatewayVersion("1.2.3"),
		)
		gw.Start()
	})

	DescribeTable("upgrades HTTP requests for LogCache into gRPC requests", func(pathSourceID, expectedSourceID string) {
		path := fmt.Sprintf("api/v1/read/%s?start_time=99&end_time=101&limit=103&envelope_types=LOG&envelope_types=GAUGE", pathSourceID)
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

	It("adds newlines to the end of HTTP responses", func() {
		path := `api/v1/meta`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		respBytes, err := ioutil.ReadAll(resp.Body)
		Expect(string(respBytes)).To(MatchRegexp(`\n$`))
	})

	It("upgrades HTTP requests for instant queries via PromQLQuerier GETs into gRPC requests", func() {
		path := `api/v1/query?query=metric{source_id="some-id"}&time=1234.000`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyLogCache.getQueryRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].Query).To(Equal(`metric{source_id="some-id"}`))
		Expect(reqs[0].Time).To(Equal("1234.000"))
	})

	It("upgrades HTTP requests for range queries via PromQLQuerier GETs into gRPC requests", func() {
		path := `api/v1/query_range?query=metric{source_id="some-id"}&start=1234.000&end=5678.000&step=30s`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		reqs := spyLogCache.getRangeQueryRequests()
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].Query).To(Equal(`metric{source_id="some-id"}`))
		Expect(reqs[0].Start).To(Equal("1234.000"))
		Expect(reqs[0].End).To(Equal("5678.000"))
		Expect(reqs[0].Step).To(Equal("30s"))
	})

	It("outputs json with zero-value points and correct Prometheus API fields", func() {
		path := `api/v1/query?query=metric{source_id="some-id"}&time=1234`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		spyLogCache.SetValue(0)

		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, _ := ioutil.ReadAll(resp.Body)
		Expect(body).To(MatchJSON(`{"status":"success","data":{"resultType":"scalar","result":[99,"0"]}}`))
	})

	It("returns version information from an info endpoint", func() {
		path := `api/v1/info`
		URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
		req, _ := http.NewRequest("GET", URL, nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		respBytes, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(respBytes).To(MatchJSON(`{"version":"1.2.3"}`))
		Expect(strings.HasSuffix(string(respBytes), "\n")).To(BeTrue())
	})

	Context("errors", func() {
		It("passes through content-type correctly on errors", func() {
			path := `api/v1/query?query=metric{source_id="some-id"}&time=1234`
			spyLogCache.queryError = errors.New("expected error")
			URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
			req, _ := http.NewRequest("GET", URL, nil)

			resp, err := http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))
			Expect(resp.Header).To(HaveKeyWithValue("Content-Type", []string{"application/json"}))
		})

		It("adds necessary fields to match Prometheus API", func() {
			path := `api/v1/query?query=metric{source_id="some-id"}&time=1234`
			spyLogCache.queryError = errors.New("expected error")
			URL := fmt.Sprintf("http://%s/%s", gw.Addr(), path)
			req, _ := http.NewRequest("GET", URL, nil)

			resp, err := http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			body, _ := ioutil.ReadAll(resp.Body)
			Expect(body).To(MatchJSON(`{
				"status": "error",

				"errorType": "internal",
				"error": "expected error"
			}`))
		})
	})
})
