package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/pkg/client"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

// Assert that client.Reader is fulfilled by Client.Read
var _ client.Reader = client.Reader(client.NewClient("").Read)

var _ = Describe("Log Cache Client", func() {
	Context("HTTP client", func() {
		Describe("LogCacheVersion", func() {
			It("returns the log cache version", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				version, err := logcache_client.LogCacheVersion(context.Background())
				Expect(err).ToNot(HaveOccurred())

				Expect(version.String()).To(Equal("2.0.0"))

				Expect(logCache.reqs).To(HaveLen(1))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/info"))
			})
		})

		Describe("LogCacheVMUptime", func() {
			It("returns the log cache gateway VM uptime", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				uptime, err := logcache_client.LogCacheVMUptime(context.Background())
				Expect(err).ToNot(HaveOccurred())

				Expect(uptime).To(Equal(int64(789)))

				Expect(logCache.reqs).To(HaveLen(1))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/info"))
			})

			It("returns an error if /info endpoint does not exist", func() {
				logCache := newStubOldLogCache()
				logcache_client := client.NewClient(logCache.addr())

				uptime, err := logcache_client.LogCacheVMUptime(context.Background())
				Expect(err).To(MatchError("unexpected status code 404"))

				Expect(uptime).To(Equal(int64(-1)))
			})
		})

		Describe("Read", func() {
			It("reads envelopes", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				envelopes, err := logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99))
				Expect(err).ToNot(HaveOccurred())

				Expect(envelopes).To(HaveLen(2))

				Expect(envelopes[0].Timestamp).To(BeEquivalentTo(99))
				Expect(envelopes[1].Timestamp).To(BeEquivalentTo(100))

				Expect(logCache.reqs).To(HaveLen(2))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/info"))
				Expect(logCache.reqs[1].URL.Path).To(Equal("/api/v1/read/some-id"))

				assertQueryParam(logCache.reqs[1].URL, "start_time", "99")

				Expect(logCache.reqs[1].URL.Query()).To(HaveLen(1))
			})

			It("falls back to pre-1.4.7 endpoint", func() {
				logCache := newStubOldLogCache()
				logcache_client := client.NewClient(logCache.addr())

				envelopes, err := logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99))
				Expect(err).ToNot(HaveOccurred())

				Expect(envelopes).To(HaveLen(2))

				Expect(envelopes[0].Timestamp).To(BeEquivalentTo(99))
				Expect(envelopes[1].Timestamp).To(BeEquivalentTo(100))

				Expect(logCache.reqs).To(HaveLen(2))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/info"))
				Expect(logCache.reqs[1].URL.Path).To(Equal("/v1/read/some-id"))

				assertQueryParam(logCache.reqs[1].URL, "start_time", "99")

				Expect(logCache.reqs[1].URL.Query()).To(HaveLen(1))
			})

			It("respects options", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.Read(
					context.Background(),
					"some-id",
					time.Unix(0, 99),
					client.WithEndTime(time.Unix(0, 101)),
					client.WithLimit(103),
					client.WithEnvelopeTypes(rpc.EnvelopeType_LOG, rpc.EnvelopeType_GAUGE),
					client.WithDescending(),
					client.WithNameFilter("name.*"),
				)

				Expect(err).ToNot(HaveOccurred())

				Expect(logCache.reqs).To(HaveLen(2))
				Expect(logCache.reqs[1].URL.Path).To(Equal("/api/v1/read/some-id"))

				assertQueryParam(logCache.reqs[1].URL, "start_time", "99")
				assertQueryParam(logCache.reqs[1].URL, "end_time", "101")
				assertQueryParam(logCache.reqs[1].URL, "limit", "103")
				assertQueryParam(logCache.reqs[1].URL, "envelope_types", "LOG", "GAUGE")
				assertQueryParam(logCache.reqs[1].URL, "name_filter", "name.*")
				assertQueryParam(logCache.reqs[1].URL, "descending", "true")

				Expect(logCache.reqs[1].URL.Query()).To(HaveLen(6))
			})

			It("closes the body", func() {
				spyHTTPClient := newSpyHTTPClient()
				logcache_client := client.NewClient("", client.WithHTTPClient(spyHTTPClient))
				logcache_client.Read(context.Background(), "some-name", time.Now())

				Expect(spyHTTPClient.body.closed).To(BeTrue())
			})

			It("returns an error on a non-200 status", func() {
				logCache := newStubLogCache()
				logCache.statusCode = 500
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99))
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on invalid JSON", func() {
				logCache := newStubLogCache()
				logCache.result["GET/api/v1/read/some-id"] = []byte("invalid")
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99))
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on empty JSON", func() {
				logCache := newStubLogCache()
				logCache.result["GET/api/v1/read/some-id"] = []byte("{}")
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99))
				Expect(err).ToNot(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logcache_client := client.NewClient("http://invalid.url")

				_, err := logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99))
				Expect(err).To(HaveOccurred())

				logcache_client = client.NewClient("-:-invalid")

				_, err = logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99))
				Expect(err).To(HaveOccurred())
			})

			It("returns an error when the read is cancelled", func() {
				logCache := newStubLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.Read(ctx, "some-id", time.Unix(0, 99))
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("Meta", func() {
			It("retrieves meta information", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				meta, err := logcache_client.Meta(context.Background())
				Expect(err).ToNot(HaveOccurred())

				Expect(meta).To(HaveLen(2))
				Expect(meta).To(HaveKey("source-0"))
				Expect(meta).To(HaveKey("source-1"))
			})

			It("falls back to the pre-1.4.7 endpoint", func() {
				logCache := newStubOldLogCache()
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.Meta(context.Background())
				Expect(err).ToNot(HaveOccurred())

				Expect(logCache.reqs).To(HaveLen(2))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/info"))
				Expect(logCache.reqs[1].URL.Path).To(Equal("/v1/meta"))
			})

			It("closes the body", func() {
				spyHTTPClient := newSpyHTTPClient()
				logcache_client := client.NewClient("", client.WithHTTPClient(spyHTTPClient))
				logcache_client.Meta(context.Background())

				Expect(spyHTTPClient.body.closed).To(BeTrue())
			})

			It("returns an error when the request fails", func() {
				logcache_client := client.NewClient("https://some-bad-addr")
				_, err := logcache_client.Meta(context.Background())
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on a non-200 status", func() {
				logCache := newStubLogCache()
				logCache.statusCode = http.StatusNotFound
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.Meta(context.Background())
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on invalid JSON", func() {
				logCache := newStubLogCache()
				logCache.result["GET/api/v1/meta"] = []byte("not-real-result")
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.Meta(context.Background())
				Expect(err).To(HaveOccurred())
			})

			It("returns an error when the context is cancelled", func() {
				logCache := newStubLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.Meta(ctx)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("PromQL", func() {
			It("reads points", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				result, err := logcache_client.PromQL(context.Background(), `some-query`)
				Expect(err).ToNot(HaveOccurred())

				samples := result.GetVector().GetSamples()
				Expect(samples).To(HaveLen(1))
				Expect(samples[0].Point).To(PointTo(MatchFields(
					IgnoreExtras, Fields{
						"Time":  Equal("1234.000"),
						"Value": BeEquivalentTo(99),
					},
				)))

				Expect(logCache.reqs).To(HaveLen(1))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/query"))
				assertQueryParam(logCache.reqs[0].URL, "query", "some-query")
				Expect(logCache.reqs[0].URL.Query()).To(HaveLen(1))
			})

			It("respects options", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQL(
					context.Background(),
					"some-query",
					client.WithPromQLTime(time.Unix(101, 455700000)),
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(logCache.reqs).To(HaveLen(1))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/query"))
				assertQueryParam(logCache.reqs[0].URL, "query", "some-query")
				assertQueryParam(logCache.reqs[0].URL, "time", "101.456")
				Expect(logCache.reqs[0].URL.Query()).To(HaveLen(2))
			})

			It("closes the body", func() {
				spyHTTPClient := newSpyHTTPClient()
				logcache_client := client.NewClient("", client.WithHTTPClient(spyHTTPClient))
				logcache_client.PromQL(context.Background(), "some-query")

				Expect(spyHTTPClient.body.closed).To(BeTrue())
			})

			It("returns an error on a non-200 status", func() {
				logCache := newStubLogCache()
				logCache.statusCode = 500
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQL(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on invalid JSON", func() {
				logCache := newStubLogCache()
				logCache.result["GET/api/v1/query"] = []byte("invalid")
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQL(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on unreachable URL", func() {
				logcache_client := client.NewClient("http://invalid.url")

				_, err := logcache_client.PromQL(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logcache_client := client.NewClient("-:-invalid")

				_, err := logcache_client.PromQL(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logCache := newStubLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.PromQL(ctx, "some-query")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("PromQLRaw", func() {
			It("reads points", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				result, err := logcache_client.PromQLRaw(context.Background(), `some-query`)
				Expect(err).ToNot(HaveOccurred())

				Expect(logCache.reqs).To(HaveLen(1))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/query"))
				assertQueryParam(logCache.reqs[0].URL, "query", "some-query")
				Expect(logCache.reqs[0].URL.Query()).To(HaveLen(1))

				Expect(result.Status).To(Equal("success"))
				Expect(result.Data.ResultType).To(Equal("vector"))

				expected := logCache.result["GET/api/v1/query"]
				var expectedResult client.PromQLQueryResult

				err = json.Unmarshal(expected, &expectedResult)
				Expect(err).ToNot(HaveOccurred())

				Expect(expectedResult.Data.Result).To(Equal(result.Data.Result))
			})

			It("respects options", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQLRaw(
					context.Background(),
					"some-query",
					client.WithPromQLTime(time.Unix(101, 455700000)),
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(logCache.reqs).To(HaveLen(1))
				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/query"))
				assertQueryParam(logCache.reqs[0].URL, "query", "some-query")
				assertQueryParam(logCache.reqs[0].URL, "time", "101.456")
				Expect(logCache.reqs[0].URL.Query()).To(HaveLen(2))
			})

			It("closes the body", func() {
				spyHTTPClient := newSpyHTTPClient()
				logcache_client := client.NewClient("", client.WithHTTPClient(spyHTTPClient))
				logcache_client.PromQLRaw(context.Background(), "some-query")

				Expect(spyHTTPClient.body.closed).To(BeTrue())
			})

			It("returns an error on a non-200, non-404, non-500 status", func() {
				logCache := newStubLogCache()
				logCache.statusCode = 503
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQLRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on invalid JSON", func() {
				logCache := newStubLogCache()
				logCache.result["GET/api/v1/query"] = []byte("invalid")
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQLRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on unreachable URL", func() {
				logcache_client := client.NewClient("http://invalid.url")

				_, err := logcache_client.PromQLRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logcache_client := client.NewClient("-:-invalid")

				_, err := logcache_client.PromQLRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logCache := newStubLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.PromQLRaw(ctx, "some-query")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("PromQLRange", func() {
			It("retrieves points", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())
				start := time.Unix(time.Now().Unix(), 123000000)
				end := start.Add(time.Minute)

				result, err := logcache_client.PromQLRange(
					context.Background(),
					`some-query`,
					client.WithPromQLStart(start),
					client.WithPromQLEnd(end),
					client.WithPromQLStep("5m"),
				)
				Expect(err).ToNot(HaveOccurred())

				series := result.GetMatrix().GetSeries()
				Expect(series).To(HaveLen(1))

				Expect(series[0].GetPoints()[0].Value).To(BeEquivalentTo(99))
				Expect(series[0].GetPoints()[0].Time).To(Equal("1234.000"))

				Expect(series[0].GetPoints()[1].Value).To(BeEquivalentTo(100))
				Expect(series[0].GetPoints()[1].Time).To(Equal("5678.000"))
				Expect(logCache.reqs).To(HaveLen(1))

				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/query_range"))

				assertQueryParam(logCache.reqs[0].URL, "query", "some-query")
				assertQueryParam(logCache.reqs[0].URL, "start", fmt.Sprintf("%.3f", float64(start.UnixNano())/1e9))
				assertQueryParam(logCache.reqs[0].URL, "end", fmt.Sprintf("%.3f", float64(end.UnixNano())/1e9))
				assertQueryParam(logCache.reqs[0].URL, "step", "5m")

				Expect(logCache.reqs[0].URL.Query()).To(HaveLen(4))
			})

			It("closes the body", func() {
				spyHTTPClient := newSpyHTTPClient()
				logcache_client := client.NewClient("", client.WithHTTPClient(spyHTTPClient))
				logcache_client.PromQLRange(context.Background(), "some-query")

				Expect(spyHTTPClient.body.closed).To(BeTrue())
			})

			It("returns an error on a non-200 status", func() {
				logCache := newStubLogCache()
				logCache.statusCode = 500
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQLRange(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on invalid JSON", func() {
				logCache := newStubLogCache()
				logCache.result["GET/api/v1/query_range"] = []byte("invalid")
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQLRange(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on unreachable URL", func() {
				logcache_client := client.NewClient("http://invalid.url")

				_, err := logcache_client.PromQLRange(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logcache_client := client.NewClient("-:-invalid")

				_, err := logcache_client.PromQLRange(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logCache := newStubLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.PromQLRange(ctx, "some-query")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("PromQLRangeRaw", func() {
			It("retrieves points", func() {
				logCache := newStubLogCache()
				logcache_client := client.NewClient(logCache.addr())
				start := time.Unix(time.Now().Unix(), 123000000)
				end := start.Add(time.Minute)

				result, err := logcache_client.PromQLRangeRaw(
					context.Background(),
					`some-query`,
					client.WithPromQLStart(start),
					client.WithPromQLEnd(end),
					client.WithPromQLStep("5m"),
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(logCache.reqs).To(HaveLen(1))

				Expect(logCache.reqs[0].URL.Path).To(Equal("/api/v1/query_range"))

				Expect(logCache.reqs[0].URL.Query()).To(HaveLen(4))
				assertQueryParam(logCache.reqs[0].URL, "query", "some-query")
				assertQueryParam(logCache.reqs[0].URL, "start", fmt.Sprintf("%.3f", float64(start.UnixNano())/1e9))
				assertQueryParam(logCache.reqs[0].URL, "end", fmt.Sprintf("%.3f", float64(end.UnixNano())/1e9))
				assertQueryParam(logCache.reqs[0].URL, "step", "5m")

				Expect(result.Status).To(Equal("success"))
				Expect(result.Data.ResultType).To(Equal("matrix"))

				expected := logCache.result["GET/api/v1/query_range"]
				var expectedResult client.PromQLQueryResult

				err = json.Unmarshal(expected, &expectedResult)
				Expect(err).ToNot(HaveOccurred())

				Expect(expectedResult.Data.Result).To(Equal(result.Data.Result))
			})

			It("closes the body", func() {
				spyHTTPClient := newSpyHTTPClient()
				logcache_client := client.NewClient("", client.WithHTTPClient(spyHTTPClient))
				logcache_client.PromQLRangeRaw(context.Background(), "some-query")

				Expect(spyHTTPClient.body.closed).To(BeTrue())
			})

			It("returns an error on a non-200, non404, and non-500 status", func() {
				logCache := newStubLogCache()
				logCache.statusCode = 503
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQLRangeRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on invalid JSON", func() {
				logCache := newStubLogCache()
				logCache.result["GET/api/v1/query_range"] = []byte("invalid")
				logcache_client := client.NewClient(logCache.addr())

				_, err := logcache_client.PromQLRangeRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on unreachable URL", func() {
				logcache_client := client.NewClient("http://invalid.url")

				_, err := logcache_client.PromQLRangeRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logcache_client := client.NewClient("-:-invalid")

				_, err := logcache_client.PromQLRangeRaw(context.Background(), "some-query")
				Expect(err).To(HaveOccurred())
			})

			It("returns an error on an invalid URL", func() {
				logCache := newStubLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.PromQLRangeRaw(ctx, "some-query")
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("gRPC client", func() {
		Describe("Read", func() {
			It("reads envelopes", func() {
				logCache := newStubGrpcLogCache()
				logcache_client := client.NewClient(logCache.addr(), client.WithViaGRPC(grpc.WithInsecure()))

				endTime := time.Now()

				envelopes, err := logcache_client.Read(context.Background(), "some-id", time.Unix(0, 99),
					client.WithLimit(10),
					client.WithEndTime(endTime),
					client.WithEnvelopeTypes(rpc.EnvelopeType_LOG, rpc.EnvelopeType_GAUGE),
					client.WithDescending(),
					client.WithNameFilter("name.*"),
				)

				Expect(err).ToNot(HaveOccurred())

				Expect(envelopes).To(HaveLen(2))

				Expect(envelopes[0].Timestamp).To(BeEquivalentTo(99))
				Expect(envelopes[1].Timestamp).To(BeEquivalentTo(100))

				Expect(logCache.reqs).To(ConsistOf(PointTo(
					MatchFields(IgnoreExtras,
						Fields{
							"SourceId":  Equal("some-id"),
							"StartTime": BeEquivalentTo(99),
							"EndTime":   BeEquivalentTo(endTime.UnixNano()),
							"Limit":     BeEquivalentTo(10),
							"EnvelopeTypes": ConsistOf(
								Equal(rpc.EnvelopeType_LOG),
								Equal(rpc.EnvelopeType_GAUGE),
							),
							"NameFilter": Equal("name.*"),
							"Descending": Equal(true),
						},
					),
				)))
			})

			It("returns an error when the context is cancelled", func() {
				logCache := newStubGrpcLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr(), client.WithViaGRPC(grpc.WithInsecure()))

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.Read(
					ctx,
					"some-id",
					time.Unix(0, 99),
					client.WithEndTime(time.Unix(0, 101)),
					client.WithLimit(103),
					client.WithEnvelopeTypes(rpc.EnvelopeType_LOG),
				)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("Meta", func() {
			It("retrieves meta information", func() {
				logCache := newStubGrpcLogCache()
				logcache_client := client.NewClient(logCache.addr(), client.WithViaGRPC(grpc.WithInsecure()))

				meta, err := logcache_client.Meta(context.Background())
				Expect(err).ToNot(HaveOccurred())

				Expect(meta).To(HaveLen(2))
				Expect(meta).To(HaveKey("source-0"))
				Expect(meta).To(HaveKey("source-1"))
			})

			It("returns an error when the context is cancelled", func() {
				logCache := newStubGrpcLogCache()
				logcache_client := client.NewClient(logCache.addr(), client.WithViaGRPC(grpc.WithInsecure()))

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.Meta(ctx)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("PromQL", func() {
			It("retrieves points", func() {
				logCache := newStubGrpcLogCache()
				logcache_client := client.NewClient(logCache.addr(), client.WithViaGRPC(grpc.WithInsecure()))

				result, err := logcache_client.PromQL(context.Background(), "some-query",
					client.WithPromQLTime(time.Unix(99, 0)),
				)

				Expect(err).ToNot(HaveOccurred())

				scalar := result.GetScalar()
				Expect(scalar.Time).To(Equal("99.000"))
				Expect(scalar.Value).To(BeEquivalentTo(101))

				Expect(logCache.promInstantReqs).To(ConsistOf(PointTo(
					MatchFields(IgnoreExtras,
						Fields{
							"Query": Equal("some-query"),
							"Time":  Equal("99.000"),
						},
					),
				)))
			})

			It("returns an error when the context is cancelled", func() {
				logCache := newStubGrpcLogCache()
				logCache.block = true
				logcache_client := client.NewClient(logCache.addr(), client.WithViaGRPC(grpc.WithInsecure()))

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := logcache_client.PromQL(
					ctx,
					"some-query",
				)
				Expect(err).To(HaveOccurred())
			})
		})
	})
})

type stubLogCache struct {
	statusCode int
	server     *httptest.Server
	reqs       []*http.Request
	bodies     [][]byte
	result     map[string][]byte
	block      bool
}

func newStubLogCache() *stubLogCache {
	s := &stubLogCache{
		statusCode: http.StatusOK,
		result: map[string][]byte{
			"GET/api/v1/read/some-id": []byte(`{
		"envelopes": {
			"batch": [
			    {
					"timestamp": 99,
					"source_id": "some-id"
				},
			    {
					"timestamp": 100,
					"source_id": "some-id"
				}
			]
		}
	}`),
			"GET/api/v1/meta": []byte(`{
		"meta": {
			"source-0": {},
			"source-1": {}
		}
	}`),
			"GET/api/v1/query": []byte(`
    {
	  "status": "success",
	  "data": {
		"resultType": "vector",
		"result": [
          {
            "metric": {
              "deployment": "cf"
            },
            "value": [ 1234, "99" ]
          }
        ]
      }
    }
			`),
			"GET/api/v1/query_range": []byte(`
    {
	  "status": "success",
	  "data": {
		"resultType": "matrix",
        "result": [
          {
            "metric": {
              "deployment": "cf"
            },
            "values": [
              [ 1234, "99" ],
              [ 5678, "100" ]
            ]
          }
        ]
      }
    }
			`),
			"GET/api/v1/info": []byte(`
	{
	  "version": "2.0.0",
	  "vm_uptime": "789"
	}
			`),
		},
	}
	s.server = httptest.NewServer(s)
	return s
}

func newStubOldLogCache() *stubLogCache {
	s := &stubLogCache{
		statusCode: http.StatusOK,
		result: map[string][]byte{
			"GET/v1/read/some-id": []byte(`
	{
		"envelopes": {
			"batch": [
			    {
					"timestamp": 99,
					"source_id": "some-id"
				},
			    {
					"timestamp": 100,
					"source_id": "some-id"
				}
			]
		}
	}`),
			"GET/v1/meta": []byte(`{
		"meta": {
			"source-0": {},
			"source-1": {}
		}
	}`),
			"GET/api/v1/query": []byte(`
    {
	  "status": "success",
	  "data": {
		"resultType": "vector",
		"result": [
          {
            "metric": {
              "deployment": "cf"
            },
            "value": [ 1234, "99" ]
          }
        ]
      }
    }
			`),
			"GET/api/v1/query_range": []byte(`
    {
	  "status": "success",
	  "data": {
		"resultType": "matrix",
        "result": [
          {
            "metric": {
              "deployment": "cf"
            },
            "values": [
              [ 1234, "99" ],
              [ 5678, "100" ]
            ]
          }
        ]
      }
    }
			`),
		},
	}
	s.server = httptest.NewServer(s)
	return s
}

func (s *stubLogCache) addr() string {
	return s.server.URL
}

func (s *stubLogCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.block {
		var block chan struct{}
		<-block
	}

	body, err := ioutil.ReadAll(r.Body)
	Expect(err).ToNot(HaveOccurred())

	s.bodies = append(s.bodies, body)
	s.reqs = append(s.reqs, r)

	if _, ok := s.result[r.Method+r.URL.Path]; ok {
		w.WriteHeader(s.statusCode)
		w.Write(s.result[r.Method+r.URL.Path])
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func assertQueryParam(u *url.URL, name string, values ...string) {
	Expect(u.Query()).To(HaveKeyWithValue(name, ConsistOf(values)))
}

type stubGrpcLogCache struct {
	mu              sync.Mutex
	reqs            []*rpc.ReadRequest
	promInstantReqs []*rpc.PromQL_InstantQueryRequest
	promRangeReqs   []*rpc.PromQL_RangeQueryRequest
	lis             net.Listener
	block           bool
}

func newStubGrpcLogCache() *stubGrpcLogCache {
	s := &stubGrpcLogCache{}
	lis, err := net.Listen("tcp", ":0")
	Expect(err).ToNot(HaveOccurred())

	s.lis = lis
	srv := grpc.NewServer()
	rpc.RegisterEgressServer(srv, s)
	rpc.RegisterPromQLQuerierServer(srv, s)
	go srv.Serve(lis)

	return s
}

func (s *stubGrpcLogCache) addr() string {
	return s.lis.Addr().String()
}

func (s *stubGrpcLogCache) Read(c context.Context, r *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	if s.block {
		var block chan struct{}
		<-block
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, r)

	return &rpc.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: []*loggregator_v2.Envelope{
				{Timestamp: 99, SourceId: "some-id"},
				{Timestamp: 100, SourceId: "some-id"},
			},
		},
	}, nil
}

func (s *stubGrpcLogCache) InstantQuery(c context.Context, r *rpc.PromQL_InstantQueryRequest) (*rpc.PromQL_InstantQueryResult, error) {
	if s.block {
		var block chan struct{}
		<-block
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.promInstantReqs = append(s.promInstantReqs, r)

	return &rpc.PromQL_InstantQueryResult{
		Result: &rpc.PromQL_InstantQueryResult_Scalar{
			Scalar: &rpc.PromQL_Scalar{
				Time:  "99.000",
				Value: 101,
			},
		},
	}, nil
}

func (s *stubGrpcLogCache) RangeQuery(c context.Context, r *rpc.PromQL_RangeQueryRequest) (*rpc.PromQL_RangeQueryResult, error) {
	if s.block {
		var block chan struct{}
		<-block
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.promRangeReqs = append(s.promRangeReqs, r)

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
								Value: 101,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (s *stubGrpcLogCache) Meta(context.Context, *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	return &rpc.MetaResponse{
		Meta: map[string]*rpc.MetaInfo{
			"source-0": {},
			"source-1": {},
		},
	}, nil
}

func (s *stubGrpcLogCache) requests() []*rpc.ReadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.ReadRequest, len(s.reqs))
	copy(r, s.reqs)
	return r
}

func (s *stubGrpcLogCache) promQLRequests() []*rpc.PromQL_InstantQueryRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.PromQL_InstantQueryRequest, len(s.promInstantReqs))
	copy(r, s.promInstantReqs)
	return r
}

type stubBufferCloser struct {
	*bytes.Buffer
	closed bool
}

func newStubBufferCloser() *stubBufferCloser {
	return &stubBufferCloser{}
}

func (s *stubBufferCloser) Close() error {
	s.closed = true
	return nil
}

type spyHTTPClient struct {
	body *stubBufferCloser
}

func newSpyHTTPClient() *spyHTTPClient {
	return &spyHTTPClient{
		body: newStubBufferCloser(),
	}
}

func (s *spyHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Body: s.body,
	}, nil
}
