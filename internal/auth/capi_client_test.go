package auth_test

import (
	"io/ioutil"
	"log"
	"sync"
	"time"

	"code.cloudfoundry.org/log-cache/internal/auth"

	"errors"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CAPIClient", func() {
	type testContext struct {
		capiClient *spyHTTPClient
		client     *auth.CAPIClient
		metrics    *spyMetrics
	}

	var setup = func(capiOpts ...auth.CAPIOption) *testContext {
		capiClient := newSpyHTTPClient()
		metrics := newSpyMetrics()
		client := auth.NewCAPIClient(
			"http://internal.capi.com",
			capiClient,
			metrics,
			log.New(ioutil.Discard, "", 0),
			capiOpts...,
		)

		return &testContext{
			capiClient: capiClient,
			metrics:    metrics,
			client:     client,
		}
	}

	Describe("IsAuthorized", func() {
		It("caches CAPI response", func() {
			tc := setup(
				auth.WithCacheExpirationInterval(250 * time.Millisecond),
			)

			tc.capiClient.resps = []response{
				newCapiResp(http.StatusOK),
			}

			By("calling isAuthorized the first time and caching response")
			Expect(tc.client.IsAuthorized("37cbff06-79ef-4146-a7b0-01838940f185", "some-token")).To(BeTrue())
			Expect(len(tc.capiClient.requests)).To(Equal(1))

			By("calling isAuthorized the second time and pulling from the cache")
			Expect(tc.client.IsAuthorized("37cbff06-79ef-4146-a7b0-01838940f185", "some-token")).To(BeTrue())
			Expect(len(tc.capiClient.requests)).To(Equal(1))
		})

		It("sourceIDs from expired cached tokens are not authorized", func() {
			tc := setup(
				auth.WithCacheExpirationInterval(250 * time.Millisecond),
			)

			tc.capiClient.resps = []response{
				newCapiResp(http.StatusOK),
			}

			Expect(tc.client.IsAuthorized(
				"8208c86c-7afe-45f8-8999-4883d5868cf2",
				"token-0",
			)).To(BeTrue())

			time.Sleep(251 * time.Millisecond)

			tc.capiClient.resps = []response{
				newCapiResp(http.StatusNotFound), // app not found
				newCapiResp(http.StatusNotFound), // fallthrough to see if it's a service
			}

			Expect(tc.client.IsAuthorized(
				"8208c86c-7afe-45f8-8999-4883d5868cf2",
				"token-0",
			)).To(BeFalse())
		})

		It("regularly removes tokens from cache", func() {
			tc := setup(
				auth.WithCacheExpirationInterval(250 * time.Millisecond),
			)

			tc.capiClient.resps = []response{
				newCapiResp(http.StatusOK),
				newCapiResp(http.StatusOK),
			}

			tc.client.IsAuthorized("8208c86c-7afe-45f8-8999-4883d5868cf2", "token-1")
			tc.client.IsAuthorized("8208c86c-7afe-45f8-8999-4883d5868cf2", "token-2")

			Expect(tc.client.TokenCacheSize()).To(Equal(2))
			Eventually(tc.client.TokenCacheSize).Should(BeZero())
		})

		It("Has App returns true if capi returns 200", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: nil},
			}

			Expect(tc.client.HasApp("some-source", "some-token")).To(BeTrue())

			Expect(tc.capiClient.requests).To(HaveLen(1))
			appsReq := tc.capiClient.requests[0]
			Expect(appsReq.Method).To(Equal(http.MethodGet))
			Expect(appsReq.URL.String()).To(Equal("http://internal.capi.com/v3/apps/some-source"))
			Expect(appsReq.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("Has App returns false if capi returns non 200", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusNotFound, body: nil},
			}

			Expect(tc.client.HasApp("some-source", "some-token")).To(BeFalse())

			Expect(tc.capiClient.requests).To(HaveLen(1))
			appsReq := tc.capiClient.requests[0]
			Expect(appsReq.Method).To(Equal(http.MethodGet))
			Expect(appsReq.URL.String()).To(Equal("http://internal.capi.com/v3/apps/some-source"))
			Expect(appsReq.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("Has Service returns true if capi returns 200", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: nil},
			}
			Expect(tc.client.HasService("some-source", "some-token")).To(BeTrue())

			Expect(tc.capiClient.requests).To(HaveLen(1))
			serviceReq := tc.capiClient.requests[0]
			Expect(serviceReq.Method).To(Equal(http.MethodGet))
			Expect(serviceReq.URL.String()).To(Equal("http://internal.capi.com/v3/service_instances/some-source"))
			Expect(serviceReq.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("Has Service returns false if capi returns non 200", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusNotFound, body: nil},
			}
			Expect(tc.client.HasService("some-source", "some-token")).To(BeFalse())

			Expect(tc.capiClient.requests).To(HaveLen(1))
			serviceReq := tc.capiClient.requests[0]
			Expect(serviceReq.Method).To(Equal(http.MethodGet))
			Expect(serviceReq.URL.String()).To(Equal("http://internal.capi.com/v3/service_instances/some-source"))
			Expect(serviceReq.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("succeeds when a CAPI returns 200 for either app or service", func() {
			tc := setup(
				auth.WithCacheExpirationInterval(250 * time.Millisecond),
			)

			tc.capiClient.resps = []response{
				newCapiResp(http.StatusOK),
			}

			authorized := tc.client.IsAuthorized("app-guid", "some-token")
			Expect(len(tc.capiClient.requests)).To(Equal(1))
			Expect(authorized).To(BeTrue())

			tc.capiClient.resps = []response{
				newCapiResp(http.StatusOK),
			}
			Expect(tc.client.IsAuthorized("service-guid", "some-token")).To(BeTrue())
			Expect(len(tc.capiClient.requests)).To(Equal(2))
		})

		It("stores the latency", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				emptyCapiResp,
				emptyCapiResp,
			}
			tc.client.HasApp("app-guid", "my-token")

			Expect(tc.metrics.m["LastCAPIV3AppsLatency"]).ToNot(BeZero())
			Expect(tc.metrics.m["LastCAPIV3ListServiceInstancesLatency"]).To(BeZero())
		})

		It("stores the latency", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				emptyCapiResp,
				emptyCapiResp,
			}
			tc.client.HasService("service-guid", "my-token")

			Expect(tc.metrics.m["LastCAPIV3AppsLatency"]).To(BeZero())
			Expect(tc.metrics.m["LastCAPIV3ListServiceInstancesLatency"]).ToNot(BeZero())
		})

		It("is goroutine safe", func() {
			tc := setup()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < 1000; i++ {
					tc.client.IsAuthorized("app-guid", "some-token")
				}

				wg.Done()
			}()

			for i := 0; i < 1000; i++ {
				tc.client.IsAuthorized("app-guid", "some-token")
			}
			wg.Wait()
		})
	})

	Describe("AvailableSourceIDs", func() {
		It("returns the available app and service instance IDs", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "app-0"}, {"guid": "app-1"}]}`)},
				{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "service-2"}, {"guid": "service-3"}]}`)},
			}
			sourceIDs := tc.client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(ConsistOf("app-0", "app-1", "service-2", "service-3"))

			Expect(tc.capiClient.requests).To(HaveLen(2))

			appsReq := tc.capiClient.requests[0]
			Expect(appsReq.Method).To(Equal(http.MethodGet))
			Expect(appsReq.URL.String()).To(Equal("http://internal.capi.com/v3/apps?per_page=5000"))
			Expect(appsReq.Header.Get("Authorization")).To(Equal("some-token"))
			Expect(appsReq.URL.Query().Get("per_page")).To(Equal("5000"))

			servicesReq := tc.capiClient.requests[1]
			Expect(servicesReq.Method).To(Equal(http.MethodGet))
			Expect(servicesReq.URL.String()).To(Equal("http://internal.capi.com/v3/service_instances?per_page=5000"))
			Expect(servicesReq.Header.Get("Authorization")).To(Equal("some-token"))
			Expect(servicesReq.URL.Query().Get("per_page")).To(Equal("5000"))
		})

		It("iterates through all pages returned by /v3/apps", func() {
			tc := setup()

			By("replacing the scheme and host to match original requests' capi addr")
			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{
                  "pagination": {
                    "next": {
                      "href": "https://external.capi.com/v3/apps?page=2&per_page=1"
                    }
                  },
                  "resources": [
                      {"guid": "app-1", "name": "app-name"}
                  ]
                }`)},
				{status: http.StatusOK, body: []byte(`{
                  "resources": [
                      {"guid": "app-2", "name": "app-name"}
                  ]
                }`)},
				emptyCapiResp,
			}

			sourceIDs := tc.client.AvailableSourceIDs("some-token")
			Expect(tc.capiClient.requests).To(HaveLen(3))
			secondPageReq := tc.capiClient.requests[1]

			Expect(secondPageReq.URL.String()).To(Equal("http://internal.capi.com/v3/apps?page=2&per_page=1"))
			Expect(sourceIDs).To(ConsistOf("app-1", "app-2"))
		})

		It("iterates through all pages returned by /v3/service_instances", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				emptyCapiResp,
				{status: http.StatusOK, body: []byte(`{
                  "pagination": {
                    "next": {
                      "href": "https://external.capi.com/v3/service_instances?page=2&per_page=2"
                    }
                  },
                  "resources": [
                    {"guid": "service-1"},
                    {"guid": "service-2"}
                  ]
                }`)},
				{status: http.StatusOK, body: []byte(`{
                  "resources": [
                    {"guid": "service-3"}
                  ]
                }`)},
			}
			sourceIDs := tc.client.AvailableSourceIDs("some-token")
			Expect(tc.capiClient.requests).To(HaveLen(3))
			secondPageReq := tc.capiClient.requests[2]
			Expect(secondPageReq.URL.String()).To(Equal("http://internal.capi.com/v3/service_instances?page=2&per_page=2"))

			Expect(sourceIDs).To(ConsistOf("service-1", "service-2", "service-3"))
		})

		It("returns empty slice when CAPI apps request returns non 200", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusNotFound},
				{status: http.StatusOK, body: []byte(`{"resources": [{"metadata":{"guid": "service-2"}}, {"metadata":{"guid": "service-3"}}]}`)},
			}
			sourceIDs := tc.client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("returns empty slice when CAPI apps request fails", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{err: errors.New("intentional error")},
				{status: http.StatusOK, body: []byte(`{"resources": [{"metadata":{"guid": "service-2"}}, {"metadata":{"guid": "service-3"}}]}`)},
			}
			sourceIDs := tc.client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("returns empty slice when CAPI service_instances request returns non 200", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "app-0"}, {"guid": "app-1"}]}`)},
				{status: http.StatusNotFound},
			}
			sourceIDs := tc.client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("returns empty slice when CAPI service_instances request fails", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "app-0"}, {"guid": "app-1"}]}`)},
				{err: errors.New("intentional error")},
			}
			sourceIDs := tc.client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("stores the latency", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				emptyCapiResp,
				emptyCapiResp,
			}
			tc.client.AvailableSourceIDs("my-token")

			Expect(tc.metrics.m["LastCAPIV3AppsLatency"]).ToNot(BeZero())
			Expect(tc.metrics.m["LastCAPIV3ListServiceInstancesLatency"]).ToNot(BeZero())
		})

		It("is goroutine safe", func() {
			tc := setup()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < 1000; i++ {
					tc.client.AvailableSourceIDs("some-token")
				}

				wg.Done()
			}()

			for i := 0; i < 1000; i++ {
				tc.client.AvailableSourceIDs("some-token")
			}
			wg.Wait()
		})
	})

	Describe("GetRelatedSourceIds", func() {
		It("hits CAPI correctly", func() {
			tc := setup()

			tc.client.GetRelatedSourceIds([]string{"app-name-1", "app-name-2"}, "some-token")
			Expect(tc.capiClient.requests).To(HaveLen(1))

			appsReq := tc.capiClient.requests[0]
			Expect(appsReq.Method).To(Equal(http.MethodGet))
			Expect(appsReq.URL.Host).To(Equal("internal.capi.com"))
			Expect(appsReq.URL.Path).To(Equal("/v3/apps"))
			Expect(appsReq.URL.Query().Get("names")).To(Equal("app-name-1,app-name-2"))
			Expect(appsReq.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("gets related source IDs for a single app", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(
					`{
                  "resources": [
                      {"guid": "app-0", "name": "app-name"},
                      {"guid": "app-1", "name": "app-name"}
                  ]
                }`)},
			}

			sourceIds := tc.client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveKeyWithValue("app-name", ConsistOf("app-0", "app-1")))
		})

		It("iterates through all pages returned by /v3/apps", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{
                  "pagination": {
                    "next": {
                      "href": "https://external.capi.com/v3/apps?page=2&per_page=2"
                    }
                  },
                  "resources": [
                      {"guid": "app-0", "name": "app-name"},
                      {"guid": "app-1", "name": "app-name"}
                  ]
                }`)},
				{status: http.StatusOK, body: []byte(`{
                  "resources": [
                      {"guid": "app-2", "name": "app-name"}
                  ]
                }`)},
			}

			sourceIds := tc.client.GetRelatedSourceIds([]string{"app-name"}, "some-token")

			Expect(tc.capiClient.requests).To(HaveLen(2))
			secondPageReq := tc.capiClient.requests[1]
			Expect(secondPageReq.URL.String()).To(Equal("http://internal.capi.com/v3/apps?page=2&per_page=2"))
			Expect(sourceIds).To(HaveKeyWithValue("app-name", ConsistOf("app-0", "app-1", "app-2")))
		})

		It("gets related source IDs for multiple apps", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{
                "pagination": {
                  "next": {
                    "href": "https://api.example.org/v3/apps?page=2&per_page=2"
                  }
                },
                "resources": [
                  {"guid": "app-a-0", "name": "app-a"},
                  {"guid": "app-a-1", "name": "app-a"}
                ]
              }`)},
				{status: http.StatusOK, body: []byte(`{
                "resources": [
                  {"guid": "app-b-0", "name": "app-b"}
                ]
              }`)},
			}

			sourceIds := tc.client.GetRelatedSourceIds([]string{"app-a", "app-b"}, "some-token")
			Expect(sourceIds).To(HaveKeyWithValue("app-a", ConsistOf("app-a-0", "app-a-1")))
			Expect(sourceIds).To(HaveKeyWithValue("app-b", ConsistOf("app-b-0")))
		})

		It("doesn't issue a request when given no app names", func() {
			tc := setup()

			tc.client.GetRelatedSourceIds([]string{}, "some-token")
			Expect(tc.capiClient.requests).To(HaveLen(0))
		})

		It("stores the latency", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusNotFound},
			}
			tc.client.GetRelatedSourceIds([]string{"app-name"}, "some-token")

			Expect(tc.metrics.m["LastCAPIV3AppsByNameLatency"]).ToNot(BeZero())
		})

		It("returns no source IDs when the request fails", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusNotFound},
			}
			sourceIds := tc.client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveLen(0))
		})

		It("returns no source IDs when the request returns a non-200 status code", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{err: errors.New("intentional error")},
			}
			sourceIds := tc.client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveLen(0))
		})

		It("returns no source IDs when JSON decoding fails", func() {
			tc := setup()

			tc.capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{`)},
			}
			sourceIds := tc.client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveLen(0))
		})
	})
})

func newCapiResp(status int) response {
	return response{
		status: status,
		body:   nil,
	}
}

var emptyCapiResp = response{
	status: http.StatusOK,
	body:   []byte(`{"resources": []}`),
}
