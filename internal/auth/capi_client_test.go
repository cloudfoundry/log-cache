package auth_test

import (
	"fmt"
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
	var (
		capiClient *spyHTTPClient
		client     *auth.CAPIClient
		metrics    *spyMetrics
	)

	BeforeEach(func() {
		capiClient = newSpyHTTPClient()
		metrics = newSpyMetrics()
		client = auth.NewCAPIClient(
			"https://capi.com",
			"http://external.capi.com",
			capiClient,
			metrics,
			log.New(ioutil.Discard, "", 0),
			auth.WithTokenPruningInterval(250*time.Millisecond),
		)
	})

	Describe("IsAuthorized", func() {
		It("caches CAPI response for /v3/apps and /v2/service_instances request", func() {
			capiClient.resps = []response{
				newCapiAppResp("37cbff06-79ef-4146-a7b0-01838940f185", "my_app", http.StatusOK),
				newCapiServiceResp("afbdcab7-6fd1-418d-bfd0-95c60276507b", http.StatusOK),
			}
			clientToken := auth.Oauth2Client{
				Expiration: time.Now().Add(time.Minute),
				Token:      "some-token",
			}

			authorized := client.IsAuthorized("37cbff06-79ef-4146-a7b0-01838940f185", clientToken)

			Expect(len(capiClient.requests)).To(Equal(2))
			Expect(authorized).To(BeTrue())

			authorized = client.IsAuthorized("37cbff06-79ef-4146-a7b0-01838940f185", clientToken)

			Expect(len(capiClient.requests)).To(Equal(2))
			Expect(authorized).To(BeTrue())

			authorized = client.IsAuthorized("afbdcab7-6fd1-418d-bfd0-95c60276507b", clientToken)

			Expect(len(capiClient.requests)).To(Equal(2))
			Expect(authorized).To(BeTrue())
		})

		It("invalidates cached CAPI response once token expiration occurs", func() {
			token0 := auth.Oauth2Client{
				Token:      "token-0",
				Expiration: time.Now().Add(250 * time.Millisecond),
			}

			var isExpiredTokenAuthorized = func() bool {
				capiClient.resps = []response{
					newCapiAppResp("8208c86c-7afe-45f8-8999-4883d5868cf2", "my_other_app", http.StatusOK),
					newCapiServiceResp("dc94ebb2-5038-4645-afbf-1093bbd58e94", http.StatusOK),
				}

				return client.IsAuthorized(
					"8208c86c-7afe-45f8-8999-4883d5868cf2",
					token0,
				)
			}

			Expect(isExpiredTokenAuthorized()).To(BeTrue())
			Eventually(isExpiredTokenAuthorized).Should(BeFalse())
		})

		It("sourceIDs from expired cached tokens are not authorized", func() {
			token0 := auth.Oauth2Client{
				Token:      "token-0",
				Expiration: time.Now().Add(250 * time.Millisecond),
			}

			var isExpiredTokenAuthorized = func() bool {
				capiClient.resps = []response{
					newCapiAppResp("8208c86c-7afe-45f8-8999-4883d5868cf2", "my_other_app", http.StatusOK),
					newCapiServiceResp("dc94ebb2-5038-4645-afbf-1093bbd58e94", http.StatusOK),
				}

				return client.IsAuthorized(
					"8208c86c-7afe-45f8-8999-4883d5868cf2",
					token0,
				)
			}

			Expect(isExpiredTokenAuthorized()).To(BeTrue())
			Eventually(isExpiredTokenAuthorized).Should(BeFalse())

			capiClient.resps = []response{
				newCapiAppResp("37cbff06-79ef-4146-a7b0-01838940f185", "my_app", http.StatusOK),
				newCapiServiceResp("afbdcab7-6fd1-418d-bfd0-95c60276507b", http.StatusOK),
			}
			token1 := auth.Oauth2Client{
				Token:      "token-1",
				Expiration: time.Now().Add(time.Second),
			}

			authorized := client.IsAuthorized(
				"8208c86c-7afe-45f8-8999-4883d5868cf2",
				token1,
			)

			Expect(authorized).To(BeFalse())
		})

		It("immediately rejects expired token", func() {
			capiClient.resps = []response{
				newCapiAppResp("8208c86c-7afe-45f8-8999-4883d5868cf2", "my_other_app", http.StatusOK),
				newCapiServiceResp("dc94ebb2-5038-4645-afbf-1093bbd58e94", http.StatusOK),
			}
			authorized := client.IsAuthorized(
				"8208c86c-7afe-45f8-8999-4883d5868cf2",
				auth.Oauth2Client{
					Token:      "expired-token",
					Expiration: time.Now().Add(-time.Second),
				})

			Expect(capiClient.requests).To(BeEmpty())
			Expect(authorized).To(BeFalse())
		})

		It("regularly removes expired tokens from cache", func() {
			client.IsAuthorized(
				"8208c86c-7afe-45f8-8999-4883d5868cf2",
				auth.Oauth2Client{
					Token:      "token",
					Expiration: time.Now().Add(250 * time.Millisecond),
				},
			)

			Eventually(client.TokenCacheSize).Should(BeZero())
		})
	})

	Describe("AvailableSourceIDs", func() {
		It("hits CAPI correctly", func() {
			client.AvailableSourceIDs("some-token")
			Expect(capiClient.requests).To(HaveLen(2))

			appsReq := capiClient.requests[0]
			Expect(appsReq.Method).To(Equal(http.MethodGet))
			Expect(appsReq.URL.String()).To(Equal("http://external.capi.com/v3/apps"))
			Expect(appsReq.Header.Get("Authorization")).To(Equal("some-token"))

			servicesReq := capiClient.requests[1]
			Expect(servicesReq.Method).To(Equal(http.MethodGet))
			Expect(servicesReq.URL.String()).To(Equal("http://external.capi.com/v2/service_instances"))
			Expect(servicesReq.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("returns the available app and service instance IDs", func() {
			capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "app-0"}, {"guid": "app-1"}]}`)},
				{status: http.StatusOK, body: []byte(`{"resources": [{"metadata":{"guid": "service-2"}}, {"metadata":{"guid": "service-3"}}]}`)},
			}
			sourceIDs := client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(ConsistOf("app-0", "app-1", "service-2", "service-3"))
		})

		It("returns empty slice when CAPI apps request returns non 200", func() {
			capiClient.resps = []response{
				{status: http.StatusNotFound},
				{status: http.StatusOK, body: []byte(`{"resources": [{"metadata":{"guid": "service-2"}}, {"metadata":{"guid": "service-3"}}]}`)},
			}
			sourceIDs := client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("returns empty slice when CAPI apps request fails", func() {
			capiClient.resps = []response{
				{err: errors.New("intentional error")},
				{status: http.StatusOK, body: []byte(`{"resources": [{"metadata":{"guid": "service-2"}}, {"metadata":{"guid": "service-3"}}]}`)},
			}
			sourceIDs := client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("returns empty slice when CAPI service_instances request returns non 200", func() {
			capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "app-0"}, {"guid": "app-1"}]}`)},
				{status: http.StatusNotFound},
			}
			sourceIDs := client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("returns empty slice when CAPI service_instances request fails", func() {
			capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "app-0"}, {"guid": "app-1"}]}`)},
				{err: errors.New("intentional error")},
			}
			sourceIDs := client.AvailableSourceIDs("some-token")
			Expect(sourceIDs).To(BeEmpty())
		})

		It("stores the latency", func() {
			client.AvailableSourceIDs("my-token")

			Expect(metrics.m["LastCAPIV3AppsLatency"]).ToNot(BeZero())
			Expect(metrics.m["LastCAPIV2ListServiceInstancesLatency"]).ToNot(BeZero())
		})

		It("is goroutine safe", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < 1000; i++ {
					client.AvailableSourceIDs("some-token")
				}

				wg.Done()
			}()

			for i := 0; i < 1000; i++ {
				client.AvailableSourceIDs("some-token")
			}
			wg.Wait()
		})
	})

	Describe("GetRelatedSourceIds", func() {
		It("hits CAPI correctly", func() {
			client.GetRelatedSourceIds([]string{"app-name-1", "app-name-2"}, "some-token")
			Expect(capiClient.requests).To(HaveLen(1))

			appsReq := capiClient.requests[0]
			Expect(appsReq.Method).To(Equal(http.MethodGet))
			Expect(appsReq.URL.Host).To(Equal("external.capi.com"))
			Expect(appsReq.URL.Path).To(Equal("/v3/apps"))
			Expect(appsReq.URL.Query().Get("names")).To(Equal("app-name-1,app-name-2"))
			Expect(appsReq.URL.Query().Get("per_page")).To(Equal("5000"))
			Expect(appsReq.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("gets related source IDs for a single app", func() {
			capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{
					"resources": [
						{"guid": "app-0", "name": "app-name"},
						{"guid": "app-1", "name": "app-name"}
					]
				}`)},
			}

			sourceIds := client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveKeyWithValue("app-name", ConsistOf("app-0", "app-1")))
		})

		It("gets related source IDs for multiple apps", func() {
			capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{
					"resources": [
						{"guid": "app-a-0", "name": "app-a"},
						{"guid": "app-a-1", "name": "app-a"},
						{"guid": "app-b-0", "name": "app-b"}
					]
				}`)},
			}

			sourceIds := client.GetRelatedSourceIds([]string{"app-a", "app-b"}, "some-token")
			Expect(sourceIds).To(HaveKeyWithValue("app-a", ConsistOf("app-a-0", "app-a-1")))
			Expect(sourceIds).To(HaveKeyWithValue("app-b", ConsistOf("app-b-0")))
		})

		It("doesn't issue a request when given no app names", func() {
			client.GetRelatedSourceIds([]string{}, "some-token")
			Expect(capiClient.requests).To(HaveLen(0))
		})

		It("stores the latency", func() {
			capiClient.resps = []response{
				{status: http.StatusNotFound},
			}
			client.GetRelatedSourceIds([]string{"app-name"}, "some-token")

			Expect(metrics.m["LastCAPIV3AppsByNameLatency"]).ToNot(BeZero())
		})

		It("returns no source IDs when the request fails", func() {
			capiClient.resps = []response{
				{status: http.StatusNotFound},
			}
			sourceIds := client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveLen(0))
		})

		It("returns no source IDs when the request returns a non-200 status code", func() {
			capiClient.resps = []response{
				{err: errors.New("intentional error")},
			}
			sourceIds := client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveLen(0))
		})

		It("returns no source IDs when JSON decoding fails", func() {
			capiClient.resps = []response{
				{status: http.StatusOK, body: []byte(`{`)},
			}
			sourceIds := client.GetRelatedSourceIds([]string{"app-name"}, "some-token")
			Expect(sourceIds).To(HaveLen(0))
		})
	})
})

func newCapiAppResp(guid, name string, status int) response {
	return response{
		status: status,
		body: []byte(fmt.Sprintf(
			`{
               "resources": [
                 {
                   "guid": "%s",
                   "name": "%s"
                 }
               ]
             }`, guid, name)),
	}
}

func newCapiServiceResp(guid string, status int) response {
	return response{
		status: status,
		body: []byte(fmt.Sprintf(
			`{
               "resources": [
                 {
                   "metadata": {
                     "guid": "%s"
                   }
                 }
               ]
             }`, guid)),
	}
}
