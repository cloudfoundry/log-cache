package auth_test

import (
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
	)

	BeforeEach(func() {
		capiClient = newSpyHTTPClient()
		client = auth.NewCAPIClient("https://capi.com", "http://external.capi.com", capiClient)
	})

	Describe("IsAuthorized", func() {
		It("hits CAPI correctly", func() {
			client.IsAuthorized("some-id", "some-token")
			r := capiClient.request

			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.String()).To(Equal("https://capi.com/internal/v4/log_access/some-id"))
			Expect(r.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("returns true for authorized token", func() {
			capiClient.status = http.StatusOK
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeTrue())
		})

		It("returns false when CAPI returns non 200", func() {
			capiClient.status = http.StatusNotFound
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeFalse())
		})

		It("returns false when CAPI request fails", func() {
			capiClient.err = errors.New("intentional error")
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeFalse())
		})
	})

	Describe("AvailableSourceIDs", func() {
		It("hits CAPI correctly", func() {
			client.AvailableSourceIDs("some-token")
			r := capiClient.request

			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.String()).To(Equal("http://external.capi.com/v3/apps"))
			Expect(r.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("returns the available source IDs", func() {
			capiClient.body = []byte(`{"resources": [{"guid": "source-0"}, {"guid": "source-1"}]}`)
			sourceIDs := client.AvailableSourceIDs("some-token")

			Expect(sourceIDs).To(ConsistOf("source-0", "source-1"))
		})

		It("returns empty slice when CAPI returns non 200", func() {
			capiClient.body = []byte(`{"resources": [{"guid": "source-0"}, {"guid": "source-1"}]}`)
			capiClient.status = http.StatusNotFound
			Expect(client.AvailableSourceIDs("some-token")).To(BeEmpty())
		})

		It("returns empty slice when CAPI request fails", func() {
			capiClient.body = []byte(`{"resources": [{"guid": "source-0"}, {"guid": "source-1"}]}`)
			capiClient.err = errors.New("intentional error")
			Expect(client.AvailableSourceIDs("some-token")).To(BeEmpty())
		})

	})
})
