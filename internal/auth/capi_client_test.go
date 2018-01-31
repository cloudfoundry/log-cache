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
		client = auth.NewCAPIClient("https://capi.com", capiClient)
	})

	Describe("IsAuthorized", func() {
		It("hits CAPI correctly", func() {
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeTrue())
			r := capiClient.request

			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.String()).To(Equal("https://capi.com/internal/v4/log_access/some-id"))
			Expect(r.Header.Get("Authorization")).To(Equal("some-token"))
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
})
