package auth_test

import (
	"net/http"
	"net/http/httptest"

	"code.cloudfoundry.org/log-cache/internal/auth"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CfAuthMiddleware", func() {
	var (
		spyAdminChecker *spyAdminChecker
		spyLogAuthorizer *spyLogAuthorizer

		recorder *httptest.ResponseRecorder
		request  *http.Request
		provider auth.CFAuthMiddlewareProvider
	)

	BeforeEach(func() {
		spyAdminChecker = newAdminChecker()
		spyLogAuthorizer = newSpyLogAuthorizer()

		provider = auth.NewCFAuthMiddlewareProvider(
			spyAdminChecker,
			spyLogAuthorizer,
		)

		recorder = httptest.NewRecorder()
	})

	Describe("/v1/read", func() {
		BeforeEach(func() {
			request = httptest.NewRequest(http.MethodGet, "/v1/read/12345", nil)
		})

		It("forwards the /v1/read request to the handler if user is an admin", func() {
			var baseHandlerCalled bool
			baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				baseHandlerCalled = true
			})
			authHandler := provider.Middleware(baseHandler)

			spyAdminChecker.result = true

			request.Header.Set("Authorization", "bearer valid-token")

			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusOK))
			Expect(baseHandlerCalled).To(BeTrue())

			Expect(spyAdminChecker.token).To(Equal("bearer valid-token"))
		})

		It("forwards the /v1/read request to the handler if non-admin user has log access", func() {
			spyLogAuthorizer.result = true
			var baseHandlerCalled bool
			baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				baseHandlerCalled = true
			})
			authHandler := provider.Middleware(baseHandler)

			request.Header.Set("Authorization", "valid-token")

			// Call result
			authHandler.ServeHTTP(recorder, request)
			Expect(recorder.Code).To(Equal(http.StatusOK))
			Expect(baseHandlerCalled).To(BeTrue())

			//verify CAPI called with correct info
			Expect(spyLogAuthorizer.token).To(Equal("valid-token"))
			Expect(spyLogAuthorizer.sourceID).To(Equal("12345"))
		})

		It("returns 404 if there's no authorization header present", func() {
			var baseHandlerCalled bool
			baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				baseHandlerCalled = true
			})
			authHandler := provider.Middleware(baseHandler)

			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusNotFound))
			Expect(baseHandlerCalled).To(BeFalse())
		})
	})

	It("returns 404 if the request is invalid", func() {
		request.URL.Path = "/invalid/endpoint"

		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		authHandler := provider.Middleware(baseHandler)

		spyAdminChecker.result = true

		request.Header.Set("Authorization", "valid-token")

		authHandler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})
})

type spyAdminChecker struct {
	token  string
	result bool
}

func newAdminChecker() *spyAdminChecker {
	return &spyAdminChecker{}
}

func (s *spyAdminChecker) IsAdmin(token string) bool {
	s.token = token
	return s.result
}

type spyLogAuthorizer struct{
	result bool
	sourceID string
	token string
}

func newSpyLogAuthorizer()*spyLogAuthorizer{
	return &spyLogAuthorizer{}
}

func (s *spyLogAuthorizer) IsAuthorized(sourceID, token string)bool{
	s.sourceID=sourceID
	s.token=token
	return s.result
}