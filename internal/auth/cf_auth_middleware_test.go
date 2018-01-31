package auth_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"code.cloudfoundry.org/log-cache/internal/auth"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CfAuthMiddleware", func() {
	var (
		spyAdminChecker *spyAdminChecker

		apiClient   *stubAPIClient
		recorder    *httptest.ResponseRecorder
		readRequest *http.Request
		provider    auth.CFAuthMiddlewareProvider

		apiHost = "http://apiHost.com"
	)

	BeforeEach(func() {
		spyAdminChecker = newAdminChecker()
		apiClient = &stubAPIClient{}

		provider = auth.NewCFAuthMiddlewareProvider(
			spyAdminChecker,
			apiHost,
			apiClient,
		)

		recorder = httptest.NewRecorder()
		readRequest = httptest.NewRequest(http.MethodGet, "/v1/read/12345", nil)
	})

	It("forwards the /v1/read request to the handler if user is an admin", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		authHandler := provider.Middleware(baseHandler)

		spyAdminChecker.result = true

		readRequest.Header.Set("Authorization", "bearer valid-token")

		authHandler.ServeHTTP(recorder, readRequest)

		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(baseHandlerCalled).To(BeTrue())

		Expect(spyAdminChecker.token).To(Equal("bearer valid-token"))
	})

	It("forwards the /v1/read request to the handler if non-admin user has log access", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		authHandler := provider.Middleware(baseHandler)

		apiClient.status = http.StatusOK

		readRequest.Header.Set("Authorization", "valid-token")

		// Call result
		authHandler.ServeHTTP(recorder, readRequest)
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(baseHandlerCalled).To(BeTrue())

		//verify CAPI called with correct info
		capiRequest := apiClient.request
		Expect(capiRequest).ToNot(BeNil())
		Expect(capiRequest.URL.String()).To(Equal(apiHost + "/internal/v4/log_access/12345"))
		Expect(capiRequest.Header.Get("Authorization")).To(Equal("valid-token"))
	})

	It("returns 404 if there's no authorization header present", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		authHandler := provider.Middleware(baseHandler)

		authHandler.ServeHTTP(recorder, readRequest)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
		Expect(baseHandlerCalled).To(BeFalse())
	})

	It("returns 404 if CAPI returns non 200", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		authHandler := provider.Middleware(baseHandler)
		apiClient.status = http.StatusUnauthorized
		readRequest.Header.Set("Authorization", "valid-token")

		authHandler.ServeHTTP(recorder, readRequest)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
		Expect(baseHandlerCalled).To(BeFalse())
	})

	It("returns 404 if the request is invalid", func() {
		readRequest.URL.Path = "/invalid/endpoint"

		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		authHandler := provider.Middleware(baseHandler)

		spyAdminChecker.result = true

		readRequest.Header.Set("Authorization", "valid-token")

		authHandler.ServeHTTP(recorder, readRequest)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})
})

type stubAPIClient struct {
	status   int
	scopes   []string
	err      error
	request  *http.Request
	response *http.Response
}

func (s *stubAPIClient) Do(r *http.Request) (*http.Response, error) {
	s.request = r

	if s.err != nil {
		return nil, s.err
	}

	if s.response != nil {
		return s.response, nil
	}

	data, err := json.Marshal(map[string]interface{}{
		"scope": s.scopes,
		"extra": 99,
	})

	if err != nil {
		panic(err)
	}

	resp := http.Response{
		StatusCode: s.status,
		Body:       ioutil.NopCloser(bytes.NewReader(data)),
	}

	return &resp, nil
}

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
