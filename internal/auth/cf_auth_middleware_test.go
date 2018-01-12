package auth_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"

	"code.cloudfoundry.org/log-cache/internal/auth"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CfAuthMiddleware", func() {
	var (
		uaaClient *stubHTTPClient
		apiClient *stubHTTPClient
		recorder  *httptest.ResponseRecorder
		request   *http.Request
		provider  auth.CFAuthMiddlewareProvider

		uaaHost = "http://uaaHost.com"
		apiHost = "http://apiHost.com"

		clientID     = "client_id"
		clientSecret = "client_secret"
	)

	BeforeEach(func() {
		uaaClient = &stubHTTPClient{}
		apiClient = &stubHTTPClient{}

		provider = auth.NewCFAuthMiddlewareProvider(
			clientID,
			clientSecret,
			uaaHost,
			uaaClient,
			apiHost,
			apiClient,
		)

		recorder = httptest.NewRecorder()
		request = httptest.NewRequest(http.MethodGet, "/v1/read/12345", nil)
	})

	It("forwards the request to the handler if user is an admin", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		authHandler := provider.Middleware(baseHandler)

		uaaClient.status = http.StatusOK
		uaaClient.scopes = []string{"doppler.firehose"}

		request.Header.Set("Authorization", "bearer valid-token")

		authHandler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(baseHandlerCalled).To(BeTrue())

		r := uaaClient.requests
		Expect(r.Method).To(Equal(http.MethodPost))
		Expect(r.Header.Get("Content-Type")).To(
			Equal("application/x-www-form-urlencoded"),
		)
		Expect(r.URL.String()).To(Equal(uaaHost + "/check_token"))
		username, password, ok := r.BasicAuth()
		Expect(ok).To(BeTrue())
		Expect(username).To(Equal(clientID))
		Expect(password).To(Equal(clientSecret))

		reqBytes, err := ioutil.ReadAll(r.Body)
		Expect(err).ToNot(HaveOccurred())

		urlValues, err := url.ParseQuery(string(reqBytes))
		Expect(err).ToNot(HaveOccurred())

		Expect(urlValues.Get("token")).To(Equal("valid-token"))
	})

	It("forwards the request to the handler if non-admin user has log access", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		authHandler := provider.Middleware(baseHandler)

		uaaClient.status = http.StatusOK
		uaaClient.scopes = []string{"unauthorized_scopes"}
		apiClient.status = http.StatusOK

		request.Header.Set("Authorization", "valid-token")

		// Call result
		authHandler.ServeHTTP(recorder, request)
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(baseHandlerCalled).To(BeTrue())

		//verify CAPI called with correct info
		capiRequest := apiClient.requests
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

		authHandler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
		Expect(baseHandlerCalled).To(BeFalse())
	})

	It("returns 404 if the request fails", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		uaaClient.err = errors.New("an error")
		authHandler := provider.Middleware(baseHandler)

		request.Header.Set("Authorization", "valid-token")

		authHandler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
		Expect(baseHandlerCalled).To(BeFalse())
	})

	It("returns 404 if UAA and CAPI returns non 200", func() {
		var baseHandlerCalled bool
		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			baseHandlerCalled = true
		})
		authHandler := provider.Middleware(baseHandler)
		uaaClient.status = http.StatusUnauthorized
		apiClient.status = http.StatusUnauthorized
		request.Header.Set("Authorization", "valid-token")

		authHandler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
		Expect(baseHandlerCalled).To(BeFalse())
	})

	It("returns 404 if the request isn't for /v1/read", func() {
		request.URL.Path = "/something/else"

		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		authHandler := provider.Middleware(baseHandler)

		uaaClient.status = http.StatusOK

		request.Header.Set("Authorization", "valid-token")

		authHandler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})
})

type stubHTTPClient struct {
	status   int
	scopes   []string
	err      error
	requests *http.Request
}

func (s *stubHTTPClient) Do(r *http.Request) (*http.Response, error) {
	if s.err != nil {
		return nil, s.err
	}

	m := map[string]interface{}{
		"scope": s.scopes,
		"extra": 99,
	}

	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	s.requests = r
	resp := http.Response{
		StatusCode: s.status,
		Body:       ioutil.NopCloser(bytes.NewReader(data)),
	}

	return &resp, nil
}
