package auth_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"

	"code.cloudfoundry.org/log-cache/internal/auth"

	"errors"

	"context"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"github.com/golang/protobuf/jsonpb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("CfAuthMiddleware", func() {
	var (
		spyOauth2ClientReader *spyOauth2ClientReader
		spyLogAuthorizer      *spyLogAuthorizer
		spyMetaFetcher        *spyMetaFetcher

		recorder *httptest.ResponseRecorder
		request  *http.Request
		provider auth.CFAuthMiddlewareProvider
	)

	BeforeEach(func() {
		spyOauth2ClientReader = newAdminChecker()
		spyLogAuthorizer = newSpyLogAuthorizer()
		spyMetaFetcher = newSpyMetaFetcher()

		provider = auth.NewCFAuthMiddlewareProvider(
			spyOauth2ClientReader,
			spyLogAuthorizer,
			spyMetaFetcher,
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

			spyOauth2ClientReader.result = true

			request.Header.Set("Authorization", "bearer valid-token")

			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusOK))
			Expect(baseHandlerCalled).To(BeTrue())

			Expect(spyOauth2ClientReader.token).To(Equal("bearer valid-token"))
		})

		DescribeTable("forwards the /v1/read request to the handler if non-admin user has log access", func(sourceID string) {
			spyLogAuthorizer.result = true
			var baseHandlerCalled bool
			baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				baseHandlerCalled = true
			})
			authHandler := provider.Middleware(baseHandler)

			request.URL.Path = fmt.Sprintf("/v1/read/%s", sourceID)
			request.Header.Set("Authorization", "valid-token")

			// Call result
			authHandler.ServeHTTP(recorder, request)
			Expect(recorder.Code).To(Equal(http.StatusOK))
			Expect(baseHandlerCalled).To(BeTrue())

			//verify CAPI called with correct info
			Expect(spyLogAuthorizer.token).To(Equal("valid-token"))
			Expect(spyLogAuthorizer.sourceID).To(Equal(sourceID))
		},
			Entry("without slash", "12345"),
			Entry("with slash", "12/345"),
			Entry("with encoded slash", "12%2F345"),
		)

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

		It("returns 404 if Oauth2ClientReader returns an error", func() {
			var baseHandlerCalled bool
			baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				baseHandlerCalled = true
			})
			authHandler := provider.Middleware(baseHandler)

			spyOauth2ClientReader.err = errors.New("some-error")
			spyOauth2ClientReader.result = true
			spyLogAuthorizer.result = true

			request.Header.Set("Authorization", "valid-token")
			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusNotFound))
			Expect(baseHandlerCalled).To(BeFalse())
		})

		It("returns 404 if user is not authorized", func() {
			var baseHandlerCalled bool
			baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				baseHandlerCalled = true
			})
			authHandler := provider.Middleware(baseHandler)

			spyOauth2ClientReader.result = false
			spyLogAuthorizer.result = false

			request.Header.Set("Authorization", "valid-token")
			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusNotFound))
			Expect(baseHandlerCalled).To(BeFalse())
		})
	})

	Describe("/v1/meta", func() {
		var (
			authHandler http.Handler
		)

		BeforeEach(func() {
			request = httptest.NewRequest(http.MethodGet, "/v1/meta", nil)
			authHandler = provider.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				panic("should not be called")
			}))
		})

		It("returns all source IDs from MetaFetcher for an admin", func() {
			spyMetaFetcher.result = map[string]*rpc.MetaInfo{
				"source-0": {},
				"source-1": {},
			}
			spyOauth2ClientReader.result = true
			request.Header.Set("Authorization", "valid-token")
			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusOK))

			var m rpc.MetaResponse
			Expect(jsonpb.Unmarshal(recorder.Body, &m)).To(Succeed())

			Expect(m.Meta).To(HaveLen(2))
			Expect(m.Meta).To(HaveKey("source-0"))
			Expect(m.Meta).To(HaveKey("source-1"))
			Expect(spyLogAuthorizer.availableCalled).To(BeZero())
		})

		It("uses the requests context", func() {
			request.Header.Set("Authorization", "valid-token")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			request = request.WithContext(ctx)

			authHandler.ServeHTTP(recorder, request)

			Expect(spyMetaFetcher.called).To(Equal(1))
			Expect(spyMetaFetcher.ctx.Done()).To(BeClosed())
		})

		It("returns 502 if MetaFetcher fails", func() {
			spyMetaFetcher.err = errors.New("expected")
			spyOauth2ClientReader.result = true
			request.Header.Set("Authorization", "valid-token")
			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusBadGateway))
		})

		It("only returns source IDs that are available for a non-admin token", func() {
			spyMetaFetcher.result = map[string]*rpc.MetaInfo{
				"source-0": {},
				"source-1": {},
				"source-2": {},
			}
			spyOauth2ClientReader.result = false
			spyLogAuthorizer.available = []string{
				"source-0",
				"source-1",
			}
			request.Header.Set("Authorization", "valid-token")

			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusOK))
			var m rpc.MetaResponse
			Expect(jsonpb.Unmarshal(recorder.Body, &m)).To(Succeed())
			Expect(m.Meta).To(HaveLen(2))
			Expect(m.Meta).To(HaveKey("source-0"))
			Expect(m.Meta).To(HaveKey("source-1"))
			Expect(spyLogAuthorizer.token).To(Equal("valid-token"))
		})

		It("returns 404 if Oauth2ClientReader returns an error", func() {
			spyOauth2ClientReader.err = errors.New("some-error")
			spyOauth2ClientReader.result = true
			spyLogAuthorizer.result = true

			request.Header.Set("Authorization", "valid-token")
			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusNotFound))
			Expect(spyMetaFetcher.called).To(BeZero())
		})

		It("returns 404 if there's no authorization header present", func() {
			authHandler.ServeHTTP(recorder, request)

			Expect(recorder.Code).To(Equal(http.StatusNotFound))
		})
	})

	Describe("/v1/shard_group", func() {
		BeforeEach(func() {
			spyOauth2ClientReader.client = "some-client-id"
			spyOauth2ClientReader.user = "some-user-id"

			request = httptest.NewRequest(http.MethodGet, "/v1/shard_group/some-name", nil)
		})

		Describe("Add to group", func() {
			BeforeEach(func() {
				request.URL.Path = "/v1/shard_group/some-name"
				request.Method = "PUT"
				request.Body = ioutil.NopCloser(strings.NewReader(`{"subGroup":{"sourceIds":["some-id"]}}`))
			})

			DescribeTable("prefixes group name for GET request with the client_id and user_id", func(sourceID string) {
				request.Header.Set("Authorization", "valid-token")

				var req *http.Request
				var reqBody []byte
				baseHandler := http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
					req = r
					body, err := ioutil.ReadAll(req.Body)
					Expect(err).ToNot(HaveOccurred())
					reqBody = body
				})
				authHandler := provider.Middleware(baseHandler)

				request.URL.Path = "/v1/shard_group/some-name"
				request.Method = "PUT"
				request.Body = ioutil.NopCloser(strings.NewReader(fmt.Sprintf(`{"subGroup":{"sourceIds":["%s"]}}`, sourceID)))

				spyLogAuthorizer.result = true

				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))

				Expect(req.URL.Path).To(Equal("/v1/shard_group/some-client-id-some-user-id-some-name"))
				Expect(spyOauth2ClientReader.token).To(Equal("valid-token"))

				Expect(reqBody).To(MatchJSON(fmt.Sprintf(`{"subGroup":{"sourceIds":["%s"]}}`, sourceID)))

				Expect(spyLogAuthorizer.sourceID).To(Equal(sourceID))
				Expect(spyLogAuthorizer.token).To(Equal("valid-token"))
			},
				Entry("without slash", "12345"),
				Entry("with slash", "12/345"),
				Entry("with encoded slash", "12%2F345"),
			)

			It("returns 404 if Oauth2ClientReader returns an error", func() {
				spyOauth2ClientReader.err = errors.New("some-error")
				spyOauth2ClientReader.result = true
				spyLogAuthorizer.result = true

				request.Header.Set("Authorization", "valid-token")

				var baseHandlerCalled bool
				baseHandler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					baseHandlerCalled = true
				})
				authHandler := provider.Middleware(baseHandler)
				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
				Expect(baseHandlerCalled).To(BeFalse())
			})

			It("returns 404 if there's no authorization header present", func() {
				baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
				provider.Middleware(baseHandler).ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
			})

			It("returns 404 if user is not authorized", func() {
				var baseHandlerCalled bool
				baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					baseHandlerCalled = true
				})
				authHandler := provider.Middleware(baseHandler)

				spyOauth2ClientReader.result = false
				spyLogAuthorizer.result = false

				request.Header.Set("Authorization", "valid-token")
				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
				Expect(baseHandlerCalled).To(BeFalse())
			})
		})

		Describe("Read from group", func() {
			BeforeEach(func() {
				request.URL.Path = "/v1/shard_group/some-name"
				request.Method = "GET"
			})

			It("prefixes group name for GET request with the client_id and user_id", func() {
				request.Header.Set("Authorization", "valid-token")

				var req *http.Request
				baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					req = r
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"group": "some-client-id-some-user-id-some-name"}`))
				})
				authHandler := provider.Middleware(baseHandler)

				spyLogAuthorizer.result = true

				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))
				Expect(recorder.Body.String()).To(MatchJSON(`{
					"group": "some-client-id-some-user-id-some-name"
				}`))

				Expect(req.URL.Path).To(Equal("/v1/shard_group/some-client-id-some-user-id-some-name"))
				Expect(spyOauth2ClientReader.token).To(Equal("valid-token"))
			})

			It("removes prefixes from group name on error", func() {
				request.Header.Set("Authorization", "valid-token")

				var req *http.Request
				baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					req = r
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{
						"error":"unknown group name: some-client-id-some-user-id-some-name",
						"code":5
					}`))
				})

				authHandler := provider.Middleware(baseHandler)

				spyLogAuthorizer.result = true

				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
				Expect(recorder.Body.String()).To(MatchJSON(`{
					"error":"unknown group name: some-name",
					"code":5
				}`))

				Expect(req.URL.Path).To(Equal("/v1/shard_group/some-client-id-some-user-id-some-name"))
				Expect(spyOauth2ClientReader.token).To(Equal("valid-token"))
			})

			It("returns 404 if Oauth2ClientReader returns an error", func() {
				spyOauth2ClientReader.err = errors.New("some-error")
				spyOauth2ClientReader.result = true

				request.Header.Set("Authorization", "valid-token")

				var baseHandlerCalled bool
				baseHandler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					baseHandlerCalled = true
				})
				authHandler := provider.Middleware(baseHandler)
				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
				Expect(baseHandlerCalled).To(BeFalse())
			})

			It("returns 404 if there's no authorization header present", func() {
				baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
				provider.Middleware(baseHandler).ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
			})
		})

		Describe("Read meta for group", func() {
			BeforeEach(func() {
				request.URL.Path = "/v1/shard_group/some-name/meta"
				request.Method = "GET"
			})

			It("prefixes group name for GET request with the client_id and user_id", func() {
				request.Header.Set("Authorization", "valid-token")

				var req *http.Request
				baseHandler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					req = r
				})
				authHandler := provider.Middleware(baseHandler)

				spyLogAuthorizer.result = true

				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusOK))

				Expect(req.URL.Path).To(Equal("/v1/shard_group/some-client-id-some-user-id-some-name/meta"))
				Expect(spyOauth2ClientReader.token).To(Equal("valid-token"))
			})

			It("returns 404 if Oauth2ClientReader returns an error", func() {
				spyOauth2ClientReader.err = errors.New("some-error")
				spyOauth2ClientReader.result = true

				request.Header.Set("Authorization", "valid-token")

				var baseHandlerCalled bool
				baseHandler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					baseHandlerCalled = true
				})
				authHandler := provider.Middleware(baseHandler)
				authHandler.ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
				Expect(baseHandlerCalled).To(BeFalse())
			})

			It("returns 404 if there's no authorization header present", func() {
				baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
				provider.Middleware(baseHandler).ServeHTTP(recorder, request)

				Expect(recorder.Code).To(Equal(http.StatusNotFound))
			})
		})
	})

	It("returns 404 if the request is invalid", func() {
		request = httptest.NewRequest(http.MethodGet, "/invalid/endpoint", nil)

		baseHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		authHandler := provider.Middleware(baseHandler)

		spyOauth2ClientReader.result = true

		request.Header.Set("Authorization", "valid-token")

		authHandler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})
})

type spyOauth2ClientReader struct {
	token  string
	result bool
	client string
	user   string
	err    error
}

func newAdminChecker() *spyOauth2ClientReader {
	return &spyOauth2ClientReader{}
}

func (s *spyOauth2ClientReader) Read(token string) (auth.Oauth2Client, error) {
	s.token = token
	return auth.Oauth2Client{
		IsAdmin:  s.result,
		ClientID: s.client,
		UserID:   s.user,
	}, s.err
}

type spyLogAuthorizer struct {
	result          bool
	sourceID        string
	token           string
	available       []string
	availableCalled int
}

func newSpyLogAuthorizer() *spyLogAuthorizer {
	return &spyLogAuthorizer{}
}

func (s *spyLogAuthorizer) IsAuthorized(sourceID, token string) bool {
	s.sourceID = sourceID
	s.token = token
	return s.result
}

func (s *spyLogAuthorizer) AvailableSourceIDs(token string) []string {
	s.availableCalled++
	s.token = token
	return s.available
}

type spyMetaFetcher struct {
	result map[string]*rpc.MetaInfo
	err    error
	ctx    context.Context
	called int
}

func newSpyMetaFetcher() *spyMetaFetcher {
	return &spyMetaFetcher{}
}

func (s *spyMetaFetcher) Meta(ctx context.Context) (map[string]*rpc.MetaInfo, error) {
	s.called++
	s.ctx = ctx
	return s.result, s.err
}
