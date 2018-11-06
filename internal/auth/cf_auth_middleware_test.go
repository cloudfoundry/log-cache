package auth_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"

	"code.cloudfoundry.org/log-cache/internal/auth"

	"context"

	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"github.com/Benjamintf1/unmarshalledmatchers"
	"github.com/golang/protobuf/jsonpb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

type testContext struct {
	spyOauth2ClientReader *spyOauth2ClientReader
	spyLogAuthorizer      *spyLogAuthorizer
	spyMetaFetcher        *spyMetaFetcher
	spyPromQLParser       *spyPromQLParser
	spyAppNameTranslator  *spyAppNameTranslator

	recorder *httptest.ResponseRecorder
	request  *http.Request
	provider auth.CFAuthMiddlewareProvider

	baseHandlerCalled  bool
	baseHandlerRequest *http.Request
	authHandler        http.Handler
}

func setup(requestPath string) *testContext {
	spyOauth2ClientReader := newAdminChecker()
	spyLogAuthorizer := newSpyLogAuthorizer()
	spyMetaFetcher := newSpyMetaFetcher()
	spyPromQLParser := newSpyPromQLParser()
	spyAppNameTranslator := newSpyAppNameTranslator()

	provider := auth.NewCFAuthMiddlewareProvider(
		spyOauth2ClientReader,
		spyLogAuthorizer,
		spyMetaFetcher,
		spyPromQLParser.ExtractSourceIds,
		spyAppNameTranslator,
	)

	request := httptest.NewRequest(http.MethodGet, requestPath, nil)
	request.Header.Set("Authorization", "bearer valid-token")

	tc := &testContext{
		spyOauth2ClientReader: spyOauth2ClientReader,
		spyLogAuthorizer:      spyLogAuthorizer,
		spyMetaFetcher:        spyMetaFetcher,
		spyPromQLParser:       spyPromQLParser,
		spyAppNameTranslator:  spyAppNameTranslator,

		recorder: httptest.NewRecorder(),
		request:  request,
		provider: provider,
	}

	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc.baseHandlerCalled = true
		tc.baseHandlerRequest = r
	})
	tc.authHandler = provider.Middleware(baseHandler)

	return tc
}

func (tc *testContext) invokeAuthHandler() {
	tc.authHandler.ServeHTTP(tc.recorder, tc.request)
}

var _ = Describe("CfAuthMiddleware", func() {
	Describe("/api/v1/read", func() {
		It("forwards the request to the handler if user is an admin", func() {
			tc := setup("/api/v1/read/12345")

			tc.spyOauth2ClientReader.isAdminResult = true

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())

			Expect(tc.spyOauth2ClientReader.token).To(Equal("bearer valid-token"))
		})

		DescribeTable("forwards the request to the handler if non-admin user has log access", func(sourceID string) {
			// request path set this way to preserve URL encoding
			tc := setup("/")
			tc.request.URL.Path = fmt.Sprintf("/api/v1/read/%s", sourceID)

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())

			//verify CAPI called with correct info
			Expect(tc.spyLogAuthorizer.token).To(Equal("bearer valid-token"))
			Expect(tc.spyLogAuthorizer.sourceIDsCalledWith).To(HaveKey(sourceID))
		},
			Entry("without slash", "12345"),
			Entry("with slash", "12/345"),
			Entry("with encoded slash", "12%2F345"),
		)

		It("returns 404 Not Found if there's no authorization header present", func() {
			tc := setup("/api/v1/read/12345")
			tc.request.Header.Del("Authorization")

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})

		It("returns 404 Not Found if Oauth2ClientReader returns an error", func() {
			tc := setup("/")
			tc.spyOauth2ClientReader.err = errors.New("some-error")

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})

		It("returns 404 Not Found if user is not authorized", func() {
			tc := setup("/api/v1/read/12345")
			tc.spyLogAuthorizer.unauthorizedSourceIds["12345"] = struct{}{}

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})
	})

	Describe("/api/v1/meta", func() {
		It("returns all source IDs from MetaFetcher for an admin", func() {
			tc := setup("/api/v1/meta")
			tc.spyMetaFetcher.result = map[string]*rpc.MetaInfo{
				"source-0": {},
				"source-1": {},
			}
			tc.spyOauth2ClientReader.isAdminResult = true

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))

			var m rpc.MetaResponse
			Expect(jsonpb.Unmarshal(tc.recorder.Body, &m)).To(Succeed())

			Expect(m.Meta).To(HaveLen(2))
			Expect(m.Meta).To(HaveKey("source-0"))
			Expect(m.Meta).To(HaveKey("source-1"))
			Expect(tc.spyLogAuthorizer.availableCalled).To(BeZero())
		})

		It("returns only source IDs that are available for a non-admin token", func() {
			tc := setup("/api/v1/meta")
			tc.spyMetaFetcher.result = map[string]*rpc.MetaInfo{
				"source-0": {},
				"source-1": {},
				"source-2": {},
			}
			tc.spyLogAuthorizer.available = []string{
				"source-0",
				"source-1",
			}

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			var m rpc.MetaResponse
			Expect(jsonpb.Unmarshal(tc.recorder.Body, &m)).To(Succeed())
			Expect(m.Meta).To(HaveLen(2))
			Expect(m.Meta).To(HaveKey("source-0"))
			Expect(m.Meta).To(HaveKey("source-1"))
			Expect(tc.spyLogAuthorizer.token).To(Equal("bearer valid-token"))
		})

		It("respects the request's context", func() {
			tc := setup("/api/v1/meta")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tc.request = tc.request.WithContext(ctx)

			tc.invokeAuthHandler()

			Expect(tc.spyMetaFetcher.called).To(Equal(1))
			Expect(tc.spyMetaFetcher.ctx.Done()).To(BeClosed())
		})

		It("appends a newline to the response", func() {
			tc := setup("/api/v1/meta")
			tc.spyMetaFetcher.result = map[string]*rpc.MetaInfo{}
			tc.spyOauth2ClientReader.isAdminResult = true

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.recorder.Body.String()).To(MatchRegexp(`\n$`))
		})

		It("returns 502 Bad Gateway if MetaFetcher fails", func() {
			tc := setup("/api/v1/meta")
			tc.spyMetaFetcher.err = errors.New("expected")

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusBadGateway))
		})

		It("returns 404 Not Found if Oauth2ClientReader returns an error", func() {
			tc := setup("/api/v1/meta")
			tc.spyOauth2ClientReader.err = errors.New("some-error")

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
			Expect(tc.spyMetaFetcher.called).To(BeZero())
		})

		It("returns 404 Not Found if Oauth2ClientReader returns an error", func() {
			tc := setup("/api/v1/meta")
			tc.request.Header.Del("Authorization")

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
		})
	})

	Describe("/api/v1/query", func() {
		It("forwards the request to the handler if user is an admin", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)
			tc.spyOauth2ClientReader.isAdminResult = true

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())

			Expect(tc.spyOauth2ClientReader.token).To(Equal("bearer valid-token"))
			Expect(tc.spyPromQLParser.query).To(Equal(`metric{source_id="some-id"}`))
		})

		It("forwards the request to the handler if non-admin user has log access", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())

			Expect(tc.spyLogAuthorizer.token).To(Equal("bearer valid-token"))
			Expect(tc.spyLogAuthorizer.sourceIDsCalledWith).To(HaveKey("some-id"))
		})

		It("expands queries to include all related source IDs if user is an admin", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)
			tc.spyAppNameTranslator.relatedIds = map[string][]string{"some-id": {"app-guid-1"}}
			tc.spyOauth2ClientReader.isAdminResult = true

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())

			Expect(tc.spyAppNameTranslator.calledWith).To(ConsistOf("some-id"))
			Expect(tc.baseHandlerRequest.URL.Query().Get("query")).To(
				Equal(`metric{source_id=~"app-guid-1|some-id"}`),
			)
		})

		It("expands queries to include all authorized related source IDs if user is not an admin", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)
			tc.spyLogAuthorizer.unauthorizedSourceIds["some-id"] = struct{}{}
			tc.spyLogAuthorizer.unauthorizedSourceIds["unauthorized-app-guid-2"] = struct{}{}
			tc.spyAppNameTranslator.relatedIds = map[string][]string{"some-id": {"app-guid-1", "unauthorized-app-guid-2"}}

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())

			Expect(tc.baseHandlerRequest.URL.Query().Get("query")).To(
				Equal(`metric{source_id="app-guid-1"}`),
			)
			Expect(tc.spyLogAuthorizer.sourceIDsCalledWith).To(HaveKey("app-guid-1"))
		})

		It("returns 400 Bad Request if a query doesn't have a source_id", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)
			tc.spyPromQLParser.sourceIDs = nil

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusBadRequest))
			Expect(tc.recorder.Header()).To(HaveKeyWithValue("Content-Type", []string{"application/json"}))
			Expect(tc.recorder.Body.String()).To(unmarshalledmatchers.ContainUnorderedJSON(`{
				"status": "error",
				"errorType": "bad_data"
			}`))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})

		It("returns 400 Bad Request for an invalid query", func() {
			tc := setup(`/api/v1/query?query=wrong{source_id=some-id}`)
			tc.spyPromQLParser.err = errors.New("some-error")

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusBadRequest))
			Expect(tc.recorder.Header()).To(HaveKeyWithValue("Content-Type", []string{"application/json"}))
			Expect(tc.recorder.Body.String()).To(unmarshalledmatchers.ContainUnorderedJSON(`{
				"status": "error",
				"errorType": "bad_data"
			}`))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})

		It("returns 404 Not Found if Oauth2ClientReader returns an error", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)

			tc.spyOauth2ClientReader.err = errors.New("some-error")

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})

		It("returns 404 Not Found if user is not authorized", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)
			tc.spyLogAuthorizer.unauthorizedSourceIds["some-id"] = struct{}{}

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})

		It("returns 404 Not Found if user is not authorized to see any apps", func() {
			tc := setup(`/api/v1/query?query=metric{source_id="some-id"}`)
			tc.spyAppNameTranslator.relatedIds = nil

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusNotFound))
			Expect(tc.baseHandlerCalled).To(BeFalse())
		})
	})

	Describe("/api/v1/query_range", func() {
		It("forwards the /api/v1/query_range request to the handler if user is an admin", func() {
			tc := setup(`/api/v1/query_range?query=metric{source_id="some-id"}`)
			tc.spyOauth2ClientReader.isAdminResult = true

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())

			Expect(tc.spyOauth2ClientReader.token).To(Equal("bearer valid-token"))
			Expect(tc.spyPromQLParser.query).To(Equal(`metric{source_id="some-id"}`))
		})
	})

	Describe("/api/v1/info", func() {
		It("forwards the request to the handler without requiring authentication", func() {
			tc := setup(`/api/v1/info`)
			tc.request.Header.Del("Authorization")
			tc.spyOauth2ClientReader.isAdminResult = false

			tc.invokeAuthHandler()

			Expect(tc.recorder.Code).To(Equal(http.StatusOK))
			Expect(tc.baseHandlerCalled).To(BeTrue())
		})
	})
})

type spyOauth2ClientReader struct {
	token         string
	isAdminResult bool
	client        string
	user          string
	err           error
}

func newAdminChecker() *spyOauth2ClientReader {
	return &spyOauth2ClientReader{}
}

func (s *spyOauth2ClientReader) Read(token string) (auth.Oauth2ClientContext, error) {
	s.token = token
	return auth.Oauth2ClientContext{
		IsAdmin: s.isAdminResult,
		Token:   token,
	}, s.err
}

type spyLogAuthorizer struct {
	unauthorizedSourceIds map[string]struct{}
	sourceIDsCalledWith   map[string]struct{}
	token                 string
	available             []string
	availableCalled       int
}

func newSpyLogAuthorizer() *spyLogAuthorizer {
	return &spyLogAuthorizer{
		unauthorizedSourceIds: make(map[string]struct{}),
		sourceIDsCalledWith:   make(map[string]struct{}),
	}
}

func (s *spyLogAuthorizer) IsAuthorized(sourceId string, clientToken string) bool {
	s.sourceIDsCalledWith[sourceId] = struct{}{}
	s.token = clientToken

	_, exists := s.unauthorizedSourceIds[sourceId]

	return !exists
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

type spyPromQLParser struct {
	query     string
	sourceIDs []string
	err       error
}

func newSpyPromQLParser() *spyPromQLParser {
	return &spyPromQLParser{
		sourceIDs: []string{"some-id"},
	}
}

func (s *spyPromQLParser) ExtractSourceIds(query string) ([]string, error) {
	s.query = query
	return s.sourceIDs, s.err
}

type spyAppNameTranslator struct {
	calledWith []string
	relatedIds map[string][]string
}

func newSpyAppNameTranslator() *spyAppNameTranslator {
	return &spyAppNameTranslator{
		relatedIds: make(map[string][]string),
	}
}

func (s *spyAppNameTranslator) GetRelatedSourceIds(appNames []string, token string) map[string][]string {
	s.calledWith = append(s.calledWith, appNames...)

	return s.relatedIds
}
