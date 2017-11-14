package web_test

import (
	"net/http"
	"net/http/httptest"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/web"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Router", func() {
	var (
		r *web.Router

		recorder *httptest.ResponseRecorder

		sourceID  string
		envelopes []*loggregator_v2.Envelope
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		envelopes = []*loggregator_v2.Envelope{
			buildEnvelope("app-a"),
			buildEnvelope("app-a"),
		}
		r = web.NewRouter(func(s string) []*loggregator_v2.Envelope {
			sourceID = s
			return envelopes
		})
	})

	It("returns a 405 for anything other than a GET", func() {
		req := httptest.NewRequest(http.MethodDelete, "/", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusMethodNotAllowed))
		Expect(recorder.Body.String()).To(BeEmpty())
	})

	It("returns a 404 for an unsupported route", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/non-existent", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
		Expect(recorder.Body.String()).To(BeEmpty())
	})

	It("returns envelopes for a given source ID", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/", nil)

		r.ServeHTTP(recorder, req)

		Expect(sourceID).To(Equal("app-a"))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Body.String()).To(MatchJSON(`{
			"envelopes": [
				{
					"sourceId": "app-a"
				},
				{
					"sourceId": "app-a"
				}
			]
		}`))
	})
})

func buildEnvelope(sourceID string) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		SourceId: sourceID,
	}
}
