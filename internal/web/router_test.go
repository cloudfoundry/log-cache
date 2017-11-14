package web_test

import (
	"net/http"
	"net/http/httptest"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/web"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Router", func() {
	var (
		r *web.Router

		recorder *httptest.ResponseRecorder

		start     time.Time
		end       time.Time
		sourceID  string
		envelopes []*loggregator_v2.Envelope
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		envelopes = []*loggregator_v2.Envelope{
			buildEnvelope("app-a"),
			buildEnvelope("app-a"),
		}
		r = web.NewRouter(func(s string, st, e time.Time) []*loggregator_v2.Envelope {
			sourceID = s
			start = st
			end = e
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

	It("returns a 404 for the root route", func() {
		req := httptest.NewRequest(http.MethodGet, "/", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusNotFound))
		Expect(recorder.Body.String()).To(BeEmpty())
	})

	It("returns envelopes for a given source ID and time slice", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/?starttime=99&endtime=101", nil)

		r.ServeHTTP(recorder, req)

		Expect(sourceID).To(Equal("app-a"))
		Expect(start).To(Equal(time.Unix(99, 0)))
		Expect(end).To(Equal(time.Unix(101, 0)))

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

	It("defaults start time to 0 and end time to now", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a", nil)

		r.ServeHTTP(recorder, req)

		Expect(start).To(Equal(time.Unix(0, 0)))
		Expect(end.Unix()).To(BeNumerically("~", time.Now().Unix(), 1))
	})

	It("returns a 400 if the start time is not a positive number", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/?starttime=-99&endtime=101", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusBadRequest))
	})

	It("returns a 400 if the end time is not a positive number", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/?endtime=-101", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusBadRequest))
	})

	It("returns a 400 if end time is before start time", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/?starttime=99&endtime=20", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusBadRequest))
	})
})

func buildEnvelope(sourceID string) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		SourceId: sourceID,
	}
}
