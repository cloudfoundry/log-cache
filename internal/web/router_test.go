package web_test

import (
	"net/http"
	"net/http/httptest"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"code.cloudfoundry.org/log-cache/internal/web"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Router", func() {
	var (
		r *web.Router

		recorder *httptest.ResponseRecorder

		start        time.Time
		end          time.Time
		sourceID     string
		limit        int
		envelopeType store.EnvelopeType
		envelopes    []*loggregator_v2.Envelope
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		envelopes = []*loggregator_v2.Envelope{
			buildEnvelope("app-a"),
			buildEnvelope("app-a"),
		}
		r = web.NewRouter(func(s string, st, e time.Time, t store.EnvelopeType, l int) []*loggregator_v2.Envelope {
			sourceID = s
			start = st
			end = e
			envelopeType = t
			limit = l
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
		req := httptest.NewRequest(http.MethodGet, "/app-a/?starttime=99&endtime=101&limit=103", nil)

		r.ServeHTTP(recorder, req)

		Expect(sourceID).To(Equal("app-a"))
		Expect(start).To(Equal(time.Unix(99, 0)))
		Expect(end).To(Equal(time.Unix(101, 0)))
		Expect(envelopeType).To(BeNil())
		Expect(limit).To(Equal(103))

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

	DescribeTable("fetches data based on envelope",
		func(url string, expectedType store.EnvelopeType) {
			req := httptest.NewRequest(http.MethodGet, url, nil)
			r.ServeHTTP(recorder, req)
			Expect(envelopeType).To(Equal(expectedType))
		},

		Entry("Log", "/app-a/?envelopetype=log", &loggregator_v2.Log{}),
		Entry("Counter", "/app-a/?envelopetype=counter", &loggregator_v2.Counter{}),
		Entry("Gauge", "/app-a/?envelopetype=gauge", &loggregator_v2.Gauge{}),
		Entry("Timer", "/app-a/?envelopetype=timer", &loggregator_v2.Timer{}),
		Entry("Event", "/app-a/?envelopetype=event", &loggregator_v2.Event{}),
	)

	It("defaults start time to 0 and end time to now", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a", nil)

		r.ServeHTTP(recorder, req)

		Expect(start).To(Equal(time.Unix(0, 0)))
		Expect(end.Unix()).To(BeNumerically("~", time.Now().Unix(), 1))
	})

	It("defaults limit to 100", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a", nil)

		r.ServeHTTP(recorder, req)

		Expect(limit).To(Equal(100))
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

	It("returns a 400 if limit is not a positive number", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/?limit=-99", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusBadRequest))
	})

	It("returns a 400 if limit is greater than 1000", func() {
		req := httptest.NewRequest(http.MethodGet, "/app-a/?limit=1001", nil)

		r.ServeHTTP(recorder, req)

		Expect(recorder.Code).To(Equal(http.StatusBadRequest))
	})
})

func buildEnvelope(sourceID string) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		SourceId: sourceID,
	}
}
