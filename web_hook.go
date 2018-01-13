package logcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"golang.org/x/net/context"
)

// WebHook reads a window of time from the LogCache and hands the resulting
// envelope batches to a user defined go text/template. The template has a
// Post function available to it which will post data.
type WebHook struct {
	log        *log.Logger
	sourceID   string
	t          *template.Template
	reader     Reader
	width      time.Duration
	interval   time.Duration
	errHandler func(error)
	follow     bool
}

// Reader reads envelopes from LogCache. It will be invoked by Walker several
// time to traverse the length of the cache.
//
// The given template should handle a slice of loggregator V2 envelopes. There
// are several provided functions to help analyze the envelopes. If in follow
// mode, the template will be given one envelope instead of a slice.
type Reader func(
	ctx context.Context,
	sourceID string,
	start time.Time,
	opts ...logcache.ReadOption,
) ([]*loggregator_v2.Envelope, error)

// NewWebHook creates and returns a WebHook.
func NewWebHook(
	sourceID string,
	txtTemplate string,
	r Reader,
	opts ...WebHookOption,
) *WebHook {
	h := &WebHook{
		log:        log.New(ioutil.Discard, "", 0),
		width:      time.Hour,
		interval:   time.Minute,
		sourceID:   sourceID,
		reader:     r,
		errHandler: func(error) {},
	}

	for _, o := range opts {
		o(h)
	}

	if h.follow {
		h.interval = time.Second
	}

	var err error
	t := template.New("WebHook")
	t.Funcs(map[string]interface{}{
		"post": func(url string, headers Map, data interface{}) int {
			d, err := json.Marshal(data)
			if err != nil {
				h.errHandler(err)
				return 0
			}

			req, err := http.NewRequest("POST", url, bytes.NewReader(d))
			if err != nil {
				h.errHandler(err)
				return 0
			}

			// Set the headers
			for k, v := range headers {
				req.Header.Set(k, fmt.Sprint(v))
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				h.errHandler(err)
				return 0
			}

			if resp.StatusCode > 299 {
				h.errHandler(fmt.Errorf("unexpected status code %d", resp.StatusCode))
				return 0
			}

			return resp.StatusCode
		},

		"mapInit": func(key string, value interface{}) Map {
			m := make(map[string]interface{})
			m[key] = value
			return m
		},
		"mapAdd": func(key string, value interface{}, m Map) Map {
			m[key] = value
			return m
		},
		"sliceInit": func(values ...interface{}) []interface{} {
			return append([]interface{}{}, values...)
		},
		"sliceAppend": func(s []interface{}, values ...interface{}) []interface{} {
			return append(s, values...)
		},
		"nsToTime": func(ns int64) time.Time {
			return time.Unix(0, ns)
		},
		"averageEnvelopes": func(es []*loggregator_v2.Envelope) float64 {
			var total float64
			for _, e := range es {
				switch x := e.Message.(type) {
				case *loggregator_v2.Envelope_Counter:
					total += float64(x.Counter.Total)
				case *loggregator_v2.Envelope_Gauge:
					for _, v := range x.Gauge.Metrics {
						total += float64(v.GetValue())
					}
				}
			}
			return total / float64(len(es))
		},
		"countEnvelopes": func(e []*loggregator_v2.Envelope) int {
			return len(e)
		},
	})

	t, err = t.Parse(txtTemplate)
	if err != nil {
		h.log.Panicf("failed to parse given template: %s", err)
	}
	h.t = t

	return h
}

// WebHookOption configures a WebHook.
type WebHookOption func(*WebHook)

// WithWebHookLogger returns a WebHookOption that configures a WebHook's logger.
// It defaults to silent logging.
func WithWebHookLogger(l *log.Logger) WebHookOption {
	return func(h *WebHook) {
		h.log = l
	}
}

// WithWebHookWindowWidth returns a WebHookOption that configures a WebHook's
// window width. This is the amount of time that the WebHook will request.
// This equates to time.Now().Add(-width). It defaults to an hour.
func WithWebHookWindowWidth(width time.Duration) WebHookOption {
	return func(h *WebHook) {
		h.width = width
	}
}

// WithWebHookInterval returns a WebHookOption that configures a WebHook's
// interval. This dictates how often to read from LogCache. It defaults to 1
// minute.
func WithWebHookInterval(interval time.Duration) WebHookOption {
	return func(h *WebHook) {
		h.interval = interval
	}
}

// WithWebHookErrorHandler returns a WebHookOption that configures a
// WebHook's error handler for any error or non-200 status code.
func WithWebHookErrorHandler(f func(error)) WebHookOption {
	return func(h *WebHook) {
		h.errHandler = f
	}
}

// WithWebHookFollow returns a WebHookOption that configures a WebHook to
// follow a sourceID via polling quickly for smaller windows. If follow is
// enabled, then any WindowWidth and Interval are ignored.
// When in follow mode, the given template will only be given a single
// envelope at a time.
func WithWebHookFollow() WebHookOption {
	return func(h *WebHook) {
		h.follow = true
	}
}

// Start starts reading from LogCache and posting data according to the
// provided template. It blocks indefinately.
func (h *WebHook) Start() {
	now := time.Now()
	opts := []logcache.WalkOption{
		logcache.WithWalkStartTime(now.Add(-h.width)),
		logcache.WithWalkBackoff(logcache.NewAlwaysRetryBackoff(h.interval)),
	}

	visitor := h.followingVisitor

	if !h.follow {
		opts = append(opts, logcache.WithWalkEndTime(now))
		visitor = h.visitor
	}

	logcache.Walk(
		context.Background(),
		h.sourceID,
		visitor,
		logcache.Reader(h.reader),
		opts...,
	)
}

func (h *WebHook) visitor(envelopes []*loggregator_v2.Envelope) bool {
	b := new(bytes.Buffer)
	if err := h.t.Execute(b, envelopes); err != nil {
		h.errHandler(err)
	}
	return true
}

func (h *WebHook) followingVisitor(envelopes []*loggregator_v2.Envelope) bool {
	for _, e := range envelopes {
		b := new(bytes.Buffer)
		if err := h.t.Execute(b, e); err != nil {
			h.errHandler(err)
			continue
		}

	}
	return true
}

type Map map[string]interface{}

func (m Map) Add(key string, value interface{}) Map {
	m[key] = value
	return m
}
