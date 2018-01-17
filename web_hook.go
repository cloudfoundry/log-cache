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
//
// WebHook has two modes, follow and windowing. Follow mode is default.
//
// Follow mode configures a WebHook to follow a sourceID via polling quickly
// for smaller windows. If follow is enabled, then any WindowWidth and
// Interval are ignored. When in follow mode, the given template will only be
// given a single envelope at a time.
//
// Windowing mode windows through data to allow templates to assert against
// time periods. The LogCache is queried for a time range and the given
// StartTime is incremented by given duration. This is useful when asserting
// against a certain condition (e.g., CPU is above 50% over the last hour).
// When in windowing mode, the given template will be given a slice of
// envelopes.
type WebHook struct {
	log        *log.Logger
	sourceID   string
	t          *template.Template
	reader     Reader
	width      time.Duration
	interval   time.Duration
	start      time.Time
	errHandler func(error)
	windowing  bool
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
		sourceID:   sourceID,
		reader:     r,
		errHandler: func(error) {},
	}

	for _, o := range opts {
		o(h)
	}

	// Both WithWebHookInterval and WithWebHookWindowing can set this value.
	// If neither did, give it a default.
	if h.interval == 0 {
		h.interval = time.Second
	}

	if h.start.IsZero() {
		h.start = time.Now().Add(-h.width)
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

// WithWebHookWindowing sets the WebHook into windowing mode. The given
// template will receive slices of envelopes. The window will be moved by the
// given interval. If interval is not set, it defaults to 1 minute.
func WithWebHookWindowing(width time.Duration) WebHookOption {
	return func(h *WebHook) {
		h.windowing = true
		h.width = width

		if h.interval == 0 {
			h.interval = time.Minute
		}
	}
}

// WithWebHookStartTime sets the start time. It defaults to time.Now().
func WithWebHookStartTime(t time.Time) WebHookOption {
	return func(h *WebHook) {
		h.start = t
	}
}

// Start starts reading from LogCache and posting data according to the
// provided template. It blocks indefinately.
func (h *WebHook) Start() {
	if h.windowing {
		ww := logcache.BuildWalker(
			h.sourceID,
			logcache.Reader(h.reader),
			h.interval,
			3600,
		)

		logcache.Window(
			context.Background(),
			h.windowingVisitor,
			ww,
			logcache.WithWindowWidth(h.width),
			logcache.WithWindowInterval(h.interval),
			logcache.WithWindowStartTime(h.start),
		)
		return
	}

	logcache.Walk(
		context.Background(),
		h.sourceID,
		h.followingVisitor,
		logcache.Reader(h.reader),
		logcache.WithWalkStartTime(h.start),
		logcache.WithWalkBackoff(logcache.NewAlwaysRetryBackoff(h.interval)),
	)
}

func (h *WebHook) windowingVisitor(envelopes []*loggregator_v2.Envelope) bool {
	if err := h.t.Execute(ioutil.Discard, envelopes); err != nil {
		h.errHandler(err)
	}
	return true
}

func (h *WebHook) followingVisitor(envelopes []*loggregator_v2.Envelope) bool {
	for _, e := range envelopes {
		if err := h.t.Execute(ioutil.Discard, e); err != nil {
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
