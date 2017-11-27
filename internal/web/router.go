package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"github.com/golang/protobuf/jsonpb"
)

// Router handles http.Requests.
type Router struct {
	get       Getter
	marshaler *jsonpb.Marshaler
}

// Getter returns data for the given sourceID.
type Getter func(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeType store.EnvelopeType,
	limit int,
) []*loggregator_v2.Envelope

// NewRouter returns a new Router.
func NewRouter(g Getter) *Router {
	return &Router{
		get:       g,
		marshaler: &jsonpb.Marshaler{},
	}
}

// ServeHTTP implements http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	appID := strings.Trim(req.URL.Path, "/")
	if strings.Contains(appID, "/") || appID == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	startTime, endTime, err := getTimeRange(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	envelopeType, err := stringToEnvelopeType(req.URL.Query().Get("envelopetype"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	limit, err := stringToInt(req.URL.Query().Get("limit"), 100)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if limit > 1000 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var marshaledEnvs []string
	for _, e := range r.get(appID, startTime, endTime, envelopeType, limit) {
		str, err := r.marshaler.MarshalToString(e)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		marshaledEnvs = append(marshaledEnvs, str)
	}

	w.Write([]byte(
		fmt.Sprintf(`{"envelopes": [%s]}`, strings.Join(marshaledEnvs, ",")),
	))
}

func stringToInt(s string, d int) (int, error) {
	if s == "" {
		return d, nil
	}

	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}

	return int(v), nil
}

func stringToEnvelopeType(s string) (store.EnvelopeType, error) {
	switch s {
	case "":
		return nil, nil
	case "log":
		return &loggregator_v2.Log{}, nil
	case "counter":
		return &loggregator_v2.Counter{}, nil
	case "gauge":
		return &loggregator_v2.Gauge{}, nil
	case "timer":
		return &loggregator_v2.Timer{}, nil
	case "event":
		return &loggregator_v2.Event{}, nil
	default:
		return nil, errors.New("invalid envelope type")
	}
}

func stringToTime(ts string, d time.Time) (time.Time, error) {
	if ts == "" {
		return d, nil
	}

	intTS, err := strconv.ParseUint(ts, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(int64(intTS), 0), nil
}

func getTimeRange(req *http.Request) (time.Time, time.Time, error) {
	startTime, err := stringToTime(req.URL.Query().Get("starttime"), time.Unix(0, 0))
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	endTime, err := stringToTime(req.URL.Query().Get("endtime"), time.Now())
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	if startTime.After(endTime) {
		return time.Time{}, time.Time{}, errors.New("start time must be after end time")
	}

	return startTime, endTime, nil
}
