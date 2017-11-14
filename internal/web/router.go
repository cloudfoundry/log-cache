package web

import (
	"fmt"
	"net/http"
	"strings"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/golang/protobuf/jsonpb"
)

// Router handles http.Requests.
type Router struct {
	get       Getter
	marshaler *jsonpb.Marshaler
}

// Getter returns data for the given sourceID.
type Getter func(sourceID string) []*loggregator_v2.Envelope

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
	if strings.Contains(appID, "/") {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var marshaledEnvs []string
	for _, e := range r.get(appID) {
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
