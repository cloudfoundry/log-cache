package auth

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"log"

	"context"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"github.com/golang/protobuf/jsonpb"
	"github.com/gorilla/mux"
)

type CFAuthMiddlewareProvider struct {
	oauth2Reader  Oauth2ClientReader
	logAuthorizer LogAuthorizer
	metaFetcher   MetaFetcher
	marshaller    jsonpb.Marshaler
}

type Oauth2Client struct {
	IsAdmin  bool
	ClientID string
	UserID   string
}

type Oauth2ClientReader interface {
	Read(token string) (Oauth2Client, error)
}

type LogAuthorizer interface {
	IsAuthorized(sourceID, token string) bool
	AvailableSourceIDs(token string) []string
}

type MetaFetcher interface {
	Meta(context.Context) (map[string]*rpc.MetaInfo, error)
}

func NewCFAuthMiddlewareProvider(
	oauth2Reader Oauth2ClientReader,
	logAuthorizer LogAuthorizer,
	metaFetcher MetaFetcher,
) CFAuthMiddlewareProvider {
	return CFAuthMiddlewareProvider{
		oauth2Reader:  oauth2Reader,
		logAuthorizer: logAuthorizer,
		metaFetcher:   metaFetcher,
	}
}

func (m CFAuthMiddlewareProvider) Middleware(h http.Handler) http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/v1/read/{sourceID:.*}", func(w http.ResponseWriter, r *http.Request) {
		sourceID, ok := mux.Vars(r)["sourceID"]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		c, err := m.oauth2Reader.Read(authToken)
		if err != nil {
			log.Printf("failed to read from Oauth2 server: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if !c.IsAdmin {
			if !m.logAuthorizer.IsAuthorized(sourceID, authToken) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		h.ServeHTTP(w, r)
	})

	router.HandleFunc("/v1/shard_group/{name}", func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
		}()

		vars := mux.Vars(r)

		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var groupedSourceIDs rpc.GroupedSourceIds
		if err := jsonpb.Unmarshal(r.Body, &groupedSourceIDs); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			println(err.Error())
			w.Write([]byte(fmt.Sprintf(`{"error": %q}`, err)))
			return
		}

		c, err := m.oauth2Reader.Read(authToken)
		if err != nil {
			log.Printf("failed to read from Oauth2 server: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		for _, sourceID := range groupedSourceIDs.GetSourceIds() {
			if !c.IsAdmin {
				if !m.logAuthorizer.IsAuthorized(sourceID, authToken) {
					w.WriteHeader(http.StatusNotFound)
					return
				}
			}
		}

		r.URL.Path = fmt.Sprintf(
			"/v1/shard_group/%s-%s-%s",
			c.ClientID,
			c.UserID,
			vars["name"],
		)

		buf := &bytes.Buffer{}
		m := jsonpb.Marshaler{}
		m.Marshal(buf, &groupedSourceIDs)
		r.Body = ioutil.NopCloser(buf)

		h.ServeHTTP(w, r)
	}).Methods("PUT")

	router.HandleFunc("/v1/shard_group/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		c, err := m.oauth2Reader.Read(authToken)
		if err != nil {
			log.Printf("failed to read from Oauth2 server: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		prefixedName := fmt.Sprintf("%s-%s-%s", c.ClientID, c.UserID, vars["name"])
		r.URL.Path = "/v1/shard_group/" + prefixedName

		interceptor := &interceptingResponseWriter{
			ResponseWriter: w,
			search:         []byte(prefixedName),
			replace:        []byte(vars["name"]),
		}
		h.ServeHTTP(interceptor, r)
	}).Methods("GET")

	router.HandleFunc("/v1/shard_group/{name}/meta", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		c, err := m.oauth2Reader.Read(authToken)
		if err != nil {
			log.Printf("failed to read from Oauth2 server: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		r.URL.Path = fmt.Sprintf(
			"/v1/shard_group/%s-%s-%s/meta",
			c.ClientID,
			c.UserID,
			vars["name"],
		)

		h.ServeHTTP(w, r)
	}).Methods("GET")

	router.HandleFunc("/v1/meta", func(w http.ResponseWriter, r *http.Request) {
		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		c, err := m.oauth2Reader.Read(authToken)
		if err != nil {
			log.Printf("failed to read from Oauth2 server: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		meta, err := m.metaFetcher.Meta(r.Context())
		if err != nil {
			log.Printf("failed to fetch meta information: %s", err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}

		// We don't care if writing to the client fails. They can come back and ask again.
		_ = m.marshaller.Marshal(w, &rpc.MetaResponse{
			Meta: m.onlyAuthorized(authToken, meta, c),
		})
	})

	return router
}

func (m CFAuthMiddlewareProvider) onlyAuthorized(authToken string, meta map[string]*rpc.MetaInfo, c Oauth2Client) map[string]*rpc.MetaInfo {
	if c.IsAdmin {
		return meta
	}

	authorized := m.logAuthorizer.AvailableSourceIDs(authToken)
	intersection := make(map[string]*rpc.MetaInfo)
	for _, id := range authorized {
		if v, ok := meta[id]; ok {
			intersection[id] = v
		}
	}

	return intersection
}

type interceptingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	search     []byte
	replace    []byte
}

func (w *interceptingResponseWriter) WriteHeader(n int) {
	w.statusCode = n

	w.ResponseWriter.WriteHeader(n)
}

func (w *interceptingResponseWriter) Write(b []byte) (int, error) {
	if w.statusCode >= 400 {
		return w.ResponseWriter.Write(bytes.Replace(b, w.search, w.replace, -1))
	}

	return w.ResponseWriter.Write(b)
}
