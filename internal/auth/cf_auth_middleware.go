package auth

import (
	"encoding/json"
	"net/http"
	"time"

	"log"

	"context"

	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"github.com/golang/protobuf/jsonpb"
	"github.com/gorilla/mux"

	"code.cloudfoundry.org/log-cache/internal/promql"
)

type CFAuthMiddlewareProvider struct {
	oauth2Reader            Oauth2ClientReader
	logAuthorizer           LogAuthorizer
	metaFetcher             MetaFetcher
	marshaller              jsonpb.Marshaler
	promQLSourceIdExtractor PromQLSourceIdExtractor
	appNameTranslator       AppNameTranslator
}

type Oauth2ClientContext struct {
	IsAdmin   bool
	Token     string
	ExpiresAt time.Time
}

type Oauth2ClientReader interface {
	Read(token string) (Oauth2ClientContext, error)
}

type LogAuthorizer interface {
	IsAuthorized(sourceID string, clientToken string) bool
	AvailableSourceIDs(token string) []string
}

type MetaFetcher interface {
	Meta(context.Context) (map[string]*rpc.MetaInfo, error)
}

type AppNameTranslator interface {
	GetRelatedSourceIds(appNames []string, token string) map[string][]string
}

type PromQLSourceIdExtractor func(query string) ([]string, error)

func NewCFAuthMiddlewareProvider(
	oauth2Reader Oauth2ClientReader,
	logAuthorizer LogAuthorizer,
	metaFetcher MetaFetcher,
	promQLSourceIdExtractor PromQLSourceIdExtractor, appNameTranslator AppNameTranslator,
) CFAuthMiddlewareProvider {
	return CFAuthMiddlewareProvider{
		oauth2Reader:            oauth2Reader,
		logAuthorizer:           logAuthorizer,
		metaFetcher:             metaFetcher,
		promQLSourceIdExtractor: promQLSourceIdExtractor,
		appNameTranslator:       appNameTranslator,
	}
}

type promqlErrorBody struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

func (m CFAuthMiddlewareProvider) Middleware(h http.Handler) http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/api/v1/read/{sourceID:.*}", func(w http.ResponseWriter, r *http.Request) {
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

		userContext, err := m.oauth2Reader.Read(authToken)
		if err != nil {
			log.Printf("failed to read from Oauth2 server: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if !userContext.IsAdmin {
			if !m.logAuthorizer.IsAuthorized(sourceID, userContext.Token) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		h.ServeHTTP(w, r)
	})

	router.HandleFunc("/api/v1/{subpath:query|query_range}", func(w http.ResponseWriter, r *http.Request) {
		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		query := r.URL.Query().Get("query")
		sourceIds, err := m.promQLSourceIdExtractor(query)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)

			json.NewEncoder(w).Encode(&promqlErrorBody{
				Status:    "error",
				ErrorType: "bad_data",
				Error:     err.Error(),
			})

			return
		}

		if len(sourceIds) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)

			json.NewEncoder(w).Encode(&promqlErrorBody{
				Status:    "error",
				ErrorType: "bad_data",
				Error:     "query does not request any source_ids",
			})

			return
		}

		c, err := m.oauth2Reader.Read(authToken)
		if err != nil {
			log.Printf("failed to read from Oauth2 server: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		relatedSourceIds := m.appNameTranslator.GetRelatedSourceIds(sourceIds, authToken)
		if relatedSourceIds == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		for _, sourceId := range sourceIds {
			sourceIdSet := append(relatedSourceIds[sourceId], sourceId)

			if !c.IsAdmin {
				sourceIdSet = m.authorizeSourceIds(sourceIdSet, c)

				if len(sourceIdSet) == 0 {
					w.WriteHeader(http.StatusNotFound)
					return
				}
			}

			relatedSourceIds[sourceId] = sourceIdSet
		}

		modifiedQuery, err := promql.ReplaceSourceIdSets(query, relatedSourceIds)
		if err != nil {
			log.Printf("failed to expand source IDs: %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		q := r.URL.Query()
		q.Set("query", modifiedQuery)
		r.URL.RawQuery = q.Encode()

		h.ServeHTTP(w, r)
	})

	router.HandleFunc("/api/v1/meta", func(w http.ResponseWriter, r *http.Request) {
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
		w.Write([]byte("\n"))
	})

	router.HandleFunc("/api/v1/info", h.ServeHTTP)

	return router
}

func (m CFAuthMiddlewareProvider) authorizeSourceIds(sourceIds []string, c Oauth2ClientContext) []string {
	var authorizedSourceIds []string

	for _, sourceId := range sourceIds {
		if m.logAuthorizer.IsAuthorized(sourceId, c.Token) {
			authorizedSourceIds = append(authorizedSourceIds, sourceId)
		}
	}

	return authorizedSourceIds
}

func (m CFAuthMiddlewareProvider) onlyAuthorized(authToken string, meta map[string]*rpc.MetaInfo, c Oauth2ClientContext) map[string]*rpc.MetaInfo {
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
