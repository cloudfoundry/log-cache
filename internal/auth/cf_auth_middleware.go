package auth

import (
	"net/http"

	"github.com/gorilla/mux"
)

type CFAuthMiddlewareProvider struct {
	adminChecker AdminChecker
	logAuthorizer LogAuthorizer
}

type AdminChecker interface {
	IsAdmin(token string) bool
}

type LogAuthorizer interface{
	IsAuthorized(sourceID, token string) bool
}

func NewCFAuthMiddlewareProvider(
	adminChecker AdminChecker,
	logAuthorizer LogAuthorizer,
) CFAuthMiddlewareProvider {
	return CFAuthMiddlewareProvider{
		adminChecker:  adminChecker,
		logAuthorizer: logAuthorizer,
	}
}

func (m CFAuthMiddlewareProvider) Middleware(h http.Handler) http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/v1/read/{sourceID}", func(w http.ResponseWriter, r *http.Request) {
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

		if !m.adminChecker.IsAdmin(authToken) {
			if !m.logAuthorizer.IsAuthorized(sourceID, authToken) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		h.ServeHTTP(w, r)
	})

	return router
}
