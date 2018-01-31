package auth

import (
	"errors"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type CFAuthMiddlewareProvider struct {
	adminChecker AdminChecker
	apiHost      string
	apiClient    HTTPClient
}

type AdminChecker interface {
	IsAdmin(token string) bool
}

func NewCFAuthMiddlewareProvider(
	adminChecker AdminChecker,
	apiHost string,
	apiClient HTTPClient,
) CFAuthMiddlewareProvider {
	return CFAuthMiddlewareProvider{
		adminChecker: adminChecker,
		apiHost:      apiHost,
		apiClient:    apiClient,
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
			err := m.checkUserHasLogAccess(authToken, sourceID)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		h.ServeHTTP(w, r)
	})

	return router

}

func (m CFAuthMiddlewareProvider) checkUserIsAdmin(authToken string) error {
	if !m.adminChecker.IsAdmin(authToken) {
		return errors.New("unauthorized")
	}

	return nil
}

func (m CFAuthMiddlewareProvider) checkUserHasLogAccess(authToken string, sourceID string) error {
	req, err := http.NewRequest(
		http.MethodGet,
		m.apiHost+"/internal/v4/log_access/"+sourceID,
		nil,
	)
	if err != nil {
		log.Printf("failed to build authorize log access request: %s", err)
		return err
	}

	req.Header.Set("Authorization", authToken)

	resp, err := m.apiClient.Do(req)
	if err != nil {
		log.Printf("failed to contact capi: %s", err)
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("unauthorized")
	}

	return nil
}

func (m CFAuthMiddlewareProvider) getSourcesForUser(authToken string) []string {
	req, _ := http.NewRequest(
		http.MethodGet,
		m.apiHost+"/v3/apps",
		nil,
	)

	req.Header.Set("Authorization", authToken)
	m.apiClient.Do(req)

	return nil
}
