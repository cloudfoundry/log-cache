package auth

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"github.com/gorilla/mux"
)

var (
	adminAccessScope = "doppler.firehose"
	sourceIDMatcher  = regexp.MustCompile("^/v1/read/(.*)/?$")
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type CFAuthMiddlewareProvider struct {
	clientID     string
	clientSecret string
	uaaHost      string
	uaaClient    HTTPClient
	apiHost      string
	apiClient    HTTPClient
}

func NewCFAuthMiddlewareProvider(
	clientID string,
	clientSecret string,
	uaaHost string,
	uaaClient HTTPClient,
	apiHost string,
	apiClient HTTPClient,
) CFAuthMiddlewareProvider {
	return CFAuthMiddlewareProvider{
		clientID:     clientID,
		clientSecret: clientSecret,
		uaaHost:      uaaHost,
		uaaClient:    uaaClient,
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

		err := m.checkUserIsAdmin(authToken)
		if err != nil {
			er := m.checkUserHasLogAccess(authToken, sourceID)
			if er != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		h.ServeHTTP(w, r)
	})

	return router

}

func (m CFAuthMiddlewareProvider) checkUserIsAdmin(authToken string) error {
	form := url.Values{
		"token": {trimBearer(authToken)},
	}

	req, err := http.NewRequest(
		http.MethodPost,
		m.uaaHost+"/check_token",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		log.Printf("failed to build authorize admin request: %s", err)
		return err
	}

	req.SetBasicAuth(m.clientID, m.clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.uaaClient.Do(req)
	if err != nil {
		log.Printf("failed to authorize admin: %s", err)
		return err
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK || !adminToken(resp.Body) {
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

func trimBearer(authToken string) string {
	return strings.TrimSpace(strings.TrimPrefix(authToken, "bearer"))
}

func adminToken(body io.ReadCloser) bool {
	scopes := scopesFromRespBody(body)
	for _, scope := range scopes {
		if scope == adminAccessScope {
			return true
		}
	}

	return false
}

func scopesFromRespBody(body io.ReadCloser) []string {
	scopes := struct {
		Scopes []string `json:"scope"`
	}{}

	err := json.NewDecoder(body).Decode(&scopes)
	if err != nil {
		log.Printf("error deserializing response from UAA: %s", err)
		return nil
	}

	return scopes.Scopes
}
