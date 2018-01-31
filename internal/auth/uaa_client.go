package auth

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type HttpClient interface {
	Do(r *http.Request) (*http.Response, error)
}

type UAAClient struct {
	httpClient   HttpClient
	uaa          *url.URL
	client       string
	clientSecret string
}

func NewUAAClient(uaaAddr, client, clientSecret string, httpClient HttpClient) *UAAClient {
	u, err := url.Parse(uaaAddr)
	if err != nil {
		panic(err)
	}
	u.Path = "check_token"

	return &UAAClient{
		uaa:          u,
		client:       client,
		clientSecret: clientSecret,
		httpClient:   httpClient,
	}
}

func (c *UAAClient) IsAdmin(token string) bool {
	if token == "" {
		return false
	}

	form := url.Values{
		"token": {trimBearer(token)},
	}

	req, err := http.NewRequest("POST", c.uaa.String(), strings.NewReader(form.Encode()))
	if err != nil {
		log.Printf("failed to create UAA request: %s", err)
		return false
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.client, c.clientSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("UAA request failed: %s", err)
		return false
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	return resp.StatusCode == http.StatusOK && c.hasDopplerScope(c.parseResponse(resp.Body))
}

func trimBearer(authToken string) string {
	return strings.TrimSpace(strings.TrimPrefix(authToken, "bearer"))
}

type uaaResponse struct {
	Scopes []string `json:"scope"`
}

func (c *UAAClient) hasDopplerScope(r uaaResponse) bool {
	for _, scope := range r.Scopes {
		if scope == "doppler.firehose" {
			return true
		}
	}

	return false
}

func (c *UAAClient) parseResponse(r io.Reader) uaaResponse {
	var resp uaaResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		log.Printf("unable to decode json reponse from UAA: %s", err)
	}
	return resp
}
