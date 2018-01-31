package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
)

type CAPIClient struct {
	client HTTPClient
	capi   url.URL
}

func NewCAPIClient(capiAddr string, client HTTPClient) *CAPIClient {
	u, err := url.Parse(capiAddr)
	if err != nil {
		panic(err)
	}

	return &CAPIClient{
		client: client,
		capi:   *u,
	}
}

func (c *CAPIClient) IsAuthorized(sourceID, token string) bool {
	c.capi.Path = fmt.Sprintf("/internal/v4/log_access/%s", sourceID)
	req, err := http.NewRequest(http.MethodGet, c.capi.String(), nil)
	if err != nil {
		log.Printf("failed to build authorize log access request: %s", err)
		return false
	}

	req.Header.Set("Authorization", token)
	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("CAPI request failed: %s", err)
		return false
	}

	return resp.StatusCode == http.StatusOK
}

func (c *CAPIClient) AvailableSourceIDs(token string) []string {
	c.capi.Path = "/v3/apps"
	req, err := http.NewRequest(http.MethodGet, c.capi.String(), nil)
	if err != nil {
		log.Printf("failed to build authorize log access request: %s", err)
		return nil
	}

	req.Header.Set("Authorization", token)
	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("CAPI request failed: %s", err)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("CAPI request failed: %d", resp.StatusCode)
		return nil
	}

	var sources struct {
		Resources []struct {
			Guid string `json:"guid"`
		} `json:"resources"`
	}

	json.NewDecoder(resp.Body).Decode(&sources)

	var sourceIDs []string
	for _, v := range sources.Resources {
		sourceIDs = append(sourceIDs, v.Guid)
	}
	return sourceIDs
}
