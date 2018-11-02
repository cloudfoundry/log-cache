package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type CAPIClient struct {
	client               HTTPClient
	capi                 string
	externalCapi         string
	tokenCache           *sync.Map
	tokenPruningInterval time.Duration

	storeAppsLatency                 func(float64)
	storeListServiceInstancesLatency func(float64)
	storeAppsByNameLatency           func(float64)
}

func NewCAPIClient(
	capiAddr string,
	externalCapiAddr string,
	client HTTPClient,
	m Metrics,
	log *log.Logger,
	opts ...CAPIOption,
) *CAPIClient {
	_, err := url.Parse(capiAddr)
	if err != nil {
		log.Fatalf("failed to parse internal CAPI addr: %s", err)
	}

	_, err = url.Parse(externalCapiAddr)
	if err != nil {
		log.Fatalf("failed to parse external CAPI addr: %s", err)
	}

	c := &CAPIClient{
		client:               client,
		capi:                 capiAddr,
		externalCapi:         externalCapiAddr,
		tokenCache:           &sync.Map{},
		tokenPruningInterval: time.Minute,

		storeAppsLatency:                 m.NewGauge("LastCAPIV3AppsLatency"),
		storeListServiceInstancesLatency: m.NewGauge("LastCAPIV3ListServiceInstancesLatency"),
		storeAppsByNameLatency:           m.NewGauge("LastCAPIV3AppsByNameLatency"),
	}

	for _, opt := range opts {
		opt(c)
	}

	go c.pruneTokens()

	return c
}

type CAPIOption func(c *CAPIClient)

func WithTokenPruningInterval(interval time.Duration) CAPIOption {
	return func(c *CAPIClient) {
		c.tokenPruningInterval = interval
	}
}

func (c *CAPIClient) IsAuthorized(sourceID string, clientToken Oauth2Client) bool {
	if !time.Now().Before(clientToken.Expiration) {
		c.tokenCache.Delete(clientToken)
		return false
	}

	var sourceIDs []string
	sourceIDsValue, ok := c.tokenCache.Load(clientToken)

	if ok {
		sourceIDs = sourceIDsValue.([]string)
	} else {
		sourceIDs = c.AvailableSourceIDs(clientToken.Token)
		c.tokenCache.Store(clientToken, sourceIDs)
	}

	for _, s := range sourceIDs {
		if s == sourceID {
			return true
		}
	}

	return false
}

func (c *CAPIClient) AvailableSourceIDs(authToken string) []string {
	var sourceIDs []string
	req, err := http.NewRequest(http.MethodGet, c.externalCapi+"/v3/apps", nil)
	if err != nil {
		log.Printf("failed to build authorize log access request: %s", err)
		return nil
	}

	for {
		resources, nextPageURL, err := c.doResourceRequest(req, authToken, c.storeAppsLatency)
		if err != nil {
			log.Print(err)
			return nil
		}

		for _, resource := range resources {
			sourceIDs = append(sourceIDs, resource.Guid)
		}

		if nextPageURL == nil {
			break
		}
		req.URL = nextPageURL
	}

	req, err = http.NewRequest(http.MethodGet, c.externalCapi+"/v3/service_instances", nil)
	if err != nil {
		log.Printf("failed to build authorize service instance access request: %s", err)
		return nil
	}

	for {
		resources, nextPageURL, err := c.doResourceRequest(req, authToken, c.storeListServiceInstancesLatency)
		if err != nil || resources == nil {
			log.Print(err)
			return nil
		}

		for _, resource := range resources {
			sourceIDs = append(sourceIDs, resource.Guid)
		}

		if nextPageURL == nil {
			break
		}
		req.URL = nextPageURL
	}

	return sourceIDs
}

func (c *CAPIClient) GetRelatedSourceIds(appNames []string, authToken string) map[string][]string {
	if len(appNames) == 0 {
		return nil
	}

	req, err := http.NewRequest(http.MethodGet, c.externalCapi+"/v3/apps", nil)
	if err != nil {
		log.Printf("failed to build app list request: %s", err)
		return map[string][]string{}
	}

	query := req.URL.Query()
	query.Set("names", strings.Join(appNames, ","))
	query.Set("per_page", "5000")
	req.URL.RawQuery = query.Encode()

	guidSets := make(map[string][]string)

	for {
		resources, nextPageURL, err := c.doResourceRequest(req, authToken, c.storeAppsByNameLatency)
		if err != nil {
			log.Print(err)
			return nil
		}

		for _, resource := range resources {
			guidSets[resource.Name] = append(guidSets[resource.Name], resource.Guid)
		}

		if nextPageURL == nil {
			break
		}

		req.URL = nextPageURL
	}

	return guidSets
}

type resource struct {
	Guid string `json:"guid"`
	Name string `json:"name"`
}

func (c *CAPIClient) doResourceRequest(req *http.Request, authToken string, metric func(float64)) ([]resource, *url.URL, error) {
	resp, err := c.doRequest(req, authToken, metric)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("failed CAPI request (%s) may result in incomplete service listing: %s", req.URL.Path, err)
	}

	defer func(r *http.Response) {
		cleanup(r)
	}(resp)

	var apps struct {
		Pagination struct {
			Next struct {
				Href string `json:"href"`
			} `json:"next"`
		} `json:"pagination"`
		Resources []resource `json:"resources"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apps)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode resource list request (%s): %s", req.URL.Path, err)
	}

	var nextPageURL *url.URL
	if apps.Pagination.Next.Href != "" {
		nextPageURL, err = url.Parse(apps.Pagination.Next.Href)
		if err != nil {
			return apps.Resources, nextPageURL, fmt.Errorf("failed to parse URL %s: %s", apps.Pagination.Next.Href, err)
		}
	}

	return apps.Resources, nextPageURL, nil
}

func (c *CAPIClient) TokenCacheSize() int {
	var i int
	c.tokenCache.Range(func(_, _ interface{}) bool {
		i++
		return true
	})
	return i
}

func (c *CAPIClient) pruneTokens() {
	for range time.Tick(c.tokenPruningInterval) {
		c.tokenCache.Range(func(k, _ interface{}) bool {
			clientToken := k.(Oauth2Client)

			if !time.Now().Before(clientToken.Expiration) {
				c.tokenCache.Delete(k)
			}

			return true
		})
	}
}

func (c *CAPIClient) doRequest(req *http.Request, authToken string, reporter func(float64)) (*http.Response, error) {
	req.Header.Set("Authorization", authToken)
	start := time.Now()
	resp, err := c.client.Do(req)
	reporter(float64(time.Since(start)))

	if err != nil {
		log.Printf("CAPI request (%s) failed: %s", req.URL.Path, err)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("CAPI request (%s) returned: %d", req.URL.Path, resp.StatusCode)
		cleanup(resp)
		return resp, nil
	}

	return resp, nil
}

func cleanup(resp *http.Response) {
	if resp == nil {
		return
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}
