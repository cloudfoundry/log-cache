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
	client                           HTTPClient
	externalCapi                     string
	tokenCache                       *sync.Map
	cacheExpirationInterval          time.Duration
	log                              *log.Logger
	storeAppsLatency                 func(float64)
	storeListServiceInstancesLatency func(float64)
	storeAppsByNameLatency           func(float64)
}

func NewCAPIClient(
	externalCapiAddr string,
	client HTTPClient,
	m Metrics,
	log *log.Logger,
	opts ...CAPIOption,
) *CAPIClient {
	_, err := url.Parse(externalCapiAddr)
	if err != nil {
		log.Fatalf("failed to parse external CAPI addr: %s", err)
	}

	c := &CAPIClient{
		client:                  client,
		externalCapi:            externalCapiAddr,
		tokenCache:              &sync.Map{},
		cacheExpirationInterval: time.Minute,
		log:                     log,

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

func WithCacheExpirationInterval(interval time.Duration) CAPIOption {
	return func(c *CAPIClient) {
		c.cacheExpirationInterval = interval
	}
}

func (c *CAPIClient) IsAuthorized(sourceId string, clientToken string) bool {
	_, ok := c.tokenCache.Load(clientToken + sourceId)
	if ok {
		return true
	}

	if c.HasApp(sourceId, clientToken) || c.HasService(sourceId, clientToken) {
		c.tokenCache.Store(clientToken+sourceId, time.Now())
		return true
	}
	return false
}

func (c *CAPIClient) HasApp(sourceID, authToken string) bool {
	req, err := http.NewRequest(http.MethodGet, c.externalCapi+"/v3/apps/"+sourceID, nil)
	if err != nil {
		c.log.Printf("failed to build authorize log access request: %s", err)
		return false
	}

	resp, err := c.doRequest(req, authToken, c.storeAppsLatency)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}

	return true
}

func (c *CAPIClient) HasService(sourceID, authToken string) bool {
	req, err := http.NewRequest(http.MethodGet, c.externalCapi+"/v3/service_instances/"+sourceID, nil)
	if err != nil {
		c.log.Printf("failed to build authorize log access request: %s", err)
		return false
	}

	resp, err := c.doRequest(req, authToken, c.storeListServiceInstancesLatency)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}

	return true
}

func (c *CAPIClient) GetRelatedSourceIds(appNames []string, authToken string) map[string][]string {
	if len(appNames) == 0 {
		return map[string][]string{}
	}

	req, err := http.NewRequest(http.MethodGet, c.externalCapi+"/v3/apps", nil)
	if err != nil {
		c.log.Printf("failed to build app list request: %s", err)
		return map[string][]string{}
	}

	query := req.URL.Query()
	query.Set("names", strings.Join(appNames, ","))
	query.Set("per_page", "5000")
	req.URL.RawQuery = query.Encode()

	guidSets := make(map[string][]string)

	resources, err := c.doPaginatedResourceRequest(req, authToken, c.storeAppsByNameLatency)
	if err != nil {
		c.log.Print(err)
		return map[string][]string{}
	}
	for _, resource := range resources {
		guidSets[resource.Name] = append(guidSets[resource.Name], resource.Guid)
	}

	return guidSets
}

func (c *CAPIClient) AvailableSourceIDs(authToken string) []string {
	appIDs, err := c.sourceIDsForResourceType("apps", authToken, c.storeAppsLatency)
	if err != nil {
		return nil
	}

	serviceIDs, err := c.sourceIDsForResourceType("service_instances", authToken, c.storeListServiceInstancesLatency)
	if err != nil {
		return nil
	}

	return append(appIDs, serviceIDs...)
}

func (c *CAPIClient) sourceIDsForResourceType(resourceType, authToken string, metrics func(float64)) ([]string, error) {
	var sourceIDs []string
	req, err := http.NewRequest(http.MethodGet, c.externalCapi+"/v3/"+resourceType, nil)
	if err != nil {
		c.log.Printf("failed to build authorize service instance access request: %s", err)
		return nil, err
	}

	query := req.URL.Query()
	query.Set("per_page", "5000")
	req.URL.RawQuery = query.Encode()

	resources, err := c.doPaginatedResourceRequest(req, authToken, metrics)
	if err != nil {
		c.log.Print(err)
		return nil, err
	}
	for _, resource := range resources {
		sourceIDs = append(sourceIDs, resource.Guid)
	}

	return sourceIDs, nil
}

type resource struct {
	Guid string `json:"guid"`
	Name string `json:"name"`
}

func (c *CAPIClient) doPaginatedResourceRequest(req *http.Request, authToken string, metric func(float64)) ([]resource, error) {
	var resources []resource

	for {
		page, nextPageURL, err := c.doResourceRequest(req, authToken, metric)
		if err != nil {
			return nil, err
		}

		resources = append(resources, page...)

		if nextPageURL == nil {
			break
		}
		req.URL = nextPageURL
	}

	return resources, nil
}

func (c *CAPIClient) doResourceRequest(req *http.Request, authToken string, metric func(float64)) ([]resource, *url.URL, error) {
	resp, err := c.doRequest(req, authToken, metric)
	if err != nil {
		return nil, nil, fmt.Errorf("failed CAPI request (%s) with error: %s", req.URL.Path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf(
			"failed CAPI request (%s) with status: %d (%s)",
			req.URL.Path,
			resp.StatusCode,
			http.StatusText(resp.StatusCode),
		)
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
		nextPageURL.Scheme, nextPageURL.Host = req.URL.Scheme, req.URL.Host
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
	for range time.Tick(c.cacheExpirationInterval) {

		c.tokenCache.Range(func(k, v interface{}) bool {
			requestTime := v.(time.Time)

			if time.Since(requestTime) >= c.cacheExpirationInterval {
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
		c.log.Printf("CAPI request (%s) failed: %s", req.URL.Path, err)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		c.log.Printf("CAPI request (%s) returned: %d", req.URL.Path, resp.StatusCode)
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
