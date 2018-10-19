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
	"time"
)

type CAPIClient struct {
	client                           HTTPClient
	capi                             string
	externalCapi                     string
	storeAppsLatency                 func(float64)
	storeListServiceInstancesLatency func(float64)
	storeLogAccessLatency            func(float64)
	storeServiceInstancesLatency     func(float64)
	storeAppsByNameLatency           func(float64)
}

func NewCAPIClient(
	capiAddr string,
	externalCapiAddr string,
	client HTTPClient,
	m Metrics,
	log *log.Logger,
) *CAPIClient {
	_, err := url.Parse(capiAddr)
	if err != nil {
		log.Fatalf("failed to parse internal CAPI addr: %s", err)
	}

	_, err = url.Parse(externalCapiAddr)
	if err != nil {
		log.Fatalf("failed to parse external CAPI addr: %s", err)
	}

	return &CAPIClient{
		client:                           client,
		capi:                             capiAddr,
		externalCapi:                     externalCapiAddr,
		storeAppsLatency:                 m.NewGauge("LastCAPIV3AppsLatency"),
		storeListServiceInstancesLatency: m.NewGauge("LastCAPIV2ListServiceInstancesLatency"),
		storeLogAccessLatency:            m.NewGauge("LastCAPIV4LogAccessLatency"),
		storeServiceInstancesLatency:     m.NewGauge("LastCAPIV2ServiceInstancesLatency"),
		storeAppsByNameLatency:           m.NewGauge("LastCAPIV3AppsByNameLatency"),
	}
}

func (c *CAPIClient) IsAuthorized(sourceID, authToken string) bool {
	uri := fmt.Sprintf("%s/internal/v4/log_access/%s", c.capi, sourceID)
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		log.Printf("failed to build authorize log access request: %s", err)
		return false
	}

	resp, err := c.doRequest(req, authToken, c.storeLogAccessLatency)
	if err != nil {
		return false
	}

	defer func(r *http.Response) {
		cleanup(r)
	}(resp)

	if resp.StatusCode == http.StatusOK {
		return true
	}

	uri = fmt.Sprintf("%s/v2/service_instances/%s", c.externalCapi, sourceID)
	req, err = http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		log.Printf("failed to build authorize service instance access request: %s", err)
		return false
	}

	resp, err = c.doRequest(req, authToken, c.storeServiceInstancesLatency)
	if err != nil {
		return false
	}

	defer func(r *http.Response) {
		cleanup(r)
	}(resp)

	return resp.StatusCode == http.StatusOK
}

func (c *CAPIClient) AvailableSourceIDs(authToken string) []string {
	var sourceIDs []string
	req, err := http.NewRequest(http.MethodGet, c.externalCapi+"/v3/apps", nil)
	if err != nil {
		log.Printf("failed to build authorize log access request: %s", err)
		return nil
	}

	resp, err := c.doRequest(req, authToken, c.storeAppsLatency)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}

	defer func(r *http.Response) {
		cleanup(r)
	}(resp)

	var appSources struct {
		Resources []struct {
			Guid string `json:"guid"`
		} `json:"resources"`
	}

	json.NewDecoder(resp.Body).Decode(&appSources)

	for _, v := range appSources.Resources {
		sourceIDs = append(sourceIDs, v.Guid)
	}

	req, err = http.NewRequest(http.MethodGet, c.externalCapi+"/v2/service_instances", nil)
	if err != nil {
		log.Printf("failed to build authorize service instance access request: %s", err)
		return nil
	}

	resp, err = c.doRequest(req, authToken, c.storeListServiceInstancesLatency)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}

	defer func(r *http.Response) {
		cleanup(r)
	}(resp)

	var serviceSources struct {
		Resources []struct {
			Metadata struct {
				Guid string `json:"guid"`
			} `json:"metadata"`
		} `json:"resources"`
	}

	json.NewDecoder(resp.Body).Decode(&serviceSources)

	for _, v := range serviceSources.Resources {
		sourceIDs = append(sourceIDs, v.Metadata.Guid)
	}

	return sourceIDs
}

func (c *CAPIClient) GetRelatedSourceIds(appNames []string, authToken string) map[string][]string {
	if len(appNames) == 0 {
		return map[string][]string{}
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

	resp, err := c.doRequest(req, authToken, c.storeAppsByNameLatency)
	if err != nil || resp.StatusCode != http.StatusOK {
		return map[string][]string{}
	}

	var apps struct {
		Resources []struct {
			Guid string `json:"guid"`
			Name string `json:"name"`
		} `json:"resources"`
	}

	json.NewDecoder(resp.Body).Decode(&apps)
	guidSets := make(map[string][]string)

	for _, app := range apps.Resources {
		guidSets[app.Name] = append(guidSets[app.Name], app.Guid)
	}

	return guidSets
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
