package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"code.cloudfoundry.org/log-cache/pkg/client"
)

func main() {

	logCacheAddr := os.Getenv("LOG_CACHE_ADDR")
	uaaAddr := os.Getenv("UAA_ADDR")
	uaaClient := os.Getenv("UAA_CLIENT")
	uaaClientSecret := os.Getenv("UAA_CLIENT_SECRET")
	username := os.Getenv("USERNAME")
	password := os.Getenv("PASSWORD")

	var missing []string

	if logCacheAddr == "" {
		missing = append(missing, "LOG_CACHE_ADDR")
	}
	if uaaAddr == "" {
		missing = append(missing, "UAA_ADDR")
	}
	if uaaClient == "" {
		missing = append(missing, "UAA_CLIENT")
	}
	if username == "" {
		missing = append(missing, "USERNAME")
	}
	if password == "" {
		missing = append(missing, "PASSWORD")
	}

	if len(missing) > 0 {
		panic(fmt.Sprintf("missing required environment variables: %s", strings.Join(missing, ", ")))
	}

	c := client.NewOauth2HTTPClient(uaaAddr, uaaClient, uaaClientSecret,
		client.WithOauth2HTTPUser(username, password),
	)

	req, err := http.NewRequest(http.MethodGet, logCacheAddr+"/api/v1/meta", nil)
	if err != nil {
		panic(err)
	}

	resp, err := c.Do(req)
	if err != nil {
		panic(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(b))
}

func verifyPresent(name, envVar string) {
	if envVar == "" {
		panic(fmt.Errorf(""))
	}
}
