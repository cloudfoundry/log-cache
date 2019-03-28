package auth

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/dvsekhvalnov/jose2go"
)

type Metrics interface {
	NewGauge(name, unit string) func(value float64)
}

type HTTPClient interface {
	Do(r *http.Request) (*http.Response, error)
}

type UAAClient struct {
	httpClient HTTPClient
	uaa        *url.URL
	log        *log.Logger
	publicKey  *rsa.PublicKey
}

func NewUAAClient(
	uaaAddr string,
	httpClient HTTPClient,
	m Metrics,
	log *log.Logger,
) *UAAClient {
	u, err := url.Parse(uaaAddr)
	if err != nil {
		log.Fatalf("failed to parse UAA addr: %s", err)
	}

	u.Path = "token_key"

	return &UAAClient{
		uaa:        u,
		httpClient: httpClient,
		log:        log,
	}
}

func (c *UAAClient) GetTokenKey() error {
	req, err := http.NewRequest("GET", c.uaa.String(), nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create request to UAA: %s", err))
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)

	if err != nil {
		return fmt.Errorf("failed to get token key from UAA: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got an invalid status code talking to UAA %v", resp.Status)
	}
	defer resp.Body.Close()

	tokenKey, err := decodeTokenKey(resp.Body)
	if err != nil {
		return err
	}

	if tokenKey.Value == "" {
		return fmt.Errorf("received an empty token key from UAA")
	}

	block, _ := pem.Decode([]byte(tokenKey.Value))
	if block == nil {
		return fmt.Errorf("failed to parse PEM block containing the public key")
	}

	publicKeyInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("error parsing public key: %s", err)
	}

	publicKey, isRSAPublicKey := publicKeyInterface.(*rsa.PublicKey)
	if !isRSAPublicKey {
		return fmt.Errorf("did not get a valid RSA key from UAA: %s", err)
	}

	c.publicKey = publicKey

	return nil
}

func (c *UAAClient) StartPeriodicTokenKeyRefresh(duration time.Duration) {
	go func() {
		for {
			time.Sleep(duration)
			err := c.GetTokenKey()
			if err != nil {
				c.log.Printf("background token refresh failed: %s", err)
			}
		}
	}()
}

func (c *UAAClient) Read(token string) (Oauth2ClientContext, error) {
	if token == "" {
		return Oauth2ClientContext{}, errors.New("missing token")
	}

	if c.publicKey == nil {
		return Oauth2ClientContext{}, errors.New("missing shared key from UAA")
	}

	payload, _, err := jose.Decode(trimBearer(token), c.publicKey)
	if err != nil {
		return Oauth2ClientContext{}, fmt.Errorf("failed to decode token: %s", err.Error())
	}

	decodedToken, err := decodeToken(strings.NewReader(payload))
	if err != nil {
		return Oauth2ClientContext{}, fmt.Errorf("failed to unmarshal token: %s", err.Error())
	}

	if time.Now().After(decodedToken.ExpTime) {
		return Oauth2ClientContext{}, fmt.Errorf("token is expired, exp = %s", decodedToken.ExpTime)
	}

	var isAdmin bool
	for _, scope := range decodedToken.Scopes() {
		if scope == "doppler.firehose" || scope == "logs.admin" {
			isAdmin = true
		}
	}

	return Oauth2ClientContext{
		IsAdmin:   isAdmin,
		Token:     trimBearer(token),
		ExpiresAt: decodedToken.ExpTime,
	}, err
}

var bearerRE = regexp.MustCompile(`(?i)^bearer\s+`)

func trimBearer(authToken string) string {
	return bearerRE.ReplaceAllString(authToken, "")
}

type decodedTokenKey struct {
	Value string `json:"value"`
}

func decodeTokenKey(r io.Reader) (decodedTokenKey, error) {
	var dtk decodedTokenKey
	if err := json.NewDecoder(r).Decode(&dtk); err != nil {
		return decodedTokenKey{}, fmt.Errorf("unable to decode json token key from UAA: %s", err)
	}

	return dtk, nil
}

type decodedToken struct {
	Value   string    `json:"value"`
	Scope   string    `json:"scope"`
	Exp     float64   `json:"exp"`
	ExpTime time.Time `json:"-"`
}

func decodeToken(r io.Reader) (decodedToken, error) {
	var dt decodedToken
	if err := json.NewDecoder(r).Decode(&dt); err != nil {
		return decodedToken{}, fmt.Errorf("unable to decode json token from UAA: %s", err)
	}

	dt.ExpTime = time.Unix(int64(dt.Exp), 0)

	return dt, nil
}

func (dt *decodedToken) Scopes() []string {
	return strings.Split(dt.Scope, " ")
}
