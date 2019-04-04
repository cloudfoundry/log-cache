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
	"sync"
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
	httpClient             HTTPClient
	uaa                    *url.URL
	log                    *log.Logger
	publicKeys             map[string]*rsa.PublicKey
	minimumRefreshInterval time.Duration
	lastQueryTime          time.Time
	mu                     sync.Mutex
}

func NewUAAClient(
	uaaAddr string,
	httpClient HTTPClient,
	m Metrics,
	log *log.Logger,
	opts ...UAAOption,
) *UAAClient {
	u, err := url.Parse(uaaAddr)
	if err != nil {
		log.Fatalf("failed to parse UAA addr: %s", err)
	}

	u.Path = "token_keys"

	c := &UAAClient{
		uaa:                    u,
		httpClient:             httpClient,
		log:                    log,
		publicKeys:             make(map[string]*rsa.PublicKey),
		minimumRefreshInterval: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

type UAAOption func(c *UAAClient)

func WithMinimumRefreshInterval(interval time.Duration) UAAOption {
	return func(c *UAAClient) {
		c.minimumRefreshInterval = interval
	}
}

func (c *UAAClient) RefreshTokenKeys() error {
	nextAllowedRefreshTime := c.lastQueryTime.Add(c.minimumRefreshInterval)
	if c.lastQueryTime.After(time.Unix(0, 0)) && time.Now().Before(nextAllowedRefreshTime) {
		return nil
	}
	c.lastQueryTime = time.Now()

	req, err := http.NewRequest("GET", c.uaa.String(), nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create request to UAA: %s", err))
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)

	if err != nil {
		return fmt.Errorf("failed to get token keys from UAA: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got an invalid status code talking to UAA %v", resp.Status)
	}
	defer resp.Body.Close()

	tokenKeys, err := unmarshalTokenKeys(resp.Body)
	if err != nil {
		return err
	}

	newPublicKeyMap := make(map[string]*rsa.PublicKey)

	for _, tokenKey := range tokenKeys {
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

		newPublicKeyMap[tokenKey.KeyId] = publicKey
	}

	c.publicKeys = newPublicKeyMap

	return nil
}

func (c *UAAClient) Read(token string) (Oauth2ClientContext, error) {
	if token == "" {
		return Oauth2ClientContext{}, errors.New("missing token")
	}

	payload, _, err := jose.Decode(trimBearer(token), func(headers map[string]interface{}, payload string) interface{} {
		if headers["alg"] != "RS256" {
			return fmt.Errorf("unsupported algorithm: %s", headers["alg"])
		}

		keyId := headers["kid"].(string)

		publicKey, ok := c.publicKeys[keyId]
		if !ok {
			var err error

			publicKey, err = c.GetUnknownPublicKey(keyId)
			if err != nil {
				return err
			}
		}

		return publicKey
	})

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
	for _, scope := range decodedToken.Scope {
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

func (c *UAAClient) GetUnknownPublicKey(keyId string) (*rsa.PublicKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	publicKey, ok := c.publicKeys[keyId]
	if !ok {
		c.RefreshTokenKeys()

		publicKey, ok = c.publicKeys[keyId]
		if !ok {
			return nil, fmt.Errorf("using unknown token key")
		}
	}

	return publicKey, nil
}

var bearerRE = regexp.MustCompile(`(?i)^bearer\s+`)

func trimBearer(authToken string) string {
	return bearerRE.ReplaceAllString(authToken, "")
}

// TODO: move key processing to a method of tokenKey
type tokenKey struct {
	KeyId string `json:"kid"`
	Value string `json:"value"`
}

type tokenKeys struct {
	Keys []tokenKey `json:"keys"`
}

func unmarshalTokenKeys(r io.Reader) ([]tokenKey, error) {
	var dtks tokenKeys
	if err := json.NewDecoder(r).Decode(&dtks); err != nil {
		return []tokenKey{}, fmt.Errorf("unable to decode json token keys from UAA: %s", err)
	}

	return dtks.Keys, nil
}

func unmarshalTokenKey(r io.Reader) (tokenKey, error) {
	var dtk tokenKey
	if err := json.NewDecoder(r).Decode(&dtk); err != nil {
		return tokenKey{}, fmt.Errorf("unable to decode json token key from UAA: %s", err)
	}

	return dtk, nil
}

type decodedToken struct {
	Value   string    `json:"value"`
	Scope   []string  `json:"scope"`
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
