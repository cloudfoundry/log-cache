package auth_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/log-cache/internal/auth"

	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"io/ioutil"
	"net/http"

	jose "github.com/dvsekhvalnov/jose2go"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("UAAClient", func() {
	Context("Read()", func() {
		var tc *UAATestContext

		BeforeEach(func() {
			tc = uaaSetup()
			tc.GenerateTokenKeyResponse()

			err := tc.uaaClient.GetTokenKey()
			Expect(err).ToNot(HaveOccurred())
		})

		It("only accepts tokens that are signed with RS256", func() {
			t := time.Now().Add(time.Hour).Truncate(time.Second)
			payload := fmt.Sprintf(`{"scope":["doppler.firehose"], "exp":%d}`, t.Unix())
			token := tc.CreateUnsignedToken(payload)

			_, err := tc.uaaClient.Read(withBearer(token))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("failed to decode token: unsupported algorithm: none"))
		})

		It("returns IsAdmin == true when scopes include doppler.firehose", func() {
			t := time.Now().Add(time.Hour).Truncate(time.Second)
			payload := fmt.Sprintf(`{"scope":["doppler.firehose"], "exp":%d}`, t.Unix())
			token := tc.CreateSignedToken(payload)

			c, err := tc.uaaClient.Read(withBearer(token))
			Expect(err).ToNot(HaveOccurred())
			Expect(c.IsAdmin).To(BeTrue())
			Expect(c.Token).To(Equal(token))
			Expect(c.ExpiresAt).To(Equal(t))
		})

		It("returns IsAdmin == true when scopes include logs.admin", func() {
			t := time.Now().Add(time.Hour).Truncate(time.Second)
			payload := fmt.Sprintf(`{"scope":["logs.admin"], "exp":%d}`, t.Unix())
			token := tc.CreateSignedToken(payload)

			c, err := tc.uaaClient.Read(withBearer(token))
			Expect(err).ToNot(HaveOccurred())
			Expect(c.IsAdmin).To(BeTrue())
			Expect(c.Token).To(Equal(token))
			Expect(c.ExpiresAt).To(Equal(t))
		})

		It("returns IsAdmin == false when scopes include neither logs.admin nor doppler.firehose", func() {
			t := time.Now().Add(time.Hour).Truncate(time.Second)
			payload := fmt.Sprintf(`{"scope":["foo.bar"], "exp":%d}`, t.Unix())
			token := tc.CreateSignedToken(payload)

			c, err := tc.uaaClient.Read(withBearer(token))
			Expect(err).ToNot(HaveOccurred())
			Expect(c.IsAdmin).To(BeFalse())
			Expect(c.Token).To(Equal(token))
			Expect(c.ExpiresAt).To(Equal(t))
		})

		It("does offline token validation", func() {
			numRequests := len(tc.httpClient.requests)

			payload := `{"scope":["logs.admin"]}`
			token := tc.CreateSignedToken(payload)

			tc.uaaClient.Read(withBearer(token))
			tc.uaaClient.Read(withBearer(token))

			Expect(len(tc.httpClient.requests)).To(Equal(numRequests))
		})

		It("does not allow use of an expired token", func() {
			tc := uaaSetup()
			tc.GenerateTokenKeyResponse()

			tc.uaaClient.GetTokenKey()

			payload := fmt.Sprintf(`{
	            "scope": ["logs.admin"],
		        "exp": %d
		    }`, time.Now().Add(-time.Minute).Unix())
			token := tc.CreateSignedToken(payload)

			_, err := tc.uaaClient.Read(withBearer(token))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("token is expired"))
		})

		It("returns an error when token is blank", func() {
			_, err := tc.uaaClient.Read("")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("missing token"))
		})

		It("returns an error when the shared key is nil", func() {
			// create a new testContext that hasn't been initialized with a key
			tc = uaaSetup()

			_, err := tc.uaaClient.Read("any-old-token")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("missing shared key from UAA"))
		})

		It("returns an error when the provided token cannot be decoded", func() {
			_, err := tc.uaaClient.Read("any-old-token")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to decode token"))
		})

		DescribeTable("handling the Bearer prefix in the Authorization header",
			func(prefix string) {
				t := time.Now().Add(time.Hour).Truncate(time.Second)
				payload := fmt.Sprintf(`{"scope":["foo.bar"], "exp":%d}`, t.Unix())
				token := tc.CreateSignedToken(payload)

				c, err := tc.uaaClient.Read(withBearer(token))
				Expect(err).ToNot(HaveOccurred())
				Expect(c.IsAdmin).To(BeFalse())
				Expect(c.Token).To(Equal(token))
				Expect(c.ExpiresAt).To(Equal(t))
			},
			Entry("Standard 'Bearer' prefix", "Bearer "),
			Entry("Non-Standard 'bearer' prefix", "bearer "),
			Entry("No prefix", ""),
		)

	})

	Context("GetTokenKey()", func() {
		It("calls UAA correctly", func() {
			tc := uaaSetup()
			tc.GenerateTokenKeyResponse()
			tc.uaaClient.GetTokenKey()

			r := tc.httpClient.requests[0]

			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.Header.Get("Content-Type")).To(Equal("application/json"))
			Expect(r.URL.Path).To(Equal("/token_key"))

			// confirm that we're not using any authentication
			_, _, ok := r.BasicAuth()
			Expect(ok).To(BeFalse())

			Expect(r.Body).To(BeNil())
		})

		It("returns an error when UAA cannot be reached", func() {
			tc := uaaSetup()
			tc.httpClient.resps = []response{{
				err: errors.New("error!"),
			}}

			err := tc.uaaClient.GetTokenKey()
			Expect(err).To(HaveOccurred())
		})

		It("returns an error when UAA returns a non-200 response", func() {
			tc := uaaSetup()
			tc.httpClient.resps = []response{{
				body:   []byte{},
				status: http.StatusUnauthorized,
			}}

			err := tc.uaaClient.GetTokenKey()
			Expect(err).To(HaveOccurred())
		})

		It("returns an error when the response from the UAA is malformed", func() {
			tc := uaaSetup()
			tc.httpClient.resps = []response{{
				body:   []byte("garbage"),
				status: http.StatusOK,
			}}

			err := tc.uaaClient.GetTokenKey()
			Expect(err).To(HaveOccurred())
		})

		It("returns an error when the response from the UAA has an empty key", func() {
			tc := uaaSetup()
			tc.GenerateEmptyTokenKeyResponse()

			err := tc.uaaClient.GetTokenKey()
			Expect(err).To(HaveOccurred())
		})

		It("returns an error when the response from the UAA has an unparsable PEM format", func() {
			tc := uaaSetup()
			tc.GenerateTokenKeyResponseWithInvalidPEM()

			err := tc.uaaClient.GetTokenKey()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("failed to parse PEM block containing the public key"))
		})

		It("returns an error when the response from the UAA has an invalid key format", func() {
			tc := uaaSetup()
			tc.GenerateTokenKeyResponseWithInvalidKey()

			err := tc.uaaClient.GetTokenKey()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("error parsing public key"))
		})
	})
	Context("StartPeriodicTokenKeyRefresh()", func() {
		It("continues fetching new tokens in the background", func() {
			tc := uaaSetup()
			tc.GenerateTokenKeyResponse()

			numRequests := len(tc.httpClient.requests)

			tc.uaaClient.StartPeriodicTokenKeyRefresh(100 * time.Millisecond)

			// wait 2.5 cycles so that we can count the number of new
			// background http requests to UAA
			time.Sleep(250 * time.Millisecond)
			Expect(len(tc.httpClient.requests)).To(Equal(numRequests + 2))
		})
	})

})

func uaaSetup() *UAATestContext {
	httpClient := newSpyHTTPClient()
	metrics := newSpyMetrics()
	privateKey, _ := rsa.GenerateKey(rand.Reader, 2048)

	uaaClient := auth.NewUAAClient(
		"https://uaa.com",
		httpClient,
		metrics,
		log.New(ioutil.Discard, "", 0),
	)

	return &UAATestContext{
		uaaClient:  uaaClient,
		httpClient: httpClient,
		metrics:    metrics,
		privateKey: privateKey,
	}
}

type UAATestContext struct {
	uaaClient  *auth.UAAClient
	httpClient *spyHTTPClient
	metrics    *spyMetrics
	privateKey *rsa.PrivateKey
}

func (tc *UAATestContext) GenerateTokenKeyResponse() {
	data, err := json.Marshal(map[string]string{
		"kty":   "RSA",
		"e":     publicKeyExponentToString(tc.privateKey),
		"use":   "sig",
		"kid":   "testKey",
		"alg":   "RS256",
		"value": publicKeyPEMToString(tc.privateKey),
		"n":     publicKeyModulusToString(tc.privateKey),
	})

	Expect(err).ToNot(HaveOccurred())

	tc.httpClient.resps = []response{{
		body:   data,
		status: http.StatusOK,
	}}
}

func (tc *UAATestContext) GenerateEmptyTokenKeyResponse() {
	data, err := json.Marshal(map[string]string{
		"kty":   "RSA",
		"e":     publicKeyExponentToString(tc.privateKey),
		"use":   "sig",
		"kid":   "testKey",
		"alg":   "RS256",
		"value": "",
		"n":     publicKeyModulusToString(tc.privateKey),
	})

	Expect(err).ToNot(HaveOccurred())

	tc.httpClient.resps = []response{{
		body:   data,
		status: http.StatusOK,
	}}
}

func (tc *UAATestContext) GenerateTokenKeyResponseWithInvalidPEM() {
	data, err := json.Marshal(map[string]string{
		"kty":   "RSA",
		"e":     publicKeyExponentToString(tc.privateKey),
		"use":   "sig",
		"kid":   "testKey",
		"alg":   "RS256",
		"value": "-- BEGIN SOMETHING --\nNOTVALIDPEM\n-- END SOMETHING --\n",
		"n":     publicKeyModulusToString(tc.privateKey),
	})

	Expect(err).ToNot(HaveOccurred())

	tc.httpClient.resps = []response{{
		body:   data,
		status: http.StatusOK,
	}}
}

func (tc *UAATestContext) GenerateTokenKeyResponseWithInvalidKey() {
	pem := publicKeyPEMToString(tc.privateKey)
	data, err := json.Marshal(map[string]string{
		"kty":   "RSA",
		"e":     publicKeyExponentToString(tc.privateKey),
		"use":   "sig",
		"kid":   "testKey",
		"alg":   "RS256",
		"value": strings.Replace(pem, "MIIB", "XXXX", 1),
		"n":     publicKeyModulusToString(tc.privateKey),
	})

	Expect(err).ToNot(HaveOccurred())

	tc.httpClient.resps = []response{{
		body:   data,
		status: http.StatusOK,
	}}
}

func (tc *UAATestContext) CreateSignedToken(payload string) string {
	token, err := jose.Sign(payload, jose.RS256, tc.privateKey)
	Expect(err).ToNot(HaveOccurred())

	return token
}

func (tc *UAATestContext) CreateUnsignedToken(payload string) string {
	token, err := jose.Sign(payload, jose.NONE, nil)
	Expect(err).ToNot(HaveOccurred())

	return token
}

type spyHTTPClient struct {
	mu       sync.Mutex
	requests []*http.Request
	resps    []response
	tokens   []string
}

type response struct {
	status int
	err    error
	body   []byte
}

func newSpyHTTPClient() *spyHTTPClient {
	return &spyHTTPClient{}
}

func (s *spyHTTPClient) Do(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests = append(s.requests, r)
	s.tokens = append(s.tokens, r.Header.Get("Authorization"))

	if len(s.resps) == 0 {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(nil)),
		}, nil
	}

	result := s.resps[0]
	s.resps = s.resps[1:]

	resp := http.Response{
		StatusCode: result.status,
		Body:       ioutil.NopCloser(bytes.NewReader(result.body)),
	}

	if result.err != nil {
		return nil, result.err
	}

	return &resp, nil
}

type spyMetrics struct {
	mu sync.Mutex
	m  map[string]float64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		m: make(map[string]float64),
	}
}

func (s *spyMetrics) NewGauge(name, unit string) func(float64) {
	return func(v float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.m[name] = v
	}
}

func publicKeyExponentToString(privateKey *rsa.PrivateKey) string {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(privateKey.PublicKey.E))
	return base64.StdEncoding.EncodeToString(b[0:3])
}

func publicKeyPEMToString(privateKey *rsa.PrivateKey) string {
	encodedKey, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	Expect(err).ToNot(HaveOccurred())

	var pemKey = &pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: encodedKey,
	}

	return string(pem.EncodeToMemory(pemKey))
}

func publicKeyModulusToString(privateKey *rsa.PrivateKey) string {
	return base64.StdEncoding.EncodeToString(privateKey.PublicKey.N.Bytes())
}

func withBearer(token string) string {
	return fmt.Sprintf("Bearer %s", token)
}
