package cfauthproxy_test

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"

	"code.cloudfoundry.org/log-cache/internal/auth"
	. "code.cloudfoundry.org/log-cache/internal/cfauthproxy"
	"code.cloudfoundry.org/log-cache/internal/testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CFAuthProxy", func() {
	It("only proxies requests to a secure log cache gateway", func() {
		gateway := startSecureGateway("Hello World!")
		defer gateway.Close()

		proxy := newCFAuthProxy(gateway.URL)
		proxy.Start()

		resp, err := makeTLSReq("https", proxy.Addr())
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		body, _ := ioutil.ReadAll(resp.Body)
		Expect(body).To(Equal([]byte("Hello World!")))
	})

	It("returns an error when proxying requests to an insecure log cache gateway", func() {
		// suppress tls error in test
		log.SetOutput(ioutil.Discard)

		testServer := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("Insecure Gateway not allowed, lol"))
			}))
		defer testServer.Close()

		testGatewayURL, _ := url.Parse(testServer.URL)
		testGatewayURL.Scheme = "https"

		proxy := newCFAuthProxy(testGatewayURL.String())
		proxy.Start()

		resp, err := makeTLSReq("https", proxy.Addr())
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.StatusCode).To(Equal(http.StatusBadGateway))
	})

	It("delegates to the auth middleware", func() {
		var middlewareCalled bool
		middleware := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			middlewareCalled = true
			w.WriteHeader(http.StatusNotFound)
		})

		proxy := newCFAuthProxy(
			"https://127.0.0.1",
			WithAuthMiddleware(func(http.Handler) http.Handler {
				return middleware
			}),
		)
		proxy.Start()

		resp, err := makeTLSReq("https", proxy.Addr())
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
		Expect(middlewareCalled).To(BeTrue())
	})

	It("delegates to the access middleware", func() {
		var middlewareCalled bool
		middleware := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			middlewareCalled = true
			w.WriteHeader(http.StatusNotFound)
		})

		proxy := newCFAuthProxy(
			"https://127.0.0.1",
			WithAccessMiddleware(func(http.Handler) *auth.AccessHandler {
				return auth.NewAccessHandler(middleware, auth.NewNullAccessLogger(), "0.0.0.0", "1234")
			}),
		)
		proxy.Start()

		resp, err := makeTLSReq("https", proxy.Addr())
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
		Expect(middlewareCalled).To(BeTrue())
	})

	It("does not accept unencrypted connections", func() {
		testServer := httptest.NewTLSServer(
			http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {}),
		)
		proxy := newCFAuthProxy(testServer.URL)
		proxy.Start()

		resp, err := makeTLSReq("http", proxy.Addr())
		Expect(err).NotTo(HaveOccurred())

		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
	})
})

var localhostCerts = testing.GenerateCerts("localhost-ca")

func newCFAuthProxy(gatewayURL string, opts ...CFAuthProxyOption) *CFAuthProxy {
	parsedURL, err := url.Parse(gatewayURL)
	if err != nil {
		panic("couldn't parse gateway URL")
	}

	return NewCFAuthProxy(
		parsedURL.String(),
		"127.0.0.1:0",
		localhostCerts.Cert("localhost"),
		localhostCerts.Key("localhost"),
		localhostCerts.Pool(),
		opts...,
	)
}

func startSecureGateway(responseBody string) *httptest.Server {
	testGateway := httptest.NewUnstartedServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte(responseBody))
		}),
	)

	cert, err := tls.LoadX509KeyPair(localhostCerts.Cert("localhost"), localhostCerts.Key("localhost"))
	if err != nil {
		panic(err)
	}

	testGateway.TLS = &tls.Config{
		RootCAs:      localhostCerts.Pool(),
		Certificates: []tls.Certificate{cert},
	}

	testGateway.StartTLS()

	return testGateway
}

func makeTLSReq(scheme, addr string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s://%s", scheme, addr), nil)
	Expect(err).ToNot(HaveOccurred())

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	return client.Do(req)
}
