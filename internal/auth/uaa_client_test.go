package auth_test

import (
	"code.cloudfoundry.org/log-cache/internal/auth"

	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("UAAClient", func() {
	var (
		client     *auth.UAAClient
		httpClient *spyHTTPClient
	)

	BeforeEach(func() {
		httpClient = newSpyHTTPClient()
		client = auth.NewUAAClient("https://uaa.com", "some-client", "some-client-secret", httpClient)
	})

	It("returns IsAdmin when scopes include doppler.firehose with correct clientID and UserID", func() {
		data, err := json.Marshal(map[string]interface{}{
			"scope":     []string{"doppler.firehose"},
			"user_id":   "some-user",
			"client_id": "some-client",
		})
		Expect(err).ToNot(HaveOccurred())
		httpClient.body = data
		httpClient.status = http.StatusOK

		c, err := client.Read("valid-token")
		Expect(err).ToNot(HaveOccurred())
		Expect(c.IsAdmin).To(BeTrue())
		Expect(c.UserID).To(Equal("some-user"))
		Expect(c.ClientID).To(Equal("some-client"))
	})

	It("calls UAA correctly", func() {
		token := "my-token"
		client.Read(token)

		r := httpClient.request

		Expect(r.Method).To(Equal(http.MethodPost))
		Expect(r.Header.Get("Content-Type")).To(
			Equal("application/x-www-form-urlencoded"),
		)
		Expect(r.URL.String()).To(Equal("https://uaa.com/check_token"))

		client, clientSecret, ok := r.BasicAuth()
		Expect(ok).To(BeTrue())
		Expect(client).To(Equal("some-client"))
		Expect(clientSecret).To(Equal("some-client-secret"))

		reqBytes, err := ioutil.ReadAll(r.Body)
		Expect(err).ToNot(HaveOccurred())
		urlValues, err := url.ParseQuery(string(reqBytes))
		Expect(err).ToNot(HaveOccurred())
		Expect(urlValues.Get("token")).To(Equal(token))
	})

	It("returns error when token is blank", func() {
		_, err := client.Read("")
		Expect(err).To(HaveOccurred())
	})

	It("returns false when scopes don't include doppler.firehose", func() {
		data, err := json.Marshal(map[string]interface{}{
			"scope":     []string{"some-scope"},
			"user_id":   "some-user",
			"client_id": "some-client",
		})
		Expect(err).ToNot(HaveOccurred())
		httpClient.body = data
		httpClient.status = http.StatusOK

		c, err := client.Read("invalid-token")
		Expect(err).ToNot(HaveOccurred())

		Expect(c.IsAdmin).To(BeFalse())
	})

	It("returns false when the request fails", func() {
		httpClient.err = errors.New("some-err")

		_, err := client.Read("valid-token")
		Expect(err).To(HaveOccurred())
	})

	It("returns false when the response from the UAA is invalid", func() {
		httpClient.body = []byte("garbage")

		_, err := client.Read("valid-token")
		Expect(err).To(HaveOccurred())
	})
})

type spyHTTPClient struct {
	request *http.Request
	status  int
	err     error
	body    []byte
}

func newSpyHTTPClient() *spyHTTPClient {
	return &spyHTTPClient{
		status: http.StatusOK,
	}
}

func (s *spyHTTPClient) Do(r *http.Request) (*http.Response, error) {
	s.request = r

	resp := http.Response{
		StatusCode: s.status,
		Body:       ioutil.NopCloser(bytes.NewReader(s.body)),
	}

	if s.err != nil {
		return nil, s.err
	}

	return &resp, nil
}
