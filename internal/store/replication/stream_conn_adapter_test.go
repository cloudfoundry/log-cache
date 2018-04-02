package replication_test

import (
	"errors"
	"net"

	"code.cloudfoundry.org/log-cache/internal/store/replication"
	replication_v1 "code.cloudfoundry.org/log-cache/internal/store/replication/api"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("StreamConnAdapter", func() {
	var (
		spyCloser *spyCloser
		s         *spyStream
		c         *replication.StreamConnAdapter
	)

	BeforeEach(func() {
		spyCloser = newSpyCloser()
		s = newSpyStream()

		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9999")

		c = replication.NewStreamConnAdapter(addr, s, spyCloser.Close)
	})

	It("passes writes to the stream", func() {
		n, err := c.Write([]byte("hello"))

		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(5))
		Expect(s.sent).To(HaveLen(1))
		Expect(s.sent[0].Payload).To(Equal([]byte("hello")))
	})

	It("send errors result in no bytes written", func() {
		s.sendError = errors.New("error")
		n, err := c.Write([]byte("hello"))

		Expect(err).To(HaveOccurred())
		Expect(n).To(Equal(0))
	})

	It("reads data from the stream", func() {
		s.received = []*replication_v1.Message{
			{Payload: []byte("hello")},
		}

		buffer := make([]byte, 1024)
		n, err := c.Read(buffer)

		Expect(err).ToNot(HaveOccurred())
		Expect(string(buffer[:n])).To(Equal("hello"))
		Expect(s.recCount).To(Equal(1))
	})

	It("reads data from stream for larger messages", func() {
		s.received = []*replication_v1.Message{
			{Payload: []byte("hello")},
		}

		buffer := make([]byte, 2)
		n, err := c.Read(buffer)

		Expect(err).ToNot(HaveOccurred())
		Expect(string(buffer[:n])).To(Equal("he"))
		Expect(s.recCount).To(Equal(1))

		n, err = c.Read(buffer)

		Expect(err).ToNot(HaveOccurred())
		Expect(string(buffer[:n])).To(Equal("ll"))
		Expect(s.recCount).To(Equal(1))

		n, err = c.Read(buffer)

		Expect(err).ToNot(HaveOccurred())
		Expect(string(buffer[:n])).To(Equal("o"))
		Expect(s.recCount).To(Equal(1))
	})

	It("passes the close along to the closer", func() {
		c.Close()
		Expect(spyCloser.called).To(BeTrue())
	})

	It("returns an error if the stream returns an error", func() {
		s.received = []*replication_v1.Message{nil}
		s.recvErr = errors.New("some-error")
		buffer := make([]byte, 1024)
		_, err := c.Read(buffer)

		Expect(err).To(HaveOccurred())
	})
})

type spyStream struct {
	sent      []*replication_v1.Message
	sendError error

	received []*replication_v1.Message
	recCount int
	recvErr  error
}

func newSpyStream() *spyStream {
	return &spyStream{}
}

func (s *spyStream) Send(msg *replication_v1.Message) error {
	s.sent = append(s.sent, msg)
	return s.sendError
}

func (s *spyStream) Recv() (*replication_v1.Message, error) {
	msg := s.received[s.recCount]
	s.recCount += 1
	return msg, s.recvErr
}

type spyCloser struct {
	called bool
}

func newSpyCloser() *spyCloser {
	return &spyCloser{}
}

func (s *spyCloser) Close() {
	s.called = true
}
