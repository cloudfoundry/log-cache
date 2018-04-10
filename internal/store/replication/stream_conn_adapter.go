package replication

import (
	"bytes"
	"io"
	"net"
	"time"

	replication_v1 "code.cloudfoundry.org/log-cache/internal/store/replication/api"
)

type StreamConnAdapter struct {
	s      Stream
	buf    bytes.Buffer
	closer func()
	addr   net.Addr
}

type Stream interface {
	Send(*replication_v1.Message) error
	Recv() (*replication_v1.Message, error)
}

func NewStreamConnAdapter(addr net.Addr, s Stream, closer func()) *StreamConnAdapter {
	return &StreamConnAdapter{
		addr:   addr,
		s:      s,
		closer: closer,
	}
}

func (s *StreamConnAdapter) Read(b []byte) (n int, err error) {
	if s.buf.Len() == 0 {
		msg, err := s.s.Recv()
		if err != nil {
			return 0, err
		}

		io.Copy(&s.buf, bytes.NewReader(msg.Payload))
	}

	return s.buf.Read(b)
}

func (s *StreamConnAdapter) Write(b []byte) (n int, err error) {
	if err := s.s.Send(&replication_v1.Message{Payload: b}); err != nil {
		return 0, err
	}

	return len(b), nil
}

func (s *StreamConnAdapter) Close() error {
	s.closer()
	return nil
}

func (s *StreamConnAdapter) LocalAddr() net.Addr {
	panic("not implemented")
}

func (s *StreamConnAdapter) RemoteAddr() net.Addr {
	return s.addr
}

func (s *StreamConnAdapter) SetDeadline(t time.Time) error {
	return nil
}

func (s *StreamConnAdapter) SetReadDeadline(t time.Time) error {
	return nil
}

func (s *StreamConnAdapter) SetWriteDeadline(t time.Time) error {
	return nil
}
