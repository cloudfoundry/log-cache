package replication

import (
	"encoding/binary"
	"errors"
	"io"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
)

type FSMAdapter struct {
	s SubStore
	c MarshallerCache
}

func NewFSMAdapter(s SubStore, c MarshallerCache) *FSMAdapter {
	return &FSMAdapter{
		s: s,
		c: c,
	}
}

func (a *FSMAdapter) Apply(l *raft.Log) interface{} {
	e := a.c.Get(l.Data)
	if e == nil {
		e = a.marshallEnvelope(l.Data)
		if e == nil {
			return nil
		}
	}

	a.s.Put(e, e.GetSourceId())

	return nil
}

func (a *FSMAdapter) marshallEnvelope(data []byte) *loggregator_v2.Envelope {
	var e loggregator_v2.Envelope
	err := proto.Unmarshal(data, &e)
	if err != nil {
		return nil
	}
	return &e
}

func (a *FSMAdapter) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{s: a.s}, nil
}

type snapshot struct {
	s SubStore
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	for sourceID := range s.s.Meta() {
		var start int64
		for {
			es := s.s.Get(sourceID, time.Unix(0, start), time.Now(), nil, 100, false)
			if len(es) == 0 {
				break
			}

			start = es[len(es)-1].GetTimestamp() + 1

			for _, e := range es {
				data, err := proto.Marshal(e)
				if err != nil {
					return err
				}

				l := []byte{0, 0, 0, 0}
				binary.LittleEndian.PutUint32(l, uint32(len(data)))
				if _, err := sink.Write(l); err != nil {
					return err
				}

				if _, err := sink.Write(data); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *snapshot) Release() {
	// NOP
}

func (a *FSMAdapter) Restore(r io.ReadCloser) error {
	for {
		l := []byte{0, 0, 0, 0}
		n, err := r.Read(l)
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if n != 4 {
			return errors.New("invalid snapshot")
		}

		buffer := make([]byte, int(binary.LittleEndian.Uint32(l)))
		n, err = r.Read(buffer)
		if err != nil {
			return err
		}

		if n != len(buffer) {
			return errors.New("invalid snapshot")
		}

		var e loggregator_v2.Envelope
		if err := proto.Unmarshal(buffer, &e); err != nil {
			return err
		}

		a.s.Put(&e, e.GetSourceId())
	}
}
