package replication

import (
	"io"
	"log"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
)

type Store struct {
	s      SubStore
	a      Applier
	c      MarshallerCache
	log    *log.Logger
	closer func()
}

type SubStore interface {
	io.Closer
	Put(e *loggregator_v2.Envelope, index string)
	Get(index string, start time.Time, end time.Time, envelopeTypes []logcache_v1.EnvelopeType, limit int, descending bool) []*loggregator_v2.Envelope
	Meta() map[string]logcache_v1.MetaInfo
}

type Applier interface {
	Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture
}

type MarshallerCache interface {
	Put([]byte, *loggregator_v2.Envelope)
	Get([]byte) *loggregator_v2.Envelope
}

func NewStore(s SubStore, a Applier, c MarshallerCache, closer func(), log *log.Logger) *Store {
	return &Store{
		s:      s,
		a:      a,
		c:      c,
		closer: closer,
		log:    log,
	}
}

func (s *Store) Get(index string, start time.Time, end time.Time, envelopeTypes []logcache_v1.EnvelopeType, limit int, descending bool) []*loggregator_v2.Envelope {
	return s.s.Get(index, start, end, envelopeTypes, limit, descending)
}

func (s *Store) Put(e *loggregator_v2.Envelope, index string) {
	s.s.Put(e, e.GetSourceId())
	data, err := proto.Marshal(e)
	if err != nil {
		s.log.Panic(err)
	}

	s.c.Put(data, e)
	s.a.Apply(data, time.Second)
}

func (s *Store) Meta() map[string]logcache_v1.MetaInfo {
	return s.s.Meta()
}

func (s *Store) Close() error {
	s.closer()
	return nil
}
