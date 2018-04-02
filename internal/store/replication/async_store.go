package replication

import (
	"log"

	diodes "code.cloudfoundry.org/go-diodes"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

type AsyncStore struct {
	SubStore
	d *diodes.Waiter
}

func NewAsyncStore(s SubStore, log *log.Logger) *AsyncStore {
	a := &AsyncStore{
		d: diodes.NewWaiter(
			diodes.NewManyToOne(1000, diodes.AlertFunc(func(missed int) {
				log.Printf("Dropped (via AsyncStore) %d logs", missed)
			})),
		),
	}
	a.SubStore = s

	go a.start()

	return a
}

func (s *AsyncStore) Put(e *loggregator_v2.Envelope, index string) {
	s.d.Set(diodes.GenericDataType(&wrapper{env: e, index: index}))
}

func (s *AsyncStore) start() {
	for {
		w := s.d.Next()
		if w == nil {
			return
		}

		ww := (*wrapper)(w)
		s.SubStore.Put(ww.env, ww.index)
	}
}

type wrapper struct {
	env   *loggregator_v2.Envelope
	index string
}
