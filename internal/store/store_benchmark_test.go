package store_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
)

const (
	StoreSize = 1000000
)

var (
	MinTime   = time.Unix(0, 0)
	MaxTime   = time.Unix(0, 9223372036854775807)
	gen       = randEnvGen()
	sourceIDs = []string{"0", "1", "2", "3", "4"}
	results   []*loggregator_v2.Envelope
)

func BenchmarkStoreWrite(b *testing.B) {
	s := store.NewStore(StoreSize, &staticPruner{}, nopMetrics{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}
}

func BenchmarkStoreTruncationOnWrite(b *testing.B) {
	s := store.NewStore(100, &staticPruner{}, nopMetrics{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}
}

func BenchmarkStoreWriteParallel(b *testing.B) {
	s := store.NewStore(StoreSize, &staticPruner{}, nopMetrics{})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e := gen()
			s.Put(e, e.GetSourceId())
		}
	})
}

func BenchmarkStoreGetTime5MinRange(b *testing.B) {
	s := store.NewStore(StoreSize, &staticPruner{}, nopMetrics{})

	for i := 0; i < StoreSize/10; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}
	now := time.Now()
	fiveMinAgo := now.Add(-5 * time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = s.Get(sourceIDs[i%len(sourceIDs)], fiveMinAgo, now, nil, b.N, false)
	}
}

func BenchmarkStoreGetLogType(b *testing.B) {
	s := store.NewStore(StoreSize, &staticPruner{}, nopMetrics{})

	for i := 0; i < StoreSize/10; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}

	logType := &loggregator_v2.Log{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = s.Get(sourceIDs[i%len(sourceIDs)], MinTime, MaxTime, logType, b.N, false)
	}
}

func randEnvGen() func() *loggregator_v2.Envelope {
	var s []*loggregator_v2.Envelope
	fiveMinAgo := time.Now().Add(-5 * time.Minute)
	for i := 0; i < 10000; i++ {
		s = append(s, benchBuildLog(
			fmt.Sprintf("%d", i%len(sourceIDs)),
			fiveMinAgo.Add(time.Duration(i)*time.Millisecond).UnixNano(),
		))
	}

	var i int
	return func() *loggregator_v2.Envelope {
		i++
		return s[i%len(s)]
	}
}

func benchBuildLog(appID string, ts int64) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		SourceId: appID,
		// Timestamp: ts,
		Timestamp: time.Now().Add(time.Duration(rand.Int63n(50)-100) * time.Microsecond).UnixNano(),
		Message: &loggregator_v2.Envelope_Log{
			Log: &loggregator_v2.Log{},
		},
	}
}

type nopMetrics struct{}

func (n nopMetrics) NewCounter(string) func(delta uint64) {
	return func(uint64) {}
}

func (n nopMetrics) NewGauge(string) func(value float64) {
	return func(float64) {}
}

type staticPruner struct {
	size int
}

func (s *staticPruner) Prune() int {
	s.size++
	if s.size > StoreSize {
		return s.size - StoreSize
	}

	return 0
}
