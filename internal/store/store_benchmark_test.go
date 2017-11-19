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
	s := store.NewStore(StoreSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(gen(10))
	}
}

func BenchmarkStoreTruncationOnWrite(b *testing.B) {
	s := store.NewStore(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(gen(10))
	}
}

func BenchmarkStoreWriteParallel(b *testing.B) {
	s := store.NewStore(StoreSize)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Put(gen(10))
		}
	})
}

func BenchmarkStoreGetTime5MinRange(b *testing.B) {
	s := store.NewStore(StoreSize)

	for i := 0; i < StoreSize/10; i++ {
		s.Put(gen(10))
	}
	now := time.Now()
	fiveMinAgo := now.Add(-5 * time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = s.Get(sourceIDs[i%len(sourceIDs)], fiveMinAgo, now, nil)
	}
}

func BenchmarkStoreGetLogType(b *testing.B) {
	s := store.NewStore(StoreSize)

	for i := 0; i < StoreSize/10; i++ {
		s.Put(gen(10))
	}

	logType := &loggregator_v2.Log{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = s.Get(sourceIDs[i%len(sourceIDs)], MinTime, MaxTime, logType)
	}
}

func randEnvGen() func(size int) []*loggregator_v2.Envelope {
	var s []*loggregator_v2.Envelope
	fiveMinAgo := time.Now().Add(-5 * time.Minute)
	for i := 0; i < 10000; i++ {
		s = append(s, benchBuildLog(
			fmt.Sprintf("%d", i%len(sourceIDs)),
			fiveMinAgo.Add(time.Duration(i)*time.Millisecond).UnixNano(),
		))
	}

	var i int
	return func(size int) []*loggregator_v2.Envelope {
		i++
		idx := i % (len(s) - size)
		return s[idx : idx+size]
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
