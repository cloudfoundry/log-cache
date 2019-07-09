package store_test

import (
	"code.cloudfoundry.org/go-loggregator/metrics"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache/store"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
)

const (
	MaxPerSource = 1000000
)

var (
	MinTime     = time.Unix(0, 0)
	MaxTime     = time.Unix(0, 9223372036854775807)
	gen         = randEnvGen()
	sourceIDs   = []string{"0", "1", "2", "3", "4"}
	results     []*loggregator_v2.Envelope
	metaResults map[string]logcache_v1.MetaInfo
)

func BenchmarkStoreWrite(b *testing.B) {
	s := store.NewStore(MaxPerSource, &staticPruner{}, nopMetrics{})

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
	s := store.NewStore(MaxPerSource, &staticPruner{}, nopMetrics{})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e := gen()
			s.Put(e, e.GetSourceId())
		}
	})
}

func BenchmarkStoreGetTime5MinRange(b *testing.B) {
	s := store.NewStore(MaxPerSource, &staticPruner{}, nopMetrics{})

	for i := 0; i < MaxPerSource/10; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}
	now := time.Now()
	fiveMinAgo := now.Add(-5 * time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = s.Get(sourceIDs[i%len(sourceIDs)], fiveMinAgo, now, nil, nil, b.N, false)
	}
}

func BenchmarkStoreGetLogType(b *testing.B) {
	s := store.NewStore(MaxPerSource, &staticPruner{}, nopMetrics{})

	for i := 0; i < MaxPerSource/10; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}

	logType := []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_LOG}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = s.Get(sourceIDs[i%len(sourceIDs)], MinTime, MaxTime, logType, nil, b.N, false)
	}
}

func BenchmarkMeta(b *testing.B) {
	s := store.NewStore(MaxPerSource, &staticPruner{}, nopMetrics{})

	for i := 0; i < b.N; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metaResults = s.Meta()
	}
}

func BenchmarkMetaWhileWriting(b *testing.B) {
	s := store.NewStore(MaxPerSource, &staticPruner{}, nopMetrics{})

	ready := make(chan struct{}, 1)
	go func() {
		close(ready)
		for i := 0; i < b.N; i++ {
			e := gen()
			s.Put(e, e.GetSourceId())
		}
	}()
	<-ready

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metaResults = s.Meta()
	}
}

func BenchmarkMetaWhileReading(b *testing.B) {
	s := store.NewStore(MaxPerSource, &staticPruner{}, nopMetrics{})

	for i := 0; i < b.N; i++ {
		e := gen()
		s.Put(e, e.GetSourceId())
	}
	now := time.Now()
	fiveMinAgo := now.Add(-5 * time.Minute)
	ready := make(chan struct{}, 1)
	go func() {
		close(ready)
		for i := 0; i < b.N; i++ {
			results = s.Get(sourceIDs[i%len(sourceIDs)], fiveMinAgo, now, nil, nil, b.N, false)
		}
	}()
	<-ready

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metaResults = s.Meta()
	}
}

func contextIsDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
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

type nopCounter struct {}
func (nc *nopCounter) Add(float64) {}

type nopGauge struct {}
func (ng *nopGauge) Add(float64) {}
func (ng *nopGauge) Set(float64) {}

func (n nopMetrics) NewCounter(name string, opts ...metrics.MetricOption) metrics.Counter {
	return &nopCounter{}
}

func (n nopMetrics) NewGauge(name string, opts ...metrics.MetricOption) metrics.Gauge {
	return &nopGauge{}
}

type staticPruner struct {
	size int
}

func (s *staticPruner) GetQuantityToPrune(int64) int {
	return s.size
}

func (s *staticPruner) SetMemoryReporter(metrics.Gauge) {}
