package metrics_test

import (
	"sync"

	"code.cloudfoundry.org/log-cache/internal/metrics"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MultiMetrics", func() {
	var (
		spySubMetrics *spySubMetrics
		m             *metrics.MultiMetrics
	)

	BeforeEach(func() {
		spySubMetrics = newSpySubMetrics()
		m = metrics.NewMultiMetrics(spySubMetrics, map[string]func([]float64) float64{
			"y": func(fs []float64) float64 {
				// max
				var max float64
				for _, f := range fs {
					if f > max {
						max = f
					}
				}

				return max
			},
		})
	})

	It("defaults to sum for aggregations", func() {
		s1 := m.SubMetrics()
		s2 := m.SubMetrics()
		c1 := s1.NewGauge("x")
		c2 := s2.NewGauge("x")

		c1(4)
		c2(5)
		c1(6)

		Expect(spySubMetrics.values).To(HaveKeyWithValue("x", 11.0))
	})

	It("passes counters through", func() {
		s1 := m.SubMetrics()
		s2 := m.SubMetrics()
		c1 := s1.NewCounter("x")
		c2 := s2.NewCounter("x")

		c1(4)
		c2(5)
		c1(6)

		Expect(spySubMetrics.values).To(HaveKeyWithValue("x", 15.0))
	})

	It("close removes the metric from the aggregation", func() {
		s1 := m.SubMetrics()
		s2 := m.SubMetrics()
		c1 := s1.NewGauge("x")
		c2 := s2.NewGauge("x")

		c1(4)

		Expect(s1.Close()).To(Succeed())

		// close is idempotent
		Expect(s1.Close()).To(Succeed())

		c2(5)

		Expect(spySubMetrics.values).To(HaveKeyWithValue("x", 5.0))
	})

	It("aggregates metrics", func() {
		s1 := m.SubMetrics()
		s2 := m.SubMetrics()

		// metric y has an entry in the aggregation map
		c1 := s1.NewGauge("y")
		c2 := s2.NewGauge("y")

		c1(4)
		c2(5)
		c1(6)

		Expect(spySubMetrics.values).To(HaveKeyWithValue("y", 6.0))
	})

	It("survives the race detector", func() {
		var wg sync.WaitGroup
		defer wg.Wait()

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := m.SubMetrics()
				c := s.NewGauge("x")
				for i := 0; i < 100; i++ {
					c(float64(i))
				}
			}()
		}

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := m.SubMetrics()
				c := s.NewCounter("x")
				for i := 0; i < 100; i++ {
					c(uint64(i))
				}
			}()
		}
	})
})

type spySubMetrics struct {
	mu     sync.Mutex
	values map[string]float64
}

func newSpySubMetrics() *spySubMetrics {
	return &spySubMetrics{
		values: make(map[string]float64),
	}
}

func (s *spySubMetrics) NewCounter(name string) func(delta uint64) {
	return func(d uint64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.values[name] += float64(d)
	}
}

func (s *spySubMetrics) NewGauge(name string) func(value float64) {
	return func(v float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.values[name] = v
	}
}
