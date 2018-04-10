package metrics

import "sync"

type MultiMetrics struct {
	mu       sync.Mutex
	m        SubMetrics
	gauges   map[string]*gaugeInfo
	counters map[string]func(delta uint64)

	aggregations map[string]func([]float64) float64
}

// SubMetrics is the client used for initializing counter and gauge metrics.
type SubMetrics interface {
	//NewCounter initializes a new counter metric.
	NewCounter(name string) func(delta uint64)

	//NewGauge initializes a new gauge metric.
	NewGauge(name string) func(value float64)
}

func NewMultiMetrics(m SubMetrics, aggregations map[string]func([]float64) float64) *MultiMetrics {
	return &MultiMetrics{
		m:            m,
		gauges:       make(map[string]*gaugeInfo),
		counters:     make(map[string]func(uint64)),
		aggregations: aggregations,
	}
}

func (m *MultiMetrics) SubMetrics() *SubMultiMetrics {
	s := &SubMultiMetrics{
		mu:           &m.mu,
		m:            m.m,
		gauges:       m.gauges,
		counters:     m.counters,
		aggregations: m.aggregations,
	}

	return s
}

type SubMultiMetrics struct {
	m SubMetrics

	counters map[string]func(delta uint64)

	mu           *sync.Mutex
	gauges       map[string]*gaugeInfo
	aggregations map[string]func([]float64) float64
}

func (m *SubMultiMetrics) NewCounter(name string) func(delta uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.counters[name]
	if !ok {
		c = m.m.NewCounter(name)
		m.counters[name] = c
	}

	return func(delta uint64) {
		c(delta)
	}
}

func (m *SubMultiMetrics) NewGauge(name string) func(value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gi, ok := m.gauges[name]
	if !ok {
		gi = &gaugeInfo{
			m: make(map[*SubMultiMetrics]float64),
			g: m.m.NewGauge(name),
		}
		m.gauges[name] = gi
	}

	agg, ok := m.aggregations[name]
	if !ok {
		agg = func(fs []float64) float64 {
			var total float64
			for _, f := range fs {
				total += f
			}
			return total
		}
	}

	return func(value float64) {
		gi.mu.Lock()
		defer gi.mu.Unlock()
		gi.m[m] = value

		var values []float64
		for _, v := range gi.m {
			values = append(values, v)
		}

		gi.g(agg(values))
	}
}

func (m *SubMultiMetrics) Close() error {
	for _, v := range m.gauges {
		delete(v.m, m)
	}

	return nil
}

type gaugeInfo struct {
	mu sync.Mutex
	m  map[*SubMultiMetrics]float64
	g  func(value float64)
}
