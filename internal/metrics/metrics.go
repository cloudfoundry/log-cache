package metrics

import "expvar"

// Metrics stores health metrics for the process. It has a gauge and counter
// metrics.
type Metrics struct {
	m Map
}

// Map stores the desired metrics.
type Map interface {
	// Set adds a new metric to the map.
	Set(key string, ev expvar.Var)
}

// New returns a new Metrics.
func New(m Map) *Metrics {
	return &Metrics{
		m: m,
	}
}

// NewCounter returns a func to be used increment the counter total.
func (m *Metrics) NewCounter(name string) func(delta uint64) {
	if m.m == nil {
		return func(_ uint64) {}
	}

	i := expvar.NewInt(name)
	m.m.Set(name, i)

	return func(d uint64) {
		i.Add(int64(d))
	}
}

// NewGauge returns a func to be used to set the value of a gauge metric.
func (m *Metrics) NewGauge(name string) func(value float64) {
	if m.m == nil {
		return func(_ float64) {}
	}

	f := expvar.NewFloat(name)
	m.m.Set(name, f)

	return f.Set
}
