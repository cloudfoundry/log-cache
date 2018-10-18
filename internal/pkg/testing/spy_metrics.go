package testing

import "sync"

type SpyMetrics struct {
	Names  []string
	Deltas []uint64
	Gauges map[string][]float64
	Map    map[string]uint64

	sync.Mutex
}

func NewSpyMetrics() *SpyMetrics {
	return &SpyMetrics{
		Gauges: make(map[string][]float64),
		Map:    make(map[string]uint64),
	}
}

func (s *SpyMetrics) NewCounter(name string) func(delta uint64) {
	s.Lock()
	defer s.Unlock()

	s.Names = append(s.Names, name)
	s.Map[name] = 0

	return func(delta uint64) {
		s.Lock()
		defer s.Unlock()

		s.Deltas = append(s.Deltas, delta)
		s.Map[name] += delta
	}
}

func (s *SpyMetrics) NewGauge(name string) func(value float64) {
	s.Lock()
	defer s.Unlock()
	s.Map[name] = 0

	return func(value float64) {
		s.Lock()
		defer s.Unlock()

		s.Gauges[name] = append(s.Gauges[name], value)
		s.Map[name] = uint64(value)
	}
}

func (s *SpyMetrics) Getter(key string) func() uint64 {
	return func() uint64 {
		s.Lock()
		defer s.Unlock()

		value, ok := s.Map[key]
		if !ok {
			return 99999999999
		}
		return value
	}
}
