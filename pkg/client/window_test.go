package client_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/pkg/client"
)

func TestWindowAdvancesStartTime(t *testing.T) {
	w := windowSetup(t)
	w.v.result = []bool{true, false}

	client.Window(w.ctx, w.v.visit, w.w.walk, client.WithWindowInterval(time.Nanosecond))

	if len(w.w.starts) != 2 {
		t.Fatalf("expected walk to have 2 starts: %d", len(w.w.starts))
	}

	if w.w.starts[0].IsZero() {
		t.Fatal("expected start to be non-zero")
	}

	if w.w.starts[1].Sub(w.w.starts[0]) != time.Nanosecond {
		t.Fatalf("expected interval to equal 1 nanosecond: %v", w.w.starts[1].Sub(w.w.starts[0]))
	}

	if w.w.ends[1].Sub(w.w.starts[1]) != time.Hour {
		t.Fatalf("expected range to equal 1 hour: %v", w.w.ends[1].Sub(w.w.starts[1]))
	}

	if len(w.v.e) != 2 {
		t.Fatalf("expected to have two sets of envelopes: %d", len(w.v.e))
	}

	if !reflect.DeepEqual(w.v.e[0], []*loggregator_v2.Envelope{{Timestamp: 2}}) {
		t.Fatalf("expected to have certain envelope")
	}

	if !reflect.DeepEqual(w.v.e[1], []*loggregator_v2.Envelope{{Timestamp: 2}}) {
		t.Fatalf("expected to have certain envelope")
	}
}

func TestWindowQueriesOverARange(t *testing.T) {
	w := windowSetup(t)

	w.cancel()
	client.Window(w.ctx, w.v.visit, w.w.walk, client.WithWindowInterval(time.Nanosecond))

	if len(w.w.starts) != 1 {
		t.Fatalf("expected walk to have 1 start: %d", len(w.w.starts))
	}

	if w.w.starts[0].IsZero() {
		t.Fatal("expected start to be non-zero")
	}

	if w.w.ends[0].Sub(w.w.starts[0]) != time.Hour {
		t.Fatalf("expected range to equal 1 hour: %v", w.w.ends[0].Sub(w.w.starts[0]))
	}
}

func TestWindowUsesGivenStartTime(t *testing.T) {
	w := windowSetup(t)

	w.cancel()
	client.Window(w.ctx, w.v.visit, w.w.walk,
		client.WithWindowStartTime(time.Unix(1, 0)),
		client.WithWindowInterval(time.Nanosecond),
	)

	if len(w.w.starts) != 1 {
		t.Fatalf("expected walk to have 1 start: %d", len(w.w.starts))
	}

	if w.w.starts[0] != time.Unix(1, 0) {
		t.Fatalf("expected start to equal the given value: %v", w.w.starts[0])
	}

	if w.w.ends[0].Sub(w.w.starts[0]) != time.Hour {
		t.Fatalf("expected range to equal 1 hour: %v", w.w.ends[0].Sub(w.w.starts[0]))
	}
}

func TestWindowUsesGivenWidth(t *testing.T) {
	w := windowSetup(t)

	w.cancel()
	client.Window(w.ctx, w.v.visit, w.w.walk,
		client.WithWindowStartTime(time.Unix(1, 0)),
		client.WithWindowWidth(time.Minute),
		client.WithWindowInterval(time.Nanosecond),
	)

	if len(w.w.starts) != 1 {
		t.Fatalf("expected walk to have 1 start: %d", len(w.w.starts))
	}

	if w.w.starts[0] != time.Unix(1, 0) {
		t.Fatalf("expected start to equal the given value: %v", w.w.starts[0])
	}

	if w.w.ends[0].Sub(w.w.starts[0]) != time.Minute {
		t.Fatalf("expected range to equal 1 minute: %v", w.w.ends[0].Sub(w.w.starts[0]))
	}
}

func TestWindowUsesGivenWidthWithoutStartSet(t *testing.T) {
	w := windowSetup(t)

	client.Window(w.ctx, w.v.visit, w.w.walk,
		client.WithWindowWidth(time.Minute),
		client.WithWindowInterval(time.Nanosecond),
	)

	if len(w.w.starts) != 1 {
		t.Fatalf("expected walk to have 1 start: %d", len(w.w.starts))
	}

	if time.Since(w.w.starts[0]) >= time.Hour {
		t.Fatalf("expected start to be now-Minute: %v", w.w.starts[0])
	}

	if w.w.ends[0].Sub(w.w.starts[0]) != time.Minute {
		t.Fatalf("expected range to equal 1 minute: %v", w.w.ends[0].Sub(w.w.starts[0]))
	}
}

func TestWindowCreatesWalkContextWithTimeoutAsInterval(t *testing.T) {
	w := windowSetup(t)

	now := time.Now()
	interval := 100 * time.Millisecond

	client.Window(
		w.ctx,
		w.v.visit,
		w.w.walk,
		client.WithWindowInterval(interval),
	)

	if len(w.w.ctxs) != 1 {
		t.Fatalf("expected walk to have 1 context: %d", len(w.w.ctxs))
	}

	for _, c := range w.w.ctxs {
		timeout, ok := c.Deadline()

		if !ok {
			t.Fatalf("Your deadline isn't set")
		}

		// context deadline gets set one interval into the ticker
		if !almostEquals(timeout, now.Add(2*interval), interval) {
			t.Fatalf("Deadline on walk context is too long")
		}
	}

}

func TestBuildWalker(t *testing.T) {
	w := windowSetup(t)

	w.r.envelopes = append(w.r.envelopes, []*loggregator_v2.Envelope{
		{Timestamp: 100},
		{Timestamp: 110},
	})

	w.r.envelopes = append(w.r.envelopes, []*loggregator_v2.Envelope{
		{Timestamp: 120},
		{Timestamp: 190},
	})

	w.r.errs = append(w.r.errs, nil, nil)

	ww := client.BuildWalker("some-id", w.r.read)

	es := ww(w.ctx, time.Unix(0, 100), time.Unix(0, 200))

	if len(es) != 4 {
		t.Fatalf("expected 4 envelopes: %d", len(es))
	}

	if !reflect.DeepEqual(es, []*loggregator_v2.Envelope{
		{Timestamp: 100},
		{Timestamp: 110},
		{Timestamp: 120},
		{Timestamp: 190},
	}) {
		t.Fatalf("expected to have certain envelope")
	}

	if w.r.sourceIDs[0] != "some-id" {
		t.Fatalf("expected sourceID to equal some-id: %s", w.r.sourceIDs[0])
	}

	if w.r.starts[0] != 100 {
		t.Fatalf("expected start to equal 100: %d", w.r.starts[0])
	}
}

func TestWalkerStopsReadingAfterError(t *testing.T) {
	w := windowSetup(t)

	w.r.envelopes = append(w.r.envelopes, []*loggregator_v2.Envelope{
		{Timestamp: 100},
		{Timestamp: 110},
	})

	w.r.envelopes = append(w.r.envelopes, []*loggregator_v2.Envelope{
		// Correlates with error
	})

	w.r.envelopes = append(w.r.envelopes, []*loggregator_v2.Envelope{
		{Timestamp: 120},
		{Timestamp: 190},
	})

	w.r.errs = append(w.r.errs, nil, errors.New("some-error"), nil)

	ww := client.BuildWalker("some-id", w.r.read)

	es := ww(w.ctx, time.Unix(0, 100), time.Unix(0, 200))

	if len(es) != 2 {
		t.Fatalf("expected 2 envelopes: %d", len(es))
	}

	if !reflect.DeepEqual(es, []*loggregator_v2.Envelope{
		{Timestamp: 100},
		{Timestamp: 110},
	}) {
		t.Fatalf("expected to have certain envelope")
	}

	if w.r.sourceIDs[0] != "some-id" {
		t.Fatalf("expected sourceID to equal some-id: %s", w.r.sourceIDs[0])
	}

	if w.r.starts[0] != 100 {
		t.Fatalf("expected start to equal 100: %d", w.r.starts[0])
	}
}

type windowT struct {
	ctx    context.Context
	cancel func()

	w *stubWalker
	v *stubVisitor
	r *stubReader
}

func windowSetup(t *testing.T) *windowT {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	return &windowT{
		ctx:    ctx,
		cancel: cancel,
		w:      newStubWalker(),
		v:      newStubVisitor(),
		r:      newStubReader(),
	}
}

type stubWalker struct {
	ctxs   []context.Context
	starts []time.Time
	ends   []time.Time
}

func newStubWalker() *stubWalker {
	return &stubWalker{}
}

func (s *stubWalker) walk(
	ctx context.Context,
	start time.Time,
	end time.Time,
) []*loggregator_v2.Envelope {
	s.ctxs = append(s.ctxs, ctx)
	s.starts = append(s.starts, start)
	s.ends = append(s.ends, end)

	return []*loggregator_v2.Envelope{
		{Timestamp: 2},
	}
}

type stubVisitor struct {
	e      [][]*loggregator_v2.Envelope
	result []bool
}

func newStubVisitor() *stubVisitor {
	return &stubVisitor{}
}

func (s *stubVisitor) visit(e []*loggregator_v2.Envelope) bool {
	s.e = append(s.e, e)

	if len(s.result) == 0 {
		return false
	}

	r := s.result[0]
	s.result = s.result[1:]

	return r
}

func almostEquals(value, expected time.Time, epsilon time.Duration) bool {
	return value.Before(expected.Add(epsilon)) && value.After(expected.Add(-epsilon))
}
