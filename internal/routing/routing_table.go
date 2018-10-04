package routing

import (
	"context"
	"sort"
	"sync"

	rpc "code.cloudfoundry.org/log-cache/rpc/logcache_v1"
)

// RoutingTable makes decisions for where a item should be routed.
type RoutingTable struct {
	mu         sync.RWMutex
	addrs      map[string]int
	h          func(string) uint64
	latestTerm uint64

	table []rangeInfo
}

// NewRoutingTable returns a new RoutingTable.
func NewRoutingTable(addrs []string, hasher func(string) uint64) *RoutingTable {
	a := make(map[string]int)
	for i, addr := range addrs {
		a[addr] = i
	}

	return &RoutingTable{
		addrs: a,
		h:     hasher,
	}
}

// Lookup takes a item, hash it and determine what node it should be
// routed to.
func (t *RoutingTable) Lookup(item string) []int {
	h := t.h(item)
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []int
	for _, r := range t.table {
		if h < r.r.Start || h > r.r.End {
			// Outside of range
			continue
		}
		result = append(result, r.idx)
	}

	return result
}

// LookupAll returns every index that has a range where the item would
// fall under.
func (t *RoutingTable) LookupAll(item string) []int {
	h := t.h(item)

	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []int
	ranges := t.table

	for {
		i := t.findRange(h, ranges)
		if i < 0 {
			break
		}
		result = append(result, ranges[i].idx)
		ranges = ranges[i+1:]
	}

	return result
}

// SetRanges sets the routing table.
func (t *RoutingTable) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.table = nil
	for addr, ranges := range in.Ranges {
		for _, r := range ranges.Ranges {
			var sr Range
			sr.CloneRpcRange(r)

			t.table = append(t.table, rangeInfo{
				idx: t.addrs[addr],
				r:   sr,
			})
		}
	}

	sort.Sort(rangeInfos(t.table))

	return &rpc.SetRangesResponse{}, nil
}

func (t *RoutingTable) findRange(h uint64, rs []rangeInfo) int {
	for i, r := range rs {
		if h < r.r.Start || h > r.r.End {
			// Outside of range
			continue
		}
		return i
	}

	return -1
}

type Range struct {
	Start uint64
	End   uint64
}

func (sr *Range) CloneRpcRange(r *rpc.Range) {
	sr.Start = r.Start
	sr.End = r.End
}

func (sr *Range) ToRpcRange() *rpc.Range {
	return &rpc.Range{
		Start: sr.Start,
		End:   sr.End,
	}
}

type rangeInfo struct {
	r   Range
	idx int
}

type rangeInfos []rangeInfo

func (r rangeInfos) Len() int {
	return len(r)
}

func (r rangeInfos) Less(i, j int) bool {
	if r[i].r.Start == r[j].r.Start {
		return r[i].idx > r[j].idx
	}

	return r[i].r.Start < r[j].r.Start
}

func (r rangeInfos) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
