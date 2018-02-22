package routing

import (
	"context"
	"sort"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/emirpasic/gods/utils"
)

// RoutingTable makes decisions for where a item should be routed.
type RoutingTable struct {
	mu    sync.RWMutex
	addrs map[string]int
	h     func(string) uint64

	table    []rangeInfo
	avlTable *avltree.Tree
}

// NewRoutingTable returns a new RoutingTable.
func NewRoutingTable(addrs []string, hasher func(string) uint64) *RoutingTable {
	a := make(map[string]int)
	for i, addr := range addrs {
		a[addr] = i
	}

	return &RoutingTable{
		addrs:    a,
		h:        hasher,
		avlTable: avltree.NewWith(utils.UInt64Comparator),
	}
}

// Lookup takes a item, hash it and determine what node it should be
// routed to.
func (t *RoutingTable) Lookup(item string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lookup(t.h(item), t.avlTable.Root)
}

func (t *RoutingTable) lookup(hash uint64, n *avltree.Node) int {
	if n == nil {
		return -1
	}

	r := n.Value.(rangeInfo)
	if hash < r.r.Start {
		return t.lookup(hash, n.Children[0])
	}

	if hash > r.r.End {
		return t.lookup(hash, n.Children[1])
	}

	if hash > r.r.Start && hash <= r.r.End {
		return r.idx
	}

	return -1
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
	var latestTerm uint64
	for addr, ranges := range in.Ranges {
		for _, r := range ranges.Ranges {
			t.table = append(t.table, rangeInfo{
				idx: t.addrs[addr],
				r:   *r,
			})

			if latestTerm < r.Term {
				latestTerm = r.Term
			}
		}
	}

	sort.Sort(rangeInfos(t.table))

	t.avlTable = avltree.NewWith(utils.UInt64Comparator)
	for _, r := range t.table {
		if r.r.Term == latestTerm {
			t.avlTable.Put(r.r.Start, r)
		}
	}

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

type rangeInfo struct {
	r   rpc.Range
	idx int
}

type rangeInfos []rangeInfo

func (r rangeInfos) Len() int {
	return len(r)
}

func (r rangeInfos) Less(i, j int) bool {
	if r[i].r.Start == r[j].r.Start {
		return r[i].r.Term > r[j].r.Term
	}

	return r[i].r.Start < r[j].r.Start
}

func (r rangeInfos) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
