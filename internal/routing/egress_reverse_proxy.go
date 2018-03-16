package routing

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// EgressReverseProxy is a reverse proxy for Egress requests.
type EgressReverseProxy struct {
	clients  []rpc.EgressClient
	l        Lookup
	localIdx int
	log      *log.Logger

	remoteMetaCache   unsafe.Pointer
	localMetaCache    unsafe.Pointer
	metaCacheDuration time.Duration
}

// NewEgressReverseProxy returns a new EgressReverseProxy. LocalIdx is
// required to know where to find the local node for meta lookups.
func NewEgressReverseProxy(
	l Lookup,
	clients []rpc.EgressClient,
	localIdx int,
	log *log.Logger,
	opts ...EgressReverseProxyOption,
) *EgressReverseProxy {
	e := &EgressReverseProxy{
		l:                 l,
		clients:           clients,
		localIdx:          localIdx,
		log:               log,
		metaCacheDuration: time.Second,
	}

	for _, o := range opts {
		o(e)
	}

	return e
}

// Read will either read from the local node or remote nodes.
func (e *EgressReverseProxy) Read(ctx context.Context, in *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	idx := e.l(in.GetSourceId())
	if len(idx) == 0 {
		return nil, grpc.Errorf(codes.Unavailable, "failed to find route for request. please try again")
	}
	for _, i := range idx {
		if i == e.localIdx {
			return e.clients[e.localIdx].Read(ctx, in)
		}
	}
	return e.clients[idx[rand.Intn(len(idx))]].Read(ctx, in)
}

// Meta will gather meta from the local store and remote nodes.
func (e *EgressReverseProxy) Meta(ctx context.Context, in *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	if in.LocalOnly {
		return e.localMeta(ctx, in)
	}
	return e.remoteMeta(ctx, in)
}

func (e *EgressReverseProxy) localMeta(ctx context.Context, in *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	cache := (*metaCache)(atomic.LoadPointer(&e.localMetaCache))
	if !cache.expired() {
		return cache.metaResp, nil
	}

	metaInfo, err := e.clients[e.localIdx].Meta(ctx, in)
	if err != nil {
		return nil, err
	}

	atomic.StorePointer(&e.localMetaCache, unsafe.Pointer(&metaCache{
		duration:  e.metaCacheDuration,
		timestamp: time.Now(),
		metaResp:  metaInfo,
	}))

	return metaInfo, nil
}

func (e *EgressReverseProxy) remoteMeta(ctx context.Context, in *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	cache := (*metaCache)(atomic.LoadPointer(&e.remoteMetaCache))
	if !cache.expired() {
		return cache.metaResp, nil
	}

	// Each remote should only fetch their local meta data.
	req := &rpc.MetaRequest{
		LocalOnly: true,
	}

	result := &rpc.MetaResponse{
		Meta: make(map[string]*rpc.MetaInfo),
	}

	var errs []error
	for _, c := range e.clients {
		resp, err := c.Meta(ctx, req)
		if err != nil {
			// TODO: Metric
			e.log.Printf("failed to read meta data from remote node: %s", err)
			errs = append(errs, err)
		}

		for sourceID, mi := range resp.Meta {
			result.Meta[sourceID] = mi
		}
	}

	if len(errs) == len(e.clients) {
		return nil, errors.New("failed to read meta data from remote node")
	}

	atomic.StorePointer(&e.remoteMetaCache, unsafe.Pointer(&metaCache{
		duration:  e.metaCacheDuration,
		timestamp: time.Now(),
		metaResp:  result,
	}))

	return result, nil
}

type EgressReverseProxyOption func(e *EgressReverseProxy)

// WithMetaCacheDuration is a EgressReverseProxyOption to configure how long
// to cache results from the Meta endpoint.
func WithMetaCacheDuration(d time.Duration) EgressReverseProxyOption {
	return func(e *EgressReverseProxy) {
		e.metaCacheDuration = d
	}
}

type metaCache struct {
	duration  time.Duration
	timestamp time.Time
	metaResp  *rpc.MetaResponse
}

func (c *metaCache) expired() bool {
	if c == nil {
		return true
	}

	return time.Now().After(c.timestamp.Add(c.duration))
}
