package groups

import (
	"context"
	"log"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// RPCReverseProxy routes group reader requests to the correct location. This
// may be a peer node, or it may be the local manager. Based on the request,
// it will  find the correct node and based on the type, it will make the
// correct method call.
type RPCReverseProxy struct {
	log *log.Logger
	l   Lookup
	s   []logcache_v1.GroupReaderClient
}

// Lookup is used to find the correct node.
type Lookup interface {
	Lookup(name string) int
}

// NewRequestRouter creates a new RPCReverseProxy.
func NewRPCReverseProxy(s []logcache_v1.GroupReaderClient, l Lookup, log *log.Logger) *RPCReverseProxy {
	return &RPCReverseProxy{
		s:   s,
		l:   l,
		log: log,
	}
}

// AddToGroup implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) AddToGroup(c context.Context, req *logcache_v1.AddToGroupRequest) (*logcache_v1.AddToGroupResponse, error) {
	idx := r.l.Lookup(req.GetName())
	if idx < 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	return r.s[idx].AddToGroup(c, req)
}

// RemoveFromGroup implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) RemoveFromGroup(c context.Context, req *logcache_v1.RemoveFromGroupRequest) (*logcache_v1.RemoveFromGroupResponse, error) {
	idx := r.l.Lookup(req.GetName())
	if idx < 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	return r.s[idx].RemoveFromGroup(c, req)
}

// Read implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) Read(c context.Context, req *logcache_v1.GroupReadRequest) (*logcache_v1.GroupReadResponse, error) {
	idx := r.l.Lookup(req.GetName())
	if idx < 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	return r.s[idx].Read(c, req)
}

// Group implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) Group(c context.Context, req *logcache_v1.GroupRequest) (*logcache_v1.GroupResponse, error) {
	idx := r.l.Lookup(req.GetName())
	if idx < 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	return r.s[idx].Group(c, req)
}
