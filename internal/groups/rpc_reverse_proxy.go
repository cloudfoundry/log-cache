package groups

import (
	"context"
	"log"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
)

// RPCReverseProxy routes group reader requests to the correct location. This
// may be a peer node, or it may be the local manager. Based on the request,
// it will  find the correct node and based on the type, it will make the
// correct method call.
type RPCReverseProxy struct {
	log *log.Logger
	l   Lookup
	s   []logcache.GroupReaderClient
}

// Lookup is used to find the correct node.
type Lookup interface {
	Lookup(name string) int
}

// NewRequestRouter creates a new RPCReverseProxy.
func NewRPCReverseProxy(s []logcache.GroupReaderClient, l Lookup, log *log.Logger) *RPCReverseProxy {
	return &RPCReverseProxy{
		s:   s,
		l:   l,
		log: log,
	}
}

// AddToGroup implements logcache.GroupReaderServer.
func (r *RPCReverseProxy) AddToGroup(c context.Context, req *logcache.AddToGroupRequest) (*logcache.AddToGroupResponse, error) {
	return r.s[r.l.Lookup(req.GetName())].AddToGroup(c, req)
}

// RemoveFromGroup implements logcache.GroupReaderServer.
func (r *RPCReverseProxy) RemoveFromGroup(c context.Context, req *logcache.RemoveFromGroupRequest) (*logcache.RemoveFromGroupResponse, error) {
	return r.s[r.l.Lookup(req.GetName())].RemoveFromGroup(c, req)
}

// Read implements logcache.GroupReaderServer.
func (r *RPCReverseProxy) Read(c context.Context, req *logcache.GroupReadRequest) (*logcache.GroupReadResponse, error) {
	return r.s[r.l.Lookup(req.GetName())].Read(c, req)
}

// Group implements logcache.GroupReaderServer.
func (r *RPCReverseProxy) Group(c context.Context, req *logcache.GroupRequest) (*logcache.GroupResponse, error) {
	return r.s[r.l.Lookup(req.GetName())].Group(c, req)
}
