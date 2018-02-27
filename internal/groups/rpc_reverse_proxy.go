package groups

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"strings"
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
	log   *log.Logger
	l     Lookup
	s     []logcache_v1.GroupReaderClient
	local int
}

// Lookup is used to find the correct nodes.
type Lookup interface {
	Lookup(name string) []int
}

// NewRequestRouter creates a new RPCReverseProxy.
func NewRPCReverseProxy(s []logcache_v1.GroupReaderClient, local int, l Lookup, log *log.Logger) *RPCReverseProxy {
	return &RPCReverseProxy{
		s:     s,
		l:     l,
		local: local,
		log:   log,
	}
}

// AddToGroup implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) AddToGroup(c context.Context, req *logcache_v1.AddToGroupRequest) (*logcache_v1.AddToGroupResponse, error) {
	nodes := r.l.Lookup(req.GetName())
	if len(nodes) == 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	if req.LocalOnly {
		if !r.contains(r.local, nodes) {
			return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
		}

		return r.s[r.local].AddToGroup(c, req)
	}

	req.LocalOnly = true
	errs := make(chan error, len(nodes))
	for _, n := range nodes {
		go func(n int) {
			_, err := r.s[n].AddToGroup(c, req)
			errs <- err
		}(n)
	}

	var e []string
	for i := 0; i < len(nodes); i++ {
		err := <-errs
		if err == nil {
			// If even one succeeds, then we will say it worked out.
			return &logcache_v1.AddToGroupResponse{}, nil
		}
		e = append(e, err.Error())
	}

	return nil, errors.New(strings.Join(e, ", "))
}

// RemoveFromGroup implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) RemoveFromGroup(c context.Context, req *logcache_v1.RemoveFromGroupRequest) (*logcache_v1.RemoveFromGroupResponse, error) {
	nodes := r.l.Lookup(req.GetName())
	if len(nodes) == 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	if req.LocalOnly {
		if !r.contains(r.local, nodes) {
			return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
		}

		return r.s[r.local].RemoveFromGroup(c, req)
	}

	req.LocalOnly = true
	errs := make(chan error, len(nodes))
	for _, n := range nodes {
		go func(n int) {
			_, err := r.s[n].RemoveFromGroup(c, req)
			errs <- err
		}(n)
	}

	var e []string
	for i := 0; i < len(nodes); i++ {
		err := <-errs
		if err == nil {
			// If even one succeeds, then we will say it worked out.
			return &logcache_v1.RemoveFromGroupResponse{}, nil
		}
		e = append(e, err.Error())
	}

	return nil, errors.New(strings.Join(e, ", "))
}

// Read implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) Read(c context.Context, req *logcache_v1.GroupReadRequest) (*logcache_v1.GroupReadResponse, error) {
	nodes := r.l.Lookup(req.GetName())
	if len(nodes) == 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	if req.LocalOnly {
		if !r.contains(r.local, nodes) {
			return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
		}

		return r.s[r.local].Read(c, req)
	}

	// We want each node to know about all the requester IDs for sharding.
	ping := &logcache_v1.GroupReadRequest{
		Name:        req.GetName(),
		RequesterId: req.GetRequesterId(),
		Limit:       -1,
		LocalOnly:   true,
	}

	var (
		resp *logcache_v1.GroupReadResponse
		err  error
	)

	req.LocalOnly = true
	for i, n := range nodes {
		if i != int(req.GetRequesterId())%len(nodes) {
			r.s[n].Read(c, ping)
			continue
		}

		resp, err = r.s[n].Read(c, req)
	}

	return resp, err
}

// Group implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) Group(c context.Context, req *logcache_v1.GroupRequest) (*logcache_v1.GroupResponse, error) {
	nodes := r.l.Lookup(req.GetName())
	if len(nodes) == 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	if req.LocalOnly {
		if !r.contains(r.local, nodes) {
			return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
		}

		return r.s[r.local].Group(c, req)
	}

	req.LocalOnly = true
	n := nodes[rand.Intn(len(nodes))]
	return r.s[n].Group(c, req)
}

func (r *RPCReverseProxy) contains(a int, b []int) bool {
	for _, x := range b {
		if x == a {
			return true
		}
	}
	return false
}
