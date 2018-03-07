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
	s     []logcache_v1.ShardGroupReaderClient
	local int
}

// Lookup is used to find the correct nodes.
type Lookup interface {
	Lookup(name string) []int
}

// NewRequestRouter creates a new RPCReverseProxy.
func NewRPCReverseProxy(s []logcache_v1.ShardGroupReaderClient, local int, l Lookup, log *log.Logger) *RPCReverseProxy {
	return &RPCReverseProxy{
		s:     s,
		l:     l,
		local: local,
		log:   log,
	}
}

// SetShardGroup implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) SetShardGroup(c context.Context, req *logcache_v1.SetShardGroupRequest) (*logcache_v1.SetShardGroupResponse, error) {
	nodes := r.l.Lookup(req.GetName())
	if len(nodes) == 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	if req.LocalOnly {
		if !r.contains(r.local, nodes) {
			return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
		}

		return r.s[r.local].SetShardGroup(c, req)
	}

	req.LocalOnly = true
	errs := make(chan error, len(nodes))
	for _, n := range nodes {
		go func(n int) {
			_, err := r.s[n].SetShardGroup(c, req)
			errs <- err
		}(n)
	}

	var e []string
	for i := 0; i < len(nodes); i++ {
		err := <-errs
		if err == nil {
			continue
		}
		e = append(e, err.Error())
	}

	// If even one succeeds, then we will say it worked out.
	if len(e) != len(nodes) {
		return &logcache_v1.AddToGroupResponse{}, nil
	}

	return nil, errors.New(strings.Join(e, ", "))
}

// Read implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) Read(c context.Context, req *logcache_v1.ShardGroupReadRequest) (*logcache_v1.ShardGroupReadResponse, error) {
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
	ping := &logcache_v1.ShardGroupReadRequest{
		Name:        req.GetName(),
		RequesterId: req.GetRequesterId(),

		// Limit -1 is special. It implies that we don't want to read at all
		// and really just want to keep track of the source ID.
		Limit:     -1,
		LocalOnly: true,
	}

	var (
		resp *logcache_v1.ShardGroupReadResponse
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

// ShardGroup implements logcache_v1.GroupReaderServer.
func (r *RPCReverseProxy) ShardGroup(c context.Context, req *logcache_v1.ShardGroupRequest) (*logcache_v1.ShardGroupResponse, error) {
	nodes := r.l.Lookup(req.GetName())
	if len(nodes) == 0 {
		return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
	}
	c, _ = context.WithTimeout(c, 3*time.Second)

	if req.LocalOnly {
		if !r.contains(r.local, nodes) {
			return nil, grpc.Errorf(codes.Unavailable, "unable to route request. Try again...")
		}

		return r.s[r.local].ShardGroup(c, req)
	}

	req.LocalOnly = true
	n := nodes[rand.Intn(len(nodes))]
	return r.s[n].ShardGroup(c, req)
}

func (r *RPCReverseProxy) contains(a int, b []int) bool {
	for _, x := range b {
		if x == a {
			return true
		}
	}
	return false
}
