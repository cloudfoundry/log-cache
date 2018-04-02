package replication

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	replication_v1 "code.cloudfoundry.org/log-cache/internal/store/replication/api"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type GRPCTransport struct {
	addr net.Addr
	m    sync.Map
	opts []grpc.DialOption
}

func NewGRPCTransport(serverID string, log *log.Logger, opts ...grpc.DialOption) *GRPCTransport {
	addr, err := net.ResolveTCPAddr("tcp", serverID)
	if err != nil {
		log.Panicf("failed to parse address %s: %s", serverID, err)
	}

	return &GRPCTransport{
		addr: addr,
		opts: opts,
	}
}

func (g *GRPCTransport) StreamLayerFor(raftID string) raft.StreamLayer {
	v, _ := g.m.LoadOrStore(raftID, newListenerAdapter(g.addr, raftID, g.opts))
	l := v.(*listenerAdapter)
	return l
}

func (g *GRPCTransport) Talk(s replication_v1.Transport_TalkServer) error {
	md, ok := metadata.FromIncomingContext(s.Context())
	if !ok {
		log.Fatalf("client missing raft id")
	}

	raftIDs, _ := md["raftid"]
	if len(raftIDs) != 1 {
		log.Panicf("client missing raft id")
	}

	v, _ := g.m.LoadOrStore(raftIDs[0], newListenerAdapter(g.addr, raftIDs[0], g.opts))
	l := v.(*listenerAdapter)

	err := make(chan error)
	l.streams <- streamInfo{
		stream: s,
		err:    err,
	}
	return <-err
}

type listenerAdapter struct {
	addr    net.Addr
	streams chan streamInfo
	raftID  string
	opts    []grpc.DialOption
}

type streamInfo struct {
	stream replication_v1.Transport_TalkServer
	err    chan<- error
}

func newListenerAdapter(addr net.Addr, raftID string, opts []grpc.DialOption) *listenerAdapter {
	return &listenerAdapter{
		addr:    addr,
		streams: make(chan streamInfo, 100),
		raftID:  raftID,
		opts:    opts,
	}
}

func (l *listenerAdapter) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := grpc.Dial(string(address), l.opts...)
	if err != nil {
		return nil, err
	}

	client := replication_v1.NewTransportClient(conn)
	ctx := metadata.NewOutgoingContext(
		context.Background(),
		metadata.Pairs("raftid", l.raftID),
	)
	stream, err := client.Talk(ctx)
	if err != nil {
		return nil, err
	}

	closer := func() {
		conn.Close()
	}

	return NewStreamConnAdapter(l.addr, stream, closer), nil
}

func (l *listenerAdapter) Accept() (net.Conn, error) {
	si := <-l.streams

	closer := func() {
		si.err <- io.EOF
	}

	return NewStreamConnAdapter(l.addr, si.stream, closer), nil
}

func (l *listenerAdapter) Close() error {
	// NOP
	return nil
}

func (l *listenerAdapter) Addr() net.Addr {
	return l.addr
}
