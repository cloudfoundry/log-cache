package logcache

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"google.golang.org/grpc"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

// Gateway provides a RESTful API into LogCache's gRPC API.
type Gateway struct {
	log *log.Logger

	logCacheAddr    string
	groupReaderAddr string

	gatewayAddr  string
	lis          net.Listener
	blockOnStart bool
}

// NewGateway creates a new Gateway. It will listen on the gatewayAddr and
// submit requests via gRPC to the LogCache on logCacheAddr. Start() must be
// invoked before using the Gateway.
func NewGateway(logCacheAddr, groupReaderAddr, gatewayAddr string, opts ...GatewayOption) *Gateway {
	g := &Gateway{
		log: log.New(ioutil.Discard, "", 0),

		logCacheAddr:    logCacheAddr,
		groupReaderAddr: groupReaderAddr,

		gatewayAddr: gatewayAddr,
	}

	for _, o := range opts {
		o(g)
	}

	return g
}

// GatewayOption configures a Gateway.
type GatewayOption func(*Gateway)

// WithGatewayLogger returns a GatewayOption that configures the logger for
// the Gateway. It defaults to no logging.
func WithGatewayLogger(l *log.Logger) GatewayOption {
	return func(g *Gateway) {
		g.log = l
	}
}

// WithGatewayBlock returns a GatewayOption that determines if Start launches
// a go-routine or not. It defaults to launching a go-routine. If this is set,
// start will block on serving the HTTP endpoint.
func WithGatewayBlock() GatewayOption {
	return func(g *Gateway) {
		g.blockOnStart = true
	}
}

// Start starts the gateway to start receiving and forwarding requests. It
// does not block unless WithGatewayBlock was set.
func (g *Gateway) Start() {
	lis, err := net.Listen("tcp", g.gatewayAddr)
	if err != nil {
		g.log.Fatalf("failed to listen on addr %s: %s", g.gatewayAddr, err)
	}
	g.lis = lis
	g.log.Printf("listening on %s...", lis.Addr().String())

	if g.blockOnStart {
		g.listenAndServe()
		return
	}

	go g.listenAndServe()
}

// Addr returns the address the gateway is listening on. Start must be called
// first.
func (g *Gateway) Addr() string {
	return g.lis.Addr().String()
}

func (g *Gateway) listenAndServe() {
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}

	err := logcache.RegisterEgressHandlerFromEndpoint(
		context.Background(),
		mux,
		g.logCacheAddr,
		opts,
	)
	if err != nil {
		g.log.Fatalf("failed to register LogCache handler: %s", err)
	}

	err = logcache.RegisterGroupReaderHandlerFromEndpoint(
		context.Background(),
		mux,
		g.groupReaderAddr,
		opts,
	)
	if err != nil {
		g.log.Fatalf("failed to register GroupReader handler: %s", err)
	}

	server := &http.Server{Handler: mux}
	if err := server.Serve(g.lis); err != nil {
		g.log.Fatalf("failed to serve HTTP endpoint: %s", err)
	}
}
