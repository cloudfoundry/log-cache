package gateway

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/shirou/gopsutil/host"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"code.cloudfoundry.org/log-cache/pkg/marshaler"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
)

// Gateway provides a RESTful API into LogCache's gRPC API.
type Gateway struct {
	log *log.Logger

	logCacheAddr    string
	logCacheVersion string
	uptimeFn        func() int64

	gatewayAddr      string
	lis              net.Listener
	blockOnStart     bool
	logCacheDialOpts []grpc.DialOption
	certPath         string
	keyPath          string
}

// NewGateway creates a new Gateway. It will listen on the gatewayAddr and
// submit requests via gRPC to the LogCache on logCacheAddr. Start() must be
// invoked before using the Gateway.
func NewGateway(logCacheAddr, gatewayAddr, certPath, keyPath string, opts ...GatewayOption) *Gateway {
	g := &Gateway{
		log:          log.New(ioutil.Discard, "", 0),
		logCacheAddr: logCacheAddr,
		gatewayAddr:  gatewayAddr,
		uptimeFn:     uptimeInSeconds,
		certPath:     certPath,
		keyPath:      keyPath,
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

// WithGatewayLogCacheDialOpts returns a GatewayOption that sets grpc.DialOptions on the
// log-cache dial
func WithGatewayLogCacheDialOpts(opts ...grpc.DialOption) GatewayOption {
	return func(g *Gateway) {
		g.logCacheDialOpts = opts
	}
}

// WithGatewayLogCacheDialOpts returns a GatewayOption that the log-cache
// version returned by the info endpoint.
func WithGatewayVersion(version string) GatewayOption {
	return func(g *Gateway) {
		g.logCacheVersion = version
	}
}

// WithGatewayLogCacheDialOpts returns a GatewayOption that the log-cache
// version returned by the info endpoint.
func WithGatewayVMUptimeFn(uptimeFn func() int64) GatewayOption {
	return func(g *Gateway) {
		g.uptimeFn = uptimeFn
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
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(
			runtime.MIMEWildcard, marshaler.NewPromqlMarshaler(&runtime.JSONPb{OrigName: true, EmitDefaults: true}),
		),
	)

	runtime.HTTPError = g.httpErrorHandler

	conn, err := grpc.Dial(g.logCacheAddr, g.logCacheDialOpts...)
	if err != nil {
		g.log.Fatalf("failed to dial Log Cache: %s", err)
	}

	err = logcache_v1.RegisterEgressHandlerClient(
		context.Background(),
		mux,
		logcache_v1.NewEgressClient(conn),
	)
	if err != nil {
		g.log.Fatalf("failed to register LogCache handler: %s", err)
	}

	err = logcache_v1.RegisterPromQLQuerierHandlerClient(
		context.Background(),
		mux,
		logcache_v1.NewPromQLQuerierClient(conn),
	)
	if err != nil {
		g.log.Fatalf("failed to register PromQLQuerier handler: %s", err)
	}

	topLevelMux := http.NewServeMux()
	topLevelMux.HandleFunc("/api/v1/info", g.handleInfoEndpoint)
	topLevelMux.Handle("/", mux)

	server := &http.Server{Handler: topLevelMux}
	if err := server.ServeTLS(g.lis, g.certPath, g.keyPath); err != nil {
		g.log.Fatalf("failed to serve HTTPS endpoint: %s", err)
	}
}

func (g *Gateway) handleInfoEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf(`{"version":"%s","vm_uptime":"%d"}`+"\n", g.logCacheVersion, g.uptimeFn())))
}

func uptimeInSeconds() int64 {
	hostStats, _ := host.Info()
	return int64(hostStats.Uptime)
}

type errorBody struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

func (g *Gateway) httpErrorHandler(
	ctx context.Context,
	mux *runtime.ServeMux,
	marshaler runtime.Marshaler,
	w http.ResponseWriter,
	r *http.Request,
	err error,
) {
	if r.URL.Path != "/api/v1/query" && r.URL.Path != "/api/v1/query_range" {
		runtime.DefaultHTTPError(ctx, mux, marshaler, w, r, err)
		return
	}

	const fallback = `{"error": "failed to marshal error message"}`

	w.Header().Del("Trailer")
	w.Header().Set("Content-Type", marshaler.ContentType())

	body := &errorBody{
		Status:    "error",
		ErrorType: "internal",
		Error:     grpc.ErrorDesc(err),
	}

	buf, merr := marshaler.Marshal(body)
	if merr != nil {
		g.log.Printf("Failed to marshal error message %q: %v", body, merr)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := io.WriteString(w, fallback); err != nil {
			g.log.Printf("Failed to write response: %v", err)
		}
		return
	}

	w.WriteHeader(runtime.HTTPStatusFromCode(grpc.Code(err)))
	if _, err := w.Write(buf); err != nil {
		g.log.Printf("Failed to write response: %v", err)
	}
}
