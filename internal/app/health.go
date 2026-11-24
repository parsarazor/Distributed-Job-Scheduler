package app

import (
	"context"
	"crypto/subtle"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	maxHealthWatches     = 32
	maxHealthServiceName = 128
)

// RuntimeHealth reports the same readiness for the application and the whole
// server. Only these fixed service names are tracked, independent of requests.
type RuntimeHealth struct {
	healthpb.UnimplementedHealthServer
	name         string
	workerToken  string
	adminToken   string
	watches      chan struct{}
	stopped      chan struct{}
	mu           sync.Mutex
	changed      chan struct{}
	serving      bool
	ready        bool
	shuttingDown bool
}

func newRuntimeHealth(name string, cfg serverConfig) *RuntimeHealth {
	return &RuntimeHealth{
		name: name, workerToken: cfg.sharedWorkerToken(), adminToken: cfg.adminToken,
		watches: make(chan struct{}, maxHealthWatches), stopped: make(chan struct{}), changed: make(chan struct{}),
	}
}

func (h *RuntimeHealth) known(service string) bool { return service == "" || service == h.name }

func (h *RuntimeHealth) statusLocked() healthpb.HealthCheckResponse_ServingStatus {
	if h.serving && h.ready && !h.shuttingDown {
		return healthpb.HealthCheckResponse_SERVING
	}
	return healthpb.HealthCheckResponse_NOT_SERVING
}

func (h *RuntimeHealth) update(changed func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.shuttingDown {
		return
	}
	previous := h.statusLocked()
	changed()
	if previous != h.statusLocked() {
		close(h.changed)
		h.changed = make(chan struct{})
	}
}

func (h *RuntimeHealth) setReady(ready bool) { h.update(func() { h.ready = ready }) }

func (h *RuntimeHealth) isServing() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.serving && !h.shuttingDown
}

// Serve binds readiness to the gRPC accept loop and reports unexpected exit.
func (h *RuntimeHealth) Serve(server *grpc.Server, listener net.Listener) error {
	defer h.Shutdown()
	return server.Serve(readyListener{Listener: listener, start: sync.OnceFunc(func() {
		h.update(func() { h.serving = true })
	})})
}

type readyListener struct {
	net.Listener
	start func()
}

func (l readyListener) Accept() (net.Conn, error) {
	l.start()
	return l.Listener.Accept()
}

// Shutdown marks both service names unavailable and wakes all Watch handlers.
func (h *RuntimeHealth) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.shuttingDown {
		h.shuttingDown = true
		close(h.stopped)
	}
}

func (h *RuntimeHealth) Check(_ context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if !h.known(req.GetService()) {
		return nil, status.Error(codes.NotFound, "unknown service")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return &healthpb.HealthCheckResponse{Status: h.statusLocked()}, nil
}

func (h *RuntimeHealth) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.statusLocked()
	statuses := map[string]*healthpb.HealthCheckResponse{"": {Status: current}}
	if h.name != "" {
		statuses[h.name] = &healthpb.HealthCheckResponse{Status: current}
	}
	return &healthpb.HealthListResponse{Statuses: statuses}, nil
}

// watchInterceptor authenticates and acquires capacity before receiving the
// request; even a client that stalls before sending its message costs a slot.
func (h *RuntimeHealth) watchInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if info.FullMethod != healthpb.Health_Watch_FullMethodName {
		return status.Error(codes.PermissionDenied, "streaming RPC is not authorized")
	}
	md, _ := metadata.FromIncomingContext(stream.Context())
	values := md.Get("authorization")
	if len(values) != 1 {
		return status.Error(codes.Unauthenticated, "health watch credential required")
	}
	key, ok := strings.CutPrefix(values[0], "Bearer ")
	matches := func(token string) bool {
		return token != "" && subtle.ConstantTimeCompare([]byte(key), []byte(token)) == 1
	}
	if !ok || (!matches(h.workerToken) && !matches(h.adminToken)) {
		return status.Error(codes.PermissionDenied, "health watch credential rejected")
	}
	select {
	case <-h.stopped:
		return status.Error(codes.Unavailable, "server is stopping")
	default:
	}
	select {
	case h.watches <- struct{}{}:
		defer func() { <-h.watches }()
		return handler(srv, healthWatchStream{ServerStream: stream, stopped: h.stopped})
	default:
		return status.Error(codes.ResourceExhausted, "too many health watches")
	}
}

type healthWatchStream struct {
	grpc.ServerStream
	stopped <-chan struct{}
}

func (s healthWatchStream) RecvMsg(msg any) error {
	// The generated Watch handler does a single receive before entering Watch.
	// If the peer never sends, returning an RPC status closes that transport
	// stream and releases the blocked receive. The goroutine only reads.
	received := make(chan error, 1)
	go func() { received <- s.ServerStream.RecvMsg(msg) }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-received:
		return err
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "missing health watch request")
	case <-s.stopped:
		return status.Error(codes.Unavailable, "server is stopping")
	case <-s.Context().Done():
		return status.FromContextError(s.Context().Err()).Err()
	}
}

func (h *RuntimeHealth) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	if len(req.GetService()) > maxHealthServiceName {
		return status.Error(codes.InvalidArgument, "health service name is too long")
	}
	known := h.known(req.GetService())
	// Limit idle watches too, including unknown services, which never need an
	// entry in an updates map. Clients may reconnect after the stream expires.
	timeout := time.NewTimer(5 * time.Minute)
	defer timeout.Stop()
	previous := healthpb.HealthCheckResponse_UNKNOWN
	for {
		h.mu.Lock()
		next, changed, stopped := h.statusLocked(), h.changed, h.shuttingDown
		h.mu.Unlock()
		if !known {
			next, changed = healthpb.HealthCheckResponse_SERVICE_UNKNOWN, nil
		}
		if next != previous {
			if err := stream.Send(&healthpb.HealthCheckResponse{Status: next}); err != nil {
				return err
			}
			previous = next
		}
		if stopped {
			return nil
		}
		select {
		case <-h.stopped:
		case <-changed:
		case <-timeout.C:
			return status.Error(codes.DeadlineExceeded, "health watch expired")
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
}
