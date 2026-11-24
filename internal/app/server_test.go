package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRuntimeConfig(t *testing.T) {
	t.Setenv("STORAGE", "memory")
	t.Setenv("APP_ENV", "production")
	if _, err := NewFromEnv(t.Context()); err == nil || !strings.Contains(err.Error(), "requires APP_ENV=development") {
		t.Fatalf("production bypass via memory: %v", err)
	}
	t.Setenv("STORAGE", "postgres")
	t.Setenv("TLS_CERT_FILE", "")
	t.Setenv("TLS_KEY_FILE", "")
	if _, _, err := NewServer(&Deps{}, "v1.SchedulerService"); err == nil || !strings.Contains(err.Error(), "TLS_CERT_FILE") {
		t.Fatalf("production accepted plaintext: %v", err)
	}
	if validProductionToken(demoWorkerToken) {
		t.Fatal("production accepted known demo worker token")
	}
	t.Setenv("APP_ENV", "development")
	t.Setenv("STORAGE", "memory")
	t.Setenv("DEMO_SEED", "true")
	t.Setenv("GRPC_HOST", "0.0.0.0")
	if listener, err := ListenRPC("0"); err == nil {
		listener.Close()
		t.Fatal("memory demo accepted a non-loopback listener")
	}
}

func TestProductionBackendTLS(t *testing.T) {
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSSLMODE", "")
	t.Setenv("PGSSLROOTCERT", "")
	t.Setenv("PGSSLNEGOTIATION", "")
	for _, dsn := range []string{
		"postgres://app:custom@localhost/scheduler",
		"postgres://app:custom@localhost/scheduler?sslmode=prefer",
		"postgres://app:custom@localhost/scheduler?sslmode=verify-ca",
		"host=localhost,/tmp user=app password=custom sslmode=verify-full",
		"postgres://app:scheduler@localhost/scheduler?sslmode=verify-full",
	} {
		if _, err := parseDatabaseConfig(dsn, false); err == nil {
			t.Errorf("unverified PostgreSQL configuration accepted")
		}
	}
	if _, err := parseDatabaseConfig("host=first.example,second.example user=app password=custom sslmode=verify-full", false); err != nil {
		t.Fatalf("verified PostgreSQL fallback rejected: %v", err)
	}
	if err := validateRabbitURL("amqp://app:custom@localhost/", false); err == nil {
		t.Fatal("accepted plaintext RabbitMQ")
	}
	if err := validateRabbitURL("amqps://app:custom@localhost/", false); err != nil {
		t.Fatalf("rejected TLS RabbitMQ: %v", err)
	}
}

const testService = "v1.SchedulerService"

func testGRPCHealth(t *testing.T) (*grpc.ClientConn, *RuntimeHealth) {
	t.Helper()
	deps := &Deps{serverConfig: &serverConfig{development: true, workerToken: "watch-test"}}
	server, health, err := NewServer(deps, testService)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	go func() { _ = health.Serve(server, listener) }()
	t.Cleanup(func() { health.Shutdown(); server.Stop() })
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, health
}

func TestHealthWatch(t *testing.T) {
	conn, health := testGRPCHealth(t)
	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	request := &healthpb.HealthCheckRequest{Service: testService}
	if reply, err := client.Check(ctx, request); err != nil || reply.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("initial named health = %v, %v", reply, err)
	}
	read := func(ctx context.Context, service string) grpc.ServerStreamingClient[healthpb.HealthCheckResponse] {
		t.Helper()
		watch, err := client.Watch(ctx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatal(err)
		}
		return watch
	}
	if _, err := read(ctx, testService).Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated watch: %v", err)
	}
	auth := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer watch-test")
	if _, err := read(auth, strings.Repeat("x", maxHealthServiceName+1)).Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized watch: %v", err)
	}
	known := read(auth, testService)
	if reply, err := known.Recv(); err != nil || reply.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("initial watch = %v, %v", reply, err)
	}
	health.setReady(true)
	if reply, err := known.Recv(); err != nil || reply.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("ready watch = %v, %v", reply, err)
	}
	unknown := make([]grpc.ServerStreamingClient[healthpb.HealthCheckResponse], 0, maxHealthWatches-1)
	for i := 0; i < cap(unknown); i++ {
		watch := read(auth, fmt.Sprintf("unknown-%d", i))
		reply, err := watch.Recv()
		if err != nil || reply.GetStatus() != healthpb.HealthCheckResponse_SERVICE_UNKNOWN {
			t.Fatalf("unknown watch = %v, %v", reply, err)
		}
		unknown = append(unknown, watch)
	}
	if _, err := read(auth, "over-capacity").Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unbounded watch capacity: %v", err)
	}
	health.Shutdown()
	if reply, err := known.Recv(); err != nil || reply.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("shutdown watch = %v, %v", reply, err)
	}
	if _, err := known.Recv(); err != io.EOF {
		t.Fatalf("watch failed to close on shutdown: %v", err)
	}
	for _, watch := range unknown {
		if _, err := watch.Recv(); err != io.EOF {
			t.Fatalf("unknown watch failed to close on shutdown: %v", err)
		}
	}
}

func TestStopServerReturnsWithStuckHandler(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		close(entered)
		<-release
		return handler(ctx, req)
	}))
	defer close(release)
	healthpb.RegisterHealthServer(server, newRuntimeHealth(testService, serverConfig{}))
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() { _, _ = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	stopped := make(chan struct{})
	go func() { stopServer(server, 20*time.Millisecond, 20*time.Millisecond); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited on a stuck handler")
	}
}

func TestHealthWatchBoundsInitialReceive(t *testing.T) {
	conn, health := testGRPCHealth(t)
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer watch-test")
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, healthpb.Health_Watch_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	// Send headers without a request. The admission slot must still expire.
	var response healthpb.HealthCheckResponse
	if err := stream.RecvMsg(&response); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("initial receive=%v", err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(health.watches) != 0 {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("stalled initial request leaked watch capacity")
		}
	}
}

func TestOperationsShutdownCancelsOwnedSampler(t *testing.T) {
	t.Setenv("STORAGE", "memory")
	t.Setenv("METRICS_ADDR", "127.0.0.1:0")
	b := memory.NewBundle()
	health := newRuntimeHealth(testService, serverConfig{development: true})
	ops, err := ServeOperations(context.Background(), &Deps{Operations: b.Tx}, health, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := ops.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown with live parent context: %v", err)
	}
	select {
	case <-ops.done:
	default:
		t.Fatal("sampler still running after shutdown")
	}
}

type metadataStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s metadataStream) Context() context.Context { return s.ctx }

// A retired shared token, or a worker's own scheduler credential on its
// admin-only port, must not occupy health watch slots.
func TestHealthWatchRejectsRetiredSharedToken(t *testing.T) {
	health := newRuntimeHealth(testService, serverConfig{workerToken: "retired-shared", adminToken: "admin", noSharedWorkerToken: true})
	info := &grpc.StreamServerInfo{FullMethod: healthpb.Health_Watch_FullMethodName}
	for token, want := range map[string]codes.Code{"retired-shared": codes.PermissionDenied, "admin": codes.Unavailable} {
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token))
		health.Shutdown()
		err := health.watchInterceptor(nil, metadataStream{ctx: ctx}, info, func(any, grpc.ServerStream) error { return nil })
		if status.Code(err) != want {
			t.Fatalf("token %q: err=%v, want %v", token, err, want)
		}
	}
}
