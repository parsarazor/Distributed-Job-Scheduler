package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestWorkerCredentials_BoundRevocableAndRotatable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, _ := newWiredPair()
	s.SetWorkerCredentials(memory.NewWorkerCredentialRepo())
	admin := dialScheduler(t, s, adminTestKey)

	first, err := admin.CreateWorkerCredential(ctx, &apiv1.CreateWorkerCredentialRequest{WorkerId: "edge-1"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	probe := func(token, workerID string) codes.Code {
		_, err := dialScheduler(t, s, token).GetExecutionJob(ctx, &apiv1.GetExecutionJobRequest{JobId: uuid.Nil.String(), WorkerId: workerID})
		return status.Code(err)
	}
	if c := probe(first.Token, "edge-1"); c != codes.NotFound {
		t.Fatalf("own worker_id must authenticate: %v", c)
	}
	if c := probe(first.Token, "edge-2"); c != codes.PermissionDenied {
		t.Fatalf("token must be bound to its worker_id: %v", c)
	}
	if _, err := dialScheduler(t, s, first.Token).ListTenants(ctx, &apiv1.ListTenantsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("worker token must not administer: %v", err)
	}
	if c := probe(workerTestKey, "anything"); c != codes.NotFound {
		t.Fatalf("shared token stays accepted: %v", c)
	}

	second, err := admin.CreateWorkerCredential(ctx, &apiv1.CreateWorkerCredentialRequest{WorkerId: "edge-1"})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	revoked, err := admin.RevokeWorkerCredential(ctx, &apiv1.RevokeWorkerCredentialRequest{CredentialId: first.Credential.Id})
	if err != nil || revoked.Credential.RevokedAt == nil {
		t.Fatalf("revoke: %+v %v", revoked, err)
	}
	if c := probe(first.Token, "edge-1"); c != codes.Unauthenticated {
		t.Fatalf("revoked token must fail: %v", c)
	}
	if c := probe(second.Token, "edge-1"); c != codes.NotFound {
		t.Fatalf("replacement token must work: %v", c)
	}
	list, err := admin.ListWorkerCredentials(ctx, &apiv1.ListWorkerCredentialsRequest{})
	if err != nil || len(list.Credentials) != 2 {
		t.Fatalf("list: %+v %v", list, err)
	}
	if _, err := admin.CreateWorkerCredential(ctx, &apiv1.CreateWorkerCredentialRequest{WorkerId: " padded"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid worker_id: %v", err)
	}
	if _, err := admin.RevokeWorkerCredential(ctx, &apiv1.RevokeWorkerCredentialRequest{CredentialId: uuid.NewString()}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown credential: %v", err)
	}
}

func TestWorkerCredentials_SharedTokenCanBeDisabled(t *testing.T) {
	t.Parallel()
	b := memory.NewBundle()
	auth := AuthenticatorWithWorkers(b.Tenants, b.WorkerCredentials, "", adminTestKey)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+workerTestKey))
	info := &grpc.UnaryServerInfo{FullMethod: apiv1.SchedulerService_GetExecutionJob_FullMethodName}
	_, err := auth(ctx, nil, info, func(context.Context, any) (any, error) { return nil, nil })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("shared token must be rejected once disabled: %v", err)
	}
}

func TestWorkerCredentials_LegacyTenantKeyWithWorkerPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, _ := newWiredPair()
	s.SetWorkerCredentials(memory.NewWorkerCredentialRepo())
	const legacy = "djw_legacy-tenant-key-created-before-worker-credentials"
	if err := s.tenants.Create(ctx, &domain.Tenant{ID: uuid.New(), Name: "legacy", APIKeyHash: domain.HashAPIKey(legacy), CreatedAt: time.Now(), MaxInFlight: domain.DefaultTenantMaxInFlight}); err != nil {
		t.Fatal(err)
	}
	if _, err := dialScheduler(t, s, legacy).ListJobs(ctx, &apiv1.ListJobsRequest{}); err != nil {
		t.Fatalf("legacy tenant key must still authenticate: %v", err)
	}
	if _, err := dialScheduler(t, s, legacy).GetExecutionJob(ctx, &apiv1.GetExecutionJobRequest{JobId: uuid.Nil.String(), WorkerId: "edge-1"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("tenant key must not act as a worker: %v", err)
	}
	admin := dialScheduler(t, s, adminTestKey)
	if _, err := admin.CreateWorkerCredential(ctx, &apiv1.CreateWorkerCredentialRequest{WorkerId: "edge 1"}); err != nil {
		t.Fatalf("worker IDs with inner spaces are valid: %v", err)
	}
}
