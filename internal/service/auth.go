package service

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type identityKey struct{}
type authRole uint8

var errTenantCredentialRequired = errors.New("tenant credential required")

const (
	rolePublic authRole = iota
	roleTenant
	roleWorker
	roleAdmin
)

type identity struct {
	tenant uuid.UUID
	role   authRole
	// worker is the worker ID a per-worker credential is bound to; empty for
	// the shared WORKER_TOKEN.
	worker string
}

// Every RPC must be explicitly classified. New methods remain inaccessible
// until their authorization policy is added here.
var methodRoles = map[string]authRole{
	"/grpc.health.v1.Health/Check":                               rolePublic,
	"/grpc.health.v1.Health/List":                                rolePublic,
	apiv1.SchedulerService_SubmitJob_FullMethodName:              roleTenant,
	apiv1.SchedulerService_GetJob_FullMethodName:                 roleTenant,
	apiv1.SchedulerService_ListJobs_FullMethodName:               roleTenant,
	apiv1.SchedulerService_CancelJob_FullMethodName:              roleTenant,
	apiv1.SchedulerService_GetJobEvents_FullMethodName:           roleTenant,
	apiv1.SchedulerService_RequeueDeadJob_FullMethodName:         roleTenant,
	apiv1.SchedulerService_ReportJobCompletion_FullMethodName:    roleWorker,
	apiv1.SchedulerService_ClaimJobExecution_FullMethodName:      roleWorker,
	apiv1.SchedulerService_RenewJobLease_FullMethodName:          roleWorker,
	apiv1.SchedulerService_GetExecutionJob_FullMethodName:        roleWorker,
	apiv1.SchedulerService_Dispatch_FullMethodName:               roleAdmin,
	apiv1.SchedulerService_TriggerReconciliation_FullMethodName:  roleAdmin,
	apiv1.SchedulerService_CreateTenant_FullMethodName:           roleAdmin,
	apiv1.SchedulerService_ListTenants_FullMethodName:            roleAdmin,
	apiv1.SchedulerService_UpdateTenant_FullMethodName:           roleAdmin,
	apiv1.SchedulerService_RotateTenantKey_FullMethodName:        roleAdmin,
	apiv1.SchedulerService_CreateWorkerCredential_FullMethodName: roleAdmin,
	apiv1.SchedulerService_ListWorkerCredentials_FullMethodName:  roleAdmin,
	apiv1.SchedulerService_RevokeWorkerCredential_FullMethodName: roleAdmin,
	apiv1.WorkerService_Execute_FullMethodName:                   roleWorker,
	apiv1.WorkerService_Heartbeat_FullMethodName:                 roleWorker,
	apiv1.WorkerService_GetWorkerStatus_FullMethodName:           roleAdmin,
	apiv1.WorkerService_CancelExecution_FullMethodName:           roleAdmin,
	apiv1.WorkerService_DrainWorker_FullMethodName:               roleAdmin,
}

// Authenticator resolves tenant identity from a bearer API key. Worker control
// and scheduler administration require distinct, separately scoped credentials.
// Omitting adminToken disables administration for compatibility with old callers.
// Invalid API keys are negative-cached for a short TTL so a flood of bad
// credentials cannot amplify into unbounded database load (review H2).
func Authenticator(tenants repository.TenantRepository, workerToken string, adminToken ...string) grpc.UnaryServerInterceptor {
	return AuthenticatorWithWorkers(tenants, nil, workerToken, adminToken...)
}

// AuthenticatorWithWorkers also accepts per-worker credentials (tokens with
// workerTokenPrefix) from workers. An empty workerToken disables the shared
// fleet token so only per-worker credentials authenticate workers.
func AuthenticatorWithWorkers(tenants repository.TenantRepository, workers repository.WorkerCredentialRepository, workerToken string, adminToken ...string) grpc.UnaryServerInterceptor {
	invalid := newNegativeKeyCache(4096, 5*time.Second)
	var admin string
	if len(adminToken) == 1 {
		admin = adminToken[0]
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		required, known := methodRoles[info.FullMethod]
		if !known {
			return nil, status.Error(codes.PermissionDenied, "method is not authorized")
		}
		if required == rolePublic {
			return handler(withoutBearerMetadata(ctx), req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			return nil, status.Error(codes.Unauthenticated, "bearer credential required")
		}
		key := strings.TrimPrefix(values[0], "Bearer ")
		if key == "" || strings.TrimSpace(key) != key {
			return nil, status.Error(codes.Unauthenticated, "bearer credential required")
		}
		ctx = withoutBearerMetadata(ctx)
		who := identity{role: roleTenant}
		workerMatch := workerToken != "" && subtle.ConstantTimeCompare([]byte(key), []byte(workerToken)) == 1
		adminMatch := admin != "" && subtle.ConstantTimeCompare([]byte(key), []byte(admin)) == 1
		switch {
		case len(adminToken) > 1 || (workerMatch && adminMatch):
			return nil, status.Error(codes.Unavailable, "authentication configuration unavailable")
		case workerMatch:
			who.role = roleWorker
		case adminMatch:
			who.role = roleAdmin
		case workers != nil && strings.HasPrefix(key, workerTokenPrefix):
			digest := domain.HashAPIKey(key)
			if invalid.recent(digest) {
				return nil, status.Error(codes.Unauthenticated, "invalid credential")
			}
			cred, err := workers.GetByTokenHash(ctx, digest)
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				return nil, toGRPCError(err)
			case errors.Is(err, domain.ErrWorkerCredentialNotFound):
				invalid.remember(digest)
				return nil, status.Error(codes.Unauthenticated, "invalid credential")
			case err != nil || cred == nil || cred.WorkerID == "":
				return nil, status.Error(codes.Unavailable, "authentication unavailable")
			case cred.RevokedAt != nil:
				return nil, status.Error(codes.Unauthenticated, "worker credential revoked")
			}
			who.role, who.worker = roleWorker, cred.WorkerID
		default:
			if tenants == nil {
				return nil, status.Error(codes.Unavailable, "authentication unavailable")
			}
			digest := domain.HashAPIKey(key)
			if invalid.recent(digest) {
				return nil, status.Error(codes.Unauthenticated, "invalid credential")
			}
			tenant, err := tenants.GetByAPIKeyHash(ctx, digest)
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				return nil, toGRPCError(err)
			case errors.Is(err, domain.ErrTenantNotFound):
				invalid.remember(digest)
				return nil, status.Error(codes.Unauthenticated, "invalid credential")
			case err != nil || tenant == nil || tenant.ID == uuid.Nil:
				return nil, status.Error(codes.Unavailable, "authentication unavailable")
			}
			if tenant.DisabledAt != nil {
				return nil, status.Error(codes.PermissionDenied, "tenant is disabled")
			}
			who.tenant = tenant.ID
		}
		if who.role != required {
			return nil, status.Error(codes.PermissionDenied, "credential does not permit this method")
		}
		return handler(context.WithValue(withoutBearerMetadata(ctx), identityKey{}, who), req)
	}
}

// workerBound rejects a per-worker credential acting under another worker ID,
// so one leaked token cannot renew or complete another worker's leases.
func workerBound(ctx context.Context, workerID string) error {
	who, ok := ctx.Value(identityKey{}).(identity)
	if !ok {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	if who.worker != "" && who.worker != workerID {
		return status.Error(codes.PermissionDenied, "credential is bound to another worker_id")
	}
	return nil
}

// Business handlers and detached work must never inherit bearer credentials.
func withoutBearerMetadata(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		md.Delete("authorization")
		md.Delete("proxy-authorization")
		ctx = metadata.NewIncomingContext(ctx, md)
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		md.Delete("authorization")
		md.Delete("proxy-authorization")
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func requestTenant(ctx context.Context, requested string) (uuid.UUID, error) {
	if id, ok := ctx.Value(identityKey{}).(identity); ok {
		if id.role != roleTenant {
			return uuid.Nil, status.Error(codes.PermissionDenied, "tenant credential required")
		}
		return id.tenant, nil
	}
	// Fail closed: a context without an authenticated identity must never fall
	// back to a client-supplied tenant_id, or any transport that forgets to
	// chain the auth interceptor silently grants cross-tenant access.
	return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
}

func (s *SchedulerServer) ownedJob(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	who, authenticated := ctx.Value(identityKey{}).(identity)
	if !authenticated {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	if who.role != roleTenant {
		return nil, errTenantCredentialRequired
	}
	job, err := s.sched.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	if who.tenant != job.TenantID {
		return nil, domain.ErrJobNotFound
	}
	return job, nil
}

func (s *SchedulerServer) ownsJobMetadata(ctx context.Context, id uuid.UUID) error {
	who, authenticated := ctx.Value(identityKey{}).(identity)
	if !authenticated {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	if who.role != roleTenant {
		return errTenantCredentialRequired
	}
	_, err := s.sched.jobs.GetDependency(ctx, who.tenant, id, false)
	if errors.Is(err, domain.ErrDependencyNotMet) {
		return domain.ErrJobNotFound
	}
	return err
}
