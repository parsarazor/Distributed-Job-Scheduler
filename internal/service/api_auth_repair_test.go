package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func authContext(ctx context.Context, key string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+key))
}

func TestAuthenticatorCredentialScopesForEveryRPC(t *testing.T) {
	b := memory.NewBundle()
	tenant, _ := domain.NewTenant("tenant", "tenant-key")
	if err := b.Tenants.Create(t.Context(), tenant); err != nil {
		t.Fatal(err)
	}
	auth := Authenticator(b.Tenants, "worker-key", "admin-key")
	groups := []struct {
		service, key string
		methods      []string
	}{
		{"SchedulerService", "tenant-key", []string{"SubmitJob", "GetJob", "ListJobs", "CancelJob", "GetJobEvents", "RequeueDeadJob"}},
		{"SchedulerService", "worker-key", []string{"ReportJobCompletion"}},
		{"WorkerService", "worker-key", []string{"Execute", "Heartbeat", "GetWorkerStatus", "CancelExecution", "DrainWorker"}},
		{"SchedulerService", "admin-key", []string{"Dispatch", "TriggerReconciliation"}},
	}
	for _, group := range groups {
		for _, method := range group.methods {
			for _, key := range []string{"", "unknown-key", "tenant-key", "worker-key", "admin-key"} {
				t.Run(group.service+"/"+method+"/"+key, func(t *testing.T) {
					ctx := t.Context()
					if key != "" {
						ctx = authContext(ctx, key)
					}
					called := false
					_, err := auth(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/v1." + group.service + "/" + method}, func(ctx context.Context, _ any) (any, error) {
						called = true
						if md, _ := metadata.FromIncomingContext(ctx); len(md.Get("authorization")) != 0 {
							t.Fatal("handler received bearer metadata")
						}
						return nil, nil
					})
					want := codes.PermissionDenied
					if key == group.key {
						want = codes.OK
					} else if key == "" || key == "unknown-key" {
						want = codes.Unauthenticated
					}
					if status.Code(err) != want || called != (want == codes.OK) {
						t.Fatalf("called=%v err=%v, want %v", called, err, want)
					}
				})
			}
		}
	}
}

func TestAuthenticatorFailsClosedAndRemovesBearerMetadata(t *testing.T) {
	auth := Authenticator(nil, "worker-key", "admin-key")
	for _, method := range []string{"/v1.NewService/Dispatch", "/v1.WorkerService/NewMethod", "/v1.SchedulerService/NewMethod", "/grpc.health.v1.Health/NewMethod"} {
		_, err := auth(authContext(t.Context(), "worker-key"), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			t.Fatal("unknown method reached handler")
			return nil, nil
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unknown method %s: %v", method, err)
		}
	}
	for _, values := range [][]string{nil, {"Bearer "}, {"Basic worker-key"}, {"Bearer worker-key", "Bearer admin-key"}, {"Bearer worker-key "}} {
		ctx := metadata.NewIncomingContext(t.Context(), metadata.MD{"authorization": values})
		_, err := auth(ctx, nil, &grpc.UnaryServerInfo{FullMethod: apiv1.WorkerService_GetWorkerStatus_FullMethodName}, func(context.Context, any) (any, error) {
			t.Fatal("malformed credentials reached handler")
			return nil, nil
		})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("malformed credential status: %v", err)
		}
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer worker-key", "traceparent", "trace-value"))
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer worker-key", "tracestate", "trace-state"))
	_, err := auth(ctx, nil, &grpc.UnaryServerInfo{FullMethod: apiv1.WorkerService_GetWorkerStatus_FullMethodName}, func(ctx context.Context, _ any) (any, error) {
		incoming, _ := metadata.FromIncomingContext(ctx)
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		if len(incoming.Get("authorization")) != 0 || len(outgoing.Get("authorization")) != 0 || incoming.Get("traceparent")[0] != "trace-value" || outgoing.Get("tracestate")[0] != "trace-state" {
			t.Fatal("credential removal changed unrelated metadata")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if md, _ := metadata.FromIncomingContext(ctx); len(md.Get("authorization")) != 1 {
		t.Fatal("interceptor mutated original incoming metadata")
	}
	for _, method := range []string{"/grpc.health.v1.Health/Check", "/grpc.health.v1.Health/List"} {
		called := false
		_, err := auth(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { called = true; return nil, nil })
		if err != nil || !called {
			t.Fatalf("public health method: %v", err)
		}
	}
	_, err = Authenticator(nil, "worker-key")(authContext(t.Context(), "worker-key"), nil, &grpc.UnaryServerInfo{FullMethod: apiv1.SchedulerService_Dispatch_FullMethodName}, func(context.Context, any) (any, error) {
		t.Fatal("legacy worker key accessed admin method")
		return nil, nil
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("legacy constructor grants administration: %v", err)
	}
	_, err = Authenticator(nil, "same-key", "same-key")(authContext(t.Context(), "same-key"), nil, &grpc.UnaryServerInfo{FullMethod: apiv1.SchedulerService_Dispatch_FullMethodName}, nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ambiguous role credentials must fail closed: %v", err)
	}
}

type authLookupResult struct {
	repository.TenantRepository
	tenant *domain.Tenant
	err    error
}

func (r authLookupResult) GetByAPIKeyHash(context.Context, string) (*domain.Tenant, error) {
	return r.tenant, r.err
}

func TestAuthenticatorSeparatesMissingTenantFromStorageFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"absent", fmt.Errorf("secret lookup: %w", domain.ErrTenantNotFound), codes.Unauthenticated},
		{"storage", errors.New("postgres password=secret unavailable"), codes.Unavailable},
		{"canceled", fmt.Errorf("secret lookup: %w", context.Canceled), codes.Canceled},
		{"deadline", fmt.Errorf("secret lookup: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{"nil tenant", nil, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := Authenticator(authLookupResult{err: tc.err}, "worker-key", "admin-key")
			_, err := auth(authContext(t.Context(), "tenant-key"), nil, &grpc.UnaryServerInfo{FullMethod: apiv1.SchedulerService_ListJobs_FullMethodName}, func(context.Context, any) (any, error) {
				t.Fatal("failed authentication reached handler")
				return nil, nil
			})
			if status.Code(err) != tc.code || strings.Contains(status.Convert(err).Message(), "secret") {
				t.Fatalf("authentication misclassified or leaked error: %v", err)
			}
		})
	}
}

func TestGRPCErrorMappingNeverExposesWrappedBackendDetails(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{errors.New("password=secret SELECT * FROM jobs"), codes.Internal},
		{fmt.Errorf("password=secret: %w", context.Canceled), codes.Canceled},
		{fmt.Errorf("password=secret: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{fmt.Errorf("password=secret: %w", domain.ErrInvalidInput), codes.InvalidArgument},
		{fmt.Errorf("password=secret: %w", domain.ErrTenantLimit), codes.ResourceExhausted},
		{fmt.Errorf("password=secret: %w", domain.ErrJobNotFound), codes.NotFound},
		{fmt.Errorf("password=secret: %w", domain.ErrInvalidTransition), codes.FailedPrecondition},
		{fmt.Errorf("password=secret: %w", domain.ErrLeaseLost), codes.Aborted},
		{fmt.Errorf("password=secret: %w", domain.ErrIdempotencyConflict), codes.AlreadyExists},
		{status.Error(codes.Internal, "password=secret"), codes.Internal},
	} {
		err := toGRPCError(tc.err)
		if status.Code(err) != tc.code || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe or misclassified backend error: %v", err)
		}
	}
}

func TestTenantHandlersDenyOperationalIdentitiesBeforeStorage(t *testing.T) {
	server := NewSchedulerServer(nil)
	id := uuid.NewString()
	for _, role := range []authRole{roleWorker, roleAdmin} {
		ctx := context.WithValue(t.Context(), identityKey{}, identity{role: role})
		calls := []func() error{
			func() error { _, err := server.SubmitJob(ctx, &apiv1.SubmitJobRequest{}); return err },
			func() error { _, err := server.ListJobs(ctx, &apiv1.ListJobsRequest{}); return err },
			func() error { _, err := server.GetJob(ctx, &apiv1.GetJobRequest{JobId: id}); return err },
			func() error { _, err := server.CancelJob(ctx, &apiv1.CancelJobRequest{JobId: id}); return err },
			func() error { _, err := server.GetJobEvents(ctx, &apiv1.GetJobEventsRequest{JobId: id}); return err },
			func() error {
				_, err := server.RequeueDeadJob(ctx, &apiv1.RequeueDeadJobRequest{JobId: id})
				return err
			},
		}
		for _, call := range calls {
			if err := call(); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("operational identity reached tenant handler: %v", err)
			}
		}
	}
}

func dialAuthenticatedRepairServer(t *testing.T, s *Scheduler) apiv1.SchedulerServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(4<<20), grpc.UnaryInterceptor(Authenticator(s.tenants, "worker-key", "admin-key")))
	apiv1.RegisterSchedulerServiceServer(server, NewSchedulerServer(s))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///api-repair", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return apiv1.NewSchedulerServiceClient(conn)
}

func tenantRPCContext(ctx context.Context, key string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+key))
}

type metadataListSpy struct {
	repository.JobRepository
	metadataOnly bool
}

func (r *metadataListSpy) ListByTenant(ctx context.Context, tenant uuid.UUID, filter repository.JobListFilter) ([]*domain.Job, int64, error) {
	r.metadataOnly = filter.MetadataOnly
	jobs, total, err := r.JobRepository.ListByTenant(ctx, tenant, filter)
	for _, job := range jobs {
		if len(job.Payload) != 0 {
			return nil, 0, errors.New("list query materialized a payload")
		}
	}
	return jobs, total, err
}

func TestRPCMaximumPayloadRoundTripAndMetadataOnlyList(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	spy := &metadataListSpy{JobRepository: s.jobs}
	s.jobs = spy
	client := dialAuthenticatedRepairServer(t, s)
	ctx := tenantRPCContext(t.Context(), "k")
	payload := []byte(`"` + strings.Repeat("x", (1<<20)-2) + `"`)
	var jobID string
	for range 6 {
		response, err := client.SubmitJob(ctx, &apiv1.SubmitJobRequest{TenantId: tenant.ID.String(), Name: "max-payload", Payload: payload})
		if err != nil || !bytes.Equal(response.GetJob().GetPayload(), payload) {
			t.Fatalf("maximum valid payload submit failed: %v", err)
		}
		jobID = response.Job.Id
	}
	got, err := client.GetJob(ctx, &apiv1.GetJobRequest{JobId: jobID})
	if err != nil || !bytes.Equal(got.GetJob().GetPayload(), payload) {
		t.Fatalf("maximum valid payload get failed: %v", err)
	}
	list, err := client.ListJobs(ctx, &apiv1.ListJobsRequest{Limit: 1000})
	if err != nil {
		t.Fatalf("large payload list failed on default 4MiB client limit: %v", err)
	}
	if !spy.metadataOnly || len(list.Jobs) != 6 || list.TotalCount != 6 || list.HasMore || list.NextOffset != 6 || proto.Size(list) > maxPageBytes {
		t.Fatalf("metadata pagination contract: count=%d total=%d next=%d more=%v projected=%v", len(list.Jobs), list.TotalCount, list.NextOffset, list.HasMore, spy.metadataOnly)
	}
	for _, job := range list.Jobs {
		if len(job.Payload) != 0 {
			t.Fatal("ListJobs exposed payload")
		}
	}
}

func TestRPCDependencyPresenceAndCrossTenantListRequeue(t *testing.T) {
	s, b := newTestScheduler()
	one := seedTenant(t, s, b)
	two, _ := domain.NewTenant("other", "other-key")
	if err := b.Tenants.Create(t.Context(), two); err != nil {
		t.Fatal(err)
	}
	var own, foreign *domain.Job
	for _, tenant := range []*domain.Tenant{one, two} {
		job, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "dead", MaxAttempts: 1})
		if err != nil {
			t.Fatal(err)
		}
		token := claimTestJob(t, s, job.ID, "w")
		if _, err := s.ReportCompletion(t.Context(), job.ID, "w", domain.StatusFailed, token, "handler failed"); err != nil {
			t.Fatal(err)
		}
		if tenant == one {
			own = job
		} else {
			foreign = job
		}
	}
	client := dialAuthenticatedRepairServer(t, s)
	ctx := tenantRPCContext(t.Context(), "k")
	empty := ""
	if _, err := client.SubmitJob(ctx, &apiv1.SubmitJobRequest{Name: "empty-dependency", DependsOn: &empty}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("present empty dependency must be rejected: %v", err)
	}
	if _, err := client.SubmitJob(ctx, &apiv1.SubmitJobRequest{Name: "absent-dependency"}); err != nil {
		t.Fatalf("absent dependency must be accepted: %v", err)
	}
	dead := apiv1.JobStatus_JOB_STATUS_DEAD
	list, err := client.ListJobs(ctx, &apiv1.ListJobsRequest{TenantId: two.ID.String(), Status: &dead})
	if err != nil || len(list.GetJobs()) != 1 || list.Jobs[0].Id != own.ID.String() || list.Jobs[0].TenantId != one.ID.String() {
		t.Fatalf("credential must scope a valid foreign tenant list request: %v %v", list, err)
	}
	if _, err := client.RequeueDeadJob(ctx, &apiv1.RequeueDeadJobRequest{JobId: foreign.ID.String()}); status.Code(err) != codes.NotFound {
		t.Fatalf("cross-tenant requeue: %v", err)
	}
	response, err := client.RequeueDeadJob(ctx, &apiv1.RequeueDeadJobRequest{JobId: own.ID.String()})
	if err != nil || response.GetJob().GetStatus() != apiv1.JobStatus_JOB_STATUS_PENDING || response.Job.AttemptCount != 1 {
		t.Fatalf("own dead job requeue failed: %v %v", response, err)
	}
	foreignJob, err := s.GetJob(t.Context(), foreign.ID)
	if err != nil || foreignJob.Status != domain.StatusDead {
		t.Fatalf("foreign dead job changed: %v %v", foreignJob, err)
	}
}

func TestRPCIdempotencyConflictAndOmittedSchedule(t *testing.T) {
	s, b := newTestScheduler()
	seedTenant(t, s, b)
	client := dialAuthenticatedRepairServer(t, s)
	ctx := tenantRPCContext(t.Context(), "k")
	request := &apiv1.SubmitJobRequest{Name: "idempotent", Payload: []byte(`{"value":1}`), IdempotencyKey: "request-1"}
	first, err := client.SubmitJob(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.SubmitJob(ctx, request)
	if err != nil || second.GetJob().GetId() != first.Job.Id {
		t.Fatalf("same request with omitted schedule was not idempotent: %v", err)
	}
	request.Payload = []byte(`{"value":2}`)
	if _, err := client.SubmitJob(ctx, request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting submission input must return AlreadyExists: %v", err)
	}
}

type auditPageSpy struct {
	repository.JobEventRepository
	limit, offset int
}

func (r *auditPageSpy) ListByJobID(context.Context, uuid.UUID) ([]*domain.JobEvent, error) {
	return nil, errors.New("unbounded audit query used")
}

func (r *auditPageSpy) ListByJobIDPage(ctx context.Context, id uuid.UUID, limit, offset int) ([]*domain.JobEvent, error) {
	r.limit, r.offset = limit, offset
	return r.JobEventRepository.ListByJobIDPage(ctx, id, limit, offset)
}

func TestRPCLargeAuditHistoryIsPagedAndLegacyErrorsAreSuppressed(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	job, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "history"})
	if err != nil {
		t.Fatal(err)
	}
	raw := "password=secret " + strings.Repeat("x", 8192)
	for i := range 1100 {
		message := raw
		if i == 1 {
			message = "handler timed out"
		}
		event := &domain.JobEvent{ID: uuid.New(), JobID: job.ID, OccurredAt: time.Now().UTC().Add(time.Duration(i) * time.Millisecond), FromStatus: domain.StatusRunning, ToStatus: domain.StatusFailed, ErrorMessage: &message}
		if err := b.Events.Record(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	spy := &auditPageSpy{JobEventRepository: s.events}
	s.events = spy
	client := dialAuthenticatedRepairServer(t, s)
	ctx := tenantRPCContext(t.Context(), "k")
	first, err := client.GetJobEvents(ctx, &apiv1.GetJobEventsRequest{JobId: job.ID.String()})
	if err != nil || len(first.GetEvents()) != 100 || first.NextOffset != 100 || !first.HasMore || spy.limit != 101 || spy.offset != 0 {
		t.Fatalf("default audit bound failed: %v limit=%d offset=%d err=%v", first, spy.limit, spy.offset, err)
	}
	seen := make(map[string]bool)
	for offset := int32(0); ; {
		page, err := client.GetJobEvents(ctx, &apiv1.GetJobEventsRequest{JobId: job.ID.String(), Limit: 1000, Offset: offset})
		if err != nil || len(page.GetEvents()) > 1000 || proto.Size(page) > maxPageBytes {
			t.Fatalf("large audit page failed: size=%d err=%v", proto.Size(page), err)
		}
		for _, event := range page.Events {
			if seen[event.Id] || strings.Contains(event.Message, "secret") || (event.Message != "" && event.Message != "handler timed out") {
				t.Fatalf("unsafe or duplicate event: %v", event)
			}
			seen[event.Id] = true
		}
		if !page.HasMore {
			break
		}
		if page.NextOffset <= offset {
			t.Fatal("audit pagination made no progress")
		}
		offset = page.NextOffset
	}
	if len(seen) != 1101 {
		t.Fatalf("audit rows lost across pages: %d", len(seen))
	}
}

func TestRPCPageByteBudgetIncludesEnvelopeAndMakesProgress(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	for range 2 {
		job, _ := domain.NewJob(tenant.ID, strings.Repeat("x", maxPageBytes/2), nil, domain.PriorityDefault, 1, nil, time.Now().UTC())
		if err := b.Jobs.Create(t.Context(), job); err != nil {
			t.Fatal(err)
		}
	}
	client := dialAuthenticatedRepairServer(t, s)
	ctx := tenantRPCContext(t.Context(), "k")
	for offset := int32(0); offset < 2; offset++ {
		page, err := client.ListJobs(ctx, &apiv1.ListJobsRequest{Limit: 1000, Offset: offset})
		if err != nil || len(page.GetJobs()) != 1 || page.NextOffset != offset+1 || page.HasMore != (offset == 0) || proto.Size(page) > maxPageBytes {
			t.Fatalf("byte-truncated page contract failed: size=%d next=%d more=%v err=%v", proto.Size(page), page.GetNextOffset(), page.GetHasMore(), err)
		}
	}
	oversized, _ := domain.NewJob(tenant.ID, strings.Repeat("x", maxPageBytes), nil, domain.PriorityCritical, 1, nil, time.Now().UTC())
	if err := b.Jobs.Create(t.Context(), oversized); err != nil {
		t.Fatal(err)
	}
	critical := apiv1.JobPriority_JOB_PRIORITY_CRITICAL
	if _, err := client.ListJobs(ctx, &apiv1.ListJobsRequest{Priority: &critical}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized first metadata row must fail without a stuck empty page: %v", err)
	}
	for _, req := range []*apiv1.ListJobsRequest{{Limit: -1}, {Limit: 1001}, {Offset: -1}, {Limit: 1000, Offset: 1<<31 - 2}} {
		if _, err := client.ListJobs(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid/overflowing list pagination accepted: %v", err)
		}
	}
	for _, req := range []*apiv1.GetJobEventsRequest{{JobId: oversized.ID.String(), Limit: -1}, {JobId: oversized.ID.String(), Limit: 1001}, {JobId: oversized.ID.String(), Offset: -1}, {JobId: oversized.ID.String(), Limit: 1000, Offset: 1<<31 - 2}} {
		if _, err := client.GetJobEvents(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid/overflowing event pagination accepted: %v", err)
		}
	}
}

func TestRPCPageNearByteLimitIncludesPaginationFields(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	job, _ := domain.NewJob(tenant.ID, "x", nil, domain.PriorityDefault, 1, nil, time.Now().UTC())
	metadata := DomainJobMetadataToProto(job)
	initialSize := proto.Size(&apiv1.ListJobsResponse{Jobs: []*apiv1.Job{metadata}, TotalCount: 1, NextOffset: 1})
	job.Name += strings.Repeat("x", maxPageBytes-64-initialSize)
	if err := b.Jobs.Create(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	client := dialAuthenticatedRepairServer(t, s)
	page, err := client.ListJobs(tenantRPCContext(t.Context(), "k"), &apiv1.ListJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if size := proto.Size(page); size > maxPageBytes || size < maxPageBytes-128 || len(page.Jobs) != 1 || page.NextOffset != 1 || page.HasMore {
		t.Fatalf("near-limit page failed envelope accounting: size=%d count=%d next=%d more=%v", size, len(page.Jobs), page.NextOffset, page.HasMore)
	}
}

func TestRPCWorkerCompletionCannotPersistRawHandlerErrors(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	client := dialAuthenticatedRepairServer(t, s)
	for _, final := range []apiv1.JobStatus{apiv1.JobStatus_JOB_STATUS_SUCCEEDED, apiv1.JobStatus_JOB_STATUS_FAILED} {
		job, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "report", MaxAttempts: 1})
		if err != nil {
			t.Fatal(err)
		}
		token := claimTestJob(t, s, job.ID, "worker")
		_, err = client.ReportJobCompletion(tenantRPCContext(t.Context(), "worker-key"), &apiv1.ReportJobCompletionRequest{JobId: job.ID.String(), WorkerId: "worker", FinalStatus: final, FencingToken: token, ErrorMessage: "Bearer secret: payload=private"})
		if err != nil {
			t.Fatal(err)
		}
		events, err := b.Events.ListByJobID(t.Context(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, event := range events {
			if event.ErrorMessage != nil && strings.Contains(*event.ErrorMessage, "secret") {
				t.Fatal("raw worker-supplied handler error was persisted")
			}
			if event.FromStatus == domain.StatusRunning && event.ToStatus == ProtoToDomainStatus(final) {
				found = true
				if final == apiv1.JobStatus_JOB_STATUS_FAILED && (event.ErrorMessage == nil || *event.ErrorMessage != "handler failed") {
					t.Fatalf("failure did not record the safe category: %v", event.ErrorMessage)
				}
				if final == apiv1.JobStatus_JOB_STATUS_SUCCEEDED && event.ErrorMessage != nil && *event.ErrorMessage != "" {
					t.Fatalf("success recorded a handler error: %v", event.ErrorMessage)
				}
			}
		}
		if !found {
			t.Fatal("worker completion audit event missing")
		}
	}
}
