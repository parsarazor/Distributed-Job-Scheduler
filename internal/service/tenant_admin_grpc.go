package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	apiv1 "github.com/parsarazor/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func tenantToProto(t *domain.Tenant) *apiv1.Tenant {
	out := &apiv1.Tenant{
		Id:          t.ID.String(),
		Name:        t.Name,
		MaxInFlight: int32(t.InFlightLimit()),
		CreatedAt:   timestamppb.New(t.CreatedAt),
	}
	if t.DisabledAt != nil {
		out.DisabledAt = timestamppb.New(*t.DisabledAt)
	}
	return out
}

func parseTenantID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid tenant_id")
	}
	return id, nil
}

func (s *SchedulerServer) CreateTenant(ctx context.Context, req *apiv1.CreateTenantRequest) (*apiv1.CreateTenantResponse, error) {
	tenant, key, err := s.sched.CreateTenant(ctx, req.Name, int(req.MaxInFlight))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.CreateTenantResponse{Tenant: tenantToProto(tenant), ApiKey: key}, nil
}

func (s *SchedulerServer) ListTenants(ctx context.Context, req *apiv1.ListTenantsRequest) (*apiv1.ListTenantsResponse, error) {
	tenants, err := s.sched.ListTenants(ctx, int(req.Limit), int(req.Offset))
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := &apiv1.ListTenantsResponse{Tenants: make([]*apiv1.Tenant, 0, len(tenants))}
	for _, t := range tenants {
		out.Tenants = append(out.Tenants, tenantToProto(t))
	}
	return out, nil
}

func (s *SchedulerServer) UpdateTenant(ctx context.Context, req *apiv1.UpdateTenantRequest) (*apiv1.UpdateTenantResponse, error) {
	id, err := parseTenantID(req.TenantId)
	if err != nil {
		return nil, err
	}
	var limit *int
	if req.MaxInFlight != nil {
		n := int(*req.MaxInFlight)
		limit = &n
	}
	tenant, err := s.sched.UpdateTenant(ctx, id, limit, req.Disabled)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.UpdateTenantResponse{Tenant: tenantToProto(tenant)}, nil
}

func (s *SchedulerServer) RotateTenantKey(ctx context.Context, req *apiv1.RotateTenantKeyRequest) (*apiv1.RotateTenantKeyResponse, error) {
	id, err := parseTenantID(req.TenantId)
	if err != nil {
		return nil, err
	}
	tenant, key, err := s.sched.RotateTenantKey(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.RotateTenantKeyResponse{Tenant: tenantToProto(tenant), ApiKey: key}, nil
}
