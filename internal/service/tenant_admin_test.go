package service

import (
	"context"
	"testing"

	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestTenantAdmin_Lifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, _ := newWiredPair()
	admin := dialScheduler(t, s, adminTestKey)

	created, err := admin.CreateTenant(ctx, &apiv1.CreateTenantRequest{Name: "acme", MaxInFlight: 40})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ApiKey == "" || created.Tenant.MaxInFlight != 40 {
		t.Fatalf("create returned %+v", created)
	}
	submit := func(key string) codes.Code {
		_, err := dialScheduler(t, s, key).SubmitJob(ctx, &apiv1.SubmitJobRequest{Name: "default"})
		return status.Code(err)
	}
	if c := submit(created.ApiKey); c != codes.OK {
		t.Fatalf("new key must work: %v", c)
	}
	if _, err := dialScheduler(t, s, created.ApiKey).ListTenants(ctx, &apiv1.ListTenantsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("tenants must not administer: %v", err)
	}

	rotated, err := admin.RotateTenantKey(ctx, &apiv1.RotateTenantKeyRequest{TenantId: created.Tenant.Id})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if c := submit(created.ApiKey); c != codes.Unauthenticated {
		t.Fatalf("old key must stop working: %v", c)
	}
	if c := submit(rotated.ApiKey); c != codes.OK {
		t.Fatalf("rotated key must work: %v", c)
	}

	updated, err := admin.UpdateTenant(ctx, &apiv1.UpdateTenantRequest{TenantId: created.Tenant.Id, Disabled: proto.Bool(true), MaxInFlight: proto.Int32(5)})
	if err != nil || updated.Tenant.DisabledAt == nil || updated.Tenant.MaxInFlight != 5 {
		t.Fatalf("disable: %+v %v", updated, err)
	}
	if c := submit(rotated.ApiKey); c != codes.PermissionDenied {
		t.Fatalf("disabled tenant must be rejected: %v", c)
	}
	if _, err := admin.UpdateTenant(ctx, &apiv1.UpdateTenantRequest{TenantId: created.Tenant.Id, Disabled: proto.Bool(false)}); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if c := submit(rotated.ApiKey); c != codes.OK {
		t.Fatalf("re-enabled tenant must work: %v", c)
	}

	list, err := admin.ListTenants(ctx, &apiv1.ListTenantsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, tn := range list.Tenants {
		found = found || tn.Id == created.Tenant.Id && tn.MaxInFlight == 5 && tn.DisabledAt == nil
	}
	if !found {
		t.Fatalf("tenant missing from list: %+v", list.Tenants)
	}
}

func TestTenantAdmin_RejectsInvalidInput(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, _ := newWiredPair()
	admin := dialScheduler(t, s, adminTestKey)
	cases := map[string]error{}
	_, cases["empty name"] = admin.CreateTenant(ctx, &apiv1.CreateTenantRequest{Name: " "})
	_, cases["limit too high"] = admin.CreateTenant(ctx, &apiv1.CreateTenantRequest{Name: "x", MaxInFlight: 10001})
	_, cases["negative limit"] = admin.UpdateTenant(ctx, &apiv1.UpdateTenantRequest{TenantId: "00000000-0000-0000-0000-000000000001", MaxInFlight: proto.Int32(0)})
	_, cases["bad id"] = admin.RotateTenantKey(ctx, &apiv1.RotateTenantKeyRequest{TenantId: "nope"})
	_, cases["page too large"] = admin.ListTenants(ctx, &apiv1.ListTenantsRequest{Limit: 5000})
	for name, err := range cases {
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v", name, err)
		}
	}
	if _, err := admin.RotateTenantKey(ctx, &apiv1.RotateTenantKeyRequest{TenantId: "00000000-0000-0000-0000-000000000001"}); status.Code(err) != codes.NotFound {
		t.Errorf("unknown tenant: %v", err)
	}
}
