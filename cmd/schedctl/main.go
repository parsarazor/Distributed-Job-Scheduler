// Command schedctl administers tenants through the scheduler's admin RPCs.
//
//	schedctl tenant create -name acme [-max-in-flight 40]
//	schedctl tenant list
//	schedctl tenant update -id ID [-max-in-flight N] [-disable | -enable]
//	schedctl tenant rotate-key -id ID
//
// SCHEDULER_ADDR (default 127.0.0.1:50051) and ADMIN_TOKEN select the target.
// TLS is used unless SCHEDULER_INSECURE=1; SCHEDULER_CA_FILE adds a CA.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "schedctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 || args[0] != "tenant" {
		return errors.New("usage: schedctl tenant create|list|update|rotate-key [flags]")
	}
	fs := flag.NewFlagSet(args[1], flag.ContinueOnError)
	name := fs.String("name", "", "tenant name (create)")
	id := fs.String("id", "", "tenant ID")
	limit := fs.Int("max-in-flight", 0, "dispatched/running job limit")
	disable := fs.Bool("disable", false, "block the tenant's API key")
	enable := fs.Bool("enable", false, "unblock the tenant's API key")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	client, closeConn, err := dial()
	if err != nil {
		return err
	}
	defer closeConn()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	limitSet := false
	fs.Visit(func(f *flag.Flag) { limitSet = limitSet || f.Name == "max-in-flight" })

	var resp proto.Message
	switch args[1] {
	case "create":
		resp, err = client.CreateTenant(ctx, &apiv1.CreateTenantRequest{Name: *name, MaxInFlight: int32(*limit)})
	case "list":
		resp, err = listAll(ctx, client)
	case "update":
		req := &apiv1.UpdateTenantRequest{TenantId: *id}
		if limitSet {
			req.MaxInFlight = proto.Int32(int32(*limit))
		}
		switch {
		case *disable && *enable:
			return errors.New("-disable and -enable are exclusive")
		case *disable, *enable:
			req.Disabled = proto.Bool(*disable)
		}
		resp, err = client.UpdateTenant(ctx, req)
	case "rotate-key":
		resp, err = client.RotateTenantKey(ctx, &apiv1.RotateTenantKeyRequest{TenantId: *id})
	default:
		return fmt.Errorf("unknown tenant command %q", args[1])
	}
	if err != nil {
		return err
	}
	out, err := protojson.MarshalOptions{Multiline: true}.Marshal(resp)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func listAll(ctx context.Context, client apiv1.SchedulerServiceClient) (*apiv1.ListTenantsResponse, error) {
	const page = 1000
	all := &apiv1.ListTenantsResponse{}
	for {
		resp, err := client.ListTenants(ctx, &apiv1.ListTenantsRequest{Limit: page, Offset: int32(len(all.Tenants))})
		if err != nil {
			return nil, err
		}
		all.Tenants = append(all.Tenants, resp.Tenants...)
		if len(resp.Tenants) < page {
			return all, nil
		}
	}
}

type bearer struct {
	token  string
	secure bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearer) RequireTransportSecurity() bool { return b.secure }

func dial() (apiv1.SchedulerServiceClient, func(), error) {
	token := os.Getenv("ADMIN_TOKEN")
	if token == "" {
		return nil, nil, errors.New("ADMIN_TOKEN is required")
	}
	addr := os.Getenv("SCHEDULER_ADDR")
	if addr == "" {
		addr = "127.0.0.1:50051"
	}
	transport := insecure.NewCredentials()
	secure := os.Getenv("SCHEDULER_INSECURE") != "1"
	if secure {
		config := &tls.Config{MinVersion: tls.VersionTLS12}
		if caFile := os.Getenv("SCHEDULER_CA_FILE"); caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return nil, nil, err
			}
			config.RootCAs = x509.NewCertPool()
			if !config.RootCAs.AppendCertsFromPEM(pem) {
				return nil, nil, errors.New("SCHEDULER_CA_FILE contains no PEM certificates")
			}
		}
		transport = credentials.NewTLS(config)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(transport), grpc.WithPerRPCCredentials(bearer{token: token, secure: secure}))
	if err != nil {
		return nil, nil, err
	}
	return apiv1.NewSchedulerServiceClient(conn), func() { _ = conn.Close() }, nil
}
