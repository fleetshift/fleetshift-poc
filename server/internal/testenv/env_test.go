package testenv_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testenv"
)

func TestStart_HermeticAPI_AuthenticatedProbe(t *testing.T) {
	env := testenv.StartT(t)

	if env.Profile != testenv.ProfileHermeticAPI {
		t.Fatalf("Profile = %q", env.Profile)
	}
	if !slices.Equal(env.Capabilities, testenv.HermeticCapabilities) {
		t.Fatalf("Capabilities = %v, want %v", env.Capabilities, testenv.HermeticCapabilities)
	}
	if env.Delivery == nil || env.Inventory == nil {
		t.Fatal("expected delivery and inventory controllers")
	}
	if env.Endpoints.GRPC.Dial == "" || env.Endpoints.GRPC.Dial == "127.0.0.1:0" {
		t.Fatalf("unresolved grpc endpoint: %q", env.Endpoints.GRPC.Dial)
	}
	testenv.AssertAllowListedArtifacts(t, env)
	env.Artifacts.RecordTestResult(t.Name(), true, nil, time.Millisecond)
	testenv.AssertArtifactTestResult(t, env, t.Name())

	token, err := env.IssueToken(oidctest.TokenClaims{Subject: "probe-user"})
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	conn, err := env.DialGRPC()
	if err != nil {
		t.Fatalf("DialGRPC: %v", err)
	}
	defer conn.Close()

	ctx, cancel := testenv.CallContext(context.Background())
	defer cancel()
	_, err = pb.NewDeploymentServiceClient(conn).ListDeployments(
		testenv.AuthedContext(ctx, token),
		&pb.ListDeploymentsRequest{},
	)
	if err != nil {
		t.Fatalf("authenticated ListDeployments: %v", err)
	}

	// Unauthenticated must fail once AuthMethod exists.
	unauthCtx, unauthCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer unauthCancel()
	_, err = pb.NewDeploymentServiceClient(conn).ListDeployments(unauthCtx, &pb.ListDeploymentsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated ListDeployments code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestPollCallContext_CancelsBlockedIO(t *testing.T) {
	env := testenv.StartT(t)

	token, err := env.IssueToken(oidctest.TokenClaims{Subject: "timeout-user"})
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	conn, err := env.DialGRPC()
	if err != nil {
		t.Fatalf("DialGRPC: %v", err)
	}
	defer conn.Close()

	// Parent poll budget already expired; PollCallContext must inherit it
	// without relying on wall-clock sleeps.
	pollCtx, pollCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer pollCancel()

	callCtx, callCancel := testenv.PollCallContext(pollCtx)
	defer callCancel()
	if callCtx.Err() != context.DeadlineExceeded {
		t.Fatalf("callCtx.Err() = %v, want %v", callCtx.Err(), context.DeadlineExceeded)
	}

	_, err = pb.NewDeploymentServiceClient(conn).ListDeployments(
		testenv.AuthedContext(callCtx, token),
		&pb.ListDeploymentsRequest{},
	)
	if err == nil {
		t.Fatal("expected ListDeployments to fail on an already-expired context")
	}
	if callCtx.Err() != context.DeadlineExceeded {
		t.Fatalf("callCtx.Err() = %v, want %v", callCtx.Err(), context.DeadlineExceeded)
	}
}

func TestStart_UnsupportedProfile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := testenv.Start(ctx, testenv.WithProfile("hermetic-ui"))
	if err == nil {
		t.Fatal("expected unsupported-profile error")
	}
}
