package observability_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
)

func TestAuthnObserver_FullLifecycle_Authenticated(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewAuthnObserver(logger)
	ctx, probe := obs.Authenticate(context.Background(), domain.AuthnRequestInfo{
		Method:   "/fleetshift.v1.DeploymentService/ListDeployments",
		PeerAddr: "127.0.0.1:54321",
	})
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	probe.MethodsLoaded(2)
	probe.VerifyingCredential("oidc-1", domain.AuthMethodTypeOIDC)
	probe.Authenticated(domain.AuthMethodTypeOIDC, domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "user-123",
			Issuer:  "https://issuer.example.com",
		},
	})
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	want := []string{
		"auth methods loaded",
		"verifying credential",
		"authenticated",
	}
	if len(messages) != len(want) {
		t.Fatalf("got %d records %v, want %d %v", len(messages), messages, len(want), want)
	}
	for i, w := range want {
		if messages[i] != w {
			t.Errorf("record[%d] message = %q, want %q", i, messages[i], w)
		}
	}

	last := records[len(records)-1]
	if last.Level != slog.LevelInfo {
		t.Errorf("final record level = %v, want %v", last.Level, slog.LevelInfo)
	}
}

func TestAuthnObserver_FullLifecycle_Anonymous(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewAuthnObserver(logger)
	_, probe := obs.Authenticate(context.Background(), domain.AuthnRequestInfo{
		Method:   "/fleetshift.v1.DeploymentService/ListDeployments",
		PeerAddr: "127.0.0.1:54321",
	})

	probe.MethodsLoaded(1)
	probe.CredentialMissing(domain.AuthMethodTypeOIDC)
	probe.Anonymous()
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	want := []string{
		"auth methods loaded",
		"credential missing for method",
		"anonymous request",
	}
	if len(messages) != len(want) {
		t.Fatalf("got %d records %v, want %d %v", len(messages), messages, len(want), want)
	}
	for i, w := range want {
		if messages[i] != w {
			t.Errorf("record[%d] message = %q, want %q", i, messages[i], w)
		}
	}
}

func TestAuthnObserver_FullLifecycle_VerificationError(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewAuthnObserver(logger)
	_, probe := obs.Authenticate(context.Background(), domain.AuthnRequestInfo{
		Method:   "/fleetshift.v1.DeploymentService/CreateDeployment",
		PeerAddr: "10.0.0.1:12345",
	})

	probe.MethodsLoaded(1)
	probe.VerifyingCredential("oidc-1", domain.AuthMethodTypeOIDC)
	probe.Error(errors.New("token verification failed"))
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	want := []string{
		"auth methods loaded",
		"verifying credential",
		"authentication failed",
	}
	if len(messages) != len(want) {
		t.Fatalf("got %d records %v, want %d %v", len(messages), messages, len(want), want)
	}
	for i, w := range want {
		if messages[i] != w {
			t.Errorf("record[%d] message = %q, want %q", i, messages[i], w)
		}
	}

	last := records[len(records)-1]
	if last.Level != slog.LevelWarn {
		t.Errorf("failure record level = %v, want %v", last.Level, slog.LevelWarn)
	}
}

func TestAuthnObserver_MethodsLoaded_LogsAtDebug(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelInfo}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewAuthnObserver(logger)
	_, probe := obs.Authenticate(context.Background(), domain.AuthnRequestInfo{
		Method:   "/fleetshift.v1.DeploymentService/ListDeployments",
		PeerAddr: "127.0.0.1:54321",
	})

	probe.MethodsLoaded(3)
	probe.Anonymous()
	probe.End()

	records := handler.Records()
	for _, r := range records {
		if r.Message == "auth methods loaded" {
			t.Error("MethodsLoaded should not log at INFO level")
		}
	}
}

func TestAuthnObserver_CredentialMissing_LogsAtDebug(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelInfo}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewAuthnObserver(logger)
	_, probe := obs.Authenticate(context.Background(), domain.AuthnRequestInfo{
		Method:   "/fleetshift.v1.DeploymentService/ListDeployments",
		PeerAddr: "127.0.0.1:54321",
	})

	probe.MethodsLoaded(1)
	probe.CredentialMissing(domain.AuthMethodTypeOIDC)
	probe.Anonymous()
	probe.End()

	records := handler.Records()
	for _, r := range records {
		if r.Message == "credential missing for method" {
			t.Error("CredentialMissing should not log at INFO level")
		}
	}
}

func TestAuthnObserver_ErrorTakesPrecedence(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewAuthnObserver(logger)
	_, probe := obs.Authenticate(context.Background(), domain.AuthnRequestInfo{
		Method:   "/fleetshift.v1.DeploymentService/CreateDeployment",
		PeerAddr: "10.0.0.1:12345",
	})

	probe.MethodsLoaded(1)
	probe.VerifyingCredential("oidc-1", domain.AuthMethodTypeOIDC)
	probe.Authenticated(domain.AuthMethodTypeOIDC, domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{Subject: "user-1"},
	})
	probe.Error(errors.New("something went wrong"))
	probe.End()

	records := handler.Records()
	var last slog.Record
	for _, r := range records {
		last = r
	}
	if last.Message != "authentication failed" {
		t.Errorf("error path should take precedence; message = %q, want %q", last.Message, "authentication failed")
	}
}
