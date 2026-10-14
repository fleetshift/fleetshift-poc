package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type agentTestVault struct {
	secrets map[domain.SecretRef][]byte
	err     error
}

func (v *agentTestVault) Get(_ context.Context, ref domain.SecretRef) ([]byte, error) {
	if v.err != nil {
		return nil, v.err
	}
	val, ok := v.secrets[ref]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return val, nil
}

func (v *agentTestVault) Put(_ context.Context, ref domain.SecretRef, val []byte) error {
	if v.secrets == nil {
		v.secrets = make(map[domain.SecretRef][]byte)
	}
	v.secrets[ref] = val
	return nil
}

func (v *agentTestVault) Delete(_ context.Context, ref domain.SecretRef) error {
	delete(v.secrets, ref)
	return nil
}

func TestResolvePlatformTokenOptional(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("direct token wins over ref", func(t *testing.T) {
		vault := &agentTestVault{
			secrets: map[domain.SecretRef][]byte{"targets/t1/sa": []byte("vault-token")},
		}
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{
				PropServiceAccountToken:    "direct-token",
				PropServiceAccountTokenRef: "targets/t1/sa",
			},
		})
		got, err := resolvePlatformTokenOptional(ctx, vault, target)
		if err != nil {
			t.Fatalf("resolvePlatformTokenOptional: %v", err)
		}
		if got != "direct-token" {
			t.Fatalf("token = %q, want direct-token", got)
		}
	})

	t.Run("vault ref", func(t *testing.T) {
		vault := &agentTestVault{
			secrets: map[domain.SecretRef][]byte{"targets/t1/sa": []byte("vault-token")},
		}
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{
				PropServiceAccountTokenRef: "targets/t1/sa",
			},
		})
		got, err := resolvePlatformTokenOptional(ctx, vault, target)
		if err != nil {
			t.Fatalf("resolvePlatformTokenOptional: %v", err)
		}
		if got != "vault-token" {
			t.Fatalf("token = %q, want vault-token", got)
		}
	})

	t.Run("missing credentials returns empty", func(t *testing.T) {
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{},
		})
		got, err := resolvePlatformTokenOptional(ctx, nil, target)
		if err != nil {
			t.Fatalf("resolvePlatformTokenOptional: %v", err)
		}
		if got != "" {
			t.Fatalf("token = %q, want empty", got)
		}
	})

	t.Run("ref without vault", func(t *testing.T) {
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{
				PropServiceAccountTokenRef: "targets/t1/sa",
			},
		})
		_, err := resolvePlatformTokenOptional(ctx, nil, target)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "no vault configured") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("vault get error", func(t *testing.T) {
		vault := &agentTestVault{err: errors.New("vault unavailable")}
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{
				PropServiceAccountTokenRef: "targets/t1/sa",
			},
		})
		_, err := resolvePlatformTokenOptional(ctx, vault, target)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "vault unavailable") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestBuildPlatformRESTConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("success with ca", func(t *testing.T) {
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{
				PropAPIServer:           "https://cluster.example:6443",
				PropCACert:              "pem-bytes",
				PropServiceAccountToken: "tok",
			},
		})
		cfg, err := BuildPlatformRESTConfig(ctx, nil, target)
		if err != nil {
			t.Fatalf("BuildPlatformRESTConfig: %v", err)
		}
		if cfg.Host != "https://cluster.example:6443" {
			t.Fatalf("Host = %q", cfg.Host)
		}
		if cfg.BearerToken != "tok" {
			t.Fatalf("BearerToken = %q", cfg.BearerToken)
		}
		if string(cfg.TLSClientConfig.CAData) != "pem-bytes" {
			t.Fatalf("CAData = %q", cfg.TLSClientConfig.CAData)
		}
		if cfg.Timeout != defaultDeliveryClientTimeout {
			t.Fatalf("Timeout = %v, want %v", cfg.Timeout, defaultDeliveryClientTimeout)
		}
	})

	t.Run("missing api server", func(t *testing.T) {
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{
				PropServiceAccountToken: "tok",
			},
		})
		_, err := BuildPlatformRESTConfig(ctx, nil, target)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("missing credentials", func(t *testing.T) {
		target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "t1", Type: TargetType, Name: "t1",
			Properties: map[string]string{
				PropAPIServer: "https://cluster.example",
			},
		})
		_, err := BuildPlatformRESTConfig(ctx, nil, target)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), PropServiceAccountToken) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestBuildCallerRESTConfig_IncludesCA(t *testing.T) {
	t.Parallel()
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: TargetType, Name: "t1",
		Properties: map[string]string{
			PropAPIServer: "https://cluster.example",
			PropCACert:    "ca-pem",
		},
	})
	cfg, err := buildCallerRESTConfig(target, "caller-token")
	if err != nil {
		t.Fatalf("buildCallerRESTConfig: %v", err)
	}
	if cfg.BearerToken != "caller-token" {
		t.Fatalf("BearerToken = %q", cfg.BearerToken)
	}
	if string(cfg.TLSClientConfig.CAData) != "ca-pem" {
		t.Fatalf("CAData = %q", cfg.TLSClientConfig.CAData)
	}
	if cfg.Timeout != defaultDeliveryClientTimeout {
		t.Fatalf("Timeout = %v, want %v", cfg.Timeout, defaultDeliveryClientTimeout)
	}
}

func TestBuildCallerRESTConfig_MissingAPIServer(t *testing.T) {
	t.Parallel()
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: TargetType, Name: "t1",
		Properties: map[string]string{},
	})
	_, err := buildCallerRESTConfig(target, "tok")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeliverAsync_ReportsFailureWhenAPIServerMissing(t *testing.T) {
	t.Parallel()
	// Call the async helper directly: Deliver rejects missing api_server
	// synchronously, so this branch is otherwise unreachable.
	reporter := &deliveryRecordingReporter{}
	a := NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: TargetType, Name: "t1",
		Properties: map[string]string{},
	})
	a.deliverAsync(context.Background(), target, "d1", 1, nil, domain.DeliveryAuth{Token: "tok"})
	if reporter.result.State != domain.DeliveryStateFailed {
		t.Fatalf("State = %q, want Failed; msg=%q", reporter.result.State, reporter.result.Message)
	}
}

func TestDeleteManifests_PropagatesDeleteError(t *testing.T) {
	t.Parallel()
	a := NewDeliveryAgent(nopReporterForInternal{})
	cfg := &rest.Config{Host: "https://127.0.0.1:1", BearerToken: "tok"}
	err := a.deleteManifests(context.Background(), cfg, []domain.Manifest{{
		ManifestType: ManifestManifestType,
		Raw:          json.RawMessage(jsonRawCM("doomed")),
	}})
	if err == nil {
		t.Fatal("expected delete error against unreachable API")
	}
	if !strings.Contains(err.Error(), "delete manifest") {
		t.Fatalf("error = %v", err)
	}
}

func TestDeleteManifests_InvalidManifestJSON(t *testing.T) {
	t.Parallel()
	a := NewDeliveryAgent(nopReporterForInternal{})
	cfg := &rest.Config{Host: "https://127.0.0.1:6443", BearerToken: "tok"}
	err := a.deleteManifests(context.Background(), cfg, []domain.Manifest{{
		ManifestType: ManifestManifestType,
		Raw:          json.RawMessage(`{not-json`),
	}})
	if err == nil {
		t.Fatal("expected error for invalid manifest JSON")
	}
}

func TestDeliveryAgentOptions(t *testing.T) {
	t.Parallel()
	vault := &agentTestVault{secrets: map[domain.SecretRef][]byte{}}
	resolver := &domain.KeyResolver{}
	a := NewDeliveryAgent(nopReporterForInternal{},
		WithVault(vault),
		WithKeyResolver(resolver),
		WithHTTPClient(nil), // explicit nil is fine; option still applied
	)
	if a.vault != vault {
		t.Fatal("WithVault not applied")
	}
	if a.keyResolver != resolver {
		t.Fatal("WithKeyResolver not applied")
	}
}

func TestDeliverAsyncPlatform_ReportsFailureWithoutCredentials(t *testing.T) {
	t.Parallel()
	reporter := &deliveryRecordingReporter{}
	a := NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: TargetType, Name: "t1",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})
	a.deliverAsyncPlatform(context.Background(), target, "d1", 1, nil)
	if reporter.result.State != domain.DeliveryStateFailed {
		t.Fatalf("State = %q, want Failed; msg=%q", reporter.result.State, reporter.result.Message)
	}
	if !strings.Contains(reporter.result.Message, "build platform") {
		t.Fatalf("message = %q", reporter.result.Message)
	}
}

func TestDeliverAsyncPlatform_EmptyManifestsSucceeds(t *testing.T) {
	t.Parallel()
	reporter := &deliveryRecordingReporter{}
	a := NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: TargetType, Name: "t1",
		Properties: map[string]string{
			"api_server":            "https://127.0.0.1:6443",
			"service_account_token": "tok",
		},
	})
	a.deliverAsyncPlatform(context.Background(), target, "d1", 1, nil)
	if reporter.result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want Delivered; msg=%q", reporter.result.State, reporter.result.Message)
	}
}

func TestApplyManifests_ReportsProgress(t *testing.T) {
	t.Parallel()
	reporter := &deliveryRecordingReporter{}
	a := NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: TargetType, Name: "t1",
	})
	// Unreachable API server: client construction succeeds, apply fails,
	// but progress for the first manifest is still reported.
	cfg := &rest.Config{Host: "https://127.0.0.1:1", BearerToken: "tok"}
	manifests := []domain.Manifest{{
		ManifestType: ManifestManifestType,
		Raw:          json.RawMessage(jsonRawCM("progress-cm")),
	}}
	a.applyManifests(context.Background(), target, "d1", 1, cfg, manifests)
	if len(reporter.events) == 0 {
		t.Fatal("expected at least one progress event")
	}
	if reporter.events[0].Kind != domain.DeliveryEventProgress {
		t.Fatalf("event kind = %q", reporter.events[0].Kind)
	}
	if reporter.result.State != domain.DeliveryStateFailed && reporter.result.State != domain.DeliveryStateAuthFailed {
		t.Fatalf("State = %q, want Failed or AuthFailed; msg=%q", reporter.result.State, reporter.result.Message)
	}
}

func TestDeleteManifests_EmptyManifestsSucceeds(t *testing.T) {
	t.Parallel()
	a := NewDeliveryAgent(nopReporterForInternal{})
	// Invalid host still builds clients; empty manifests succeed.
	cfg := &rest.Config{Host: "https://127.0.0.1:6443", BearerToken: "tok"}
	if err := a.deleteManifests(context.Background(), cfg, nil); err != nil {
		t.Fatalf("empty deleteManifests: %v", err)
	}
}

func TestVerifierForTarget_InvalidTrustBundleJSON(t *testing.T) {
	t.Parallel()
	a := NewDeliveryAgent(nopReporterForInternal{})
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: TargetType, Name: "t1",
		Properties: map[string]string{
			"trust_bundle": "{not-json",
		},
	})
	_, err := a.verifierForTarget(target)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "parse trust_bundle") {
		t.Fatalf("error = %v", err)
	}
}

func jsonRawCM(name string) []byte {
	return []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"` + name + `","namespace":"default"},"data":{"k":"v"}}`)
}

// nopReporterForInternal is a discard reporter for internal tests.
type nopReporterForInternal struct{}

func (nopReporterForInternal) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}
func (nopReporterForInternal) ReportResult(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryResult) error {
	return nil
}
func (nopReporterForInternal) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

type deliveryRecordingReporter struct {
	events []domain.DeliveryEvent
	result domain.DeliveryResult
}

func (r *deliveryRecordingReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	r.events = append(r.events, event)
	return nil
}
func (r *deliveryRecordingReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.result = result
	return nil
}
func (r *deliveryRecordingReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}
