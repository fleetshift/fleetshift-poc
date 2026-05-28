package gcphcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestClusterAccess_MintCredential_Success(t *testing.T) {
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("STS method = %s, want POST", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.FormValue("subject_token"); got != "caller-jwt" {
			t.Errorf("subject_token = %q, want caller-jwt", got)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"access_token": "workforce-access-tok", "expires_in": 3600})
	}))
	defer sts.Close()

	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("IAM method = %s, want POST", r.Method)
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer workforce-access-tok" {
			t.Errorf("IAM Authorization = %q, want 'Bearer workforce-access-tok'", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"token": "broker-id-token"})
	}))
	defer iam.Close()

	ca := gcphcp.NewClusterAccess(gcphcp.GatewayConfig{
		URL:      "https://gateway.example.com",
		Audience: "gateway-audience",
	}, gcphcp.WithEndpoints(sts.URL, iam.URL))

	target := domain.TargetInfo{
		ID:   "gcphcp-target",
		Type: "gcphcp",
		Properties: map[string]string{
			"workforce_pool":     "my-pool",
			"workforce_provider": "my-provider",
			"gcp_project":        "my-project",
			"broker_sa_email":    "broker@my-project.iam.gserviceaccount.com",
		},
	}

	cred, err := ca.MintCredential(context.Background(), "caller-jwt", target)
	if err != nil {
		t.Fatalf("MintCredential: %v", err)
	}
	if cred.Token != "broker-id-token" {
		t.Errorf("Token = %q, want broker-id-token", cred.Token)
	}
	if cred.Expiration.IsZero() {
		t.Error("Expiration should not be zero")
	}
}

func TestClusterAccess_MintCredential_EmptyToken(t *testing.T) {
	ca := gcphcp.NewClusterAccess(gcphcp.GatewayConfig{
		Audience: "gateway-audience",
	})

	target := domain.TargetInfo{
		ID:   "gcphcp-target",
		Type: "gcphcp",
		Properties: map[string]string{
			"workforce_pool":     "my-pool",
			"workforce_provider": "my-provider",
			"gcp_project":        "my-project",
			"broker_sa_email":    "broker@my-project.iam.gserviceaccount.com",
		},
	}

	_, err := ca.MintCredential(context.Background(), "", target)
	if err == nil {
		t.Fatal("expected error for empty caller token")
	}
}

func TestClusterAccess_MintCredential_STSFailure(t *testing.T) {
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid_request"}`))
	}))
	defer sts.Close()

	ca := gcphcp.NewClusterAccess(gcphcp.GatewayConfig{
		Audience: "gateway-audience",
	}, gcphcp.WithEndpoints(sts.URL, "http://unused"))

	target := domain.TargetInfo{
		ID:   "gcphcp-target",
		Type: "gcphcp",
		Properties: map[string]string{
			"workforce_pool":     "my-pool",
			"workforce_provider": "my-provider",
			"gcp_project":        "my-project",
			"broker_sa_email":    "broker@my-project.iam.gserviceaccount.com",
		},
	}

	_, err := ca.MintCredential(context.Background(), "caller-jwt", target)
	if err == nil {
		t.Fatal("expected error when STS returns error")
	}
}
