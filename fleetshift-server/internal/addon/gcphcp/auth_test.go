package gcphcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

func TestBrokerAuth_ExchangeAndMint(t *testing.T) {
	// Setup fake STS server
	stsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST, got %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("Expected application/x-www-form-urlencoded, got %s", r.Header.Get("Content-Type"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Failed to read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		values, err := url.ParseQuery(string(body))
		if err != nil {
			t.Errorf("Failed to parse form: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Validate STS exchange parameters
		if got := values.Get("grant_type"); got != "urn:ietf:params:oauth:grant-type:token-exchange" {
			t.Errorf("Expected grant_type token-exchange, got %s", got)
		}
		if got := values.Get("requested_token_type"); got != "urn:ietf:params:oauth:token-type:access_token" {
			t.Errorf("Expected requested_token_type access_token, got %s", got)
		}
		if got := values.Get("subject_token_type"); got != "urn:ietf:params:oauth:token-type:jwt" {
			t.Errorf("Expected subject_token_type jwt, got %s", got)
		}
		if got := values.Get("scope"); got != "https://www.googleapis.com/auth/cloud-platform" {
			t.Errorf("Expected scope cloud-platform, got %s", got)
		}
		if got := values.Get("subject_token"); got != "fake-caller-token" {
			t.Errorf("Expected subject_token fake-caller-token, got %s", got)
		}
		expectedAudience := "//iam.googleapis.com/locations/global/workforcePools/test-pool/providers/test-provider"
		if got := values.Get("audience"); got != expectedAudience {
			t.Errorf("Expected audience %s, got %s", expectedAudience, got)
		}

		// Return fake workforce access token
		response := map[string]interface{}{
			"access_token": "fake-workforce-access-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer stsServer.Close()

	// Setup fake IAM server
	iamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST, got %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		expectedPath := "/v1/projects/-/serviceAccounts/broker@test-project.iam.gserviceaccount.com:generateIdToken"
		if r.URL.Path != expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		auth := r.Header.Get("Authorization")
		if auth != "Bearer fake-workforce-access-token" {
			t.Errorf("Expected Bearer fake-workforce-access-token, got %s", auth)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		userProject := r.Header.Get("x-goog-user-project")
		if userProject != "test-project" {
			t.Errorf("Expected x-goog-user-project test-project, got %s", userProject)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Failed to read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("Failed to parse JSON: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if req["audience"] != "test-gateway-audience" {
			t.Errorf("Expected audience test-gateway-audience, got %v", req["audience"])
		}
		if req["includeEmail"] != true {
			t.Errorf("Expected includeEmail true, got %v", req["includeEmail"])
		}

		// Return fake broker ID token
		response := map[string]interface{}{
			"token": "fake-broker-id-token",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer iamServer.Close()

	// Create BrokerAuth with test servers
	cfg := gcphcp.BrokerAuthConfig{
		WorkforcePool:     "test-pool",
		WorkforceProvider: "test-provider",
		GCPProject:        "test-project",
		BrokerSAEmail:     "broker@test-project.iam.gserviceaccount.com",
		GatewayAudience:   "test-gateway-audience",
		STSEndpoint:       stsServer.URL,
		IAMEndpoint:       iamServer.URL,
		HTTPClient:        http.DefaultClient,
	}

	auth := gcphcp.NewBrokerAuth(cfg)

	// Execute exchange
	ctx := context.Background()
	result, err := auth.Exchange(ctx, "fake-caller-token")
	if err != nil {
		t.Fatalf("Exchange failed: %v", err)
	}

	// Verify results
	if result.BrokerToken != "fake-broker-id-token" {
		t.Errorf("Expected BrokerToken fake-broker-id-token, got %s", result.BrokerToken)
	}
	if result.BrokerEmail != "broker@test-project.iam.gserviceaccount.com" {
		t.Errorf("Expected BrokerEmail broker@test-project.iam.gserviceaccount.com, got %s", result.BrokerEmail)
	}
	if result.WorkforceToken != "fake-workforce-access-token" {
		t.Errorf("Expected WorkforceToken fake-workforce-access-token, got %s", result.WorkforceToken)
	}
}

func TestBrokerAuth_STSFailure(t *testing.T) {
	// Setup fake STS server that returns 403
	stsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":"access_denied","error_description":"Invalid subject token"}`))
	}))
	defer stsServer.Close()

	// Create BrokerAuth with test server
	cfg := gcphcp.BrokerAuthConfig{
		WorkforcePool:     "test-pool",
		WorkforceProvider: "test-provider",
		GCPProject:        "test-project",
		BrokerSAEmail:     "broker@test-project.iam.gserviceaccount.com",
		GatewayAudience:   "test-gateway-audience",
		STSEndpoint:       stsServer.URL,
		IAMEndpoint:       "http://should-not-be-called.invalid",
		HTTPClient:        http.DefaultClient,
	}

	auth := gcphcp.NewBrokerAuth(cfg)

	// Execute exchange and expect failure
	ctx := context.Background()
	_, err := auth.Exchange(ctx, "fake-caller-token")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Verify error contains STS failure information
	errMsg := err.Error()
	if !strings.Contains(errMsg, "STS") && !strings.Contains(errMsg, "403") {
		t.Errorf("Expected error message to mention STS or 403, got: %s", errMsg)
	}
}

func TestWorkforceCredentialConfig(t *testing.T) {
	cfg := gcphcp.TargetConfig{
		ID:                "test-target",
		GCPProject:        "test-project",
		Region:            "us-central1",
		WorkforcePool:     "test-pool",
		WorkforceProvider: "test-provider",
		BrokerSAEmail:     "broker@test-project.iam.gserviceaccount.com",
	}

	subjectTokenFile := "/var/run/secrets/openshift/serviceaccount/token"

	result := gcphcp.WorkforceCredentialConfig(cfg, subjectTokenFile)

	// Parse JSON to verify structure
	var credConfig map[string]interface{}
	if err := json.Unmarshal(result, &credConfig); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Verify external_account type
	if credConfig["type"] != "external_account" {
		t.Errorf("Expected type external_account, got %v", credConfig["type"])
	}

	// Verify audience
	expectedAudience := "//iam.googleapis.com/locations/global/workforcePools/test-pool/providers/test-provider"
	if credConfig["audience"] != expectedAudience {
		t.Errorf("Expected audience %s, got %v", expectedAudience, credConfig["audience"])
	}

	// Verify subject_token_type
	if credConfig["subject_token_type"] != "urn:ietf:params:oauth:token-type:jwt" {
		t.Errorf("Expected subject_token_type jwt, got %v", credConfig["subject_token_type"])
	}

	// Verify token_url
	if credConfig["token_url"] != "https://sts.googleapis.com/v1/token" {
		t.Errorf("Expected token_url https://sts.googleapis.com/v1/token, got %v", credConfig["token_url"])
	}

	// Verify credential_source
	credSource, ok := credConfig["credential_source"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected credential_source to be a map, got %T", credConfig["credential_source"])
	}

	fileConfig, ok := credSource["file"].(string)
	if !ok || fileConfig != subjectTokenFile {
		t.Errorf("Expected credential_source.file %s, got %v", subjectTokenFile, credSource["file"])
	}

	// Verify service_account_impersonation_url
	expectedImpersonationURL := fmt.Sprintf("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken", cfg.BrokerSAEmail)
	if credConfig["service_account_impersonation_url"] != expectedImpersonationURL {
		t.Errorf("Expected service_account_impersonation_url %s, got %v", expectedImpersonationURL, credConfig["service_account_impersonation_url"])
	}
}
