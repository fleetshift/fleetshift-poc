package gcphcp

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestStartLocalSTSForwarder_ServesWorkforceTokenInSTSFormat(t *testing.T) {
	forwarder, err := startLocalSTSForwarder(
		"workforce-token",
		time.Now().Add(45*time.Minute),
		"workspace-nonce",
		"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
	)
	if err != nil {
		t.Fatalf("startLocalSTSForwarder() error = %v", err)
	}
	defer forwarder.Close()

	resp, err := http.PostForm(forwarder.URL(), url.Values{
		"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"audience":             {"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider"},
		"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
		"subject_token":        {"workspace-nonce"},
		"scope":                {"https://www.googleapis.com/auth/cloud-platform"},
	})
	if err != nil {
		t.Fatalf("http.PostForm() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode error = %v", err)
	}

	if got := body["access_token"]; got != "workforce-token" {
		t.Fatalf("access_token = %v, want workforce-token", got)
	}
	if got := body["token_type"]; got != "Bearer" {
		t.Fatalf("token_type = %v, want Bearer", got)
	}
	if got := body["issued_token_type"]; got != "urn:ietf:params:oauth:token-type:access_token" {
		t.Fatalf("issued_token_type = %v, want access_token token type", got)
	}

	expiresIn, ok := body["expires_in"].(float64)
	if !ok {
		t.Fatalf("expires_in type = %T, want float64", body["expires_in"])
	}
	if expiresIn <= 0 {
		t.Fatalf("expires_in = %v, want > 0", expiresIn)
	}
}

func TestStartLocalSTSForwarder_RejectsWrongSubjectToken(t *testing.T) {
	forwarder, err := startLocalSTSForwarder(
		"workforce-token",
		time.Now().Add(45*time.Minute),
		"workspace-nonce",
		"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
	)
	if err != nil {
		t.Fatalf("startLocalSTSForwarder() error = %v", err)
	}
	defer forwarder.Close()

	resp, err := http.PostForm(forwarder.URL(), url.Values{
		"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"audience":             {"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider"},
		"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
		"subject_token":        {"wrong-workspace-nonce"},
		"scope":                {"https://www.googleapis.com/auth/cloud-platform"},
	})
	if err != nil {
		t.Fatalf("http.PostForm() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestStartLocalSTSForwarder_ReturnsInvalidGrantWhenWorkforceTokenExpired(t *testing.T) {
	forwarder, err := startLocalSTSForwarder(
		"expired-workforce-token",
		time.Now().Add(-time.Minute),
		"workspace-nonce",
		"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
	)
	if err != nil {
		t.Fatalf("startLocalSTSForwarder() error = %v", err)
	}
	defer forwarder.Close()

	resp, err := http.PostForm(forwarder.URL(), url.Values{
		"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"audience":             {"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider"},
		"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
		"subject_token":        {"workspace-nonce"},
		"scope":                {"https://www.googleapis.com/auth/cloud-platform"},
	})
	if err != nil {
		t.Fatalf("http.PostForm() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	var body struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode error = %v", err)
	}
	if body.Error != "invalid_grant" {
		t.Fatalf("error = %q, want invalid_grant", body.Error)
	}
	if !strings.Contains(body.ErrorDescription, "expired") {
		t.Fatalf("error_description = %q, want expired context", body.ErrorDescription)
	}
}

func TestStartLocalSTSForwarder_RejectsUnexpectedSTSFields(t *testing.T) {
	forwarder, err := startLocalSTSForwarder(
		"workforce-token",
		time.Now().Add(45*time.Minute),
		"workspace-nonce",
		"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
	)
	if err != nil {
		t.Fatalf("startLocalSTSForwarder() error = %v", err)
	}
	defer forwarder.Close()

	testCases := []struct {
		name  string
		field string
		value string
	}{
		{
			name:  "audience",
			field: "audience",
			value: "//iam.googleapis.com/locations/global/workforcePools/pool/providers/other",
		},
		{
			name:  "requested token type",
			field: "requested_token_type",
			value: "urn:ietf:params:oauth:token-type:id_token",
		},
		{
			name:  "subject token type",
			field: "subject_token_type",
			value: "urn:ietf:params:oauth:token-type:access_token",
		},
		{
			name:  "scope",
			field: "scope",
			value: "https://www.googleapis.com/auth/compute",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{
				"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
				"audience":             {"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider"},
				"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
				"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
				"subject_token":        {"workspace-nonce"},
				"scope":                {"https://www.googleapis.com/auth/cloud-platform"},
			}
			form.Set(tc.field, tc.value)

			resp, err := http.PostForm(forwarder.URL(), form)
			if err != nil {
				t.Fatalf("http.PostForm() error = %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestStartLocalSTSForwarder_ConfiguresHTTPServerTimeouts(t *testing.T) {
	forwarder, err := startLocalSTSForwarder(
		"workforce-token",
		time.Now().Add(45*time.Minute),
		"workspace-nonce",
		"//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
	)
	if err != nil {
		t.Fatalf("startLocalSTSForwarder() error = %v", err)
	}
	defer forwarder.Close()

	if forwarder.server.ReadHeaderTimeout <= 0 {
		t.Fatalf("ReadHeaderTimeout = %v, want > 0", forwarder.server.ReadHeaderTimeout)
	}
	if forwarder.server.IdleTimeout <= 0 {
		t.Fatalf("IdleTimeout = %v, want > 0", forwarder.server.IdleTimeout)
	}
	if forwarder.server.MaxHeaderBytes <= 0 {
		t.Fatalf("MaxHeaderBytes = %d, want > 0", forwarder.server.MaxHeaderBytes)
	}
}
