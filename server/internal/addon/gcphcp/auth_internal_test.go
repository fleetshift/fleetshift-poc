package gcphcp

import (
	"net/http"
	"testing"
	"time"
)

func TestNewBrokerAuth_DefaultHTTPClientTimeout(t *testing.T) {
	auth := NewBrokerAuth(BrokerAuthConfig{})

	if auth.cfg.HTTPClient == nil {
		t.Fatal("expected default HTTP client to be configured")
	}
	if auth.cfg.HTTPClient.Timeout != 30*time.Second {
		t.Fatalf("expected default timeout 30s, got %v", auth.cfg.HTTPClient.Timeout)
	}
}

func TestNewBrokerAuth_PreservesProvidedHTTPClient(t *testing.T) {
	customClient := &http.Client{Timeout: 5 * time.Second}

	auth := NewBrokerAuth(BrokerAuthConfig{HTTPClient: customClient})

	if auth.cfg.HTTPClient != customClient {
		t.Fatal("expected provided HTTP client to be preserved")
	}
}
