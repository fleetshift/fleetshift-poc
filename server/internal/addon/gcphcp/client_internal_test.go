package gcphcp

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type failingReadCloser struct{}

func (failingReadCloser) Read(_ []byte) (int, error) {
	return 0, errors.New("boom")
}

func (failingReadCloser) Close() error {
	return nil
}

func TestNewCLSClient_DefaultHTTPClientTimeout(t *testing.T) {
	client := NewCLSClient("https://example.invalid", "token", "user@example.com", nil)

	if client.httpClient == nil {
		t.Fatal("expected default HTTP client to be configured")
	}
	if client.httpClient.Timeout != 30*time.Second {
		t.Fatalf("expected default timeout 30s, got %v", client.httpClient.Timeout)
	}
}

func TestCLSClient_DoJSON_ReturnsBodyReadErrors(t *testing.T) {
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       failingReadCloser{},
				Request:    req,
			}, nil
		}),
	}

	client := NewCLSClient("https://example.invalid", "token", "user@example.com", httpClient)
	_, err := client.GetCluster(context.Background(), "cluster-123")
	if err == nil {
		t.Fatal("expected read error")
	}
	if !strings.Contains(err.Error(), "read CLS response body") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewCLSClient_PreservesProvidedHTTPClient(t *testing.T) {
	customClient := &http.Client{Timeout: 5 * time.Second}

	client := NewCLSClient("https://example.invalid", "token", "user@example.com", customClient)

	if client.httpClient != customClient {
		t.Fatal("expected provided HTTP client to be preserved")
	}
}
