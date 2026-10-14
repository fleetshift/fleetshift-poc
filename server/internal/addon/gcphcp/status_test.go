package gcphcp_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

func TestResolveGuestAPIEndpoint_FromControllerStatus(t *testing.T) {
	statusData := map[string]any{
		"controller_status": []any{
			map[string]any{
				"name": "cls-hypershift-client",
				"conditions": []any{
					map[string]any{
						"type":    "SomeOtherCondition",
						"message": "not an endpoint",
					},
					map[string]any{
						"type":    "APIServer",
						"message": "https://guest-api.example.com:6443",
					},
				},
			},
			map[string]any{
				"name": "another-controller",
				"conditions": []any{
					map[string]any{
						"type":    "APIServer",
						"message": "should not be picked",
					},
				},
			},
		},
	}

	endpoint, err := gcphcp.ResolveGuestAPIEndpoint(statusData)
	if err != nil {
		t.Fatalf("ResolveGuestAPIEndpoint() error = %v", err)
	}
	want := "https://guest-api.example.com:6443"
	if endpoint != want {
		t.Errorf("ResolveGuestAPIEndpoint() = %q, want %q", endpoint, want)
	}
}

func TestResolveGuestAPIEndpoint_FallbackToAPIEndpoint(t *testing.T) {
	statusData := map[string]any{
		"api_endpoint": "https://fallback-api.example.com:6443",
		"controller_status": []any{
			map[string]any{
				"name": "cls-hypershift-client",
				"conditions": []any{
					map[string]any{
						"type":    "SomeOtherCondition",
						"message": "not an endpoint",
					},
				},
			},
		},
	}

	endpoint, err := gcphcp.ResolveGuestAPIEndpoint(statusData)
	if err != nil {
		t.Fatalf("ResolveGuestAPIEndpoint() error = %v", err)
	}
	want := "https://fallback-api.example.com:6443"
	if endpoint != want {
		t.Errorf("ResolveGuestAPIEndpoint() = %q, want %q", endpoint, want)
	}
}

func TestResolveGuestAPIEndpoint_NotFound(t *testing.T) {
	tests := []struct {
		name       string
		statusData map[string]any
	}{
		{
			name:       "empty status",
			statusData: map[string]any{},
		},
		{
			name: "no controller_status and no api_endpoint",
			statusData: map[string]any{
				"phase": "Ready",
			},
		},
		{
			name: "controller_status without APIServer condition",
			statusData: map[string]any{
				"controller_status": []any{
					map[string]any{
						"name": "cls-hypershift-client",
						"conditions": []any{
							map[string]any{
								"type":    "SomeOtherCondition",
								"message": "not an endpoint",
							},
						},
					},
				},
			},
		},
		{
			name: "APIServer condition message does not start with https://",
			statusData: map[string]any{
				"controller_status": []any{
					map[string]any{
						"name": "cls-hypershift-client",
						"conditions": []any{
							map[string]any{
								"type":    "APIServer",
								"message": "not ready yet",
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, err := gcphcp.ResolveGuestAPIEndpoint(tt.statusData)
			if err == nil {
				t.Errorf("ResolveGuestAPIEndpoint() = %q, want error", endpoint)
			}
		})
	}
}
