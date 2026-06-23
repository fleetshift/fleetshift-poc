package domain

import (
	"errors"
	"testing"
)

func TestNewResourceType(t *testing.T) {
	tests := []struct {
		name     string
		service  ServiceName
		typeName string
		want     ResourceType
		wantErr  bool
	}{
		{
			name:     "valid",
			service:  "kind.fleetshift.io",
			typeName: "Cluster",
			want:     "kind.fleetshift.io/Cluster",
		},
		{
			name:     "multi-segment service name",
			service:  "gcphcp.fleetshift.io",
			typeName: "Cluster",
			want:     "gcphcp.fleetshift.io/Cluster",
		},
		{
			name:     "empty type name rejected",
			service:  "kind.fleetshift.io",
			typeName: "",
			wantErr:  true,
		},
		{
			name:     "lowercase type name rejected",
			service:  "kind.fleetshift.io",
			typeName: "cluster",
			wantErr:  true,
		},
		{
			name:     "type name with slash rejected",
			service:  "kind.fleetshift.io",
			typeName: "Cluster/Sub",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewResourceType(tt.service, tt.typeName)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseResourceType(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    ResourceType
		wantErr bool
	}{
		{
			name:  "valid kind cluster",
			input: "kind.fleetshift.io/Cluster",
			want:  "kind.fleetshift.io/Cluster",
		},
		{
			name:  "valid gcphcp cluster",
			input: "gcphcp.fleetshift.io/Cluster",
			want:  "gcphcp.fleetshift.io/Cluster",
		},
		{
			name:    "empty rejected",
			input:   "",
			wantErr: true,
		},
		{
			name:    "no slash rejected",
			input:   "api.kind.cluster",
			wantErr: true,
		},
		{
			name:    "too many slashes rejected",
			input:   "kind.fleetshift.io/Cluster/Sub",
			wantErr: true,
		},
		{
			name:    "empty service segment rejected",
			input:   "/Cluster",
			wantErr: true,
		},
		{
			name:    "empty type segment rejected",
			input:   "kind.fleetshift.io/",
			wantErr: true,
		},
		{
			name:    "lowercase type segment rejected",
			input:   "kind.fleetshift.io/cluster",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseResourceType(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResourceType_Accessors(t *testing.T) {
	rt := ResourceType("kind.fleetshift.io/Cluster")

	if got := rt.ServiceName(); got != "kind.fleetshift.io" {
		t.Errorf("ServiceName() = %q, want %q", got, "kind.fleetshift.io")
	}
	if got := rt.TypeName(); got != "Cluster" {
		t.Errorf("TypeName() = %q, want %q", got, "Cluster")
	}
}

func TestResourceType_Accessors_Malformed(t *testing.T) {
	rt := ResourceType("no-slash-here")

	if got := rt.ServiceName(); got != "" {
		t.Errorf("ServiceName() on malformed = %q, want empty", got)
	}
	if got := rt.TypeName(); got != "" {
		t.Errorf("TypeName() on malformed = %q, want empty", got)
	}
}
