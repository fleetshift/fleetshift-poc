package application_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type fakeClusterAccessProvider struct {
	token string
}

func (f *fakeClusterAccessProvider) MintCredential(_ context.Context, _ string, _ domain.TargetInfo) (*domain.ClusterCredential, error) {
	return &domain.ClusterCredential{Token: f.token, Expiration: time.Now().Add(time.Hour)}, nil
}

func TestClusterAccessRegistry_RegisterAndLookup(t *testing.T) {
	reg := application.NewClusterAccessRegistry()
	provider := &fakeClusterAccessProvider{token: "tok-a"}

	reg.Register("gcphcp", provider)

	got := reg.ClusterAccessProvider("gcphcp")
	if got != provider {
		t.Fatalf("ClusterAccessProvider(gcphcp) = %v, want %v", got, provider)
	}
}

func TestClusterAccessRegistry_LookupMissingReturnsNil(t *testing.T) {
	reg := application.NewClusterAccessRegistry()

	got := reg.ClusterAccessProvider("nonexistent")
	if got != nil {
		t.Fatalf("ClusterAccessProvider(nonexistent) = %v, want nil", got)
	}
}

func TestClusterAccessRegistry_RegisterOverwrites(t *testing.T) {
	reg := application.NewClusterAccessRegistry()
	first := &fakeClusterAccessProvider{token: "first"}
	second := &fakeClusterAccessProvider{token: "second"}

	reg.Register("gcphcp", first)
	reg.Register("gcphcp", second)

	got := reg.ClusterAccessProvider("gcphcp")
	if got != second {
		t.Fatalf("ClusterAccessProvider(gcphcp) after overwrite = %v, want %v", got, second)
	}
}

func TestClusterAccessRegistry_Deregister(t *testing.T) {
	reg := application.NewClusterAccessRegistry()
	reg.Register("gcphcp", &fakeClusterAccessProvider{token: "tok"})

	reg.Deregister("gcphcp")

	got := reg.ClusterAccessProvider("gcphcp")
	if got != nil {
		t.Fatalf("ClusterAccessProvider(gcphcp) after Deregister = %v, want nil", got)
	}
}

func TestClusterAccessRegistry_DeregisterMissingIsNoop(t *testing.T) {
	reg := application.NewClusterAccessRegistry()
	reg.Deregister("nonexistent") // should not panic
}

func TestClusterAccessRegistry_MultipleTypes(t *testing.T) {
	reg := application.NewClusterAccessRegistry()
	a := &fakeClusterAccessProvider{token: "a"}
	b := &fakeClusterAccessProvider{token: "b"}

	reg.Register("gcphcp", a)
	reg.Register("kind", b)

	if got := reg.ClusterAccessProvider("gcphcp"); got != a {
		t.Errorf("gcphcp provider = %v, want %v", got, a)
	}
	if got := reg.ClusterAccessProvider("kind"); got != b {
		t.Errorf("kind provider = %v, want %v", got, b)
	}

	reg.Deregister("gcphcp")
	if got := reg.ClusterAccessProvider("gcphcp"); got != nil {
		t.Errorf("gcphcp after deregister = %v, want nil", got)
	}
	if got := reg.ClusterAccessProvider("kind"); got != b {
		t.Errorf("kind should be unaffected, got %v, want %v", got, b)
	}
}
