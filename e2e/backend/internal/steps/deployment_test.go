package steps

import (
	"testing"
)

func TestParseDeploymentList(t *testing.T) {
	t.Parallel()
	deps, err := parseDeploymentList(`[{"name":"deployments/idp-trust-default","state":"STATE_ACTIVE"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 1 {
		t.Fatalf("len = %d, want 1", len(deps))
	}
	if deps[0].Name != "deployments/idp-trust-default" || deps[0].State != "STATE_ACTIVE" {
		t.Fatalf("got %+v", deps[0])
	}
}

func TestParseDeploymentList_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := parseDeploymentList("{"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseDeployment(t *testing.T) {
	t.Parallel()
	dep, err := parseDeployment(`{"name":"deployments/idp-trust-default","state":"STATE_CREATING"}`)
	if err != nil {
		t.Fatal(err)
	}
	if dep.Name != "deployments/idp-trust-default" || dep.State != "STATE_CREATING" {
		t.Fatalf("got %+v", dep)
	}
}

func TestParseDeployment_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := parseDeployment("["); err == nil {
		t.Fatal("expected error")
	}
}

func TestJSONDeploymentName(t *testing.T) {
	t.Parallel()
	if got := jsonDeploymentName("idp-trust-default"); got != "deployments/idp-trust-default" {
		t.Fatalf("got %q", got)
	}
	if got := jsonDeploymentName("deployments/idp-trust-default"); got != "deployments/idp-trust-default" {
		t.Fatalf("got %q", got)
	}
}
