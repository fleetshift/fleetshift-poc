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
	dep, err := parseDeployment(`{"name":"deployments/idp-trust-default","state":"STATE_CREATING","pauseReason":"apply manifest 1: auth_failed"}`)
	if err != nil {
		t.Fatal(err)
	}
	if dep.Name != "deployments/idp-trust-default" || dep.State != "STATE_CREATING" {
		t.Fatalf("got %+v", dep)
	}
	if dep.PauseReason != "apply manifest 1: auth_failed" {
		t.Fatalf("PauseReason = %q", dep.PauseReason)
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

func TestDeploymentTerminalFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dep  deploymentView
		want string
	}{
		{name: "creating", dep: deploymentView{Name: "deployments/a", State: "STATE_CREATING"}},
		{name: "active", dep: deploymentView{Name: "deployments/a", State: "STATE_ACTIVE"}},
		{
			name: "paused creating",
			dep:  deploymentView{Name: "deployments/a", State: "STATE_CREATING", PauseReason: "auth_failed"},
			want: "deployment deployments/a paused (STATE_CREATING): auth_failed",
		},
		{
			name: "failed",
			dep:  deploymentView{Name: "deployments/a", State: "STATE_FAILED"},
			want: "deployment deployments/a STATE_FAILED",
		},
		{
			name: "failed with reason",
			dep:  deploymentView{Name: "deployments/a", State: "STATE_FAILED", PauseReason: "boom"},
			want: "deployment deployments/a STATE_FAILED: boom",
		},
		{
			name: "blank pause is not terminal",
			dep:  deploymentView{Name: "deployments/a", State: "STATE_CREATING", PauseReason: "  "},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := deploymentTerminalFailure(tt.dep); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}
