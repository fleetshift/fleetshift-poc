package steps

import (
	"strings"
	"testing"
)

func TestParseGatewayDeploymentNames(t *testing.T) {
	t.Parallel()
	names, err := parseGatewayDeploymentNames([]byte(`{"deployments":[{"name":"deployments/idp-trust-default"},{"name":"deployments/other"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "deployments/idp-trust-default" || names[1] != "deployments/other" {
		t.Fatalf("got %v", names)
	}
}

func TestParseGatewayDeploymentNames_Empty(t *testing.T) {
	t.Parallel()
	names, err := parseGatewayDeploymentNames([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("got %v", names)
	}
}

func TestParseGatewayDeploymentNames_Invalid(t *testing.T) {
	t.Parallel()
	_, err := parseGatewayDeploymentNames([]byte("{"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "parse gateway") {
		t.Fatalf("error = %v", err)
	}
}
