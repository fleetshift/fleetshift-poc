package aioinit

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	defaultGCPHCPRenderer  = "/usr/local/bin/render-gcphcp-config.sh"
	defaultGCPHCPConfigOut = "/data/gcphcp.yaml"
)

// GCPHCPResult is the addon list and optional gcphcp config path after ResolveGCPHCP.
type GCPHCPResult struct {
	Addons       string
	GCPHCPConfig string
}

// ResolveGCPHCP applies AIO GCP HCP intent (gateway/config → addon list + optional rendered config).
func ResolveGCPHCP() (GCPHCPResult, error) {
	enabledSet, enabledOn, err := parseGCPHCPEnabled(os.Getenv("GCPHCP_ENABLED"))
	if err != nil {
		return GCPHCPResult{}, err
	}
	if enabledSet && !enabledOn {
		return GCPHCPResult{Addons: resolveServerAddons("kind,kubernetes")}, nil
	}
	if cfg := strings.TrimSpace(os.Getenv("GCPHCP_CONFIG")); cfg != "" {
		return GCPHCPResult{Addons: resolveServerAddons("kind,kubernetes,gcphcp"), GCPHCPConfig: cfg}, nil
	}
	if gw := strings.TrimSpace(os.Getenv("GCPHCP_GATEWAY_URL")); gw != "" {
		out := getenvDefault("GCPHCP_CONFIG_OUT", defaultGCPHCPConfigOut)
		renderer := getenvDefault("RENDER_GCPHCP_CONFIG", defaultGCPHCPRenderer)
		cmd := exec.Command(renderer, "--output", out)
		cmd.Env = append(os.Environ(), "GCPHCP_ENABLED=true")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return GCPHCPResult{}, fmt.Errorf("render gcphcp config: %w", err)
		}
		return GCPHCPResult{Addons: resolveServerAddons("kind,kubernetes,gcphcp"), GCPHCPConfig: out}, nil
	}
	if enabledSet && enabledOn {
		return GCPHCPResult{}, fmt.Errorf("GCP HCP was requested but GCPHCP_GATEWAY_URL is not set")
	}
	if hasGCPHCPOptionalOverrides() {
		return GCPHCPResult{}, fmt.Errorf("GCP HCP optional overrides were set without GCPHCP_GATEWAY_URL or GCPHCP_CONFIG")
	}
	return GCPHCPResult{Addons: resolveServerAddons("kind,kubernetes")}, nil
}

// parseGCPHCPEnabled reports whether GCPHCP_ENABLED is set and, if so, whether it is on.
// Unset/empty means not set (gateway URL may still enable). Non-empty values use strconv.ParseBool.
func parseGCPHCPEnabled(raw string) (set, on bool, err error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return false, false, nil
	}
	on, err = strconv.ParseBool(v)
	if err != nil {
		return false, false, fmt.Errorf("GCPHCP_ENABLED: %w", err)
	}
	return true, on, nil
}

// resolveServerAddons returns FLEETSHIFT_SERVER_ADDONS when set, otherwise desired.
func resolveServerAddons(desired string) string {
	if v := strings.TrimSpace(os.Getenv("FLEETSHIFT_SERVER_ADDONS")); v != "" {
		return v
	}
	return desired
}

// hasGCPHCPOptionalOverrides reports whether any GCPHCP_* override env is set.
func hasGCPHCPOptionalOverrides() bool {
	for _, k := range []string{
		"GCPHCP_GATEWAY_AUDIENCE", "GCPHCP_TARGET_ID", "GCPHCP_GCP_PROJECT",
		"GCPHCP_GCP_REGION", "GCPHCP_WORKFORCE_POOL", "GCPHCP_WORKFORCE_PROVIDER",
		"GCPHCP_BROKER_SA_EMAIL",
	} {
		if strings.TrimSpace(os.Getenv(k)) != "" {
			return true
		}
	}
	return false
}

// getenvDefault returns the trimmed env value for k, or def when empty.
func getenvDefault(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
