// Command aio-init is the AIO packaging initialization helper.
// It selects Dex-on vs Dex-off by issuer presence, renders sandbox PKI and
// peer Dex when needed (or forwards an external issuer/CA on Dex-off), and
// writes the ordinary fleetshift serve argv for s6 to exec.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
)

const (
	fleetshiftUID = 1000
	fleetshiftGID = 1000
	dexUID        = 1001
	dexGID        = 1001

	// dexEnabledFlag is written on Dex-on so the s6-rc dex longrun execs Dex
	// instead of parking on s6-pause (Dex-off).
	dexEnabledFlag = "/run/fleetshift/dex.enabled"
	runDir         = "/run/fleetshift"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "aio-init: %v\n", err)
		os.Exit(1)
	}
}

// run performs packaging init and writes /run/fleetshift/exec-serve.
func run() error {
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return err
	}
	if err := prepareDataLayout(); err != nil {
		return err
	}
	endpoints := aioinit.FixedEndpoints

	gcp, err := aioinit.ResolveGCPHCP()
	if err != nil {
		return err
	}

	issuerEnv := strings.TrimSpace(os.Getenv("OIDC_ISSUER_URL"))
	dexOn := issuerEnv == ""

	in := aioinit.ServeConfig{
		Endpoints:          endpoints,
		UIClientID:         strings.TrimSpace(os.Getenv("OIDC_UI_CLIENT_ID")),
		UIScope:            strings.TrimSpace(os.Getenv("OIDC_UI_SCOPE")),
		ResourceAudience:   strings.TrimSpace(os.Getenv("OIDC_RESOURCE_AUDIENCE")),
		EnrollmentAudience: strings.TrimSpace(os.Getenv("OIDC_KEY_ENROLLMENT_AUDIENCE")),
		RegistryID:         strings.TrimSpace(os.Getenv("OIDC_REGISTRY_ID")),
		RegistryExpr:       strings.TrimSpace(os.Getenv("OIDC_REGISTRY_SUBJECT_EXPRESSION")),
		PublicKeyExpr:      strings.TrimSpace(os.Getenv("OIDC_PUBLIC_KEY_CLAIM_EXPRESSION")),
		LogLevel:           strings.TrimSpace(os.Getenv("FLEETSHIFT_LOG_LEVEL")),
		Addons:             gcp.Addons,
		GCPHCPConfig:       gcp.GCPHCPConfig,
	}

	if dexOn {
		if err := enableDex(); err != nil {
			return err
		}
		sandboxPKI := aioinit.DefaultSandboxPKIPaths()
		if err := aioinit.EnsureSandboxPKI(sandboxPKI, dexUID, dexGID); err != nil {
			return fmt.Errorf("sandbox pki: %w", err)
		}
		if err := aioinit.InstallDexConfig(aioinit.DexRenderInput{
			Issuer:    aioinit.PeerDexIssuer,
			Endpoints: endpoints,
			TLSCert:   sandboxPKI.LeafCert,
			TLSKey:    sandboxPKI.LeafKey,
		}, aioinit.DefaultDexPaths(), dexUID, dexGID); err != nil {
			return fmt.Errorf("dex config: %w", err)
		}
		in.Issuer = aioinit.PeerDexIssuer
		in.CAFile = sandboxPKI.CACert
	} else {
		disableDex()
		in.Issuer = issuerEnv
		externalCA := strings.TrimSpace(os.Getenv("OIDC_CA_FILE"))
		if externalCA != "" {
			in.CAFile = externalCA
		}
	}

	in, err = aioinit.ApplyServeDefaults(in)
	if err != nil {
		return err
	}
	args := aioinit.ServeArgs(in)
	if err := aioinit.WriteServeExecScript(aioinit.ServeExecPath, args); err != nil {
		return fmt.Errorf("write serve script: %w", err)
	}
	if err := os.Chown(aioinit.ServeExecPath, 0, 0); err != nil {
		return err
	}
	if err := aioinit.ConfigureKindEnv(aioinit.KindEnvPath, dexOn, endpoints.DexListen); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "aio-init: dexOn=%v issuer=%s\n", dexOn, in.Issuer)
	return nil
}

// enableDex records that the s6-rc dex longrun should exec peer Dex.
func enableDex() error {
	return os.WriteFile(dexEnabledFlag, []byte("1\n"), 0644)
}

// prepareDataLayout ensures /data is writable by FleetShift and /data/sandbox
// remains root-owned after volume mounts replace image contents.
func prepareDataLayout() error {
	if err := ensureOwnedDir("/data", fleetshiftUID, fleetshiftGID); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}
	if err := ensureOwnedDir("/data/sandbox", 0, 0); err != nil {
		return fmt.Errorf("sandbox dir: %w", err)
	}
	return os.Chmod("/data/sandbox", 0755)
}

// ensureOwnedDir creates path and, when running as root, sets ownership to uid:gid.
// aio-init runs as root under s6 (then fleetshift/dex drop privileges); chown only
// works in that case. Non-root callers (e.g. unit tests) still get the directory
// but skip chown, which would fail with EPERM.
func ensureOwnedDir(path string, uid, gid int) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		return nil
	}
	return os.Chown(path, uid, gid)
}

// disableDex clears the Dex-on flag so the s6-rc dex longrun parks on s6-pause.
func disableDex() {
	_ = os.Remove(dexEnabledFlag)
}
