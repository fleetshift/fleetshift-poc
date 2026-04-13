package preflight

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"

	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/prereq"
)

// CheckFiles validates that all referenced files exist and are readable.
func CheckFiles(engine *config.EngineConfig) error {
	if _, err := os.Stat(engine.PullSecretFile); err != nil {
		return fmt.Errorf("pull_secret_file not accessible: %w", err)
	}

	if engine.SSHPublicKeyFile != "" {
		if _, err := os.Stat(engine.SSHPublicKeyFile); err != nil {
			return fmt.Errorf("ssh_public_key_file not accessible: %w", err)
		}
	}
	if engine.AdditionalTrustBundleFile != "" {
		if _, err := os.Stat(engine.AdditionalTrustBundleFile); err != nil {
			return fmt.Errorf("additional_trust_bundle_file not accessible: %w", err)
		}
	}
	return nil
}

// CheckInstallConfig validates required fields in the install-config pass-through.
func CheckInstallConfig(ic map[string]any) error {
	if _, ok := ic["baseDomain"]; !ok {
		return fmt.Errorf("baseDomain is required in install-config")
	}

	metadata, ok := ic["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("metadata is required in install-config")
	}
	if _, ok := metadata["name"]; !ok {
		return fmt.Errorf("metadata.name is required in install-config")
	}

	platform, ok := ic["platform"].(map[string]any)
	if !ok {
		return fmt.Errorf("platform is required in install-config")
	}
	aws, ok := platform["aws"].(map[string]any)
	if !ok {
		return fmt.Errorf("platform.aws is required in install-config")
	}
	if _, ok := aws["region"]; !ok {
		return fmt.Errorf("platform.aws.region is required in install-config")
	}
	return nil
}

// CheckDNSCollision checks if api.<cluster>.<baseDomain> already resolves.
// Returns a warning string if it resolves, empty string if not. Never errors.
func CheckDNSCollision(clusterName, baseDomain string) string {
	host := fmt.Sprintf("api.%s.%s", clusterName, baseDomain)
	addrs, err := net.LookupHost(host)
	if err == nil && len(addrs) > 0 {
		return fmt.Sprintf("%s already resolves to %v", host, addrs)
	}
	return ""
}

// CheckPrereqs validates prerequisite binaries (oc, podman/docker).
func CheckPrereqs() error {
	return prereq.Validate()
}

// CheckReleaseImage verifies that the release image is accessible by running
// "oc adm release info <image>". This is a read-only operation.
func CheckReleaseImage(image string, pullSecretFile string) error {
	args := []string{"adm", "release", "info", "--output=json"}
	if pullSecretFile != "" {
		args = append(args, "--registry-config="+pullSecretFile)
	}
	args = append(args, image)
	cmd := exec.Command("oc", args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("release image %s is not accessible: %w", image, err)
	}
	return nil
}
