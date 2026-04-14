package preflight

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/credentials"
	"github.com/ocp-engine/internal/output"
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

// RunPreflight executes all preflight checks in sequence.
// Returns the resolved AWS env vars on success.
func RunPreflight(cfg *config.ClusterConfig, w io.Writer, attempt int) (map[string]string, error) {
	start := time.Now()

	// 1. Check prerequisite binaries
	if err := prereq.Validate(); err != nil {
		return nil, fmt.Errorf("prerequisite check failed: %w", err)
	}

	// 2. Validate install-config fields
	if err := CheckInstallConfig(cfg.InstallConfig); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	// 3. Check referenced files exist
	if err := CheckFiles(&cfg.Engine); err != nil {
		return nil, fmt.Errorf("file check failed: %w", err)
	}

	// 4. Resolve and test AWS credentials
	awsEnv, err := credentials.ResolveFromConfig(&cfg.Engine.Credentials)
	if err != nil {
		return nil, fmt.Errorf("credential resolution failed: %w", err)
	}

	region := extractRegion(cfg.InstallConfig)
	identity, err := credentials.TestCredentials(awsEnv, region)
	if err != nil {
		return nil, err
	}

	// 5. DNS collision check (warning only)
	clusterName := extractClusterName(cfg.InstallConfig)
	baseDomain, _ := cfg.InstallConfig["baseDomain"].(string)
	dnsWarning := CheckDNSCollision(clusterName, baseDomain)
	if dnsWarning != "" {
		fmt.Fprintf(os.Stderr, "WARNING: %s\n", dnsWarning)
	}

	// 6. Release image accessibility check
	releaseImage := cfg.Engine.ReleaseImage
	if releaseImage == "" {
		releaseImage = "quay.io/openshift-release-dev/ocp-release:4.20.18-multi"
	}
	if err := CheckReleaseImage(releaseImage, cfg.Engine.PullSecretFile); err != nil {
		return nil, err
	}

	elapsed := int(time.Since(start).Seconds())
	output.WritePreflightResult(w, output.PreflightResult{
		Phase:          "preflight",
		Status:         "complete",
		AWSAccount:     identity.Account,
		AWSARN:         identity.ARN,
		DNSWarning:     dnsWarning,
		ElapsedSeconds: elapsed,
		Attempt:        attempt,
	})

	return awsEnv, nil
}

func extractRegion(ic map[string]any) string {
	platform, _ := ic["platform"].(map[string]any)
	aws, _ := platform["aws"].(map[string]any)
	region, _ := aws["region"].(string)
	return region
}

func extractClusterName(ic map[string]any) string {
	metadata, _ := ic["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	return name
}
