package ocp

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// CCOctlOrchestrator manages the ccoctl workflow for CCO STS mode —
// extracting the binary, creating AWS OIDC resources, injecting manifests,
// and cleaning up on destroy.
type CCOctlOrchestrator struct {
	WorkDir    string   // cluster work directory
	BinaryPath string   // path to ccoctl binary
	AWSEnv     []string // AWS credential env vars (from AWSCredentials.Env())
}

// configureCmd applies the orchestrator's AWS env and stderr routing to a command.
func (o *CCOctlOrchestrator) configureCmd(cmd *exec.Cmd) {
	cmd.Env = append(os.Environ(), o.AWSEnv...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
}

// ExtractCCOctl extracts the ccoctl binary from the release image.
func (o *CCOctlOrchestrator) ExtractCCOctl(ctx context.Context, releaseImage, pullSecretFile string) error {
	cmd := exec.CommandContext(ctx, "oc", "adm", "release", "extract",
		"--command=ccoctl",
		"--to", o.WorkDir,
		"--registry-config", pullSecretFile,
		releaseImage,
	)
	o.configureCmd(cmd)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to extract ccoctl binary: %w", err)
	}

	o.BinaryPath = filepath.Join(o.WorkDir, "ccoctl")
	return nil
}

// ExtractCredentialsRequests extracts CredentialsRequests from the release image.
func (o *CCOctlOrchestrator) ExtractCredentialsRequests(ctx context.Context, releaseImage, pullSecretFile string) (string, error) {
	credReqDir := filepath.Join(o.WorkDir, "credrequests")
	if err := os.MkdirAll(credReqDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create credrequests directory: %w", err)
	}

	cmd := exec.CommandContext(ctx, "oc", "adm", "release", "extract",
		"--credentials-requests",
		"--cloud=aws",
		"--to", credReqDir,
		"--registry-config", pullSecretFile,
		releaseImage,
	)
	o.configureCmd(cmd)

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to extract credentials requests: %w", err)
	}

	return credReqDir, nil
}

// CreateAll runs ccoctl aws create-all to create AWS OIDC resources.
func (o *CCOctlOrchestrator) CreateAll(ctx context.Context, clusterName, region, credReqDir string) (string, error) {
	outputDir := filepath.Join(o.WorkDir, "ccoctl-output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create ccoctl output directory: %w", err)
	}

	args := ccoctlCreateAllArgs(clusterName, region, credReqDir, outputDir)
	cmd := exec.CommandContext(ctx, o.BinaryPath, args...)
	o.configureCmd(cmd)

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to run ccoctl aws create-all: %w", err)
	}

	return outputDir, nil
}

// InjectManifests copies ccoctl output manifests and TLS files to the installer directory.
func (o *CCOctlOrchestrator) InjectManifests(ccoctlOutputDir, installerDir string) error {
	manifestsSrc := filepath.Join(ccoctlOutputDir, "manifests")
	manifestsDst := filepath.Join(installerDir, "manifests")
	if err := copyDir(manifestsSrc, manifestsDst); err != nil {
		return fmt.Errorf("failed to copy manifests: %w", err)
	}

	tlsSrc := filepath.Join(ccoctlOutputDir, "tls")
	tlsDst := filepath.Join(installerDir, "tls")
	if err := copyDir(tlsSrc, tlsDst); err != nil {
		return fmt.Errorf("failed to copy tls: %w", err)
	}

	return nil
}

// Delete runs ccoctl aws delete to clean up AWS resources.
func (o *CCOctlOrchestrator) Delete(ctx context.Context, clusterName, region string) error {
	args := ccoctlDeleteArgs(clusterName, region)
	cmd := exec.CommandContext(ctx, o.BinaryPath, args...)
	o.configureCmd(cmd)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to run ccoctl aws delete: %w", err)
	}

	return nil
}

func ccoctlCreateAllArgs(name, region, credReqDir, outputDir string) []string {
	return []string{
		"aws", "create-all",
		"--name", name,
		"--region", region,
		"--credentials-requests-dir", credReqDir,
		"--output-dir", outputDir,
	}
}

func ccoctlDeleteArgs(name, region string) []string {
	return []string{
		"aws", "delete",
		"--name", name,
		"--region", region,
	}
}

func copyDir(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat source directory: %w", err)
	}

	if !srcInfo.IsDir() {
		return fmt.Errorf("source is not a directory: %s", src)
	}

	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("failed to read source directory: %w", err)
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	dstFile, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("failed to copy file contents: %w", err)
	}

	return nil
}
