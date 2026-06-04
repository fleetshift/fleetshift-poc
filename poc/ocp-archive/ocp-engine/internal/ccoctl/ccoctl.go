package ccoctl

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ExtractBinaryArgs returns oc args for extracting the ccoctl binary from a release image.
// Returns: adm release extract --command=ccoctl --to <workDir> --registry-config <pullSecretFile> <releaseImage>
func ExtractBinaryArgs(workDir, pullSecretFile, releaseImage string) []string {
	return []string{
		"adm",
		"release",
		"extract",
		"--command=ccoctl",
		"--to", workDir,
		"--registry-config", pullSecretFile,
		releaseImage,
	}
}

// ExtractCredReqArgs returns oc args for extracting credentials requests from a release image.
// Returns: adm release extract --credentials-requests --cloud=aws --to <credReqDir> --registry-config <pullSecretFile> <releaseImage>
func ExtractCredReqArgs(credReqDir, pullSecretFile, releaseImage string) []string {
	return []string{
		"adm",
		"release",
		"extract",
		"--credentials-requests",
		"--cloud=aws",
		"--to", credReqDir,
		"--registry-config", pullSecretFile,
		releaseImage,
	}
}

// CreateAllArgs returns ccoctl args for creating all AWS resources for STS mode.
// Returns: aws create-all --name <name> --region <region> --credentials-requests-dir <dir> --output-dir <dir>
func CreateAllArgs(clusterName, region, credReqDir, outputDir string) []string {
	return []string{
		"aws",
		"create-all",
		"--name", clusterName,
		"--region", region,
		"--credentials-requests-dir", credReqDir,
		"--output-dir", outputDir,
	}
}

// DeleteArgs returns ccoctl args for deleting AWS resources for STS mode.
// Returns: aws delete --name <name> --region <region>
func DeleteArgs(clusterName, region string) []string {
	return []string{
		"aws",
		"delete",
		"--name", clusterName,
		"--region", region,
	}
}

// BinaryPath returns the path to the extracted ccoctl binary.
func BinaryPath(workDir string) string {
	return filepath.Join(workDir, "ccoctl")
}

// CredReqDir returns the path to the credentials requests directory.
func CredReqDir(workDir string) string {
	return filepath.Join(workDir, "credrequests")
}

// OutputDir returns the path to the ccoctl output directory.
func OutputDir(workDir string) string {
	return filepath.Join(workDir, "ccoctl-output")
}

// InjectManifests copies manifests/ and tls/ subdirectories from ccoctlOutputDir
// into installerDir. Silently skips missing source directories.
func InjectManifests(ccoctlOutputDir, installerDir string) error {
	if err := copyDir(filepath.Join(ccoctlOutputDir, "manifests"), filepath.Join(installerDir, "manifests")); err != nil {
		return fmt.Errorf("copy ccoctl manifests: %w", err)
	}
	if err := copyDir(filepath.Join(ccoctlOutputDir, "tls"), filepath.Join(installerDir, "tls")); err != nil {
		return fmt.Errorf("copy ccoctl tls: %w", err)
	}
	return nil
}

func copyDir(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
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
		return err
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
