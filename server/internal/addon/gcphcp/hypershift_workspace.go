package gcphcp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// HypershiftWorkspace holds the tempdir-backed assets required by a hypershift subprocess.
type HypershiftWorkspace struct {
	Env              []string
	JWKSPath         string
	tempDir          string
	cleanupCallbacks []func() error
}

// CleanupOnReturn returns a defer-friendly function that joins any cleanup
// error with the caller's return error.
func (w *HypershiftWorkspace) CleanupOnReturn(retErr *error) {
	if cleanupErr := w.Cleanup(); cleanupErr != nil {
		if *retErr == nil {
			*retErr = fmt.Errorf("cleanup hypershift workspace: %w", cleanupErr)
		} else {
			*retErr = errors.Join(*retErr, fmt.Errorf("cleanup hypershift workspace: %w", cleanupErr))
		}
	}
}

// Cleanup removes the temporary workspace directory and every credential file in it.
func (w *HypershiftWorkspace) Cleanup() error {
	if w == nil {
		return nil
	}
	tempDir := w.tempDir
	cleanupCallbacks := w.cleanupCallbacks
	w.tempDir = ""
	w.Env = nil
	w.JWKSPath = ""
	w.cleanupCallbacks = nil

	var cleanupErr error
	for _, callback := range cleanupCallbacks {
		if callback == nil {
			continue
		}
		if err := callback(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if tempDir != "" {
		if err := os.RemoveAll(tempDir); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	return cleanupErr
}

// PrepareCreateHypershiftWorkspaceWithTokenURL builds a tempdir-backed
// workspace for the create-path hypershift calls, overriding token_url for ADC
// STS exchanges while preserving the JWKS payload.
func PrepareCreateHypershiftWorkspaceWithTokenURL(
	callerToken string,
	target TargetConfig,
	jwksJSON []byte,
	tokenURL string,
	cleanupCallbacks ...func() error,
) (_ *HypershiftWorkspace, retErr error) {
	return prepareHypershiftWorkspace(callerToken, target, jwksJSON, tokenURL, cleanupCallbacks)
}

// PrepareDestroyHypershiftWorkspaceWithTokenURL builds a tempdir-backed workspace for the
// destroy-path hypershift calls, overriding token_url for ADC STS exchanges.
func PrepareDestroyHypershiftWorkspaceWithTokenURL(
	callerToken string,
	target TargetConfig,
	tokenURL string,
	cleanupCallbacks ...func() error,
) (_ *HypershiftWorkspace, retErr error) {
	return prepareHypershiftWorkspace(callerToken, target, nil, tokenURL, cleanupCallbacks)
}

// prepareHypershiftWorkspace materializes the tempdir-backed ADC files and home
// directories that a hypershift subprocess expects.
func prepareHypershiftWorkspace(
	callerToken string,
	target TargetConfig,
	jwksJSON []byte,
	tokenURL string,
	cleanupCallbacks []func() error,
) (_ *HypershiftWorkspace, retErr error) {
	tempDir, err := os.MkdirTemp("", "gcphcp-hypershift-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	workspace := &HypershiftWorkspace{
		tempDir:          tempDir,
		cleanupCallbacks: cleanupCallbacks,
	}
	defer func() {
		if retErr == nil {
			return
		}
		if cleanupErr := workspace.Cleanup(); cleanupErr != nil && !errors.Is(cleanupErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("cleanup hypershift workspace: %w", cleanupErr))
		}
	}()

	subjectTokenPath := filepath.Join(tempDir, "subject_token.txt")
	if err := os.WriteFile(subjectTokenPath, []byte(callerToken), 0600); err != nil {
		return nil, fmt.Errorf("write subject token: %w", err)
	}

	credConfigJSON, err := buildWorkforceCredentialConfigWithTokenURL(target, subjectTokenPath, tokenURL)
	if err != nil {
		return nil, fmt.Errorf("build credential config: %w", err)
	}

	credConfigPath := filepath.Join(tempDir, "workforce-cred.json")
	if err := os.WriteFile(credConfigPath, credConfigJSON, 0600); err != nil {
		return nil, fmt.Errorf("write credential config: %w", err)
	}

	homeDir := filepath.Join(tempDir, "home")
	cloudSDKDir := filepath.Join(tempDir, "cloudsdk")
	xdgConfigDir := filepath.Join(tempDir, "xdg")
	for _, dir := range []string{homeDir, cloudSDKDir, xdgConfigDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	workspace.Env = buildHypershiftEnvironment(credConfigPath, homeDir, cloudSDKDir, xdgConfigDir)

	if len(jwksJSON) == 0 {
		return workspace, nil
	}

	jwksPath := filepath.Join(tempDir, "jwks.json")
	if err := os.WriteFile(jwksPath, jwksJSON, 0600); err != nil {
		return nil, fmt.Errorf("write JWKS file: %w", err)
	}
	workspace.JWKSPath = jwksPath
	return workspace, nil
}

// buildWorkforceCredentialConfigWithTokenURL builds the workforce
// external_account ADC config and overrides token_url when requested.
func buildWorkforceCredentialConfigWithTokenURL(target TargetConfig, subjectTokenPath, tokenURL string) ([]byte, error) {
	return buildCredentialConfig(target, subjectTokenPath, tokenURL, false, true)
}

// buildHypershiftEnvironment constructs the minimal environment hypershift
// needs to resolve the tempdir-backed ADC and config directories.
func buildHypershiftEnvironment(
	credentialConfigPath, homeDir, cloudSDKDir, xdgConfigDir string,
) []string {
	env := []string{
		fmt.Sprintf("GOOGLE_APPLICATION_CREDENTIALS=%s", credentialConfigPath),
		fmt.Sprintf("HOME=%s", homeDir),
		fmt.Sprintf("CLOUDSDK_CONFIG=%s", cloudSDKDir),
		fmt.Sprintf("XDG_CONFIG_HOME=%s", xdgConfigDir),
	}

	for _, key := range []string{"PATH", "USER", "LOGNAME"} {
		if val := os.Getenv(key); val != "" {
			env = append(env, fmt.Sprintf("%s=%s", key, val))
		}
	}

	return env
}
