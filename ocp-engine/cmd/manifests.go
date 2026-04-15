package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// isAuthenticationCR checks if YAML data is a config.openshift.io/v1 Authentication CR.
func isAuthenticationCR(data []byte) bool {
	var obj struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
	}
	if err := yaml.Unmarshal(data, &obj); err != nil {
		return false
	}
	return obj.APIVersion == "config.openshift.io/v1" && obj.Kind == "Authentication"
}

// mergeAuthenticationCR finds an existing Authentication CR in manifestsDir
// (written by ccoctl) and merges the OIDC fields from extraData into it.
// The ccoctl manifest provides serviceAccountIssuer; the extra manifest
// provides type: OIDC, oidcProviders, and webhookTokenAuthenticator: null.
func mergeAuthenticationCR(manifestsDir string, extraData []byte) error {
	// Find the existing Authentication CR from ccoctl
	existingPath := ""
	entries, err := os.ReadDir(manifestsDir)
	if err != nil {
		return fmt.Errorf("read manifests dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".yaml") && !strings.HasSuffix(entry.Name(), ".yml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(manifestsDir, entry.Name()))
		if err != nil {
			continue
		}
		if isAuthenticationCR(data) {
			existingPath = filepath.Join(manifestsDir, entry.Name())
			break
		}
	}

	if existingPath == "" {
		// No existing Authentication CR — write the extra one directly
		return os.WriteFile(filepath.Join(manifestsDir, "cluster-authentication-oidc.yaml"), extraData, 0600)
	}

	// Parse both
	existingData, err := os.ReadFile(existingPath)
	if err != nil {
		return fmt.Errorf("read existing auth CR: %w", err)
	}

	var existing map[string]any
	if err := yaml.Unmarshal(existingData, &existing); err != nil {
		return fmt.Errorf("parse existing auth CR: %w", err)
	}

	var extra map[string]any
	if err := yaml.Unmarshal(extraData, &extra); err != nil {
		return fmt.Errorf("parse extra auth CR: %w", err)
	}

	// Merge: take spec fields from extra and add them to existing
	existingSpec, _ := existing["spec"].(map[string]any)
	extraSpec, _ := extra["spec"].(map[string]any)

	if existingSpec == nil {
		existingSpec = make(map[string]any)
	}

	// Copy OIDC-specific fields from extra into existing
	for _, key := range []string{"type", "webhookTokenAuthenticator", "oidcProviders"} {
		if val, ok := extraSpec[key]; ok {
			existingSpec[key] = val
		}
	}
	existing["spec"] = existingSpec

	// Write merged result back to the existing file
	merged, err := yaml.Marshal(existing)
	if err != nil {
		return fmt.Errorf("marshal merged auth CR: %w", err)
	}
	return os.WriteFile(existingPath, merged, 0600)
}
