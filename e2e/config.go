//go:build e2e

package e2e

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// SSO
	KeycloakIssuer       string
	KeycloakClientID     string
	EnrollmentClientID   string
	GithubUsername        string
	RoleARN              string
	RHSSOIssuer          string
	RHSSOClientID        string

	// Cluster
	BaseDomain         string
	Region             string
	ReleaseImage       string
	WorkerCount        int
	WorkerInstanceType string

	// Generated
	ClusterName string
}

func LoadConfig() (*Config, error) {
	loadEnvFile("e2e/.env")
	loadEnvFile(".env")

	cfg := &Config{
		KeycloakIssuer:     os.Getenv("E2E_KEYCLOAK_ISSUER"),
		KeycloakClientID:   os.Getenv("E2E_KEYCLOAK_CLIENT_ID"),
		EnrollmentClientID: envOr("E2E_ENROLLMENT_CLIENT_ID", "fleetshift-signing"),
		GithubUsername:     os.Getenv("E2E_GITHUB_USERNAME"),
		RoleARN:            os.Getenv("E2E_ROLE_ARN"),
		RHSSOIssuer:        os.Getenv("E2E_RH_SSO_ISSUER"),
		RHSSOClientID:      os.Getenv("E2E_RH_SSO_CLIENT_ID"),
		BaseDomain:         envOr("E2E_BASE_DOMAIN", "aws-acm-cluster-virt.devcluster.openshift.com"),
		Region:             envOr("E2E_REGION", "us-west-2"),
		ReleaseImage:       envOr("E2E_RELEASE_IMAGE", "quay.io/openshift-release-dev/ocp-release:4.20.18-x86_64"),
		WorkerCount:        envOrInt("E2E_WORKER_COUNT", 3),
		WorkerInstanceType: envOr("E2E_WORKER_INSTANCE_TYPE", "m6i.xlarge"),
		ClusterName:        generateClusterName(),
	}

	required := []struct {
		value string
		name  string
	}{
		{cfg.KeycloakIssuer, "E2E_KEYCLOAK_ISSUER"},
		{cfg.KeycloakClientID, "E2E_KEYCLOAK_CLIENT_ID"},
		{cfg.RoleARN, "E2E_ROLE_ARN"},
		{cfg.RHSSOIssuer, "E2E_RH_SSO_ISSUER"},
		{cfg.GithubUsername, "E2E_GITHUB_USERNAME"},
		{cfg.RHSSOClientID, "E2E_RH_SSO_CLIENT_ID"},
	}
	var missing []string
	for _, r := range required {
		if r.value == "" {
			missing = append(missing, r.name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env vars: %s\nCopy .env.example to .env and fill in values", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func generateClusterName() string {
	b := make([]byte, 2)
	rand.Read(b)
	return fmt.Sprintf("fleetshift-e2etest-%x", b)
}

func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
