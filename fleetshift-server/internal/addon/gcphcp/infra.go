package gcphcp

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	defaultPSCCleanupPollInterval = 30 * time.Second
	defaultPSCCleanupWaitTimeout  = 20 * time.Minute
	defaultPSCComputeAPIEndpoint  = "https://compute.googleapis.com/compute/v1"
)

type pscResourceLookup interface {
	ForwardingRuleExists(ctx context.Context, projectID, region, name string) (bool, error)
	AddressExists(ctx context.Context, projectID, region, name string) (bool, error)
}

type httpPSCResourceLookup struct {
	client         *http.Client
	endpoint       string
	workforceToken string
}

var (
	pscCleanupPollInterval = defaultPSCCleanupPollInterval
	pscCleanupWaitTimeout  = defaultPSCCleanupWaitTimeout
	newPSCResourceLookup   = func(_ context.Context, workforceToken string) (pscResourceLookup, error) {
		if workforceToken == "" {
			return nil, fmt.Errorf("missing workforce token")
		}
		return &httpPSCResourceLookup{
			client:         &http.Client{Timeout: defaultBrokerHTTPTimeout},
			endpoint:       defaultPSCComputeAPIEndpoint,
			workforceToken: workforceToken,
		}, nil
	}
)

// ClusterKeypair holds an RSA keypair for cluster authentication.
type ClusterKeypair struct {
	// PrivateKey is the raw private key
	PrivateKey crypto.PrivateKey
	// PrivateKeyPEMBase64 is the base64-encoded PEM representation (sent to CLS API)
	PrivateKeyPEMBase64 string
	// JWKSJSON is the JWKS JSON representation (written to temp file for hypershift)
	JWKSJSON []byte
}

// GenerateClusterKeypair generates a 4096-bit RSA keypair for cluster authentication.
func GenerateClusterKeypair() (ClusterKeypair, error) {
	// Generate 4096-bit RSA key
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return ClusterKeypair{}, fmt.Errorf("failed to generate RSA key: %w", err)
	}

	// PEM encode private key (PKCS1)
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	pemBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	}
	pemBytes := pem.EncodeToMemory(pemBlock)

	// Base64 encode the PEM bytes
	pemBase64 := base64.StdEncoding.EncodeToString(pemBytes)

	// Generate JWKS JSON
	jwksJSON, err := generateJWKS(privateKey)
	if err != nil {
		return ClusterKeypair{}, fmt.Errorf("failed to generate JWKS: %w", err)
	}

	return ClusterKeypair{
		PrivateKey:          privateKey,
		PrivateKeyPEMBase64: pemBase64,
		JWKSJSON:            jwksJSON,
	}, nil
}

// generateJWKS creates a JWKS JSON representation of the RSA public key.
func generateJWKS(privateKey *rsa.PrivateKey) ([]byte, error) {
	publicKey := &privateKey.PublicKey

	// Generate kid (key ID) as base64url(SHA256(DER public key))
	derBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}
	hash := sha256.Sum256(derBytes)
	kid := base64.RawURLEncoding.EncodeToString(hash[:])

	// Encode modulus (n) and exponent (e) as base64url
	n := base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(publicKey.E)).Bytes())

	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"use": "sig",
				"alg": "RS256",
				"kid": kid,
				"n":   n,
				"e":   e,
			},
		},
	}

	jwksJSON, err := json.Marshal(jwks)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JWKS: %w", err)
	}

	return jwksJSON, nil
}

// ValidateInfraID validates that an infrastructure ID meets GCP requirements.
// Must be max 15 characters, lowercase, start with a letter, and contain only letters, digits, and hyphens.
func ValidateInfraID(id string) error {
	if id == "" {
		return fmt.Errorf("infra ID cannot be empty")
	}
	if len(id) > 15 {
		return fmt.Errorf("infra ID must be at most 15 characters, got %d", len(id))
	}
	// Must start with lowercase letter, followed by lowercase letters, digits, or hyphens
	matched, err := regexp.MatchString(`^[a-z][-a-z0-9]*$`, id)
	if err != nil {
		return fmt.Errorf("failed to validate infra ID: %w", err)
	}
	if !matched {
		return fmt.Errorf("infra ID must start with a lowercase letter and contain only lowercase letters, digits, and hyphens")
	}
	return nil
}

// InfraRunner wraps the hypershift CLI for infrastructure operations.
type InfraRunner struct {
	HypershiftBinary string
}

// NewInfraRunner creates a new InfraRunner.
// It looks for the hypershift binary via HYPERSHIFT_BINARY env var or PATH.
func NewInfraRunner() *InfraRunner {
	binary := os.Getenv("HYPERSHIFT_BINARY")
	if binary == "" {
		// Look for hypershift in PATH
		path, err := exec.LookPath("hypershift")
		if err == nil {
			binary = path
		} else {
			// Default to "hypershift" and let exec fail if not found
			binary = "hypershift"
		}
	}
	return &InfraRunner{
		HypershiftBinary: binary,
	}
}

// CreateIAM runs hypershift create iam gcp and returns the parsed JSON output.
func (r *InfraRunner) CreateIAM(ctx context.Context, infraID, projectID, jwksFile string, env []string) (map[string]any, error) {
	args := []string{
		"create", "iam", "gcp",
		"--infra-id", infraID,
		"--project-id", projectID,
		"--oidc-jwks-file", jwksFile,
	}

	cmd := exec.CommandContext(ctx, r.HypershiftBinary, args...)
	cmd.Env = env

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("hypershift create iam failed: %w (stderr: %s)", err, exitErr.Stderr)
		}
		return nil, fmt.Errorf("hypershift create iam failed: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, fmt.Errorf("failed to parse hypershift create iam output: %w", err)
	}

	return result, nil
}

// CreateInfra runs hypershift create infra gcp and returns the parsed JSON output.
func (r *InfraRunner) CreateInfra(ctx context.Context, infraID, projectID, region string, env []string) (map[string]any, error) {
	args := []string{
		"create", "infra", "gcp",
		"--infra-id", infraID,
		"--project-id", projectID,
		"--region", region,
	}

	cmd := exec.CommandContext(ctx, r.HypershiftBinary, args...)
	cmd.Env = env

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("hypershift create infra failed: %w (stderr: %s)", err, exitErr.Stderr)
		}
		return nil, fmt.Errorf("hypershift create infra failed: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, fmt.Errorf("failed to parse hypershift create infra output: %w", err)
	}

	return result, nil
}

// DestroyInfra runs hypershift destroy infra gcp.
func (r *InfraRunner) DestroyInfra(ctx context.Context, infraID, projectID, region string, env []string) error {
	args := []string{
		"destroy", "infra", "gcp",
		"--infra-id", infraID,
		"--project-id", projectID,
		"--region", region,
	}

	cmd := exec.CommandContext(ctx, r.HypershiftBinary, args...)
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("hypershift destroy infra failed: %w (output: %s)", err, output)
	}

	return nil
}

// WaitForPSCCleanup waits until PSC endpoint artifacts for the deleted
// cluster are gone from the tenant project before infra destroy begins.
func (r *InfraRunner) WaitForPSCCleanup(
	ctx context.Context,
	clusterID, projectID, region, workforceToken string,
	signaler *domain.DeliverySignaler,
) error {
	lookup, err := newPSCResourceLookup(ctx, workforceToken)
	if err != nil {
		return fmt.Errorf("create PSC cleanup lookup: %w", err)
	}

	endpointName := fmt.Sprintf("psc-%s-endpoint", clusterID)
	ipName := fmt.Sprintf("psc-%s-ip", clusterID)

	pollInterval := pscCleanupPollInterval
	if pollInterval <= 0 {
		pollInterval = time.Millisecond
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	waitTimeout := pscCleanupWaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = time.Millisecond
	}
	timeout := time.NewTimer(waitTimeout)
	defer timeout.Stop()

	// Check immediately first so already-removed artifacts don't incur a delay.
	endpointExists, err := lookup.ForwardingRuleExists(ctx, projectID, region, endpointName)
	if err != nil {
		return err
	}
	ipExists, err := lookup.AddressExists(ctx, projectID, region, ipName)
	if err != nil {
		return err
	}
	if !endpointExists && !ipExists {
		return nil
	}

	emitProgress(signaler, ctx, "Waiting for PSC endpoint cleanup")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for PSC endpoint cleanup")
		case <-ticker.C:
			endpointExists, err = lookup.ForwardingRuleExists(ctx, projectID, region, endpointName)
			if err != nil {
				return err
			}
			ipExists, err = lookup.AddressExists(ctx, projectID, region, ipName)
			if err != nil {
				return err
			}
			if !endpointExists && !ipExists {
				return nil
			}

			emitProgress(signaler, ctx, "Waiting for PSC endpoint cleanup")
		}
	}
}

func (l *httpPSCResourceLookup) ForwardingRuleExists(
	ctx context.Context,
	projectID, region, name string,
) (bool, error) {
	resourcePath := fmt.Sprintf(
		"projects/%s/regions/%s/forwardingRules/%s",
		url.PathEscape(projectID),
		url.PathEscape(region),
		url.PathEscape(name),
	)
	return l.regionalResourceExists(ctx, projectID, resourcePath, "forwarding rule")
}

func (l *httpPSCResourceLookup) AddressExists(
	ctx context.Context,
	projectID, region, name string,
) (bool, error) {
	resourcePath := fmt.Sprintf(
		"projects/%s/regions/%s/addresses/%s",
		url.PathEscape(projectID),
		url.PathEscape(region),
		url.PathEscape(name),
	)
	return l.regionalResourceExists(ctx, projectID, resourcePath, "address")
}

func (l *httpPSCResourceLookup) regionalResourceExists(
	ctx context.Context,
	projectID, resourcePath, resourceKind string,
) (bool, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fmt.Sprintf("%s/%s", strings.TrimRight(l.endpoint, "/"), resourcePath),
		nil,
	)
	if err != nil {
		return false, fmt.Errorf("create %s request: %w", resourceKind, err)
	}
	req.Header.Set("Authorization", "Bearer "+l.workforceToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-goog-user-project", projectID)

	resp, err := l.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("request %s: %w", resourceKind, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return false, fmt.Errorf(
				"%s request returned status %d and response body could not be read: %w",
				resourceKind, resp.StatusCode, readErr,
			)
		}
		return false, fmt.Errorf(
			"%s request returned status %d: %s",
			resourceKind, resp.StatusCode, strings.TrimSpace(string(body)),
		)
	}
}

// DestroyIAM runs hypershift destroy iam gcp.
func (r *InfraRunner) DestroyIAM(ctx context.Context, infraID, projectID string, env []string) error {
	args := []string{
		"destroy", "iam", "gcp",
		"--infra-id", infraID,
		"--project-id", projectID,
	}

	cmd := exec.CommandContext(ctx, r.HypershiftBinary, args...)
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("hypershift destroy iam failed: %w (output: %s)", err, output)
	}

	return nil
}

// IAMConfigToWIFSpec converts the IAM config returned by hypershift create iam
// to the workloadIdentityFederation spec format expected by the CLS API.
func IAMConfigToWIFSpec(iamConfig map[string]any) (map[string]any, error) {
	// Extract workloadIdentityPool
	wipRaw, ok := iamConfig["workloadIdentityPool"]
	if !ok {
		return nil, fmt.Errorf("workloadIdentityPool not found in IAM config")
	}
	wip, ok := wipRaw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("workloadIdentityPool is not a map")
	}

	poolIDRaw, ok := wip["poolId"]
	if !ok {
		return nil, fmt.Errorf("poolId not found in workloadIdentityPool")
	}
	poolID, ok := poolIDRaw.(string)
	if !ok {
		return nil, fmt.Errorf("poolId is not a string")
	}

	providerIDRaw, ok := wip["providerId"]
	if !ok {
		return nil, fmt.Errorf("providerId not found in workloadIdentityPool")
	}
	providerID, ok := providerIDRaw.(string)
	if !ok {
		return nil, fmt.Errorf("providerId is not a string")
	}

	// Extract projectNumber
	projectNumberRaw, ok := iamConfig["projectNumber"]
	if !ok {
		return nil, fmt.Errorf("projectNumber not found in IAM config")
	}
	projectNumber, ok := projectNumberRaw.(string)
	if !ok {
		return nil, fmt.Errorf("projectNumber is not a string")
	}

	// Extract service accounts
	saRaw, ok := iamConfig["serviceAccounts"]
	if !ok {
		return nil, fmt.Errorf("serviceAccounts not found in IAM config")
	}
	sa, ok := saRaw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("serviceAccounts is not a map")
	}

	// Map service accounts from hypershift format to CLS API format
	saMapping := map[string]string{
		"ctrlplane-op":     "controlPlaneEmail",
		"nodepool-mgmt":    "nodePoolEmail",
		"cloud-controller": "cloudControllerEmail",
		"gcp-pd-csi":       "storageEmail",
		"image-registry":   "imageRegistryEmail",
		"cloud-network":    "networkEmail",
	}

	serviceAccountsRef := make(map[string]string)
	for hypershiftKey, clsKey := range saMapping {
		emailRaw, ok := sa[hypershiftKey]
		if !ok {
			return nil, fmt.Errorf("service account %q not found in IAM config", hypershiftKey)
		}
		email, ok := emailRaw.(string)
		if !ok {
			return nil, fmt.Errorf("service account %q is not a string", hypershiftKey)
		}
		serviceAccountsRef[clsKey] = email
	}

	return map[string]any{
		"projectNumber":      projectNumber,
		"poolID":             poolID,
		"providerID":         providerID,
		"serviceAccountsRef": serviceAccountsRef,
	}, nil
}

// PrepareHypershiftEnv prepares an isolated environment for running hypershift commands.
// It writes the caller token and workforce credential config to the temp directory,
// and returns an environment with isolated credential paths.
func PrepareHypershiftEnv(callerToken string, target TargetConfig, tempDir string) ([]string, error) {
	// Write subject token
	subjectTokenPath := filepath.Join(tempDir, "subject_token.txt")
	if err := os.WriteFile(subjectTokenPath, []byte(callerToken), 0600); err != nil {
		return nil, fmt.Errorf("failed to write subject token: %w", err)
	}

	// Create workforce credential config
	audience := fmt.Sprintf("//iam.googleapis.com/locations/global/workforcePools/%s/providers/%s",
		target.WorkforcePool, target.WorkforceProvider)

	credConfig := map[string]interface{}{
		"type":               "external_account",
		"audience":           audience,
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url":          "https://sts.googleapis.com/v1/token",
		"credential_source": map[string]interface{}{
			"file": subjectTokenPath,
		},
		"workforce_pool_user_project": target.GCPProject,
	}

	credConfigJSON, err := json.MarshalIndent(credConfig, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal credential config: %w", err)
	}

	credConfigPath := filepath.Join(tempDir, "workforce-cred.json")
	if err := os.WriteFile(credConfigPath, credConfigJSON, 0600); err != nil {
		return nil, fmt.Errorf("failed to write credential config: %w", err)
	}

	// Create isolated directories
	homeDir := filepath.Join(tempDir, "home")
	cloudSDKDir := filepath.Join(tempDir, "cloudsdk")
	xdgConfigDir := filepath.Join(tempDir, "xdg")

	for _, dir := range []string{homeDir, cloudSDKDir, xdgConfigDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Build environment
	env := []string{
		fmt.Sprintf("GOOGLE_APPLICATION_CREDENTIALS=%s", credConfigPath),
		"GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1",
		fmt.Sprintf("HOME=%s", homeDir),
		fmt.Sprintf("CLOUDSDK_CONFIG=%s", cloudSDKDir),
		fmt.Sprintf("XDG_CONFIG_HOME=%s", xdgConfigDir),
	}

	// Inherit PATH and other essential variables
	for _, key := range []string{"PATH", "USER", "LOGNAME"} {
		if val := os.Getenv(key); val != "" {
			env = append(env, fmt.Sprintf("%s=%s", key, val))
		}
	}

	return env, nil
}
