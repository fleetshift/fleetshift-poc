//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zalando/go-keyring"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	cfg           *Config
	keycloakToken *TokenResponse
	pullSecret    []byte
	binDir        string
	repoRoot      string
	serverCmd     *exec.Cmd
	infraID       string // set after provision
	serverLogFile string // path to server log file
)

func TestE2E(t *testing.T) {
	// Setup: find repo root, load config, generate cluster name.
	// NOTE: The keyring must be unlocked BEFORE this process starts
	// (dbus-launch + gnome-keyring-daemon in the Makefile) because
	// godbus/dbus caches the D-Bus connection at first use.
	repoRoot = findRepoRoot(t)
	binDir = filepath.Join(repoRoot, "bin")

	var err error
	cfg, err = LoadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Logf("Cluster name: %s", cfg.ClusterName)
	t.Logf("Region:       %s", cfg.Region)
	t.Logf("Base domain:  %s", cfg.BaseDomain)

	provisioned := false
	failed := false

	t.Cleanup(func() {
		if failed && serverLogFile != "" {
			t.Logf("Server log: %s", serverLogFile)
			tail, _ := exec.Command("tail", "-50", serverLogFile).Output()
			if len(tail) > 0 {
				t.Logf("=== Last 50 lines of server log ===\n%s", tail)
			}
		}
		if provisioned && failed {
			printClusterWarning(cfg)
		}
	})

	// step gates each subtest — if a prior step failed, skip the rest.
	// Uses defer because t.Fatalf calls runtime.Goexit(), so code after
	// fn(t) never runs without defer.
	step := func(name string, fn func(t *testing.T)) {
		if failed {
			t.Run(name, func(t *testing.T) { t.Skip("skipped due to prior failure") })
			return
		}
		t.Run(name, func(t *testing.T) {
			defer func() {
				if t.Failed() {
					failed = true
				}
			}()
			fn(t)
		})
	}

	step("01_Build", func(t *testing.T) {
		buildBinaries(t, repoRoot, binDir)
	})

	// Steps 02-03: Authenticate before starting the server. The SSO logins
	// talk to external identity providers (Keycloak, Red Hat SSO) and don't
	// need the server running. The pull secret must be available when the
	// server starts because SSOCredentialProvider caches it at init time.

	step("02_KeycloakLogin", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		token, err := DeviceCodeLogin(ctx,
			cfg.KeycloakIssuer, cfg.KeycloakClientID,
			"openid", "Keycloak Login")
		if err != nil {
			t.Fatalf("Keycloak device code login: %v", err)
		}
		keycloakToken = token
		storeTokenForFleetctl(t, token)
	})

	step("03_RedHatSSOLogin", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		token, err := DeviceCodeLogin(ctx,
			cfg.RHSSOIssuer, "ocm-cli",
			"openid", "Red Hat SSO Login")
		if err != nil {
			t.Fatalf("Red Hat SSO device code login: %v", err)
		}

		ps, err := FetchPullSecret(ctx, token.AccessToken)
		if err != nil {
			t.Fatalf("fetch pull secret: %v", err)
		}
		pullSecret = ps
		t.Logf("Pull secret fetched (%d bytes)", len(pullSecret))
	})

	step("04_StartServer", func(st *testing.T) {
		// Pass the parent t for cleanup so the server survives across subtests.
		startServer(st, t, binDir, repoRoot, cfg)
		setupAuth(st, binDir, cfg)
	})

	step("05_EnrollSigningKey", func(t *testing.T) {
		enrollSigningKey(t, binDir)
	})

	step("06_CreateDeployment", func(t *testing.T) {
		createDeployment(t, binDir, cfg)
		provisioned = true
	})

	step("07_WaitForProvision", func(t *testing.T) {
		state := waitForProvision(t, binDir, cfg, 60*time.Minute)
		if state != "STATE_ACTIVE" {
			t.Fatalf("provision ended in state %s, expected STATE_ACTIVE", state)
		}
		t.Logf("Deployment reached STATE_ACTIVE")
	})

	step("08_ValidateDeployment", func(t *testing.T) {
		validateDeployment(t, binDir, cfg)
	})

	step("09_ValidateClusterOIDC", func(t *testing.T) {
		validateClusterOIDC(t, cfg, keycloakToken)
	})

	step("10_DestroyDeployment", func(t *testing.T) {
		destroyDeployment(t, binDir, cfg)
	})

	step("11_ValidateCleanup", func(t *testing.T) {
		validateCleanup(t, cfg)
	})
}

// ---------------------------------------------------------------------------
// Helper: findRepoRoot
// ---------------------------------------------------------------------------

func findRepoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	// If running from e2e/, go up one level.
	if filepath.Base(dir) == "e2e" {
		dir = filepath.Dir(dir)
	}

	// Walk up looking for the Makefile that indicates the repo root.
	for {
		if _, err := os.Stat(filepath.Join(dir, "Makefile")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root (no Makefile found)")
		}
		dir = parent
	}
}

// ---------------------------------------------------------------------------
// Helper: buildBinaries
// ---------------------------------------------------------------------------

func buildBinaries(t *testing.T, repoRoot, binDir string) {
	t.Helper()

	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create bin dir: %v", err)
	}

	type binary struct {
		name    string
		dir     string
		target  string
		outName string
	}

	binaries := []binary{
		{
			name:    "fleetshift-server",
			dir:     filepath.Join(repoRoot, "fleetshift-server"),
			target:  "./cmd/fleetshift",
			outName: "fleetshift",
		},
		{
			name:    "fleetctl",
			dir:     filepath.Join(repoRoot, "fleetshift-cli"),
			target:  "./cmd/fleetctl",
			outName: "fleetctl",
		},
		{
			name:    "ocp-engine",
			dir:     filepath.Join(repoRoot, "ocp-engine"),
			target:  ".",
			outName: "ocp-engine",
		},
	}

	for _, b := range binaries {
		t.Logf("Building %s...", b.name)
		start := time.Now()
		outPath := filepath.Join(binDir, b.outName)
		cmd := exec.Command("go", "build", "-o", outPath, b.target)
		cmd.Dir = b.dir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("build %s: %v", b.name, err)
		}
		t.Logf("Built %s in %s", b.name, elapsed(start))
	}
}

// ---------------------------------------------------------------------------
// Helper: startServer
// ---------------------------------------------------------------------------

func startServer(t, parentT *testing.T, binDir, repoRoot string, cfg *Config) {
	t.Helper()

	// Use a manual temp dir so it survives beyond this subtest.
	dbDir, err := os.MkdirTemp("", "fleetshift-e2e-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	parentT.Cleanup(func() { os.RemoveAll(dbDir) })
	dbPath := filepath.Join(dbDir, "fleetshift-e2e.db")

	// Write the pull secret file. This is populated before the server starts
	// because SSOCredentialProvider caches it at init time.
	psFile := filepath.Join(dbDir, "pull-secret.json")
	psData := pullSecret
	if len(psData) == 0 {
		psData = []byte("{}")
	}
	if err := os.WriteFile(psFile, psData, 0o600); err != nil {
		t.Fatalf("write pull secret: %v", err)
	}

	engineBin := filepath.Join(binDir, "ocp-engine")
	serverBin := filepath.Join(binDir, "fleetshift")

	serverCmd = exec.Command(serverBin, "serve",
		"--db", dbPath,
		"--http-addr", ":8080",
		"--grpc-addr", ":50051",
	)
	serverCmd.Dir = repoRoot
	serverCmd.Env = append(os.Environ(),
		"OCP_ENGINE_BINARY="+engineBin,
		"OCP_CREDENTIAL_MODE=sso",
		"OCP_PULL_SECRET_FILE="+psFile,
	)
	serverLogPath := "/tmp/fleetshift-e2e-server.log"
	serverLog, err := os.Create(serverLogPath)
	if err != nil {
		t.Fatalf("create server log: %v", err)
	}
	parentT.Cleanup(func() { serverLog.Close() })
	serverCmd.Stdout = serverLog
	serverCmd.Stderr = serverLog

	serverLogFile = serverLogPath
	t.Logf("Starting fleetshift-server (db=%s, log=%s)...", dbPath, serverLogPath)
	if err := serverCmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	parentT.Cleanup(func() {
		if serverCmd != nil && serverCmd.Process != nil {
			parentT.Logf("Stopping fleetshift-server (pid %d)...", serverCmd.Process.Pid)
			_ = serverCmd.Process.Signal(os.Interrupt)

			done := make(chan error, 1)
			go func() { done <- serverCmd.Wait() }()

			select {
			case err := <-done:
				if err != nil {
					parentT.Logf("Server exited with: %v", err)
				} else {
					parentT.Logf("Server stopped cleanly")
				}
			case <-time.After(10 * time.Second):
				parentT.Logf("Server did not exit in 10s, killing...")
				_ = serverCmd.Process.Kill()
			}
		}
	})

	waitForServer(t, "localhost:8080", 30*time.Second)
	t.Logf("Server is ready")
}

// ---------------------------------------------------------------------------
// Helper: waitForServer
// ---------------------------------------------------------------------------

func waitForServer(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://%s/v1/deployments", addr)

	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("server at %s did not become ready within %s", addr, timeout)
}

// ---------------------------------------------------------------------------
// Helper: storeTokenForFleetctl
// ---------------------------------------------------------------------------

func storeTokenForFleetctl(t *testing.T, token *TokenResponse) {
	t.Helper()

	// Store each token field in the OS keyring so fleetctl can use them.
	const service = "fleetctl"

	sets := []struct {
		key   string
		value string
	}{
		{"access_token", token.AccessToken},
		{"refresh_token", token.RefreshToken},
		{"id_token", token.IDToken},
	}

	for _, s := range sets {
		if s.value == "" {
			continue
		}
		if err := keyring.Set(service, s.key, s.value); err != nil {
			t.Fatalf("store %s in keyring: %v", s.key, err)
		}
	}

	// Store metadata (expiry + token type).
	expiry := time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
	meta := map[string]interface{}{
		"expiry":     expiry.Format(time.RFC3339),
		"token_type": token.TokenType,
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal token meta: %v", err)
	}
	if err := keyring.Set(service, "meta", string(metaJSON)); err != nil {
		t.Fatalf("store meta in keyring: %v", err)
	}

	t.Cleanup(func() {
		for _, key := range []string{"access_token", "refresh_token", "id_token", "meta"} {
			_ = keyring.Delete(service, key)
		}
	})

	t.Logf("Tokens stored in keyring for fleetctl")
}

// ---------------------------------------------------------------------------
// Helper: setupAuth
// ---------------------------------------------------------------------------

func setupAuth(t *testing.T, binDir string, cfg *Config) {
	t.Helper()

	fleetctl := filepath.Join(binDir, "fleetctl")
	cmd := exec.Command(fleetctl, "auth", "setup",
		"--issuer-url", cfg.KeycloakIssuer,
		"--client-id", cfg.KeycloakClientID,
		"--scopes", "openid,profile,email",
		"--audience", cfg.KeycloakClientID,
		"--key-enrollment-client-id", cfg.EnrollmentClientID,
		"--registry-id", "github.com",
		"--registry-subject-expression", "claims.github_username",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	t.Logf("Running: fleetctl auth setup --issuer-url %s --client-id %s",
		cfg.KeycloakIssuer, cfg.KeycloakClientID)
	if err := cmd.Run(); err != nil {
		t.Fatalf("fleetctl auth setup: %v", err)
	}
	t.Logf("Auth setup complete")
}

func enrollSigningKey(t *testing.T, binDir string) {
	t.Helper()

	fleetctl := filepath.Join(binDir, "fleetctl")

	fmt.Println()
	fmt.Println("=== Signing Key Enrollment ===")
	fmt.Println("This will open a browser for the enrollment client.")
	fmt.Println("Log in with your Keycloak credentials.")
	fmt.Println()

	cmd := exec.Command(fleetctl, "auth", "enroll-signing")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		t.Fatalf("fleetctl auth enroll-signing: %v", err)
	}

	fmt.Println()
	fmt.Println("  ================================================================")
	fmt.Println("  ACTION REQUIRED: Add your SSH signing key to GitHub")
	fmt.Println("  ================================================================")
	fmt.Println()
	fmt.Println("  The public key was printed above by enroll-signing.")
	fmt.Println("  Copy it, then:")
	fmt.Println()
	fmt.Println("    1. Go to https://github.com/settings/keys")
	fmt.Println("    2. Click 'New SSH key', set type to 'Signing Key'")
	fmt.Println("    3. Paste the key and save")
	fmt.Println()
	fmt.Print("  Press Enter when done...")
	fmt.Scanln()
}

// ---------------------------------------------------------------------------
// Helper: createDeployment
// ---------------------------------------------------------------------------

func createDeployment(t *testing.T, binDir string, cfg *Config) {
	t.Helper()

	manifest := map[string]interface{}{
		"name":          cfg.ClusterName,
		"base_domain":   cfg.BaseDomain,
		"region":        cfg.Region,
		"role_arn":      cfg.RoleARN,
		"release_image": cfg.ReleaseImage,
	}

	// Add install_config.compute section if worker count or instance type
	// differ from defaults.
	if cfg.WorkerCount != 3 || cfg.WorkerInstanceType != "m6i.xlarge" {
		manifest["install_config"] = map[string]interface{}{
			"compute": map[string]interface{}{
				"replicas":      cfg.WorkerCount,
				"instance_type": cfg.WorkerInstanceType,
			},
		}
	}

	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	manifestFile := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifestFile, manifestJSON, 0o600); err != nil {
		t.Fatalf("write manifest file: %v", err)
	}
	t.Logf("Manifest written to %s:\n%s", manifestFile, string(manifestJSON))

	fleetctl := filepath.Join(binDir, "fleetctl")
	cmd := exec.Command(fleetctl, "deployment", "create",
		"--id", cfg.ClusterName,
		"--manifest-file", manifestFile,
		"--resource-type", "api.ocp.cluster",
		"--placement-type", "static",
		"--target-ids", "ocp-aws",
		"--sign",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	t.Logf("Running: fleetctl deployment create --id %s ...", cfg.ClusterName)
	if err := cmd.Run(); err != nil {
		t.Fatalf("fleetctl deployment create: %v", err)
	}
	t.Logf("Deployment %s created", cfg.ClusterName)
}

// ---------------------------------------------------------------------------
// Helper: waitForProvision
// ---------------------------------------------------------------------------

func waitForProvision(t *testing.T, binDir string, cfg *Config, timeout time.Duration) string {
	t.Helper()

	fleetctl := filepath.Join(binDir, "fleetctl")
	deadline := time.Now().Add(timeout)
	start := time.Now()
	lastState := ""
	pollInterval := 30 * time.Second

	t.Logf("Waiting for deployment %s to reach STATE_ACTIVE (timeout %s)...", cfg.ClusterName, timeout)

	for time.Now().Before(deadline) {
		cmd := exec.Command(fleetctl, "deployment", "get", cfg.ClusterName, "-o", "json")
		out, err := cmd.Output()
		if err != nil {
			t.Logf("[%s] fleetctl deployment get failed: %v", elapsed(start), err)
			time.Sleep(pollInterval)
			continue
		}

		var dep struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(out, &dep); err != nil {
			t.Logf("[%s] parse deployment JSON: %v", elapsed(start), err)
			time.Sleep(pollInterval)
			continue
		}

		if dep.State != lastState {
			t.Logf("[%s] State: %s -> %s", elapsed(start), lastState, dep.State)
			lastState = dep.State
		}

		switch dep.State {
		case "STATE_ACTIVE":
			t.Logf("[%s] Deployment is active", elapsed(start))
			return dep.State
		case "STATE_FAILED":
			t.Fatalf("[%s] Deployment failed", elapsed(start))
			return dep.State
		}

		time.Sleep(pollInterval)
	}

	t.Fatalf("[%s] Timed out waiting for provision (last state: %s)", elapsed(start), lastState)
	return lastState
}

// ---------------------------------------------------------------------------
// Helper: elapsed
// ---------------------------------------------------------------------------

func elapsed(start time.Time) string {
	d := time.Since(start)
	mins := int(d.Minutes())
	secs := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d", mins, secs)
}

// ---------------------------------------------------------------------------
// Helper: printClusterWarning
// ---------------------------------------------------------------------------

func printClusterWarning(cfg *Config) {
	banner := strings.Repeat("!", 72)
	fmt.Fprintf(os.Stderr, "\n"+
		"%s\n"+
		"!!\n"+
		"!!  WARNING: CLUSTER MAY STILL BE RUNNING\n"+
		"!!\n"+
		"!!  Name:   %s\n"+
		"!!  Region: %s\n"+
		"!!\n"+
		"!!  The test failed after provisioning started. The cluster may still\n"+
		"!!  exist and incur costs. To clean up manually:\n"+
		"!!\n"+
		"!!    fleetctl deployment delete %s\n"+
		"!!    # or destroy via ocp-engine / AWS console\n"+
		"!!\n"+
		"%s\n",
		banner, cfg.ClusterName, cfg.Region, cfg.ClusterName, banner)
}

// ---------------------------------------------------------------------------
// Helper: validateDeployment
// ---------------------------------------------------------------------------

func getDeploymentProperties(t *testing.T, binDir, clusterName string) map[string]interface{} {
	t.Helper()

	fleetctl := filepath.Join(binDir, "fleetctl")
	cmd := exec.Command(fleetctl, "deployment", "get", clusterName, "-o", "json")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("fleetctl deployment get: %v", err)
	}

	var dep map[string]interface{}
	if err := json.Unmarshal(out, &dep); err != nil {
		t.Fatalf("parse deployment JSON: %v", err)
	}

	state, _ := dep["state"].(string)
	if !strings.Contains(strings.ToUpper(state), "ACTIVE") {
		t.Fatalf("deployment state is %q, expected to contain ACTIVE", state)
	}

	targets, ok := dep["provisioned_targets"].([]interface{})
	if !ok || len(targets) == 0 {
		t.Fatalf("no provisioned_targets in deployment")
	}
	target, ok := targets[0].(map[string]interface{})
	if !ok {
		t.Fatalf("provisioned_targets[0] is not an object")
	}
	props, ok := target["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("provisioned_targets[0].properties is not an object")
	}
	return props
}

func validateDeployment(t *testing.T, binDir string, cfg *Config) {
	t.Helper()

	props := getDeploymentProperties(t, binDir, cfg.ClusterName)
	t.Logf("Deployment state: ACTIVE")

	// Check required property keys exist.
	requiredKeys := []string{
		"api_server", "infra_id", "cluster_id", "region",
		"role_arn", "ca_cert", "service_account_token_ref",
	}
	for _, key := range requiredKeys {
		if _, exists := props[key]; !exists {
			t.Errorf("missing required property: %s", key)
		}
	}

	// Verify region and role_arn match config.
	if region, _ := props["region"].(string); region != cfg.Region {
		t.Errorf("region mismatch: got %q, want %q", region, cfg.Region)
	}
	if roleARN, _ := props["role_arn"].(string); roleARN != cfg.RoleARN {
		t.Errorf("role_arn mismatch: got %q, want %q", roleARN, cfg.RoleARN)
	}

	// Set package-level infraID.
	infraID, _ = props["infra_id"].(string)
	t.Logf("infra_id: %s", infraID)
	t.Logf("api_server: %s", props["api_server"])
}

// ---------------------------------------------------------------------------
// Helper: validateClusterOIDC
// ---------------------------------------------------------------------------

func validateClusterOIDC(t *testing.T, cfg *Config, token *TokenResponse) {
	t.Helper()

	if token == nil {
		t.Skip("Keycloak token not available")
	}

	props := getDeploymentProperties(t, binDir, cfg.ClusterName)
	apiServer, _ := props["api_server"].(string)
	caCert, _ := props["ca_cert"].(string)

	if apiServer == "" {
		t.Fatalf("api_server is empty")
	}

	// Build rest.Config with OIDC bearer token.
	restCfg := &rest.Config{
		Host:        apiServer,
		BearerToken: token.AccessToken,
		TLSClientConfig: rest.TLSClientConfig{
			CAData: []byte(caCert),
		},
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("create kubernetes client: %v", err)
	}

	ctx := context.Background()

	// 1. Check nodes are accessible and at least one is Ready.
	t.Log("Checking nodes via OIDC auth...")
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes (OIDC auth failed?): %v", err)
	}
	if len(nodes.Items) == 0 {
		t.Fatalf("no nodes found")
	}

	readyCount := 0
	for _, node := range nodes.Items {
		for _, cond := range node.Status.Conditions {
			if cond.Type == "Ready" && cond.Status == "True" {
				readyCount++
			}
		}
	}
	t.Logf("Nodes: %d total, %d Ready", len(nodes.Items), readyCount)
	if readyCount == 0 {
		t.Fatalf("no nodes in Ready state")
	}

	// 2. Check kube-system/aws-creds secret does NOT exist (proves STS mode).
	t.Log("Checking kube-system/aws-creds does NOT exist (STS mode)...")
	_, err = clientset.CoreV1().Secrets("kube-system").Get(ctx, "aws-creds", metav1.GetOptions{})
	if err == nil {
		t.Fatalf("kube-system/aws-creds secret exists — cluster is NOT in STS mode")
	}
	t.Log("kube-system/aws-creds absent (STS mode confirmed)")

	// 3. Check operator credential secrets contain role_arn, not aws_access_key_id.
	t.Log("Checking operator credential secrets for STS role_arn...")
	stsSecrets := []struct {
		namespace string
		name      string
	}{
		{"openshift-cloud-credential-operator", "cloud-credential-operator-iam-ro-creds"},
		{"openshift-machine-api", "aws-cloud-credentials"},
		{"openshift-ingress-operator", "cloud-credentials"},
		{"openshift-cluster-csi-drivers", "ebs-cloud-credentials"},
		{"openshift-image-registry", "installer-cloud-credentials"},
	}
	for _, s := range stsSecrets {
		secret, err := clientset.CoreV1().Secrets(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
		if err != nil {
			t.Errorf("get secret %s/%s: %v", s.namespace, s.name, err)
			continue
		}

		credData := secret.Data["credentials"]
		if len(credData) == 0 {
			t.Errorf("secret %s/%s has no 'credentials' key", s.namespace, s.name)
			continue
		}

		if !bytes.Contains(credData, []byte("role_arn")) {
			t.Errorf("secret %s/%s credentials missing role_arn", s.namespace, s.name)
		}
		if bytes.Contains(credData, []byte("aws_access_key_id")) {
			t.Errorf("secret %s/%s credentials contains aws_access_key_id (not STS)", s.namespace, s.name)
		}
		t.Logf("  %s/%s: STS credentials confirmed", s.namespace, s.name)
	}

	// 4. Check key operator pods are Running.
	t.Log("Checking operator pods...")
	operatorPods := []struct {
		namespace string
		label     string
		desc      string
	}{
		{"openshift-machine-api", "api=clusterapi", "machine-api"},
		{"openshift-ingress-operator", "name=ingress-operator", "ingress-operator"},
		{"openshift-cluster-csi-drivers", "app=aws-ebs-csi-driver-controller", "ebs-csi-driver"},
		{"openshift-image-registry", "name=cluster-image-registry-operator", "image-registry-operator"},
		{"openshift-cloud-network-config-controller", "app=cloud-network-config-controller", "cloud-network-config"},
	}
	for _, op := range operatorPods {
		pods, err := clientset.CoreV1().Pods(op.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: op.label,
		})
		if err != nil {
			t.Errorf("list pods in %s (label %s): %v", op.namespace, op.label, err)
			continue
		}
		if len(pods.Items) == 0 {
			t.Errorf("no pods found in %s with label %s", op.namespace, op.label)
			continue
		}

		allRunning := true
		for _, pod := range pods.Items {
			if pod.Status.Phase != "Running" {
				allRunning = false
				t.Errorf("pod %s/%s is %s, expected Running", op.namespace, pod.Name, pod.Status.Phase)
			}
		}
		if allRunning {
			t.Logf("  %s: %d pod(s) Running", op.desc, len(pods.Items))
		}
	}
}

// ---------------------------------------------------------------------------
// Helper: destroyDeployment
// ---------------------------------------------------------------------------

func destroyDeployment(t *testing.T, binDir string, cfg *Config) {
	t.Helper()

	fleetctl := filepath.Join(binDir, "fleetctl")

	// Initiate deletion.
	cmd := exec.Command(fleetctl, "deployment", "delete", cfg.ClusterName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	t.Logf("Running: fleetctl deployment delete %s", cfg.ClusterName)
	if err := cmd.Run(); err != nil {
		t.Fatalf("fleetctl deployment delete: %v", err)
	}
	t.Logf("Delete initiated, polling for removal...")

	// Poll until the deployment is gone or fails.
	start := time.Now()
	timeout := 30 * time.Minute
	pollInterval := 15 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		time.Sleep(pollInterval)

		getCmd := exec.Command(fleetctl, "deployment", "get", cfg.ClusterName, "-o", "json")
		out, err := getCmd.Output()
		if err != nil {
			// Command failed — deployment is likely gone.
			t.Logf("[%s] Deployment not found (destroy complete)", elapsed(start))
			return
		}

		var dep struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(out, &dep); err != nil {
			t.Logf("[%s] parse error: %v", elapsed(start), err)
			continue
		}

		if strings.Contains(strings.ToUpper(dep.State), "FAILED") {
			t.Fatalf("[%s] Destroy failed: state is %s", elapsed(start), dep.State)
		}

		t.Logf("[%s] Destroy in progress (state: %s)", elapsed(start), dep.State)
	}

	t.Fatalf("[%s] Timed out waiting for destroy to complete", elapsed(start))
}

// ---------------------------------------------------------------------------
// Helper: validateCleanup
// ---------------------------------------------------------------------------

func validateCleanup(t *testing.T, cfg *Config) {
	t.Helper()

	if infraID == "" {
		t.Skip("infraID not set, skipping cleanup validation")
	}

	ctx := context.Background()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}

	// 1. Check no EC2 instances with the cluster tag.
	t.Log("Checking for orphaned EC2 instances...")
	ec2Client := ec2svc.NewFromConfig(awsCfg)
	ec2Out, err := ec2Client.DescribeInstances(ctx, &ec2svc.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{
				Name:   strPtr(fmt.Sprintf("tag:kubernetes.io/cluster/%s", infraID)),
				Values: []string{"owned", "shared"},
			},
			{
				Name:   strPtr("instance-state-name"),
				Values: []string{"running", "pending", "stopping", "stopped"},
			},
		},
	})
	if err != nil {
		t.Fatalf("describe EC2 instances: %v", err)
	}
	instanceCount := 0
	for _, r := range ec2Out.Reservations {
		instanceCount += len(r.Instances)
	}
	if instanceCount > 0 {
		t.Errorf("found %d orphaned EC2 instances with tag kubernetes.io/cluster/%s", instanceCount, infraID)
	} else {
		t.Logf("  No orphaned EC2 instances")
	}

	// 2. Check no IAM OIDC providers containing the cluster name.
	t.Log("Checking for orphaned IAM OIDC providers...")
	iamClient := iamsvc.NewFromConfig(awsCfg)
	oidcOut, err := iamClient.ListOpenIDConnectProviders(ctx, &iamsvc.ListOpenIDConnectProvidersInput{})
	if err != nil {
		t.Fatalf("list OIDC providers: %v", err)
	}
	orphanedOIDC := 0
	for _, p := range oidcOut.OpenIDConnectProviderList {
		if p.Arn != nil && strings.Contains(*p.Arn, cfg.ClusterName) {
			t.Errorf("found orphaned OIDC provider: %s", *p.Arn)
			orphanedOIDC++
		}
	}
	if orphanedOIDC == 0 {
		t.Log("  No orphaned OIDC providers")
	}

	// 3. Check no S3 buckets containing the cluster name.
	t.Log("Checking for orphaned S3 buckets...")
	s3Client := s3svc.NewFromConfig(awsCfg)
	bucketsOut, err := s3Client.ListBuckets(ctx, &s3svc.ListBucketsInput{})
	if err != nil {
		t.Fatalf("list S3 buckets: %v", err)
	}
	orphanedBuckets := 0
	for _, b := range bucketsOut.Buckets {
		if b.Name != nil && strings.Contains(*b.Name, cfg.ClusterName) {
			t.Errorf("found orphaned S3 bucket: %s", *b.Name)
			orphanedBuckets++
		}
	}
	if orphanedBuckets == 0 {
		t.Log("  No orphaned S3 buckets")
	}

	// 4. Check no IAM roles containing the cluster name.
	t.Log("Checking for orphaned IAM roles...")
	orphanedRoles := 0
	var marker *string
	for {
		rolesOut, err := iamClient.ListRoles(ctx, &iamsvc.ListRolesInput{
			Marker: marker,
		})
		if err != nil {
			t.Fatalf("list IAM roles: %v", err)
		}
		for _, r := range rolesOut.Roles {
			if r.RoleName != nil && strings.Contains(*r.RoleName, cfg.ClusterName) {
				t.Errorf("found orphaned IAM role: %s", *r.RoleName)
				orphanedRoles++
			}
		}
		if !rolesOut.IsTruncated {
			break
		}
		marker = rolesOut.Marker
	}
	if orphanedRoles == 0 {
		t.Log("  No orphaned IAM roles")
	}
}

// ---------------------------------------------------------------------------
// Helper: strPtr
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }

