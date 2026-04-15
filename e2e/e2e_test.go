//go:build e2e

package e2e

import (
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

	"github.com/zalando/go-keyring"
)

var (
	cfg           *Config
	keycloakToken *TokenResponse
	pullSecret    []byte
	binDir        string
	repoRoot      string
	serverCmd     *exec.Cmd
	infraID       string // set after provision

	// pullSecretFile is the path to the pull secret file consumed by the
	// server. It is written with an empty placeholder in startServer and
	// overwritten with the real secret in step 04.
	pullSecretFile string
)

func TestE2E(t *testing.T) {
	// Setup: find repo root, load config, generate cluster name.
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
		if provisioned && failed {
			printClusterWarning(cfg)
		}
	})

	t.Run("01_Build", func(t *testing.T) {
		buildBinaries(t, repoRoot, binDir)
	})

	t.Run("02_StartServer", func(t *testing.T) {
		startServer(t, binDir, repoRoot, cfg)
	})

	t.Run("03_KeycloakLogin", func(t *testing.T) {
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
		setupAuth(t, binDir, cfg)
	})

	t.Run("04_RedHatSSOLogin", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		token, err := DeviceCodeLogin(ctx,
			cfg.RHSSOIssuer, cfg.RHSSOClientID,
			"openid", "Red Hat SSO Login")
		if err != nil {
			t.Fatalf("Red Hat SSO device code login: %v", err)
		}

		ps, err := FetchPullSecret(ctx, token.AccessToken)
		if err != nil {
			t.Fatalf("fetch pull secret: %v", err)
		}
		pullSecret = []byte(ps)
		t.Logf("Pull secret fetched (%d bytes)", len(pullSecret))

		// Overwrite the placeholder pull secret file so the server picks up
		// the real credentials for subsequent provision requests.
		if pullSecretFile != "" {
			if err := os.WriteFile(pullSecretFile, pullSecret, 0o600); err != nil {
				t.Fatalf("overwrite pull secret file: %v", err)
			}
			t.Logf("Pull secret written to %s", pullSecretFile)
		}
	})

	t.Run("05_CreateDeployment", func(t *testing.T) {
		createDeployment(t, binDir, cfg)
		provisioned = true
	})

	t.Run("06_WaitForProvision", func(t *testing.T) {
		state := waitForProvision(t, binDir, cfg, 60*time.Minute)
		if state != "STATE_ACTIVE" {
			failed = true
			t.Fatalf("provision ended in state %s, expected STATE_ACTIVE", state)
		}
		t.Logf("Deployment reached STATE_ACTIVE")
	})

	t.Run("07_ValidateDeployment", func(t *testing.T) {
		validateDeployment(t, binDir, cfg)
	})

	t.Run("08_ValidateClusterOIDC", func(t *testing.T) {
		validateClusterOIDC(t, cfg, keycloakToken)
	})

	t.Run("09_DestroyDeployment", func(t *testing.T) {
		destroyDeployment(t, binDir, cfg)
	})

	t.Run("10_ValidateCleanup", func(t *testing.T) {
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

func startServer(t *testing.T, binDir, repoRoot string, cfg *Config) {
	t.Helper()

	// Create a temp directory for the database.
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "fleetshift-e2e.db")

	// Write an empty placeholder pull secret file. This will be overwritten
	// with real credentials in step 04.
	psFile := filepath.Join(dbDir, "pull-secret.json")
	if err := os.WriteFile(psFile, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write placeholder pull secret: %v", err)
	}
	pullSecretFile = psFile

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
	serverCmd.Stdout = os.Stdout
	serverCmd.Stderr = os.Stderr

	t.Logf("Starting fleetshift-server (db=%s)...", dbPath)
	if err := serverCmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	t.Cleanup(func() {
		if serverCmd != nil && serverCmd.Process != nil {
			t.Logf("Stopping fleetshift-server (pid %d)...", serverCmd.Process.Pid)
			_ = serverCmd.Process.Signal(os.Interrupt)

			done := make(chan error, 1)
			go func() { done <- serverCmd.Wait() }()

			select {
			case err := <-done:
				if err != nil {
					t.Logf("Server exited with: %v", err)
				} else {
					t.Logf("Server stopped cleanly")
				}
			case <-time.After(10 * time.Second):
				t.Logf("Server did not exit in 10s, killing...")
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
		"--scopes", "openid",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	t.Logf("Running: fleetctl auth setup --issuer-url %s --client-id %s --scopes openid",
		cfg.KeycloakIssuer, cfg.KeycloakClientID)
	if err := cmd.Run(); err != nil {
		t.Fatalf("fleetctl auth setup: %v", err)
	}
	t.Logf("Auth setup complete")
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
// Placeholder functions — filled in by subsequent tasks
// ---------------------------------------------------------------------------

func validateDeployment(t *testing.T, binDir string, cfg *Config) {
	t.Helper()
	t.Log("TODO: validateDeployment — will be implemented in Task 5")
}

func validateClusterOIDC(t *testing.T, cfg *Config, token *TokenResponse) {
	t.Helper()
	t.Log("TODO: validateClusterOIDC — will be implemented in Task 6")
}

func destroyDeployment(t *testing.T, binDir string, cfg *Config) {
	t.Helper()
	t.Log("TODO: destroyDeployment — will be implemented in Task 7")
}

func validateCleanup(t *testing.T, cfg *Config) {
	t.Helper()
	t.Log("TODO: validateCleanup — will be implemented in Task 7")
}
