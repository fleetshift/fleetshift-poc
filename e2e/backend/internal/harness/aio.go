// Package harness starts a Kind-capable Dex-on AIO container and a host fleetctl for backend E2E tests.
package harness

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Fixture is the running Kind-capable AIO + host fleetctl for one go test process.
type Fixture struct {
	// repoRoot is the fleetshift-poc checkout (Nx, cli, e2e/web).
	repoRoot string
	// containerName is the podman name of the AIO container.
	containerName string
	// caFile is the sandbox CA copied out of the container for TLS.
	caFile string
	// configDir is fleetctl --config-dir (auth.json, and credentials.json with --insecure-storage).
	configDir string
	// fleetctl is the host fleetctl binary path.
	fleetctl string
	// workDir is the temp directory owned by this fixture (configDir, caFile). Stop removes it.
	workDir string
	// engineSocket is the host unix socket mounted at /var/run/docker.sock.
	engineSocket string
	// publishUIIPv4 maps container UIPort to 127.0.0.1. Mutually exclusive with publishUIIPv6.
	publishUIIPv4 bool
	// publishUIIPv6 maps container UIPort to [::1] when the host has no IPv4 loopback.
	publishUIIPv6 bool
}

// Start builds the AIO image from this checkout (or uses a loaded tag when
// FLEETSHIFT_E2E_AIO_PREBUILT=1), starts one Kind-capable container,
// smokes the in-container kind engine, copies the sandbox CA, waits for
// /readyz, and builds fleetctl once. It does not log in.
func Start() (*Fixture, error) {
	f := &Fixture{}
	if err := f.start(); err != nil {
		f.Stop(true)
		return nil, err
	}
	return f, nil
}

// start allocates temp state, builds fleetctl, runs a Kind-capable AIO
// container, smokes the kind engine as uid 1000, and waits until /readyz
// and unauthenticated gRPC are up.
func (f *Fixture) start() error {
	root, err := findRepoRoot()
	if err != nil {
		return err
	}
	f.repoRoot = root

	addrs, err := preflight(root)
	if err != nil {
		return err
	}
	f.publishUIIPv4, f.publishUIIPv6 = uiPublish(addrs)

	socket, err := resolveEngineSocket()
	if err != nil {
		return err
	}
	f.engineSocket = socket

	workDir, err := os.MkdirTemp("", "fleetshift-e2e-backend-")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	f.workDir = workDir
	f.configDir = filepath.Join(workDir, "fleetctl")
	if err := os.MkdirAll(f.configDir, 0o700); err != nil {
		return fmt.Errorf("config dir: %w", err)
	}
	f.caFile = filepath.Join(workDir, "ca.crt")

	if err := f.ensureFleetctl(); err != nil {
		return err
	}
	if err := f.ensurePlaywrightChromium(); err != nil {
		return err
	}
	if err := f.buildAIOImage(); err != nil {
		return err
	}
	if err := ensureKindNetwork(); err != nil {
		return err
	}

	name, err := uniqueContainerName()
	if err != nil {
		return err
	}
	f.containerName = name
	if err := f.podmanRun(); err != nil {
		return err
	}
	if err := f.smokeKindEngine(); err != nil {
		return err
	}
	if err := f.copyCA(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	if err := waitReadyz(ctx, UIOrigin+"/readyz", f.caFile); err != nil {
		return fmt.Errorf("wait /readyz: %w", err)
	}
	return f.requireUnauthenticatedRPC()
}

// findRepoRoot walks from the working directory (then this source file) until
// it finds the fleetshift-poc checkout that owns Nx, fleetctl, and e2e/web.
func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	if root, ok := walkRepoRoot(wd); ok {
		return root, nil
	}
	_, file, _, ok := runtime.Caller(0)
	if ok {
		if root, found := walkRepoRoot(filepath.Dir(file)); found {
			return root, nil
		}
	}
	return "", fmt.Errorf("fleetshift-poc repo root not found from %s", wd)
}

// walkRepoRoot walks start and its parents for isRepoRoot.
func walkRepoRoot(start string) (string, bool) {
	for dir := start; ; dir = filepath.Dir(dir) {
		if isRepoRoot(dir) {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
	}
}

// isRepoRoot reports whether dir is the fleetshift-poc checkout used by this harness.
func isRepoRoot(dir string) bool {
	for _, p := range []string{
		filepath.Join(dir, "nx.json"),
		filepath.Join(dir, "cli", "cmd", "fleetctl"),
		filepath.Join(dir, "e2e", "web", "playwright.cli-login.mts"),
	} {
		if _, err := os.Stat(p); err != nil {
			return false
		}
	}
	return true
}

// Stop dumps podman/Kind evidence when failed is true, removes the AIO
// container, then best-effort leftover Kind node containers this suite
// created. It never deletes the engine socket or the kind network.
func (f *Fixture) Stop(failed bool) {
	if f == nil {
		return
	}
	if failed {
		if f.containerName != "" {
			dumpLogs(f.containerName)
		}
		dumpKindEvidence(f.containerName, f.engineSocket)
	}
	if f.containerName != "" {
		ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		_ = exec.CommandContext(ctx, "podman", "rm", "-f", f.containerName).Run()
		cancel()
	}
	removeLeftoverKindNodes()
	if f.workDir != "" {
		_ = os.RemoveAll(f.workDir)
	}
}

// CredentialsPath is the insecure-storage tokens file under --config-dir.
func (f *Fixture) CredentialsPath() string {
	return filepath.Join(f.configDir, credentialsName)
}

// preflight checks PATH tools, Nx, PublicHost loopback DNS, and that UI/gRPC ports are free.
func preflight(repoRoot string) ([]string, error) {
	for _, bin := range []string{"podman", "npx", "go"} {
		if _, err := exec.LookPath(bin); err != nil {
			return nil, fmt.Errorf("%s is required on PATH: %w", bin, err)
		}
	}
	if _, err := os.Stat(filepath.Join(repoRoot, "node_modules", "nx")); err != nil {
		return nil, fmt.Errorf("nx is not installed under %s; run npm ci once", repoRoot)
	}
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "npx", "nx", "--version")
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("npx nx cannot run: %w\n%s", err, out)
	}

	addrs, err := net.LookupHost(PublicHost)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w (this .localhost name must map to loopback; do not edit /etc/hosts)", PublicHost, err)
	}
	if err := requireLoopbackNames(addrs); err != nil {
		return nil, err
	}

	pub4, pub6 := uiPublish(addrs)
	if pub4 {
		if err := requireLoopbackFree("127.0.0.1", UIPort); err != nil {
			return nil, fmt.Errorf("%w; stop the other AIO", err)
		}
	}
	if pub6 {
		if err := requireLoopbackFree("::1", UIPort); err != nil {
			return nil, fmt.Errorf("%w; stop the other AIO", err)
		}
	}
	if err := requireLoopbackFree("127.0.0.1", GRPCPort); err != nil {
		return nil, fmt.Errorf("%w; stop the other AIO", err)
	}
	return addrs, nil
}

// requireLoopbackNames reports an error unless every addr is a loopback IP.
func requireLoopbackNames(addrs []string) error {
	if len(addrs) == 0 {
		return fmt.Errorf("%s resolved to no addresses", PublicHost)
	}
	for _, a := range addrs {
		ip := net.ParseIP(a)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("%s resolved to %q, want loopback", PublicHost, a)
		}
	}
	return nil
}

// hasIPv4Loopback reports whether addrs includes an IPv4 loopback address.
func hasIPv4Loopback(addrs []string) bool {
	for _, a := range addrs {
		ip := net.ParseIP(a)
		if ip != nil && ip.To4() != nil && ip.IsLoopback() {
			return true
		}
	}
	return false
}

// hasIPv6Loopback reports whether addrs includes an IPv6 loopback address.
func hasIPv6Loopback(addrs []string) bool {
	for _, a := range addrs {
		ip := net.ParseIP(a)
		if ip != nil && ip.To4() == nil && ip.IsLoopback() {
			return true
		}
	}
	return false
}

// uiPublish chooses a single host mapping for container UIPort. Publishing both
// 127.0.0.1 and [::1] for the same container port trips Podman rootlessport
// ("conflict with ID 1") on macOS. Dual-stack lookup still reaches IPv4 via
// Happy Eyeballs; v6-only hosts get [::1] instead.
func uiPublish(addrs []string) (ipv4, ipv6 bool) {
	if hasIPv4Loopback(addrs) {
		return true, false
	}
	if hasIPv6Loopback(addrs) {
		return false, true
	}
	return true, false
}

// requireLoopbackFree binds ip:port to confirm it is free, then closes the listener.
func requireLoopbackFree(ip, port string) error {
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, port))
	if err != nil {
		return fmt.Errorf("host port %s is occupied", net.JoinHostPort(ip, port))
	}
	return ln.Close()
}

// uniqueContainerName returns a podman name with a random suffix.
func uniqueContainerName() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("container name: %w", err)
	}
	return fmt.Sprintf("fleetshift-e2e-backend-%x", b), nil
}

// buildAIOImage runs nx image:aio from this checkout unless
// FLEETSHIFT_E2E_AIO_PREBUILT=1 and ImageRef is already loaded.
func (f *Fixture) buildAIOImage() error {
	if prebuiltAIORequested() {
		return f.usePrebuiltAIO()
	}
	envPath := filepath.Join(f.repoRoot, ".env")
	if _, err := os.Stat(envPath); os.IsNotExist(err) {
		if err := os.WriteFile(envPath, nil, 0o600); err != nil {
			return fmt.Errorf("create .env: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), imageBuildTimeout)
	defer cancel()
	f.logf("building AIO image")
	cmd := exec.CommandContext(ctx, "npx", "nx", "run", nxImageAIO)
	cmd.Dir = f.repoRoot
	cmd.Env = append(os.Environ(), "NX_DAEMON=false")
	if err := f.runQuiet(cmd, "image-aio.log"); err != nil {
		return fmt.Errorf("npx nx run %s: %w", nxImageAIO, err)
	}
	f.logf("AIO image ready")
	return nil
}

// prebuiltAIORequested reports whether FLEETSHIFT_E2E_AIO_PREBUILT=1.
// Local runs leave it unset so TestMain still builds.
func prebuiltAIORequested() bool {
	return os.Getenv(prebuiltAIOEnv) == "1"
}

// usePrebuiltAIO requires ImageRef in the local store. CI loads that tag from
// the aio-image tar; unset FLEETSHIFT_E2E_AIO_PREBUILT to build from source.
func (f *Fixture) usePrebuiltAIO() error {
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "podman", "image", "exists", ImageRef)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s=1 but %s is not loaded (unset it to build from source): %w", prebuiltAIOEnv, ImageRef, err)
	}
	f.logf("using prebuilt AIO %s", ImageRef)
	return nil
}

// podmanRun starts the Kind-capable AIO with UI and gRPC published to loopback.
// It does not pass --user 0:0; the image drops privileges itself.
func (f *Fixture) podmanRun() error {
	args := []string{
		"run", "-d", "--pull=never",
		"--privileged",
		"--name", f.containerName,
		"--label", labelKey + "=" + labelValue,
		"--network", kindNetwork + ":alias=" + kindNetworkAlias,
	}
	if f.publishUIIPv4 {
		args = append(args, "-p", "127.0.0.1:"+UIPort+":"+UIPort)
	}
	if f.publishUIIPv6 {
		args = append(args, "-p", "[::1]:"+UIPort+":"+UIPort)
	}
	args = append(args,
		"-p", "127.0.0.1:"+GRPCPort+":"+GRPCPort,
		"-v", f.engineSocket+":"+containerEngineSocket,
		"-v", "/tmp:/tmp",
		ImageRef,
	)
	f.logf("starting container %s", f.containerName)
	ctx, cancel := context.WithTimeout(context.Background(), podmanRunTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "podman", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("podman run: %w\n%s", err, trimOutput(out))
	}
	return nil
}

// smokeKindEngine execs into the suite AIO and runs `podman ps` with Kind's
// cluster label through the mounted engine socket. Fail here instead of
// waiting for TestKindClusterLifecycle to poll. Socket access as uid 1000 is
// packaging (s6-applyuidgid -G); this only checks the mount speaks the API.
func (f *Fixture) smokeKindEngine() error {
	ctx, cancel := context.WithTimeout(context.Background(), smokeKindTimeout)
	defer cancel()
	f.logf("smoke-testing kind engine in %s (host socket %s)", f.containerName, f.engineSocket)
	if err := poll(ctx, pollInterval, func() error {
		cmd := exec.CommandContext(ctx, "podman", "exec", f.containerName, "true")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("podman exec: %w\n%s", err, trimOutput(out))
		}
		return nil
	}); err != nil {
		return fmt.Errorf("smoke kind engine: wait for exec: %w", err)
	}
	cmd := exec.CommandContext(ctx, "podman", "exec", f.containerName,
		"podman", "ps", "-a", "--filter", "label="+kindClusterLabel)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("smoke kind engine: %w\n%s", err, trimOutput(out))
	}
	return nil
}

// trimOutput returns a trimmed string copy of command combined output.
func trimOutput(b []byte) string {
	return strings.TrimSpace(string(b))
}

// copyCA polls until the sandbox CA can be copied from the container to the fixture CA file.
func (f *Fixture) copyCA() error {
	ctx, cancel := context.WithTimeout(context.Background(), copyCATimeout)
	defer cancel()
	f.logf("copying sandbox CA from %s:%s", f.containerName, containerCAPath)
	return poll(ctx, pollInterval, func() error {
		cmd := exec.CommandContext(ctx, "podman", "cp", f.containerName+":"+containerCAPath, f.caFile)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("podman cp CA: %s", trimOutput(out))
		}
		data, err := os.ReadFile(f.caFile)
		if err != nil {
			return err
		}
		if !strings.Contains(string(data), "BEGIN CERTIFICATE") {
			return fmt.Errorf("copied CA is not a PEM certificate")
		}
		return nil
	})
}

// requireUnauthenticatedRPC waits until deployment list fails with Unauthenticated.
// That error is the readiness signal (gRPC is up and requires credentials).
// begin/end log lines mark the probe so a later failure dump is not mistaken
// for a real auth outage.
func (f *Fixture) requireUnauthenticatedRPC() error {
	const reason = "deployment list before login (gRPC up, no credentials yet)"
	f.logf("begin expected Unauthenticated: %s", reason)

	ctx, cancel := context.WithTimeout(context.Background(), grpcAuthTimeout)
	defer cancel()

	var insecureAdmin bool
	err := poll(ctx, time.Second, func() error {
		runCtx, runCancel := context.WithTimeout(ctx, grpcProbeTimeout)
		defer runCancel()
		res := f.Run(runCtx, "deployment", "list")
		if res.Err == nil {
			// Stop polling immediately; success is a hard refusal, not a retry.
			insecureAdmin = true
			return nil
		}
		if unauthenticatedRPC(res) {
			return nil
		}
		return fmt.Errorf("gRPC not ready: %s", res.Stderr)
	})
	if insecureAdmin {
		f.logf("did not get expected Unauthenticated: %s (command succeeded)", reason)
		return fmt.Errorf("unauthenticated deployment list succeeded; refusing to continue (insecure admin?)")
	}
	if err != nil {
		f.logf("did not get expected Unauthenticated: %s: %v", reason, err)
		return err
	}
	f.logf("end expected Unauthenticated: %s", reason)
	return nil
}

// ensureFleetctl builds ./cmd/fleetctl into this checkout's bin directory.
func (f *Fixture) ensureFleetctl() error {
	out := filepath.Join(f.repoRoot, "bin", "fleetctl")
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), fleetctlBuildTimeout)
	defer cancel()
	f.logf("building fleetctl")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, "./cmd/fleetctl")
	cmd.Dir = filepath.Join(f.repoRoot, "cli")
	if err := f.runQuiet(cmd, "fleetctl-build.log"); err != nil {
		return fmt.Errorf("go build fleetctl: %w", err)
	}
	f.fleetctl = out
	return nil
}

// ensurePlaywrightChromium installs Playwright's Chromium browser for CLI login.
func (f *Fixture) ensurePlaywrightChromium() error {
	ctx, cancel := context.WithTimeout(context.Background(), playwrightInstallTimeout)
	defer cancel()
	f.logf("ensuring Playwright Chromium")
	cmd := exec.CommandContext(ctx, "npx", "playwright", "install", "chromium")
	cmd.Dir = filepath.Join(f.repoRoot, "e2e", "web")
	if err := f.runQuiet(cmd, "playwright-install.log"); err != nil {
		return fmt.Errorf("npx playwright install chromium: %w", err)
	}
	return nil
}

// runQuiet runs cmd, writing stdout and stderr to workDir/logName. On failure
// the log contents are included in the returned error.
func (f *Fixture) runQuiet(cmd *exec.Cmd, logName string) error {
	logPath := filepath.Join(f.workDir, logName)
	lf, err := os.Create(logPath)
	if err != nil {
		return err
	}
	cmd.Stdout = lf
	cmd.Stderr = lf
	runErr := cmd.Run()
	closeErr := lf.Close()
	if runErr != nil {
		dump, _ := os.ReadFile(logPath)
		return fmt.Errorf("%w\n%s", runErr, trimOutput(dump))
	}
	if closeErr != nil {
		return fmt.Errorf("close %s: %w", logName, closeErr)
	}
	return nil
}

// waitReadyz GETs readyURL until it returns 200, using caFile for TLS.
func waitReadyz(ctx context.Context, readyURL, caFile string) error {
	client, err := tlsClient(caFile)
	if err != nil {
		return err
	}
	return poll(ctx, pollInterval, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, readyURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("/readyz returned %d", resp.StatusCode)
		}
		return nil
	})
}

// tlsClient returns an HTTP client that trusts only the PEM CA at caFile.
func tlsClient(caFile string) (*http.Client, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("parse CA file %s: no certificates found", caFile)
	}
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    pool,
			},
		},
	}, nil
}

// poll calls fn until it succeeds or ctx is done.
func poll(ctx context.Context, interval time.Duration, fn func() error) error {
	if interval <= 0 {
		interval = pollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	last := fn()
	if last == nil {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			if last != nil {
				return fmt.Errorf("%w: %v", ctx.Err(), last)
			}
			return ctx.Err()
		case <-ticker.C:
			last = fn()
			if last == nil {
				return nil
			}
		}
	}
}

// dumpLogs writes podman logs for name to stderr.
func dumpLogs(name string) {
	fmt.Fprintf(os.Stderr, "===== podman logs %s =====\n", name)
	runToStderr("podman", "logs", name)
}

// runToStderr runs name with args and copies both streams to stderr.
func runToStderr(name string, args ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

// logf writes an e2e/backend progress line to stderr.
func (f *Fixture) logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "e2e/backend: "+format+"\n", args...)
}
