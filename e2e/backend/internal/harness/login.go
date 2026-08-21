package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Login runs auth setup + auth login --no-browser and completes Dex through
// Playwright. TestMain owns this; tests must not call it again.
func (f *Fixture) Login(persona string) error {
	if persona == "" {
		persona = "ops"
	}
	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	if err := poll(ctx, time.Second, func() error {
		setup := f.Run(ctx,
			"auth", "setup",
			"--issuer-url", Issuer,
			"--client-id", cliClientID,
			"--oidc-ca-file", f.caFile,
			"--scopes", cliScopes,
		)
		if setup.Err != nil {
			return fmt.Errorf("auth setup: %s", setup.Stderr)
		}
		return nil
	}); err != nil {
		return err
	}

	args := append(fleetctlArgs(f.configDir), "auth", "login", "--no-browser")
	cmd := exec.CommandContext(ctx, f.fleetctl, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("login stdout: %w", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("auth login: %w", err)
	}
	defer killIfRunning(cmd)

	authURL, err := readAuthURL(ctx, stdout)
	if err != nil {
		return fmt.Errorf("auth login AUTH_URL: %w\n%s", err, stderr.String())
	}

	if err := f.runPlaywrightCLILogin(ctx, persona, authURL); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("auth login: %w\n%s", err, stderr.String())
	}
	if _, err := os.Stat(f.CredentialsPath()); err != nil {
		return fmt.Errorf("credentials after login: %w", err)
	}
	f.logf("logged in as %s", persona)
	return nil
}

// AccessToken returns the suite fleetctl access token from insecure-storage
// credentials.json. Do not log the return value.
func (f *Fixture) AccessToken() (string, error) {
	if f == nil {
		return "", fmt.Errorf("nil fixture")
	}
	data, err := os.ReadFile(f.CredentialsPath())
	if err != nil {
		return "", fmt.Errorf("read credentials: %w", err)
	}
	return parseAccessToken(data)
}

// parseAccessToken reads access_token from fleetctl credentials.json bytes.
func parseAccessToken(raw []byte) (string, error) {
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &tok); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}
	if strings.TrimSpace(tok.AccessToken) == "" {
		return "", fmt.Errorf("credentials.json has no access_token")
	}
	return tok.AccessToken, nil
}

// killIfRunning kills cmd if it is still running. After Wait, ProcessState is
// set and this is a no-op.
func killIfRunning(cmd *exec.Cmd) {
	if cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

// parseAuthURLLine returns the URL from a Fleetctl `AUTH_URL <url>` stdout line.
func parseAuthURLLine(line string) (string, bool) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(line), "AUTH_URL ")
	if !ok {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	return rest, rest != ""
}

// readAuthURL scans r for an AUTH_URL line and returns the URL.
func readAuthURL(ctx context.Context, r io.Reader) (string, error) {
	type result struct {
		url string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		sc := bufio.NewScanner(r)
		buf := make([]byte, 0, 64*1024)
		sc.Buffer(buf, 1<<20)
		for sc.Scan() {
			if u, ok := parseAuthURLLine(sc.Text()); ok {
				ch <- result{url: u}
				_, _ = io.Copy(io.Discard, r)
				return
			}
		}
		if err := sc.Err(); err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{err: fmt.Errorf("process output ended before AUTH_URL")}
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-ch:
		return res.url, res.err
	}
}

// runPlaywrightCLILogin runs the CLI-login Playwright project with persona and AUTH_URL.
func (f *Fixture) runPlaywrightCLILogin(ctx context.Context, persona, authURL string) error {
	cmd := exec.CommandContext(ctx, "npx", "playwright", "test", "--config=playwright.cli-login.mts")
	cmd.Dir = filepath.Join(f.repoRoot, "e2e", "web")
	cmd.Env = append(os.Environ(), "PERSONA="+persona, "AUTH_URL="+authURL)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	f.logf("playwright CLI login persona=%s", persona)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("playwright CLI login: %w", err)
	}
	return nil
}
