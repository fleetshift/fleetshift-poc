package gcphcp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakePSCResourceLookup struct {
	forwardingRuleResults []bool
	addressResults        []bool
	forwardingRuleErr     error
	addressErr            error
	forwardingRuleCalls   int
	addressCalls          int
	forwardingRuleNames   []string
	addressNames          []string
}

func (f *fakePSCResourceLookup) ForwardingRuleExists(
	_ context.Context,
	_ string,
	_ string,
	name string,
) (bool, error) {
	f.forwardingRuleNames = append(f.forwardingRuleNames, name)
	if f.forwardingRuleErr != nil {
		return false, f.forwardingRuleErr
	}
	idx := f.forwardingRuleCalls
	f.forwardingRuleCalls++
	if len(f.forwardingRuleResults) == 0 {
		return false, nil
	}
	if idx >= len(f.forwardingRuleResults) {
		idx = len(f.forwardingRuleResults) - 1
	}
	return f.forwardingRuleResults[idx], nil
}

func (f *fakePSCResourceLookup) AddressExists(
	_ context.Context,
	_ string,
	_ string,
	name string,
) (bool, error) {
	f.addressNames = append(f.addressNames, name)
	if f.addressErr != nil {
		return false, f.addressErr
	}
	idx := f.addressCalls
	f.addressCalls++
	if len(f.addressResults) == 0 {
		return false, nil
	}
	if idx >= len(f.addressResults) {
		idx = len(f.addressResults) - 1
	}
	return f.addressResults[idx], nil
}

// withFastPSCCleanupTimers saves and restores newPSCResourceLookup,
// pscCleanupPollInterval, and pscCleanupWaitTimeout, setting fast
// defaults (1ms interval, 25ms timeout) for testing.
func withFastPSCCleanupTimers(t *testing.T) {
	t.Helper()
	origLookup := newPSCResourceLookup
	origInterval := pscCleanupPollInterval
	origTimeout := pscCleanupWaitTimeout
	pscCleanupPollInterval = time.Millisecond
	pscCleanupWaitTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		newPSCResourceLookup = origLookup
		pscCleanupPollInterval = origInterval
		pscCleanupWaitTimeout = origTimeout
	})
}

func TestWaitForPSCCleanup_PollsUntilEndpointArtifactsDisappear(t *testing.T) {
	withFastPSCCleanupTimers(t)

	lookup := &fakePSCResourceLookup{
		forwardingRuleResults: []bool{true, false},
		addressResults:        []bool{true, false},
	}
	var receivedToken string
	newPSCResourceLookup = func(_ context.Context, workforceToken string) (pscResourceLookup, error) {
		receivedToken = workforceToken
		return lookup, nil
	}

	runner := &InfraRunner{HypershiftBinary: "hypershift"}
	if err := runner.WaitForPSCCleanup(
		context.Background(),
		"cluster-123",
		"project-123",
		"us-central1",
		"workforce-token",
		nil,
	); err != nil {
		t.Fatalf("WaitForPSCCleanup() error = %v", err)
	}

	if receivedToken != "workforce-token" {
		t.Fatalf("received workforce token = %q, want workforce-token", receivedToken)
	}
	if lookup.forwardingRuleCalls != 2 {
		t.Fatalf("forwarding rule calls = %d, want 2", lookup.forwardingRuleCalls)
	}
	if lookup.addressCalls != 2 {
		t.Fatalf("address calls = %d, want 2", lookup.addressCalls)
	}
	if len(lookup.forwardingRuleNames) == 0 || lookup.forwardingRuleNames[0] != "psc-cluster-123-endpoint" {
		t.Fatalf("forwarding rule names = %v, want psc-cluster-123-endpoint", lookup.forwardingRuleNames)
	}
	if len(lookup.addressNames) == 0 || lookup.addressNames[0] != "psc-cluster-123-ip" {
		t.Fatalf("address names = %v, want psc-cluster-123-ip", lookup.addressNames)
	}
}

func TestWaitForPSCCleanup_EmitsProgressWhileArtifactsRemain(t *testing.T) {
	withFastPSCCleanupTimers(t)

	newPSCResourceLookup = func(_ context.Context, _ string) (pscResourceLookup, error) {
		return &fakePSCResourceLookup{
			forwardingRuleResults: []bool{true, true, false},
			addressResults:        []bool{true, true, false},
		}, nil
	}

	obs := &testEventRecorder{}
	runner := &InfraRunner{HypershiftBinary: "hypershift"}
	if err := runner.WaitForPSCCleanup(
		context.Background(),
		"cluster-123",
		"project-123",
		"us-central1",
		"workforce-token",
		newTestProgress(obs),
	); err != nil {
		t.Fatalf("WaitForPSCCleanup() error = %v", err)
	}

	waitMessages := 0
	for _, event := range obs.snapshot() {
		if event.Message == "Waiting for PSC endpoint cleanup" {
			waitMessages++
		}
	}
	if waitMessages != 2 {
		t.Fatalf("waiting progress messages = %d, want 2", waitMessages)
	}
}

func TestWaitForPSCCleanup_TimesOutWhenArtifactsRemain(t *testing.T) {
	withFastPSCCleanupTimers(t)
	pscCleanupWaitTimeout = 5 * time.Millisecond

	newPSCResourceLookup = func(_ context.Context, _ string) (pscResourceLookup, error) {
		return &fakePSCResourceLookup{
			forwardingRuleResults: []bool{true},
			addressResults:        []bool{true},
		}, nil
	}

	runner := &InfraRunner{HypershiftBinary: "hypershift"}
	err := runner.WaitForPSCCleanup(
		context.Background(),
		"cluster-123",
		"project-123",
		"us-central1",
		"workforce-token",
		nil,
	)
	if err == nil {
		t.Fatal("expected timeout waiting for PSC cleanup")
	}
	if err.Error() != "timeout waiting for PSC endpoint cleanup" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForPSCCleanup_ReturnsLookupCreationError(t *testing.T) {
	origLookup := newPSCResourceLookup
	defer func() {
		newPSCResourceLookup = origLookup
	}()

	newPSCResourceLookup = func(_ context.Context, _ string) (pscResourceLookup, error) {
		return nil, errors.New("compute client init failed")
	}

	runner := &InfraRunner{HypershiftBinary: "hypershift"}
	err := runner.WaitForPSCCleanup(
		context.Background(),
		"cluster-123",
		"project-123",
		"us-central1",
		"workforce-token",
		nil,
	)
	if err == nil {
		t.Fatal("expected lookup creation error")
	}
	if err.Error() != "create PSC cleanup lookup: compute client init failed" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPrepareCreateHypershiftWorkspace_WritesFilesAndCleansUp(t *testing.T) {
	workspace, err := PrepareCreateHypershiftWorkspace(
		"caller-token",
		TargetConfig{
			GCPProject:        "project-123",
			WorkforcePool:     "pool-123",
			WorkforceProvider: "provider-123",
		},
		[]byte(`{"keys":[{"kid":"test-key"}]}`),
	)
	if err != nil {
		t.Fatalf("PrepareCreateHypershiftWorkspace() error = %v", err)
	}

	if workspace.tempDir == "" {
		t.Fatal("expected workspace temp dir")
	}
	tempDir := workspace.tempDir
	if got := workspace.JWKSPath; got != filepath.Join(tempDir, "jwks.json") {
		t.Fatalf("JWKSPath = %q, want %q", got, filepath.Join(tempDir, "jwks.json"))
	}

	adcPath := lookupHypershiftEnvVar(workspace.Env, "GOOGLE_APPLICATION_CREDENTIALS")
	if adcPath != filepath.Join(tempDir, "workforce-cred.json") {
		t.Fatalf("GOOGLE_APPLICATION_CREDENTIALS = %q, want %q", adcPath, filepath.Join(tempDir, "workforce-cred.json"))
	}
	if got := lookupHypershiftEnvVar(workspace.Env, "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES"); got != "" {
		t.Fatalf("GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES = %q, want empty", got)
	}

	subjectData, err := os.ReadFile(filepath.Join(tempDir, "subject_token.txt"))
	if err != nil {
		t.Fatalf("read subject token: %v", err)
	}
	if string(subjectData) != "caller-token" {
		t.Fatalf("subject token content = %q, want caller-token", string(subjectData))
	}

	credConfigData, err := os.ReadFile(adcPath)
	if err != nil {
		t.Fatalf("read credential config: %v", err)
	}
	var credConfig map[string]any
	if err := json.Unmarshal(credConfigData, &credConfig); err != nil {
		t.Fatalf("credential config JSON parse error = %v", err)
	}
	if credConfig["type"] != "external_account" {
		t.Fatalf("credential config type = %v, want external_account", credConfig["type"])
	}
	credSource, ok := credConfig["credential_source"].(map[string]any)
	if !ok {
		t.Fatalf("credential_source type = %T, want map[string]any", credConfig["credential_source"])
	}
	if got := credSource["file"]; got != filepath.Join(tempDir, "subject_token.txt") {
		t.Fatalf("credential_source.file = %v, want %q", got, filepath.Join(tempDir, "subject_token.txt"))
	}

	jwksData, err := os.ReadFile(workspace.JWKSPath)
	if err != nil {
		t.Fatalf("read JWKS: %v", err)
	}
	if string(jwksData) != `{"keys":[{"kid":"test-key"}]}` {
		t.Fatalf("JWKS content = %q, want original payload", string(jwksData))
	}

	if err := workspace.Cleanup(); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, err := os.Stat(tempDir); !os.IsNotExist(err) {
		t.Fatalf("workspace temp dir still exists after cleanup: %v", err)
	}
}

func lookupHypershiftEnvVar(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func makeUnremovableWorkspace(t *testing.T) (*HypershiftWorkspace, func()) {
	t.Helper()
	parentDir, err := os.MkdirTemp("", "gcphcp-cleanup-parent-*")
	if err != nil {
		t.Fatalf("os.MkdirTemp() error = %v", err)
	}
	childDir := filepath.Join(parentDir, "workspace")
	if err := os.Mkdir(childDir, 0700); err != nil {
		os.RemoveAll(parentDir)
		t.Fatalf("os.Mkdir() error = %v", err)
	}
	protectedFile := filepath.Join(childDir, "protected.txt")
	if err := os.WriteFile(protectedFile, []byte("data"), 0600); err != nil {
		os.RemoveAll(parentDir)
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	if err := os.Chmod(childDir, 0500); err != nil {
		os.RemoveAll(parentDir)
		t.Fatalf("os.Chmod() error = %v", err)
	}

	cleanup := func() {
		os.Chmod(childDir, 0700)
		os.RemoveAll(parentDir)
	}

	return &HypershiftWorkspace{tempDir: childDir}, cleanup
}

func TestCleanupOnReturn_JoinsCleanupErrorWithExistingError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot make directory unremovable as root")
	}

	workspace, cleanup := makeUnremovableWorkspace(t)
	defer cleanup()

	retErr := errors.New("original reconcile error")
	workspace.CleanupOnReturn(&retErr)

	if retErr == nil {
		t.Fatal("expected joined error")
	}
	if !strings.Contains(retErr.Error(), "original reconcile error") {
		t.Fatalf("error = %q, want original error preserved", retErr.Error())
	}
	if !strings.Contains(retErr.Error(), "cleanup hypershift workspace") {
		t.Fatalf("error = %q, want cleanup error joined", retErr.Error())
	}
}

func TestCleanupOnReturn_SetsCleanupErrorWhenRetErrNil(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot make directory unremovable as root")
	}

	workspace, cleanup := makeUnremovableWorkspace(t)
	defer cleanup()

	var retErr error
	workspace.CleanupOnReturn(&retErr)

	if retErr == nil {
		t.Fatal("expected cleanup error")
	}
	if !strings.Contains(retErr.Error(), "cleanup hypershift workspace") {
		t.Fatalf("error = %q, want cleanup error", retErr.Error())
	}
}

func TestCleanupOnReturn_NoErrorWhenCleanupSucceeds(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gcphcp-cleanup-test-*")
	if err != nil {
		t.Fatalf("os.MkdirTemp() error = %v", err)
	}

	workspace := &HypershiftWorkspace{tempDir: tempDir}

	existingErr := errors.New("original error")
	workspace.CleanupOnReturn(&existingErr)

	if existingErr.Error() != "original error" {
		t.Fatalf("error = %q, want original error unchanged", existingErr.Error())
	}
}

func TestPrepareDestroyHypershiftWorkspace_CreatesWorkspaceWithoutJWKS(t *testing.T) {
	workspace, err := PrepareDestroyHypershiftWorkspace(
		"caller-token",
		TargetConfig{
			GCPProject:        "project-123",
			WorkforcePool:     "pool-123",
			WorkforceProvider: "provider-123",
		},
	)
	if err != nil {
		t.Fatalf("PrepareDestroyHypershiftWorkspace() error = %v", err)
	}
	defer workspace.Cleanup()

	if workspace.JWKSPath != "" {
		t.Fatalf("JWKSPath = %q, want empty for destroy workspace", workspace.JWKSPath)
	}
	if len(workspace.Env) == 0 {
		t.Fatal("expected non-empty Env")
	}

	adcPath := lookupHypershiftEnvVar(workspace.Env, "GOOGLE_APPLICATION_CREDENTIALS")
	if adcPath == "" {
		t.Fatal("missing GOOGLE_APPLICATION_CREDENTIALS")
	}
	if _, err := os.Stat(adcPath); err != nil {
		t.Fatalf("credential config file missing: %v", err)
	}
}
