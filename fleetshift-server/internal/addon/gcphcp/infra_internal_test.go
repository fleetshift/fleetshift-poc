package gcphcp

import (
	"context"
	"errors"
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

func TestWaitForPSCCleanup_PollsUntilEndpointArtifactsDisappear(t *testing.T) {
	origLookup := newPSCResourceLookup
	origInterval := pscCleanupPollInterval
	origTimeout := pscCleanupWaitTimeout
	pscCleanupPollInterval = time.Millisecond
	pscCleanupWaitTimeout = 25 * time.Millisecond
	defer func() {
		newPSCResourceLookup = origLookup
		pscCleanupPollInterval = origInterval
		pscCleanupWaitTimeout = origTimeout
	}()

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
	origLookup := newPSCResourceLookup
	origInterval := pscCleanupPollInterval
	origTimeout := pscCleanupWaitTimeout
	pscCleanupPollInterval = time.Millisecond
	pscCleanupWaitTimeout = 25 * time.Millisecond
	defer func() {
		newPSCResourceLookup = origLookup
		pscCleanupPollInterval = origInterval
		pscCleanupWaitTimeout = origTimeout
	}()

	newPSCResourceLookup = func(_ context.Context, _ string) (pscResourceLookup, error) {
		return &fakePSCResourceLookup{
			forwardingRuleResults: []bool{true, true, false},
			addressResults:        []bool{true, true, false},
		}, nil
	}

	obs := &recordingDeliveryObserver{}
	runner := &InfraRunner{HypershiftBinary: "hypershift"}
	if err := runner.WaitForPSCCleanup(
		context.Background(),
		"cluster-123",
		"project-123",
		"us-central1",
		"workforce-token",
		newRecordingSignaler(obs),
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
	origLookup := newPSCResourceLookup
	origInterval := pscCleanupPollInterval
	origTimeout := pscCleanupWaitTimeout
	pscCleanupPollInterval = time.Millisecond
	pscCleanupWaitTimeout = 5 * time.Millisecond
	defer func() {
		newPSCResourceLookup = origLookup
		pscCleanupPollInterval = origInterval
		pscCleanupWaitTimeout = origTimeout
	}()

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
