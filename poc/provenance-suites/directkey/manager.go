package directkey

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// Manager is the direct-key/v1 resource-manager API. It stores immutable
// TypedEvidence and a couriered public-key map used only for the RM's own
// request decision. That map is not target trust state.
type Manager struct {
	mu       sync.Mutex
	evidence map[protocol.Digest]protocol.TypedEvidence
	keys     map[protocol.Principal][]byte
}

// NewManager returns an empty resource-manager profile implementation.
func NewManager() *Manager {
	return &Manager{
		evidence: make(map[protocol.Digest]protocol.TypedEvidence),
		keys:     make(map[protocol.Principal][]byte),
	}
}

// ProvenanceType implements protocol.ResourceManagerAPI.
func (m *Manager) ProvenanceType() protocol.ProvenanceType {
	return protocol.ProvenanceTypeDirectKeyV1
}

// StoreEvidence retains original immutable TypedEvidence, content-addressed
// by its domain-separated identity.
func (m *Manager) StoreEvidence(_ context.Context, evidence protocol.TypedEvidence) (protocol.Digest, error) {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return "", fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	identity, err := evidence.Identity()
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.evidence[identity]; ok {
		if !bytes.Equal(existing.Bytes, evidence.Bytes) || existing.MediaType != evidence.MediaType {
			return "", fmt.Errorf("typed evidence %q is already stored with different bytes", identity)
		}
		return identity, nil
	}
	m.evidence[identity] = cloneEvidence(evidence)
	return identity, nil
}

// AssembleSupportMaterial returns empty support material. The verifier's
// retained public-key mapping is the verification material; the RM must not
// courier the public key as delivery evidence or as authoritative support.
func (m *Manager) AssembleSupportMaterial(_ context.Context, evidence protocol.TypedEvidence) (protocol.SupportMaterial, error) {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.SupportMaterial{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	return protocol.SupportMaterial{MediaType: evidence.MediaType}, nil
}

// CommitEnrollment is the typed lifecycle operation that records the
// enrollment public key in the RM courier map after storing the evidence.
func (m *Manager) CommitEnrollment(ctx context.Context, evidence protocol.TypedEvidence) error {
	if evidence.MediaType != MediaTypeEnrollment {
		return fmt.Errorf("%w: enrollment requires %s", protocol.ErrUnknownMediaType, MediaTypeEnrollment)
	}
	if err := m.CheckEnrollment(evidence); err != nil {
		return err
	}
	body, err := parseEnrollment(evidence)
	if err != nil {
		return err
	}
	if _, err := m.StoreEvidence(ctx, evidence); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.keys[body.Principal]; ok && !bytes.Equal(existing, body.PublicKey) {
		return fmt.Errorf("principal already has a different enrolled public key")
	}
	m.keys[body.Principal] = append([]byte(nil), body.PublicKey...)
	return nil
}

// PublicKey returns the couriered public key for principal, if present.
func (m *Manager) PublicKey(principal protocol.Principal) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok := m.keys[principal]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), key...), true
}

// Evidence returns a stored item by identity.
func (m *Manager) Evidence(identity protocol.Digest) (protocol.TypedEvidence, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	evidence, ok := m.evidence[identity]
	if !ok {
		return protocol.TypedEvidence{}, false
	}
	return cloneEvidence(evidence), true
}

func cloneEvidence(in protocol.TypedEvidence) protocol.TypedEvidence {
	in.Encoded = in.Encoded.Clone()
	return in
}
