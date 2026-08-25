package directkey

import (
	"context"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// EnrollmentTransition is a prepared direct-key enrollment. Only this package
// can construct it. CommitEnrollment is infallible and must run inside the
// resource manager's serialized accept.
type EnrollmentTransition struct {
	principal protocol.Principal
	publicKey []byte
}

// Manager is the direct-key/v1 resource-manager API. It retains a couriered
// public-key map used only for the RM's own request decision. That map is not
// target trust state. Immutable TypedEvidence lives in the common RM
// repository, not here.
type Manager struct {
	mu   sync.Mutex
	keys map[protocol.Principal][]byte
}

// NewManager returns an empty resource-manager profile implementation.
func NewManager() *Manager {
	return &Manager{
		keys: make(map[protocol.Principal][]byte),
	}
}

// ProvenanceType implements protocol.ResourceManagerAPI.
func (m *Manager) ProvenanceType() protocol.ProvenanceType {
	return protocol.ProvenanceTypeDirectKeyV1
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

// PrepareEnrollment validates enrollment evidence and returns an infallible
// transition. It does not retain the public-key mapping.
func (m *Manager) PrepareEnrollment(evidence protocol.TypedEvidence) (EnrollmentTransition, error) {
	if evidence.MediaType != MediaTypeEnrollment {
		return EnrollmentTransition{}, fmt.Errorf("%w: enrollment requires %s", protocol.ErrUnknownMediaType, MediaTypeEnrollment)
	}
	if err := m.CheckEnrollment(evidence); err != nil {
		return EnrollmentTransition{}, err
	}
	body, err := parseEnrollment(evidence)
	if err != nil {
		return EnrollmentTransition{}, err
	}
	return EnrollmentTransition{
		principal: body.Principal,
		publicKey: append([]byte(nil), body.PublicKey...),
	}, nil
}

// CommitEnrollment records the prepared public-key mapping. It is infallible
// after PrepareEnrollment under the RM accept lock: a conflicting principal
// mapping must already have been rejected, so this write cannot fail.
func (m *Manager) CommitEnrollment(transition EnrollmentTransition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys[transition.principal] = append([]byte(nil), transition.publicKey...)
}

// Enroll prepares and commits enrollment without RM evidence-log
// registration. Tests that only exercise the profile courier map use it.
func (m *Manager) Enroll(evidence protocol.TypedEvidence) error {
	transition, err := m.PrepareEnrollment(evidence)
	if err != nil {
		return err
	}
	m.CommitEnrollment(transition)
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
