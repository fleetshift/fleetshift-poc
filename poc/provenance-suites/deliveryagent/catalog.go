package deliveryagent

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

const (
	maxPackageStatements     = 256
	maxEvidenceBytesPerItem  = 1 << 20
	maxPackageEvidenceBytes  = 16 << 20
	maxSupportBytesPerItem   = 1 << 20
	maxPackageSupportBytes   = 16 << 20
	maxProofHashes           = 64
	maxPackageProofBytes     = 2 << 20
	maxCheckpointRootBytes   = len("sha256:") + sha256.Size*2
	maxPredicateCandidates   = 128
	maxActiveDependencyDepth = 32
	maxChosenSemanticEdges   = 512
)

var (
	errDuplicateEvidenceIdentity = errors.New("duplicate evidence identity in delivery package")
	errVerificationWorkLimit     = errors.New("delivery verification work limit exceeded")
	errUnknownCatalogEvidence    = errors.New("evidence identity is not in the delivery package")
	errVerificationCycle         = errors.New("evidence dependency cycle")
)

type verificationLimits struct {
	maxStatements      int
	maxEvidencePerItem int
	maxTotalEvidence   int
	maxSupportPerItem  int
	maxTotalSupport    int
	maxProofHashes     int
	maxProofBytes      int
	maxCandidates      int
	maxDepth           int
	maxEdges           int
}

func defaultVerificationLimits() verificationLimits {
	return verificationLimits{
		maxStatements:      maxPackageStatements,
		maxEvidencePerItem: maxEvidenceBytesPerItem,
		maxTotalEvidence:   maxPackageEvidenceBytes,
		maxSupportPerItem:  maxSupportBytesPerItem,
		maxTotalSupport:    maxPackageSupportBytes,
		maxProofHashes:     maxProofHashes,
		maxProofBytes:      maxPackageProofBytes,
		maxCandidates:      maxPredicateCandidates,
		maxDepth:           maxActiveDependencyDepth,
		maxEdges:           maxChosenSemanticEdges,
	}
}

func normalizeVerificationLimits(in verificationLimits) verificationLimits {
	defaults := defaultVerificationLimits()
	if in.maxStatements <= 0 {
		in.maxStatements = defaults.maxStatements
	}
	if in.maxEvidencePerItem <= 0 {
		in.maxEvidencePerItem = defaults.maxEvidencePerItem
	}
	if in.maxTotalEvidence <= 0 {
		in.maxTotalEvidence = defaults.maxTotalEvidence
	}
	if in.maxSupportPerItem <= 0 {
		in.maxSupportPerItem = defaults.maxSupportPerItem
	}
	if in.maxTotalSupport <= 0 {
		in.maxTotalSupport = defaults.maxTotalSupport
	}
	if in.maxProofHashes <= 0 {
		in.maxProofHashes = defaults.maxProofHashes
	}
	if in.maxProofBytes <= 0 {
		in.maxProofBytes = defaults.maxProofBytes
	}
	if in.maxCandidates <= 0 {
		in.maxCandidates = defaults.maxCandidates
	}
	if in.maxDepth <= 0 {
		in.maxDepth = defaults.maxDepth
	}
	if in.maxEdges <= 0 {
		in.maxEdges = defaults.maxEdges
	}
	return in
}

type hintResult struct {
	hints protocol.TentativeHints
	err   error
}

// evidenceCatalog is a package-scoped structural snapshot. Building it does
// not verify provenance or turn parsed hints into authorization dependencies.
// Items and completed candidate lists are immutable; accessors borrow their
// backing data. Profile calls receive detached copies at the API boundary.
type evidenceCatalog struct {
	rootID     protocol.Digest
	byID       map[protocol.Digest]protocol.Item
	supporting []protocol.Digest
	candidates map[protocol.PredicateType][]protocol.Digest
	hints      map[protocol.Digest]hintResult
	lookup     protocol.TargetLookup
	limits     verificationLimits
	update     *protocol.EvidenceLogUpdate
	indexBuilt bool
	indexErr   error
}

// newEvidenceCatalog checks package size and proof encodings before a profile
// or temporal verifier can observe the package. It then stores detached item
// snapshots and rejects repeated TypedEvidence identities, even if every
// replaceable field on the duplicate is byte-for-byte equal. Identity is
// computed once here for cataloging; the existing protocol and temporal log
// verifiers still recompute it defensively during occurrence verification.
func newEvidenceCatalog(pkg resourcemanager.DeliveryPackage, lookup protocol.TargetLookup, limits verificationLimits) (*evidenceCatalog, error) {
	limits = normalizeVerificationLimits(limits)
	if err := validatePackageBounds(pkg, limits); err != nil {
		return nil, err
	}

	catalog := &evidenceCatalog{
		byID:       make(map[protocol.Digest]protocol.Item, len(pkg.Supporting)+1),
		supporting: make([]protocol.Digest, 0, len(pkg.Supporting)),
		hints:      make(map[protocol.Digest]hintResult, len(pkg.Supporting)),
		lookup:     lookup,
		limits:     limits,
	}
	if pkg.EvidenceLog != nil {
		update := *pkg.EvidenceLog
		update.ConsistencyProof = cloneSlice(pkg.EvidenceLog.ConsistencyProof)
		catalog.update = &update
	}

	root := clonePackageItem(pkg.Root)
	rootID, err := root.Evidence.Identity()
	if err != nil {
		return nil, fmt.Errorf("%w: root evidence identity: %v", protocol.ErrMalformedEvidence, err)
	}
	catalog.rootID = rootID
	catalog.byID[rootID] = root

	for i := range pkg.Supporting {
		item := clonePackageItem(pkg.Supporting[i])
		identity, err := item.Evidence.Identity()
		if err != nil {
			return nil, fmt.Errorf("%w: supporting item %d evidence identity: %v", protocol.ErrMalformedEvidence, i, err)
		}
		if _, duplicate := catalog.byID[identity]; duplicate {
			return nil, fmt.Errorf("%w: %s", errDuplicateEvidenceIdentity, identity)
		}
		catalog.byID[identity] = item
		catalog.supporting = append(catalog.supporting, identity)
	}
	return catalog, nil
}

func validatePackageBounds(pkg resourcemanager.DeliveryPackage, limits verificationLimits) error {
	statementCount := len(pkg.Supporting) + 1
	if statementCount > limits.maxStatements {
		return fmt.Errorf("%w: statement count %d exceeds %d", errVerificationWorkLimit, statementCount, limits.maxStatements)
	}
	if pkg.EvidenceLog != nil {
		for _, root := range [...]struct {
			name    string
			encoded protocol.Digest
		}{
			{name: "from", encoded: pkg.EvidenceLog.From.Root},
			{name: "successor", encoded: pkg.EvidenceLog.Checkpoint.Root},
		} {
			if len(root.encoded) > maxCheckpointRootBytes {
				return fmt.Errorf("%w: evidence-log %s checkpoint root bytes %d exceed %d", errVerificationWorkLimit, root.name, len(root.encoded), maxCheckpointRootBytes)
			}
		}
	}

	totalEvidenceBytes := 0
	totalSupportBytes := 0
	validateItem := func(item protocol.Item, label string) error {
		evidenceBytes := len(item.Evidence.Bytes)
		if evidenceBytes > limits.maxEvidencePerItem {
			return fmt.Errorf("%w: %s evidence bytes %d exceed %d", errVerificationWorkLimit, label, evidenceBytes, limits.maxEvidencePerItem)
		}
		// Identity JSON-encodes the evidence envelope, so bound its
		// variable-length provenance and media-type fields before hashing it.
		for _, fieldBytes := range [...]int{len(item.Evidence.ProvenanceType), len(item.Evidence.MediaType)} {
			if fieldBytes > limits.maxEvidencePerItem-evidenceBytes {
				return fmt.Errorf("%w: %s evidence envelope bytes exceed %d", errVerificationWorkLimit, label, limits.maxEvidencePerItem)
			}
			evidenceBytes += fieldBytes
		}
		if evidenceBytes > limits.maxTotalEvidence-totalEvidenceBytes {
			return fmt.Errorf("%w: package evidence envelope bytes exceed %d", errVerificationWorkLimit, limits.maxTotalEvidence)
		}
		totalEvidenceBytes += evidenceBytes

		supportBytes := len(item.Support.Bytes)
		if supportBytes > limits.maxSupportPerItem {
			return fmt.Errorf("%w: %s support bytes %d exceed %d", errVerificationWorkLimit, label, supportBytes, limits.maxSupportPerItem)
		}
		mediaTypeBytes := len(item.Support.MediaType)
		if mediaTypeBytes > limits.maxSupportPerItem-supportBytes {
			return fmt.Errorf("%w: %s support envelope bytes exceed %d", errVerificationWorkLimit, label, limits.maxSupportPerItem)
		}
		supportBytes += mediaTypeBytes
		if supportBytes > limits.maxTotalSupport-totalSupportBytes {
			return fmt.Errorf("%w: package support envelope bytes exceed %d", errVerificationWorkLimit, limits.maxTotalSupport)
		}
		totalSupportBytes += supportBytes
		return nil
	}
	if err := validateItem(pkg.Root, "root"); err != nil {
		return err
	}
	for i := range pkg.Supporting {
		if err := validateItem(pkg.Supporting[i], fmt.Sprintf("supporting item %d", i)); err != nil {
			return err
		}
	}

	proofs := make([]struct {
		name   string
		hashes []protocol.Digest
		kind   error
	}, 0, len(pkg.Supporting)+2)
	if pkg.EvidenceLog != nil {
		proofs = append(proofs, struct {
			name   string
			hashes []protocol.Digest
			kind   error
		}{name: "consistency proof", hashes: pkg.EvidenceLog.ConsistencyProof, kind: protocol.ErrInvalidLogUpdate})
	}
	addInclusion := func(item protocol.Item, label string) {
		if item.EvidenceLog == nil {
			return
		}
		proofs = append(proofs, struct {
			name   string
			hashes []protocol.Digest
			kind   error
		}{name: label, hashes: item.EvidenceLog.InclusionProof, kind: protocol.ErrInvalidLogInclusion})
	}
	addInclusion(pkg.Root, "root inclusion proof")
	for i := range pkg.Supporting {
		addInclusion(pkg.Supporting[i], fmt.Sprintf("supporting item %d inclusion proof", i))
	}

	totalProofBytes := 0
	for _, proof := range proofs {
		if len(proof.hashes) > limits.maxProofHashes {
			return fmt.Errorf("%w: %s hash count %d exceeds %d", errVerificationWorkLimit, proof.name, len(proof.hashes), limits.maxProofHashes)
		}
		for _, encoded := range proof.hashes {
			if len(encoded) > limits.maxProofBytes-totalProofBytes {
				return fmt.Errorf("%w: encoded Merkle proof bytes exceed %d", errVerificationWorkLimit, limits.maxProofBytes)
			}
			totalProofBytes += len(encoded)
		}
	}

	// Count and encoded-byte bounds are checked before decoding any couriered
	// digest so malformed inputs cannot force unbounded parsing work.
	for _, proof := range proofs {
		for i, encoded := range proof.hashes {
			if _, err := protocol.DecodeDigest(encoded); err != nil {
				return fmt.Errorf("%w: %s hash %d: %v", proof.kind, proof.name, i, err)
			}
		}
	}
	return nil
}

func clonePackageItem(in protocol.Item) protocol.Item {
	out := protocol.Item{SignedStatement: protocol.SignedStatement{
		Evidence: protocol.TypedEvidence{
			ProvenanceType: in.Evidence.ProvenanceType,
			Encoded:        in.Evidence.Encoded.Clone(),
		},
		Support: protocol.SupportMaterial(protocol.Encoded(in.Support).Clone()),
	}}
	if in.EvidenceLog != nil {
		out.EvidenceLog = &protocol.EvidenceLogInclusion{
			Index:          in.EvidenceLog.Index,
			InclusionProof: cloneSlice(in.EvidenceLog.InclusionProof),
		}
	}
	return out
}

func (c *evidenceCatalog) item(identity protocol.Digest) protocol.Item {
	return c.byID[identity]
}

func (c *evidenceCatalog) supportingCandidates(predicate protocol.PredicateType) ([]protocol.Digest, error) {
	if err := c.buildCandidateIndex(); err != nil {
		return nil, err
	}
	return c.candidates[predicate], nil
}

func (c *evidenceCatalog) buildCandidateIndex() error {
	if c.indexBuilt {
		return c.indexErr
	}
	c.indexBuilt = true
	if len(c.supporting) > c.limits.maxCandidates {
		c.indexErr = fmt.Errorf("%w: candidate scan count %d exceeds %d", errVerificationWorkLimit, len(c.supporting), c.limits.maxCandidates)
		return c.indexErr
	}

	candidates := make(map[protocol.PredicateType][]protocol.Digest)
	for _, identity := range c.supporting {
		result, exists := c.hints[identity]
		if !exists {
			item := c.byID[identity]
			if c.lookup == nil {
				result.err = fmt.Errorf("%w: no installed verifier lookup", protocol.ErrUnknownProvenanceType)
			} else {
				verifier, ok := c.lookup(item.Evidence.ProvenanceType)
				if !ok || verifier == nil || verifier.ProvenanceType() != item.Evidence.ProvenanceType {
					result.err = fmt.Errorf("%w: no matching verifier for %s", protocol.ErrUnknownProvenanceType, item.Evidence.ProvenanceType)
				} else {
					result.hints, result.err = verifier.ParseHints(protocol.TypedEvidence{
						ProvenanceType: item.Evidence.ProvenanceType,
						Encoded:        item.Evidence.Encoded.Clone(),
					})
					if result.err == nil && result.hints.PredicateType == "" {
						result.err = fmt.Errorf("%w: predicate type hint is required", protocol.ErrMalformedEvidence)
					}
				}
			}
			c.hints[identity] = result
		}
		if result.err != nil {
			c.indexErr = result.err
			return c.indexErr
		}
		candidates[result.hints.PredicateType] = append(candidates[result.hints.PredicateType], identity)
	}
	c.candidates = candidates
	return nil
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneSlice[T any](in []T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	copy(out, in)
	return out
}
