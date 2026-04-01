"""
Hybrid attestation prototype.

Combines:
  - a single Attestation(input, output) model
  - explicit, signed CEL output constraints on inputs
  - self-contained verification bundles (no external store)
  - data-driven derivation from signed CEL update outputs
  - delivery-aware output types with strategy-implied constraints
  - explainable verification results
"""

from .build import (
    make_key_binding,
    make_placement_evidence,
    make_put_manifests,
    make_remove_by_delivery_id,
    make_signed_input,
    sign_put_manifests,
)
from .cel_runtime import CelEvaluationError
from .crypto import KeyPair, generate_keypair
from .model import (
    AddonManifestStrategy,
    AddonPlacementStrategy,
    Attestation,
    DeliveryOutput,
    DerivedInput,
    InlineManifestStrategy,
    KeyBinding,
    ManifestEnvelope,
    OutputConstraint,
    OutputSignature,
    PlacementEvidence,
    PredicatePlacementStrategy,
    PutManifests,
    RemoveByDeliveryId,
    SignedInput,
    Signature,
    TrustAnchor,
    TrustAnchorConstraint,
    TrustAnchorSubject,
    VerifiedOutput,
)
from .policy import derive_strategy_constraints
from .verify import (
    TrustStore,
    VerificationBundle,
    VerificationError,
    VerificationResult,
    explain_verification,
    verify_attestation,
)

__all__ = [
    "AddonManifestStrategy",
    "AddonPlacementStrategy",
    "Attestation",
    "CelEvaluationError",
    "DeliveryOutput",
    "DerivedInput",
    "InlineManifestStrategy",
    "KeyBinding",
    "KeyPair",
    "ManifestEnvelope",
    "OutputConstraint",
    "OutputSignature",
    "PlacementEvidence",
    "PredicatePlacementStrategy",
    "PutManifests",
    "RemoveByDeliveryId",
    "Signature",
    "SignedInput",
    "TrustAnchor",
    "TrustAnchorConstraint",
    "TrustAnchorSubject",
    "TrustStore",
    "VerificationBundle",
    "VerificationError",
    "VerificationResult",
    "VerifiedOutput",
    "derive_strategy_constraints",
    "explain_verification",
    "generate_keypair",
    "make_key_binding",
    "make_placement_evidence",
    "make_put_manifests",
    "make_remove_by_delivery_id",
    "make_signed_input",
    "sign_put_manifests",
    "verify_attestation",
]
