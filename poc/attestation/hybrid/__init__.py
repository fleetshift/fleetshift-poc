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
    make_managed_resource_input,
    make_placement_evidence,
    make_put_manifests,
    make_registered_self_target,
    make_remove_by_deployment_id,
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
    DeploymentContent,
    DerivedInput,
    FulfillmentRelation,
    InlineManifestStrategy,
    InputContent,
    ManagedResourceContent,
    RegisteredSelfTarget,
    KeyBinding,
    ManifestEnvelope,
    OutputConstraint,
    OutputSignature,
    PlacementEvidence,
    PredicatePlacementStrategy,
    PutManifests,
    RemoveByDeploymentId,
    SignedInput,
    Signature,
    StrategySpec,
    TrustAnchor,
    TrustAnchorConstraint,
    TrustAnchorSubject,
    VerifiedOutput,
)
from .policy import derive_strategy_constraints
from .verify import (
    DeploymentState,
    FulfillmentState,
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
    "DeploymentContent",
    "DeploymentState",
    "DerivedInput",
    "FulfillmentRelation",
    "FulfillmentState",
    "InlineManifestStrategy",
    "InputContent",
    "ManagedResourceContent",
    "RegisteredSelfTarget",
    "KeyBinding",
    "KeyPair",
    "ManifestEnvelope",
    "OutputConstraint",
    "OutputSignature",
    "PlacementEvidence",
    "PredicatePlacementStrategy",
    "PutManifests",
    "RemoveByDeploymentId",
    "Signature",
    "SignedInput",
    "StrategySpec",
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
    "make_managed_resource_input",
    "make_placement_evidence",
    "make_put_manifests",
    "make_registered_self_target",
    "make_remove_by_deployment_id",
    "make_signed_input",
    "sign_put_manifests",
    "verify_attestation",
]
