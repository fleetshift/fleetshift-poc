"""FleetShift content types + delivery structures for the Sigstore/TUF POC.

Reuses hybrid policy/CEL semantics; replaces KeyBinding with Sigstore bundles.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Allow importing sibling hybrid package when running from poc/attestation.
_ATTESTATION_ROOT = Path(__file__).resolve().parents[1]
if str(_ATTESTATION_ROOT) not in sys.path:
    sys.path.insert(0, str(_ATTESTATION_ROOT))

from hybrid.model import (  # noqa: E402
    DeploymentContent,
    ManagedResourceContent,
    ManifestEnvelope,
    OutputConstraint,
    StrategySpec,
)
from hybrid.policy import (  # noqa: E402
    constraint_from_document,
    constraints_to_documents,
    derive_strategy_constraints,
    signed_input_envelope,
)
from hybrid.cel_runtime import CelEvaluationError, evaluate_bool  # noqa: E402

from .dsse import canonical_json, sha256_bytes
from .sigstore_sign import SigstoreBundle

__all__ = [
    "DeploymentContent",
    "ManagedResourceContent",
    "ManifestEnvelope",
    "OutputConstraint",
    "StrategySpec",
    "SignedInputAttestation",
    "DerivedInput",
    "InputKind",
    "PutDelivery",
    "RemoveDelivery",
    "Attestation",
    "DeliveryBundle",
    "FulfillmentState",
    "RegisteredSelfTarget",
    "signed_input_envelope",
    "constraints_to_documents",
    "constraint_from_document",
    "derive_strategy_constraints",
    "evaluate_bool",
    "CelEvaluationError",
    "canonical_json",
    "sha256_bytes",
]


@dataclass(frozen=True)
class SignedInputAttestation:
    """User (or updater) signed input: Sigstore bundle over the input envelope."""

    content: DeploymentContent | ManagedResourceContent
    valid_until: float
    output_constraints: tuple[OutputConstraint, ...]
    expected_generation: int | None
    bundle: SigstoreBundle

    def envelope(self) -> dict[str, Any]:
        from hybrid.policy import constraints_to_documents, signed_input_envelope

        return signed_input_envelope(
            self.content.to_dict(),
            self.valid_until,
            self.output_constraints,
            self.expected_generation,
        )

    def content_digest(self) -> bytes:
        return sha256_bytes(canonical_json(self.envelope()))


@dataclass(frozen=True)
class DerivedInput:
    """Input derived from a prior input and a verified update attestation.

    Mirrors hybrid.DerivedInput: prior_content_id/type identify the content,
    prior_input_id references an input in the delivery bundle, and
    update_attestation_id references a full attestation whose output carries
    the CEL derive_input_expression (+ optional preconditions/constraints).
    """

    prior_content_id: str
    prior_content_type: str
    prior_input_id: str
    update_attestation_id: str


InputKind = SignedInputAttestation | DerivedInput


@dataclass(frozen=True)
class PutDelivery:
    manifests: tuple[ManifestEnvelope, ...]
    manifest_bundle: SigstoreBundle | None = None
    placement_targets: tuple[str, ...] | None = None
    placement_bundle: SigstoreBundle | None = None
    placement_deployment_id: str | None = None


@dataclass(frozen=True)
class RemoveDelivery:
    deployment_id: str
    placement_targets: tuple[str, ...] | None = None
    placement_bundle: SigstoreBundle | None = None
    placement_deployment_id: str | None = None


@dataclass(frozen=True)
class Attestation:
    attestation_id: str
    signed_input: InputKind
    output: PutDelivery | RemoveDelivery


@dataclass(frozen=True)
class RegisteredSelfTarget:
    """Addon-signed claim: I own resources of this type; fulfillments target me.

    Signed via Sigstore (Fulcio identity) instead of hybrid KeyBinding/Ed25519.
    """

    resource_type: str
    bundle: SigstoreBundle

    @property
    def signer_id(self) -> str:
        return self.bundle.identity.subject

    @property
    def trust_anchor_id(self) -> str:
        return self.bundle.trust_anchor_id


@dataclass(frozen=True)
class FulfillmentState:
    content_id: str
    generation: int


@dataclass
class DeliveryBundle:
    """Self-contained delivery package assembled by the management plane.

    Includes an embedded TUF trust snapshot so the delivery agent does not
    fetch trust material over HTTP at verification time.
    """

    tuf_snapshot: dict[str, Any]
    bootstrap_root_keyids: tuple[str, ...]
    attestation: Attestation
    # Optional prior inputs / update chain nodes for derived upgrades.
    prior_inputs: dict[str, InputKind] = field(default_factory=dict)
    update_attestations: dict[str, Attestation] = field(default_factory=dict)
    fulfillment_relations: dict[str, RegisteredSelfTarget] = field(default_factory=dict)

    def get_input(self, input_id: str) -> InputKind | None:
        return self.prior_inputs.get(input_id)

    def get_attestation(self, attestation_id: str) -> Attestation | None:
        if self.attestation.attestation_id == attestation_id:
            return self.attestation
        return self.update_attestations.get(attestation_id)

    def find_fulfillment_relation(
        self, addon_id: str, resource_type: str
    ) -> RegisteredSelfTarget | None:
        for relation in self.fulfillment_relations.values():
            if relation.signer_id == addon_id and relation.resource_type == resource_type:
                return relation
        return None
