"""FleetShift delivery graph carried to an offline delivery agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hybrid.cel_runtime import CelEvaluationError, evaluate_bool
from hybrid.model import (
    DeploymentContent,
    ManagedResourceContent,
    ManifestEnvelope,
    OutputConstraint,
    StrategySpec,
)
from hybrid.policy import (
    constraint_from_document,
    constraints_to_documents,
    signed_input_envelope,
)

from .crypto import canonical_json, sha256
from .sigstore import SigstoreBundle
from .tuf import TrustUpdate

__all__ = [
    "Attestation",
    "CelEvaluationError",
    "DeliveryPackage",
    "DeploymentContent",
    "DerivedInput",
    "FulfillmentState",
    "InputKind",
    "ManagedResourceContent",
    "ManifestEnvelope",
    "OutputConstraint",
    "PutDelivery",
    "RegisteredSelfTarget",
    "RemoveDelivery",
    "SigstoreBundle",
    "SignedInputAttestation",
    "StrategySpec",
    "canonical_json",
    "constraint_from_document",
    "constraints_to_documents",
    "evaluate_bool",
    "sha256",
    "signed_input_envelope",
]


@dataclass(frozen=True)
class SignedInputAttestation:
    """A keyless-signed user or automation authorization."""

    content: DeploymentContent | ManagedResourceContent
    valid_until: float
    output_constraints: tuple[OutputConstraint, ...]
    expected_generation: int | None
    bundle: SigstoreBundle

    def envelope(self) -> dict[str, Any]:
        return signed_input_envelope(
            self.content.to_dict(),
            self.valid_until,
            self.output_constraints,
            self.expected_generation,
        )

    def content_digest(self) -> bytes:
        return sha256(canonical_json(self.envelope()))


@dataclass(frozen=True)
class DerivedInput:
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
    """Addon-signed managed-resource -> self-target relation evidence."""

    addon_id: str
    resource_type: str
    manifest_type: str
    bundle: SigstoreBundle


@dataclass(frozen=True)
class FulfillmentState:
    content_id: str
    generation: int


@dataclass
class DeliveryPackage:
    """Complete couriered evidence; bootstrap trust is intentionally absent."""

    tenant_id: str
    delivery_id: str
    generation: int
    trust_update: TrustUpdate
    attestation: Attestation
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
        self,
        addon_id: str,
        resource_type: str,
    ) -> RegisteredSelfTarget | None:
        matches = [
            relation
            for relation in self.fulfillment_relations.values()
            if relation.addon_id == addon_id and relation.resource_type == resource_type
        ]
        return matches[0] if len(matches) == 1 else None

    def sigstore_bundles(self) -> tuple[SigstoreBundle, ...]:
        """Enumerate every Sigstore bundle materialized in this delivery."""

        bundles: list[SigstoreBundle] = []

        def add_input(value: InputKind) -> None:
            if isinstance(value, SignedInputAttestation):
                bundles.append(value.bundle)

        def add_attestation(value: Attestation) -> None:
            add_input(value.signed_input)
            if isinstance(value.output, PutDelivery) and value.output.manifest_bundle:
                bundles.append(value.output.manifest_bundle)
            if value.output.placement_bundle:
                bundles.append(value.output.placement_bundle)

        add_attestation(self.attestation)
        for prior in self.prior_inputs.values():
            add_input(prior)
        for update in self.update_attestations.values():
            add_attestation(update)
        bundles.extend(relation.bundle for relation in self.fulfillment_relations.values())
        return tuple(bundles)
