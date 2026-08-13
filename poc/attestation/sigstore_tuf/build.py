"""Construction helpers: keyless signers + management-plane delivery assembly."""

from __future__ import annotations

import time
from typing import Any, Iterable

from hybrid.model import (
    DeploymentContent,
    ManagedResourceContent,
    ManifestEnvelope,
    OutputConstraint,
)

from .fulcio import LocalFulcio
from .model import (
    Attestation,
    DeliveryBundle,
    DerivedInput,
    InputKind,
    PutDelivery,
    RegisteredSelfTarget,
    RemoveDelivery,
    SignedInputAttestation,
    canonical_json,
    sha256_bytes,
)
from .sigstore_sign import Identity, Signer
from .tsa import LocalTSA
from .tuf_store import publish_trust_repo

# Default addon subjects known to the fleet-addons trust anchor.
_DEFAULT_ADDONS = (
    "capi-provisioner",
    "placement-addon",
    "upgrade-planner",
    "cluster-lifecycle",
    "observability",
    "capacity-planner",
    "cluster-mgmt-addon",
    "other-addon",
)


class PoCEnvironment:
    """In-process Fulcio, TSA, and TUF trust repo for tests / demos."""

    def __init__(
        self,
        *,
        tenant_constraints: bool = True,
        extra_addons: Iterable[str] = (),
        user_subjects: Iterable[str] | None = None,
        user_subject_regexp: str | None = None,
        user_anchor_id: str = "tenant-users",
        user_issuer: str = "https://issuer.example/tenant-a",
        user_anchor_attributes: dict[str, Any] | None = None,
        user_anchor_constraints: list[dict[str, str]] | None = None,
    ) -> None:
        self.user_fulcio = LocalFulcio(common_name="poc-user-fulcio")
        self.workload_fulcio = LocalFulcio(common_name="poc-workload-fulcio")
        self.tsa = LocalTSA()
        self.user_anchor_id = user_anchor_id
        self.user_issuer = user_issuer

        if user_subject_regexp is None:
            user_subject_regexp = r".*@tenant-a\.example"
        allowed_users: list[dict[str, str]] = [
            {"issuer": user_issuer, "subject_regexp": user_subject_regexp}
        ]
        if user_subjects is not None:
            allowed_users = [
                {"issuer": user_issuer, "subject": s} for s in user_subjects
            ]

        constraints: list[dict[str, str]]
        if user_anchor_constraints is not None:
            constraints = list(user_anchor_constraints)
        elif tenant_constraints:
            constraints = [
                {
                    "name": "input tenant must match anchor tenant",
                    "expression": (
                        'subject.kind != "input" || '
                        'subject.content.deployment_id.startsWith('
                        'anchor.attributes.tenant + "/")'
                    ),
                }
            ]
        else:
            constraints = []

        attributes = (
            {"tenant": "tenant-a"}
            if user_anchor_attributes is None and tenant_constraints
            else (user_anchor_attributes or {})
        )

        addon_ids = tuple(dict.fromkeys((*_DEFAULT_ADDONS, *extra_addons)))
        self.anchors: dict[str, Any] = {
            user_anchor_id: {
                "allowed_identities": allowed_users,
                "attributes": attributes,
                "constraints": constraints,
            },
            "fleet-addons": {
                "allowed_identities": [
                    {"issuer": "https://oidc.addon.local", "subject": aid}
                    for aid in addon_ids
                ],
                "attributes": {},
                "constraints": [],
            },
        }
        self._repo, self.tuf_snapshot = publish_trust_repo(
            fulcio_root_pem=self.user_fulcio.root_pem,
            workload_fulcio_root_pem=self.workload_fulcio.root_pem,
            tsa_root_pem=self.tsa.root_pem,
            anchors=self.anchors,
        )
        self.bootstrap_root_keyids = self._repo.bootstrap_root_keyids()

    def republish(self) -> None:
        """Republish TUF after mutating ``self.anchors``."""
        self._repo, self.tuf_snapshot = publish_trust_repo(
            fulcio_root_pem=self.user_fulcio.root_pem,
            workload_fulcio_root_pem=self.workload_fulcio.root_pem,
            tsa_root_pem=self.tsa.root_pem,
            anchors=self.anchors,
        )
        self.bootstrap_root_keyids = self._repo.bootstrap_root_keyids()

    def user_signer(self, subject: str = "alice@tenant-a.example") -> Signer:
        return Signer(
            fulcio=self.user_fulcio,
            tsa=self.tsa,
            identity=Identity(issuer=self.user_issuer, subject=subject),
            trust_anchor_id=self.user_anchor_id,
        )

    def addon_signer(self, addon_id: str) -> Signer:
        return Signer(
            fulcio=self.workload_fulcio,
            tsa=self.tsa,
            identity=Identity(
                issuer="https://oidc.addon.local",
                subject=addon_id,
            ),
            trust_anchor_id="fleet-addons",
        )

    def assemble(
        self,
        attestation: Attestation,
        *,
        prior_inputs: dict[str, InputKind] | None = None,
        update_attestations: dict[str, Attestation] | None = None,
        fulfillment_relations: dict[str, RegisteredSelfTarget] | None = None,
    ) -> DeliveryBundle:
        """Management plane: embed current TUF snapshot into the delivery bundle."""
        return DeliveryBundle(
            tuf_snapshot=self.tuf_snapshot,
            bootstrap_root_keyids=self.bootstrap_root_keyids,
            attestation=attestation,
            prior_inputs=dict(prior_inputs or {}),
            update_attestations=dict(update_attestations or {}),
            fulfillment_relations=dict(fulfillment_relations or {}),
        )


def make_signed_input(
    signer: Signer,
    content: DeploymentContent | ManagedResourceContent,
    *,
    output_constraints: Iterable[OutputConstraint] = (),
    valid_duration_sec: float = 86400,
    expected_generation: int | None = None,
) -> SignedInputAttestation:
    constraints = tuple(output_constraints)
    valid_until = time.time() + valid_duration_sec
    from hybrid.policy import constraints_to_documents

    envelope: dict[str, Any] = {
        "content": content.to_dict(),
        "output_constraints": constraints_to_documents(constraints),
        "valid_until": valid_until,
    }
    if expected_generation is not None:
        envelope["expected_generation"] = expected_generation
    digest = sha256_bytes(canonical_json(envelope))
    subject_name = content.content_id()
    bundle = signer.sign_predicate(
        subject_name=subject_name,
        subject_digest=digest,
        predicate={"envelope": envelope, "signer_id": signer.identity.subject},
    )
    return SignedInputAttestation(
        content=content,
        valid_until=valid_until,
        output_constraints=constraints,
        expected_generation=expected_generation,
        bundle=bundle,
    )


def sign_manifests(
    signer: Signer,
    manifests: tuple[ManifestEnvelope, ...],
) -> tuple[tuple[ManifestEnvelope, ...], Any]:
    serialized = [
        {"resource_type": m.resource_type, "content": m.content} for m in manifests
    ]
    digest = sha256_bytes(canonical_json(serialized))
    bundle = signer.sign_predicate(
        subject_name="manifests",
        subject_digest=digest,
        predicate={"payload": serialized, "signer_id": signer.identity.subject},
    )
    return manifests, bundle


def sign_placement(
    signer: Signer,
    *,
    deployment_id: str,
    targets: tuple[str, ...],
) -> Any:
    payload = {"deployment_id": deployment_id, "targets": list(targets)}
    digest = sha256_bytes(canonical_json(payload))
    return signer.sign_predicate(
        subject_name=f"placement:{deployment_id}",
        subject_digest=digest,
        predicate={"payload": payload, "signer_id": signer.identity.subject},
    )


def make_registered_self_target(
    signer: Signer,
    resource_type: str,
) -> RegisteredSelfTarget:
    """Construct an addon-signed RegisteredSelfTarget relation (Sigstore)."""
    relation_doc = {
        "relation_type": "registered_self_target",
        "resource_type": resource_type,
    }
    digest = sha256_bytes(canonical_json(relation_doc))
    bundle = signer.sign_predicate(
        subject_name=f"relation:{resource_type}",
        subject_digest=digest,
        predicate={"payload": relation_doc, "signer_id": signer.identity.subject},
    )
    return RegisteredSelfTarget(resource_type=resource_type, bundle=bundle)


def make_put_attestation(
    attestation_id: str,
    signed_input: InputKind,
    manifests: tuple[ManifestEnvelope, ...],
    *,
    manifest_bundle: Any = None,
    placement_targets: tuple[str, ...] | None = None,
    placement_bundle: Any = None,
    placement_deployment_id: str | None = None,
) -> Attestation:
    return Attestation(
        attestation_id=attestation_id,
        signed_input=signed_input,
        output=PutDelivery(
            manifests=manifests,
            manifest_bundle=manifest_bundle,
            placement_targets=placement_targets,
            placement_bundle=placement_bundle,
            placement_deployment_id=placement_deployment_id,
        ),
    )


def make_remove_attestation(
    attestation_id: str,
    signed_input: InputKind,
    deployment_id: str,
    *,
    placement_targets: tuple[str, ...] | None = None,
    placement_bundle: Any = None,
    placement_deployment_id: str | None = None,
) -> Attestation:
    return Attestation(
        attestation_id=attestation_id,
        signed_input=signed_input,
        output=RemoveDelivery(
            deployment_id=deployment_id,
            placement_targets=placement_targets,
            placement_bundle=placement_bundle,
            placement_deployment_id=placement_deployment_id,
        ),
    )


def make_derived_input(
    *,
    prior_content_id: str,
    prior_content_type: str,
    prior_input_id: str,
    update_attestation_id: str,
) -> DerivedInput:
    return DerivedInput(
        prior_content_id=prior_content_id,
        prior_content_type=prior_content_type,
        prior_input_id=prior_input_id,
        update_attestation_id=update_attestation_id,
    )
