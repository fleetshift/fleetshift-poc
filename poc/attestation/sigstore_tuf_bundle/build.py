"""Construction helpers for the peer Sigstore/TUF delivery POC."""

from __future__ import annotations

import time
from typing import Any, Iterable

from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import NameOID
from sigstore_protobuf_specs.dev.sigstore.common.v1 import (
    DistinguishedName,
    TimeRange,
    X509Certificate,
    X509CertificateChain,
)
from sigstore_protobuf_specs.dev.sigstore.trustroot.v1 import (
    CertificateAuthority,
    TrustedRoot,
)

from hybrid.model import (
    DeploymentContent,
    ManagedResourceContent,
    ManifestEnvelope,
    OutputConstraint,
)
from hybrid.policy import signed_input_envelope

from .crypto import canonical_json, sha256
from .identity import FulcioCA, OIDCIdentity, OIDCIssuer
from .model import (
    Attestation,
    DeliveryPackage,
    DerivedInput,
    InputKind,
    PutDelivery,
    RegisteredSelfTarget,
    RemoveDelivery,
    SignedInputAttestation,
)
from .sigstore import (
    PREDICATE_DELIVERY_AUTHORIZATION,
    PREDICATE_FULFILLMENT_RELATION,
    PREDICATE_MANIFEST_SET,
    PREDICATE_PLACEMENT,
    Signer,
)
from .tsa import RFC3161TimestampAuthority
from .tuf import (
    FLEETSHIFT_POLICY_TARGET,
    SIGSTORE_TRUSTED_ROOT_TARGET,
    TUFClient,
    TUFTrustRepository,
)

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
    """Private Sigstore deployment plus its management-plane TUF publisher.

    Each environment models one tenant Fulcio/IdP pair, a distinct platform
    Fulcio/IdP pair for addons, and an independently operated TSA.  The local
    objects are protocol-faithful deterministic test doubles, not production
    service implementations.
    """

    def __init__(
        self,
        *,
        tenant_id: str = "tenant-a",
        tenant_constraints: bool = True,
        extra_addons: Iterable[str] = (),
        user_subjects: Iterable[str] | None = None,
        user_subject_regexp: str | None = None,
        user_anchor_id: str = "tenant-users",
        user_issuer: str | None = None,
        user_anchor_attributes: dict[str, Any] | None = None,
        user_anchor_constraints: list[dict[str, str]] | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        user_issuer = user_issuer or f"https://idp.fleetshift.local/tenants/{tenant_id}"
        self.user_anchor_id = user_anchor_id
        self.user_issuer = user_issuer
        self.tenant_idp = OIDCIssuer(user_issuer)
        self.platform_issuer = "https://idp.fleetshift.local/platform"
        self.platform_idp = OIDCIssuer(self.platform_issuer)
        self.user_fulcio = FulcioCA(f"{tenant_id} Fulcio", self.tenant_idp)
        self.workload_fulcio = FulcioCA("FleetShift platform Fulcio", self.platform_idp)
        self.tsa = RFC3161TimestampAuthority("FleetShift external TSA")
        self.user_ca_uri = f"https://fulcio.fleetshift.local/tenants/{tenant_id}"
        self.platform_ca_uri = "https://fulcio.fleetshift.local/platform"
        self.tsa_uri = "https://tsa.fleetshift.local"

        user_subject_regexp = user_subject_regexp or rf".*@{tenant_id}\.example"
        allowed_users = (
            [
                {
                    "issuer": user_issuer,
                    "subject": subject,
                    "signer_id": subject,
                }
                for subject in user_subjects
            ]
            if user_subjects is not None
            else [
                {
                    "issuer": user_issuer,
                    "subject_regexp": user_subject_regexp,
                    # Regex identities authenticate to their certificate subject.
                    "signer_id_from_subject": True,
                }
            ]
        )
        if user_anchor_constraints is not None:
            constraints = list(user_anchor_constraints)
        elif tenant_constraints:
            constraints = [
                {
                    "name": "input tenant must match anchor tenant",
                    "expression": (
                        'subject.kind != "input" || '
                        "subject.content.deployment_id.startsWith("
                        "anchor.attributes.tenant + \"/\")"
                    ),
                }
            ]
        else:
            constraints = []
        attributes = (
            {"tenant": tenant_id}
            if user_anchor_attributes is None and tenant_constraints
            else (user_anchor_attributes or {})
        )
        addon_ids = tuple(dict.fromkeys((*_DEFAULT_ADDONS, *extra_addons)))
        self.anchors: dict[str, Any] = {
            user_anchor_id: {
                "kinds": ["input"],
                "certificate_authority_uris": [self.user_ca_uri],
                "allowed_identities": allowed_users,
                "attributes": attributes,
                "constraints": constraints,
            },
            "fleet-addons": {
                "kinds": ["output", "relation"],
                "certificate_authority_uris": [self.platform_ca_uri],
                "allowed_identities": [
                    {
                        "issuer": self.platform_issuer,
                        "subject": self._addon_subject(addon_id),
                        "signer_id": addon_id,
                    }
                    for addon_id in addon_ids
                ],
                "attributes": {"operator": "fleetshift-platform"},
                "constraints": [],
            },
        }
        self._repo = TUFTrustRepository()
        self.trusted_root = self._repo.trusted_root
        self.trust_update = self._publish()

    @staticmethod
    def _addon_subject(addon_id: str) -> str:
        return f"spiffe://fleetshift.io/addons/{addon_id}"

    @staticmethod
    def _certificate_authority(
        uri: str,
        operator: str,
        chain: tuple[Any, Any],
    ) -> CertificateAuthority:
        intermediate, root = chain
        common_names = root.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        organizations = root.subject.get_attributes_for_oid(NameOID.ORGANIZATION_NAME)
        return CertificateAuthority(
            subject=DistinguishedName(
                organization=organizations[0].value if organizations else "",
                common_name=common_names[0].value if common_names else "",
            ),
            uri=uri,
            cert_chain=X509CertificateChain(
                certificates=[
                    X509Certificate(raw_bytes=intermediate.public_bytes(serialization.Encoding.DER)),
                    X509Certificate(raw_bytes=root.public_bytes(serialization.Encoding.DER)),
                ]
            ),
            valid_for=TimeRange(
                start=max(intermediate.not_valid_before_utc, root.not_valid_before_utc),
                end=min(intermediate.not_valid_after_utc, root.not_valid_after_utc),
            ),
            operator=operator,
        )

    def _trusted_sigstore_root(self) -> TrustedRoot:
        return TrustedRoot(
            media_type="application/vnd.dev.sigstore.trustedroot.v0.2+json",
            certificate_authorities=[
                self._certificate_authority(
                    self.user_ca_uri,
                    f"{self.tenant_id}.fleetshift.local",
                    (self.user_fulcio.intermediate, self.user_fulcio.root),
                ),
                self._certificate_authority(
                    self.platform_ca_uri,
                    "platform.fleetshift.local",
                    (self.workload_fulcio.intermediate, self.workload_fulcio.root),
                ),
            ],
            timestamp_authorities=[
                self._certificate_authority(
                    self.tsa_uri,
                    "external-tsa.fleetshift.local",
                    (self.tsa.leaf, self.tsa.root),
                )
            ],
            tlogs=[],
            ctlogs=[],
        )

    def _publish(self):
        targets = {
            SIGSTORE_TRUSTED_ROOT_TARGET: self._trusted_sigstore_root().to_json().encode(),
            FLEETSHIFT_POLICY_TARGET: canonical_json(self.anchors),
        }
        return self._repo.publish(targets)

    def republish(self) -> None:
        """Publish the next TUF metadata version after a policy mutation."""

        self.trust_update = self._publish()

    def delivery_agent(self, *, state_directory: str | None = None):
        """Provision an agent with trust root bytes outside the delivery path."""

        from .verify import DeliveryAgent

        return DeliveryAgent(
            tenant_id=self.tenant_id,
            tuf_client=TUFClient(self.trusted_root, state_directory=state_directory),
        )

    def user_signer(self, subject: str | None = None) -> Signer:
        subject = subject or f"alice@{self.tenant_id}.example"
        return Signer(
            oidc_issuer=self.tenant_idp,
            fulcio=self.user_fulcio,
            tsa=self.tsa,
            identity=OIDCIdentity(issuer=self.user_issuer, subject=subject),
            signer_id=subject,
        )

    def addon_signer(self, addon_id: str) -> Signer:
        return Signer(
            oidc_issuer=self.platform_idp,
            fulcio=self.workload_fulcio,
            tsa=self.tsa,
            identity=OIDCIdentity(
                issuer=self.platform_issuer,
                subject=self._addon_subject(addon_id),
            ),
            signer_id=addon_id,
        )

    def assemble(
        self,
        attestation: Attestation,
        *,
        generation: int | None = None,
        prior_inputs: dict[str, InputKind] | None = None,
        update_attestations: dict[str, Attestation] | None = None,
        fulfillment_relations: dict[str, RegisteredSelfTarget] | None = None,
    ) -> DeliveryPackage:
        if generation is None:
            generation = _inferred_generation(attestation.signed_input, prior_inputs or {})
        return DeliveryPackage(
            tenant_id=self.tenant_id,
            delivery_id=attestation.attestation_id,
            generation=generation,
            trust_update=self.trust_update,
            attestation=attestation,
            prior_inputs=dict(prior_inputs or {}),
            update_attestations=dict(update_attestations or {}),
            fulfillment_relations=dict(fulfillment_relations or {}),
        )


def _inferred_generation(inp: InputKind, prior_inputs: dict[str, InputKind]) -> int:
    if isinstance(inp, SignedInputAttestation):
        return inp.expected_generation or 1
    prior = prior_inputs.get(inp.prior_input_id)
    return 1 if prior is None else _inferred_generation(prior, prior_inputs) + 1


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
    envelope = signed_input_envelope(
        content.to_dict(),
        valid_until,
        constraints,
        expected_generation,
    )
    bundle = signer.sign_predicate(
        subject_name=content.content_id(),
        subject_digest=sha256(canonical_json(envelope)),
        predicate_type=PREDICATE_DELIVERY_AUTHORIZATION,
        predicate={"envelope": envelope},
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
):
    payload = _serialize_manifests(manifests)
    return manifests, signer.sign_predicate(
        subject_name="manifests",
        subject_digest=sha256(canonical_json(payload)),
        predicate_type=PREDICATE_MANIFEST_SET,
        predicate={"payload": payload},
    )


def sign_placement(
    signer: Signer,
    *,
    deployment_id: str,
    targets: tuple[str, ...],
):
    payload = {"deployment_id": deployment_id, "targets": list(targets)}
    return signer.sign_predicate(
        subject_name=f"placement:{deployment_id}",
        subject_digest=sha256(canonical_json(payload)),
        predicate_type=PREDICATE_PLACEMENT,
        predicate={"payload": payload},
    )


def make_registered_self_target(
    signer: Signer,
    resource_type: str,
    *,
    manifest_type: str = "managed_resource_spec",
) -> RegisteredSelfTarget:
    doc = {
        "relation_type": "registered_self_target",
        "addon_id": signer.signer_id,
        "resource_type": resource_type,
        "manifest_type": manifest_type,
    }
    return RegisteredSelfTarget(
        addon_id=signer.signer_id,
        resource_type=resource_type,
        manifest_type=manifest_type,
        bundle=signer.sign_predicate(
            subject_name=f"relation:{signer.signer_id}:{resource_type}",
            subject_digest=sha256(canonical_json(doc)),
            predicate_type=PREDICATE_FULFILLMENT_RELATION,
            predicate={"payload": doc},
        ),
    )


def make_put_attestation(
    attestation_id: str,
    signed_input: InputKind,
    manifests: tuple[ManifestEnvelope, ...],
    *,
    manifest_bundle=None,
    placement_targets: tuple[str, ...] | None = None,
    placement_bundle=None,
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
    placement_bundle=None,
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


def make_derived_input(**kwargs: Any) -> DerivedInput:
    return DerivedInput(**kwargs)


def _serialize_manifests(
    manifests: tuple[ManifestEnvelope, ...],
) -> list[dict[str, Any]]:
    return [
        {"resource_type": manifest.resource_type, "content": manifest.content}
        for manifest in manifests
    ]
