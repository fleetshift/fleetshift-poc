"""Shared helpers for sigstore_tuf_bundle hybrid-equivalence tests."""

from __future__ import annotations

import json
from typing import Any

from hybrid.model import (
    DeploymentContent,
    ManifestEnvelope,
    OutputConstraint,
    StrategySpec,
)
from sigstore_tuf_bundle.build import (
    PoCEnvironment,
    make_put_attestation,
    make_remove_attestation,
    make_signed_input,
    sign_manifests,
    sign_placement,
)
from sigstore_tuf_bundle.model import Attestation, InputKind
from sigstore_tuf_bundle.sigstore import Signer
from sigstore_tuf_bundle.verify import DeliveryAgent, VerificationError, VerificationResult


def k8s_manifests(*objects: dict) -> tuple[ManifestEnvelope, ...]:
    return tuple(
        ManifestEnvelope(resource_type="kubernetes", content=obj) for obj in objects
    )


def spec_update_manifest(directive: dict) -> tuple[ManifestEnvelope, ...]:
    return (ManifestEnvelope(resource_type="spec_update", content=directive),)


def serialize_envelopes(envelopes: tuple[ManifestEnvelope, ...]) -> list[dict]:
    return [{"resource_type": m.resource_type, "content": m.content} for m in envelopes]


def resource_manifests(*specs: dict) -> tuple[ManifestEnvelope, ...]:
    return tuple(
        ManifestEnvelope(resource_type="managed_resource_spec", content=s)
        for s in specs
    )


_NOOP_PLACEMENT = StrategySpec(type="predicate", attributes={"expression": "true"})


def addon_must_sign(
    addon_id: str,
    trust_anchor_id: str = "fleet-addons",
) -> OutputConstraint:
    return OutputConstraint(
        name=f"output must be signed by {addon_id} via {trust_anchor_id}",
        expression=(
            f"output.has_signature && "
            f'output.signature.trust_anchor_id == "{trust_anchor_id}" && '
            f'output.signer_id == "{addon_id}"'
        ),
    )


def namespace_constraint(namespace: str) -> OutputConstraint:
    return OutputConstraint(
        name=f"all manifests must be in namespace {namespace}",
        expression=f'output.manifests.all(m, m.content.metadata.namespace == "{namespace}")',
    )


def allowed_gvks(*gvks: str) -> OutputConstraint:
    allowed_literal = json.dumps(list(gvks))
    return OutputConstraint(
        name=f"only GVKs in {list(gvks)}",
        expression=(
            f"output.manifests.all(m, ((m.content.apiVersion + \"/\" + m.content.kind) in {allowed_literal}))"
        ),
    )


def no_cluster_admin() -> OutputConstraint:
    return OutputConstraint(
        name="no ClusterRoleBinding may grant cluster-admin",
        expression=(
            "output.manifests.all("
            'm, !(m.content.kind == "ClusterRoleBinding" && m.content.roleRef.name == "cluster-admin")'
            ")"
        ),
    )


def make_env(**kwargs: Any) -> PoCEnvironment:
    """Hybrid-parity env: no default tenant deployment_id constraint."""
    kwargs.setdefault("tenant_constraints", False)
    return PoCEnvironment(**kwargs)


def content(deployment_id: str = "test") -> DeploymentContent:
    return DeploymentContent(
        deployment_id=deployment_id,
        manifest_strategy=StrategySpec(type="inline", attributes={"manifests": []}),
        placement_strategy=_NOOP_PLACEMENT,
    )


def inline_content(
    manifests: tuple[ManifestEnvelope, ...],
    deployment_id: str = "test",
) -> DeploymentContent:
    return DeploymentContent(
        deployment_id=deployment_id,
        manifest_strategy=StrategySpec(
            type="inline",
            attributes={"manifests": serialize_envelopes(manifests)},
        ),
        placement_strategy=_NOOP_PLACEMENT,
    )


def addon_content(
    addon_id: str,
    trust_anchor_id: str = "fleet-addons",
    deployment_id: str = "test",
) -> DeploymentContent:
    return DeploymentContent(
        deployment_id=deployment_id,
        manifest_strategy=StrategySpec(
            type="addon",
            attributes={"addon_id": addon_id, "trust_anchor_id": trust_anchor_id},
        ),
        placement_strategy=_NOOP_PLACEMENT,
    )


def all_details(result: VerificationResult) -> str:
    parts = [result.detail]
    for child in result.children:
        parts.append(all_details(child))
    return "\n".join(parts)


def assert_rejected_with(
    case,
    agent: DeliveryAgent,
    delivery,
    expected_detail: str,
    **verify_kwargs: Any,
) -> None:
    """Require rejection at a pinned explain-tree detail, not just ``False``."""

    with case.assertRaises(VerificationError) as captured:
        agent.verify_or_raise(delivery, **verify_kwargs)
    case.assertIn(expected_detail, str(captured.exception))


def signed_put(
    env: PoCEnvironment,
    attestation_id: str,
    signer: Signer,
    inp: InputKind,
    manifests: tuple[ManifestEnvelope, ...],
    *,
    prior_inputs: dict[str, InputKind] | None = None,
    update_attestations: dict[str, Attestation] | None = None,
    fulfillment_relations: dict | None = None,
    placement_targets: tuple[str, ...] | None = None,
    placement_bundle: Any = None,
    placement_deployment_id: str | None = None,
    sign_output: bool = True,
):
    mb = None
    final_manifests = manifests
    if sign_output:
        final_manifests, mb = sign_manifests(signer, manifests)
    att = make_put_attestation(
        attestation_id,
        inp,
        final_manifests,
        manifest_bundle=mb,
        placement_targets=placement_targets,
        placement_bundle=placement_bundle,
        placement_deployment_id=placement_deployment_id,
    )
    return env.assemble(
        att,
        prior_inputs=prior_inputs,
        update_attestations=update_attestations,
        fulfillment_relations=fulfillment_relations,
    )


def unsigned_put(
    env: PoCEnvironment,
    attestation_id: str,
    inp: InputKind,
    manifests: tuple[ManifestEnvelope, ...],
    **bundle_kwargs: Any,
):
    att = make_put_attestation(attestation_id, inp, manifests)
    return env.assemble(att, **bundle_kwargs)
