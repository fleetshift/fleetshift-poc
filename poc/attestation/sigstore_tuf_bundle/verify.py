"""Stateful, offline verification of couriered FleetShift delivery packages."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any

from hybrid.model import (
    DeploymentContent,
    ManagedResourceContent,
    ManifestEnvelope,
    OutputConstraint,
    VerifiedInput,
)
from hybrid.mutation import apply_update, check_preconditions, derive_constraints
from hybrid.policy import (
    derive_manifest_strategy_constraints,
    derive_placement_strategy_constraints,
)

from .model import (
    Attestation,
    DeliveryPackage,
    DerivedInput,
    FulfillmentState,
    InputKind,
    PutDelivery,
    RegisteredSelfTarget,
    RemoveDelivery,
    SignedInputAttestation,
    canonical_json,
    sha256,
)
from .sigstore import (
    PREDICATE_DELIVERY_AUTHORIZATION,
    PREDICATE_FULFILLMENT_RELATION,
    PREDICATE_MANIFEST_SET,
    PREDICATE_PLACEMENT,
    SigstoreBundle,
    VerifiedSigstore,
    verify_sigstore_bundle,
)
from .tuf import TUFClient, TrustedMaterial

VerificationRef = tuple[str, str]


class VerificationError(Exception):
    def __init__(self, message: str, result: VerificationResult | None = None) -> None:
        super().__init__(message)
        self.result = result


@dataclass
class VerificationResult:
    valid: bool
    label: str
    detail: str = ""
    children: list[VerificationResult] = field(default_factory=list)

    def pretty(self, indent: int = 0) -> str:
        icon = "✓" if self.valid else "✗"
        header = f"{'  ' * indent}{icon} {self.label}"
        if self.detail:
            header += f": {self.detail}"
        lines = [header]
        for child in self.children:
            lines.append(child.pretty(indent + 1))
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.pretty()


def _ok(label: str, detail: str = "", children: list[VerificationResult] | None = None) -> VerificationResult:
    return VerificationResult(True, label, detail, children or [])


def _fail(label: str, detail: str, children: list[VerificationResult] | None = None) -> VerificationResult:
    return VerificationResult(False, label, detail, children or [])


def _serialize_manifests(manifests: tuple[ManifestEnvelope, ...]) -> list[dict[str, Any]]:
    return [{"resource_type": m.resource_type, "content": m.content} for m in manifests]


def _extract_update_content(serialized_manifests: Any) -> Any:
    if not isinstance(serialized_manifests, list):
        raise ValueError("update output must be a list of manifest envelopes")
    for envelope in serialized_manifests:
        if isinstance(envelope, dict) and envelope.get("resource_type") == "spec_update":
            return envelope.get("content")
    raise ValueError("no spec_update manifest found in update output")


def _reconstitute_content(
    prior: DeploymentContent | ManagedResourceContent,
    derived_dict: dict[str, Any],
) -> DeploymentContent | ManagedResourceContent:
    if isinstance(prior, DeploymentContent):
        return DeploymentContent.from_dict(derived_dict)
    if isinstance(prior, ManagedResourceContent):
        return ManagedResourceContent.from_dict(derived_dict)
    raise ValueError(f"cannot reconstitute content type: {type(prior).__name__}")


def _eval_constraints(
    constraints: tuple[OutputConstraint, ...],
    cel_ctx: dict[str, Any],
    attestation_id: str,
) -> VerificationResult:
    children: list[VerificationResult] = []
    if not constraints:
        return _ok(f"{attestation_id} constraints", "no constraints")
    for c in constraints:
        try:
            from hybrid.cel_runtime import evaluate_bool

            valid = evaluate_bool(c.expression, cel_ctx)
        except Exception as exc:  # noqa: BLE001 — surface as constraint failure
            return _fail(f"{attestation_id} constraint", f"{c.name}: {exc}", children)
        if not valid:
            children.append(_fail(f"{attestation_id} constraint", f"predicate returned false: {c.name}"))
            return _fail(f"{attestation_id} constraints", f"constraint failed: {c.name}", children)
        children.append(_ok(f"{attestation_id} constraint", f"matched: {c.name}"))
    return _ok(f"{attestation_id} constraints", "all matched", children)


def _verify_output_bundle(
    bundle: SigstoreBundle | None,
    *,
    payload: Any,
    trusted: TrustedMaterial,
    label: str,
    predicate_type: str,
    kind: str = "output",
) -> tuple[VerificationResult, str | None, str | None]:
    if bundle is None:
        return _ok(label, "unsigned"), None, None
    digest = sha256(canonical_json(payload))
    try:
        verified = verify_sigstore_bundle(
            bundle,
            trusted=trusted,
            expected_subject_digest=digest,
            expected_predicate_types=[predicate_type],
        )
        anchor_id, signer_id = _authenticate_identity(
            verified,
            trusted=trusted,
            kind=kind,
            content=payload,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(label, f"sigstore verify failed: {exc}"), None, None
    pred = verified.statement.get("predicate") or {}
    if canonical_json(pred.get("payload")) != canonical_json(payload):
        return _fail(label, "signed payload mismatch"), None, None
    return _ok(label, f"signed by {signer_id} via {anchor_id}"), signer_id, anchor_id


def _identity_mapping(
    anchor: dict[str, Any],
    verified: VerifiedSigstore,
) -> str | None:
    for candidate in anchor.get("allowed_identities", []):
        if candidate.get("issuer") != verified.identity.issuer:
            continue
        exact = candidate.get("subject")
        regexp = candidate.get("subject_regexp")
        if exact is not None and exact != verified.identity.subject:
            continue
        if regexp is not None and re.fullmatch(regexp, verified.identity.subject) is None:
            continue
        if exact is None and regexp is None:
            continue
        if candidate.get("signer_id_from_subject"):
            return verified.identity.subject
        signer_id = candidate.get("signer_id")
        if isinstance(signer_id, str) and signer_id:
            return signer_id
    return None


def _authenticate_identity(
    verified: VerifiedSigstore,
    *,
    trusted: TrustedMaterial,
    kind: str,
    content: Any,
    valid_until: float | None = None,
) -> tuple[str, str]:
    """Map a Fulcio identity to exactly one TUF-authenticated policy anchor."""

    matches: list[tuple[str, str, dict[str, Any]]] = []
    for anchor_id, anchor in trusted.anchors.items():
        if kind not in anchor.get("kinds", []):
            continue
        if verified.certificate_authority_uri not in anchor.get(
            "certificate_authority_uris", []
        ):
            continue
        signer_id = _identity_mapping(anchor, verified)
        if signer_id is not None:
            matches.append((anchor_id, signer_id, anchor))
    if not matches:
        raise ValueError(
            "unknown trust anchor for authenticated Fulcio issuer, subject, CA, and kind"
        )
    if len(matches) != 1:
        raise ValueError("authenticated identity ambiguously matches multiple trust anchors")
    anchor_id, signer_id, anchor = matches[0]
    subject = {"kind": kind, "signer_id": signer_id, "content": content}
    if valid_until is not None:
        subject["valid_until"] = valid_until
    from hybrid.cel_runtime import evaluate_bool

    for constraint in anchor.get("constraints", []):
        try:
            accepted = evaluate_bool(
                constraint["expression"],
                {
                    "anchor": {
                        "anchor_id": anchor_id,
                        "attributes": anchor.get("attributes", {}),
                    },
                    "subject": subject,
                },
            )
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                f"trust anchor constraint evaluation failed: {constraint.get('name')}: {exc}"
            ) from exc
        if not accepted:
            raise ValueError(
                f"trust anchor constraint failed: {constraint.get('name')}"
            )
    return anchor_id, signer_id


def _relation_doc(relation: RegisteredSelfTarget) -> dict[str, str]:
    return {
        "relation_type": "registered_self_target",
        "addon_id": relation.addon_id,
        "resource_type": relation.resource_type,
        "manifest_type": relation.manifest_type,
    }


def _derive_managed_resource_constraints(
    content: ManagedResourceContent,
    delivery: DeliveryPackage,
    trusted: TrustedMaterial,
) -> tuple[OutputConstraint, ...]:
    """Sigstore analogue of hybrid.policy.derive_managed_resource_constraints."""
    relation = delivery.find_fulfillment_relation(content.addon_id, content.resource_type)
    if relation is None:
        return (
            OutputConstraint(
                name=(
                    f"no fulfillment relation found for "
                    f"addon {content.addon_id!r}, "
                    f"resource type {content.resource_type!r}"
                ),
                expression="false",
            ),
        )

    if not isinstance(relation, RegisteredSelfTarget):
        return (
            OutputConstraint(
                name=f"unknown fulfillment relation type: {type(relation).__name__}",
                expression="false",
            ),
        )

    doc = _relation_doc(relation)
    digest = sha256(canonical_json(doc))
    try:
        verified = verify_sigstore_bundle(
            relation.bundle,
            trusted=trusted,
            expected_subject_digest=digest,
            expected_predicate_types=[PREDICATE_FULFILLMENT_RELATION],
            expected_subject_name=(
                f"relation:{relation.addon_id}:{relation.resource_type}"
            ),
        )
        anchor_id, signer_id = _authenticate_identity(
            verified,
            trusted=trusted,
            kind="relation",
            content=doc,
        )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        if (
            "in-toto subject digest mismatch" in msg
            or "in-toto subject name mismatch" in msg
        ):
            return (
                OutputConstraint(
                    name="fulfillment relation hash mismatch",
                    expression="false",
                ),
            )
        if "unknown trust anchor" in msg:
            return (
                OutputConstraint(
                    name="trust anchor not found for relation",
                    expression="false",
                ),
            )
        return (
            OutputConstraint(
                name="fulfillment relation signature invalid",
                expression="false",
            ),
        )

    pred = verified.statement.get("predicate") or {}
    if canonical_json(pred.get("payload")) != canonical_json(doc):
        return (
            OutputConstraint(
                name="fulfillment relation hash mismatch",
                expression="false",
            ),
        )

    if signer_id != content.addon_id or relation.addon_id != content.addon_id:
        return (
            OutputConstraint(
                name=(
                    f"relation signer mismatch: "
                    f"signed by {signer_id!r}, "
                    f"expected addon {content.addon_id!r}"
                ),
                expression="false",
            ),
        )

    if relation.resource_type != content.resource_type:
        return (
            OutputConstraint(
                name=(
                    f"relation resource_type mismatch: "
                    f"relation has {relation.resource_type!r}, "
                    f"content has {content.resource_type!r}"
                ),
                expression="false",
            ),
        )

    if relation.manifest_type != "managed_resource_spec":
        return (
            OutputConstraint(
                name=(
                    f"unsupported relation manifest_type: {relation.manifest_type!r}"
                ),
                expression="false",
            ),
        )

    addon_id = content.addon_id
    return (
        OutputConstraint(
            name=f"placement targets addon {addon_id}",
            expression=f'target.id == "{addon_id}"',
        ),
        OutputConstraint(
            name="manifests must match resource spec",
            expression=(
                'action != "put" || '
                f'output.manifests == [{{"resource_type": "{relation.manifest_type}", '
                '"content": input.spec}]'
            ),
        ),
    )


def _derive_strategy_constraints(
    content: DeploymentContent | ManagedResourceContent,
    *,
    delivery: DeliveryPackage,
    trusted: TrustedMaterial,
) -> tuple[OutputConstraint, ...]:
    if isinstance(content, DeploymentContent):
        d = content.to_dict()
        return (
            derive_manifest_strategy_constraints(d)
            + derive_placement_strategy_constraints(d)
        )
    if isinstance(content, ManagedResourceContent):
        return _derive_managed_resource_constraints(content, delivery, trusted)
    return (
        OutputConstraint(
            name=f"unknown content type: {content.content_type()}",
            expression="false",
        ),
    )


def _verify_signed_input(
    signed: SignedInputAttestation,
    *,
    trusted: TrustedMaterial,
    now: float,
    attestation_id: str,
) -> tuple[VerificationResult, VerifiedInput | None]:
    label = f"{attestation_id} input"
    children: list[VerificationResult] = []
    envelope = signed.envelope()
    digest = sha256(canonical_json(envelope))
    try:
        verified = verify_sigstore_bundle(
            signed.bundle,
            trusted=trusted,
            expected_subject_digest=digest,
            expected_predicate_types=[PREDICATE_DELIVERY_AUTHORIZATION],
            expected_subject_name=signed.content.content_id(),
        )
        anchor_id, signer_id = _authenticate_identity(
            verified,
            trusted=trusted,
            kind="input",
            content=signed.content.to_dict(),
            valid_until=signed.valid_until,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(label, f"sigstore verify failed: {exc}"), None

    predicate = verified.statement.get("predicate") or {}
    if canonical_json(predicate.get("envelope")) != canonical_json(envelope):
        return _fail(label, "predicate envelope mismatch"), None

    if now > signed.valid_until:
        return _fail(label, f"expired: {signer_id}"), None

    children.append(_ok(f"{attestation_id} sigstore", f"signed by {signer_id}"))
    return (
        _ok(label, f"verified against {anchor_id}", children),
        VerifiedInput(
            content=signed.content,
            content_hash=digest,
            output_constraints=signed.output_constraints,
            signer_id=signer_id,
            expected_generation=signed.expected_generation,
        ),
    )


def _verify_input(
    inp: InputKind,
    *,
    attestation_id: str,
    delivery: DeliveryPackage,
    trusted: TrustedMaterial,
    now: float,
    visited: frozenset[VerificationRef],
    check_generation_state: FulfillmentState | None,
    target_identity: dict[str, Any],
) -> tuple[VerificationResult, VerifiedInput | None]:
    if isinstance(inp, SignedInputAttestation):
        return _verify_signed_input(
            inp, trusted=trusted, now=now, attestation_id=attestation_id
        )
    return _verify_derived_input(
        inp,
        attestation_id=attestation_id,
        delivery=delivery,
        trusted=trusted,
        now=now,
        visited=visited,
        check_generation_state=check_generation_state,
        target_identity=target_identity,
    )


def _verify_derived_input(
    derived: DerivedInput,
    *,
    attestation_id: str,
    delivery: DeliveryPackage,
    trusted: TrustedMaterial,
    now: float,
    visited: frozenset[VerificationRef],
    check_generation_state: FulfillmentState | None,
    target_identity: dict[str, Any],
) -> tuple[VerificationResult, VerifiedInput | None]:
    del check_generation_state, target_identity  # generation only on leaf delivery
    label = f"{attestation_id} input"
    children: list[VerificationResult] = []

    prior_input = delivery.get_input(derived.prior_input_id)
    if prior_input is None:
        return _fail(label, f"prior input not found: {derived.prior_input_id}"), None

    update_attestation = delivery.get_attestation(derived.update_attestation_id)
    if update_attestation is None:
        return _fail(
            label, f"update attestation not found: {derived.update_attestation_id}"
        ), None

    prior_ref: VerificationRef = ("input", derived.prior_input_id)
    if prior_ref in visited:
        return _fail(label, "cycle detected in input graph"), None

    next_visited = visited | {prior_ref}

    prior_result, verified_prior = _verify_input(
        prior_input,
        attestation_id=derived.prior_input_id,
        delivery=delivery,
        trusted=trusted,
        now=now,
        visited=next_visited,
        # Intermediate inputs do not check target-side generation.
        check_generation_state=None,
        target_identity={"id": derived.prior_content_id},
    )
    children.append(prior_result)
    if not prior_result.valid or verified_prior is None:
        return _fail(label, "prior input verification failed", children), None

    actual_type = verified_prior.content.content_type()
    if derived.prior_content_type != actual_type:
        return _fail(
            label,
            f"content type mismatch: input declares {derived.prior_content_type!r}, "
            f"prior has {actual_type!r}",
        ), None

    actual_id = verified_prior.content.content_id()
    if derived.prior_content_id != actual_id:
        return _fail(
            label,
            f"content_id mismatch: input declares {derived.prior_content_id!r}, "
            f"prior has {actual_id!r}",
        ), None

    # Update attestation is verified against the prior content as its target,
    # and without the leaf delivery's fulfillment generation state.
    update_result, verified_update_output = _verify_attestation_core(
        update_attestation,
        delivery=delivery,
        trusted=trusted,
        now=now,
        visited=next_visited,
        target_identity={"id": derived.prior_content_id},
        current_fulfillment_state=None,
        delivery_generation=None,
    )
    children.append(update_result)
    if not update_result.valid or verified_update_output is None:
        return _fail(label, "update attestation verification failed", children), None

    try:
        update_content = _extract_update_content(verified_update_output)
        check_preconditions(verified_prior.content, update_content)
        derived_dict = apply_update(verified_prior.content, update_content)
        derived_content = _reconstitute_content(verified_prior.content, derived_dict)
        if derived_content.content_id() != derived.prior_content_id:
            raise ValueError(
                f"update must not rewrite content identity: "
                f"expected {derived.prior_content_id!r}, "
                f"got {derived_content.content_id()!r}"
            )
        output_constraints = derive_constraints(
            verified_prior.output_constraints,
            update_content,
        )
        resolved_generation = (
            verified_prior.expected_generation + 1
            if verified_prior.expected_generation is not None
            else None
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(label, f"derivation failed: {exc}", children), None

    return (
        _ok(
            label,
            (
                f"derived from prior={derived.prior_input_id} "
                f"+ update={derived.update_attestation_id}"
            ),
            children,
        ),
        VerifiedInput(
            content=derived_content,
            content_hash=sha256(canonical_json(derived_content.to_dict())),
            output_constraints=output_constraints,
            expected_generation=resolved_generation,
        ),
    )


def _verify_put(
    attestation_id: str,
    verified_input: VerifiedInput,
    output: PutDelivery,
    *,
    delivery: DeliveryPackage,
    trusted: TrustedMaterial,
    target_identity: dict[str, Any],
) -> tuple[VerificationResult, list[dict[str, Any]] | None]:
    label = f"{attestation_id} output"
    children: list[VerificationResult] = []
    serialized = _serialize_manifests(output.manifests)

    sig_result, manifest_signer, manifest_anchor = _verify_output_bundle(
        output.manifest_bundle,
        payload=serialized,
        trusted=trusted,
        label=f"{attestation_id} manifest signature",
        predicate_type=PREDICATE_MANIFEST_SET,
    )
    children.append(sig_result)
    if not sig_result.valid:
        return _fail(label, "manifest signature invalid", children), None

    placement_signer = None
    if output.placement_bundle is not None:
        pe_payload = {
            "deployment_id": output.placement_deployment_id,
            "targets": list(output.placement_targets or ()),
        }
        pe_result, placement_signer, placement_anchor = _verify_output_bundle(
            output.placement_bundle,
            payload=pe_payload,
            trusted=trusted,
            label=f"{attestation_id} placement signature",
            predicate_type=PREDICATE_PLACEMENT,
        )
        children.append(pe_result)
        if not pe_result.valid:
            return _fail(label, "placement evidence invalid", children), None
        if pe_payload["deployment_id"] != verified_input.content.content_id():
            return _fail(
                label,
                f"deployment_id mismatch: evidence has {pe_payload['deployment_id']!r}, "
                f"input has {verified_input.content.content_id()!r}",
                children,
            ), None

    implied = _derive_strategy_constraints(
        verified_input.content, delivery=delivery, trusted=trusted
    )
    all_constraints = implied + verified_input.output_constraints

    sig_ctx = None
    if output.manifest_bundle is not None:
        sig_ctx = {
            "signer_id": manifest_signer,
            "trust_anchor_id": manifest_anchor,
        }
    if output.placement_bundle is not None:
        placement_ctx: dict[str, Any] = {
            "deployment_id": output.placement_deployment_id,
            "targets": list(output.placement_targets or ()),
            "has_signature": True,
            "signer_id": placement_signer,
            "signature": {
                "signer_id": placement_signer,
                "trust_anchor_id": placement_anchor,
            },
        }
    else:
        placement_ctx = {
            "deployment_id": None,
            "targets": [],
            "has_signature": False,
            "signer_id": None,
        }

    cel_ctx = {
        "input": verified_input.content.to_dict(),
        "output": {
            "manifests": serialized,
            "has_signature": output.manifest_bundle is not None,
            "signer_id": manifest_signer,
            "signature": sig_ctx,
        },
        "target": target_identity,
        "action": "put",
        "placement": placement_ctx,
    }
    c_result = _eval_constraints(all_constraints, cel_ctx, attestation_id)
    children.append(c_result)
    if not c_result.valid:
        return _fail(label, c_result.detail, children), None
    return _ok(label, "satisfies all constraints", children), serialized


def _verify_remove(
    attestation_id: str,
    verified_input: VerifiedInput,
    output: RemoveDelivery,
    *,
    delivery: DeliveryPackage,
    trusted: TrustedMaterial,
    target_identity: dict[str, Any],
) -> tuple[VerificationResult, dict[str, Any] | None]:
    label = f"{attestation_id} output"
    children: list[VerificationResult] = []
    if output.deployment_id != verified_input.content.content_id():
        return _fail(
            label,
            f"remove deployment_id mismatch: output targets {output.deployment_id!r}, "
            f"input has {verified_input.content.content_id()!r}",
        ), None

    placement_signer = None
    if output.placement_bundle is not None:
        pe_payload = {
            "deployment_id": output.placement_deployment_id,
            "targets": list(output.placement_targets or ()),
        }
        pe_result, placement_signer, placement_anchor = _verify_output_bundle(
            output.placement_bundle,
            payload=pe_payload,
            trusted=trusted,
            label=f"{attestation_id} placement signature",
            predicate_type=PREDICATE_PLACEMENT,
        )
        children.append(pe_result)
        if not pe_result.valid:
            return _fail(label, "placement evidence invalid", children), None
        if pe_payload["deployment_id"] != verified_input.content.content_id():
            return _fail(
                label,
                f"deployment_id mismatch: evidence has {pe_payload['deployment_id']!r}, "
                f"input has {verified_input.content.content_id()!r}",
                children,
            ), None

    implied = _derive_strategy_constraints(
        verified_input.content, delivery=delivery, trusted=trusted
    )
    all_constraints = implied + verified_input.output_constraints

    if output.placement_bundle is not None:
        placement_ctx: dict[str, Any] = {
            "deployment_id": output.placement_deployment_id,
            "targets": list(output.placement_targets or ()),
            "has_signature": True,
            "signer_id": placement_signer,
            "signature": {
                "signer_id": placement_signer,
                "trust_anchor_id": placement_anchor,
            },
        }
    else:
        placement_ctx = {
            "deployment_id": None,
            "targets": [],
            "has_signature": False,
            "signer_id": None,
        }

    cel_ctx = {
        "input": verified_input.content.to_dict(),
        "output": {"deployment_id": output.deployment_id},
        "target": target_identity,
        "action": "remove",
        "placement": placement_ctx,
    }
    c_result = _eval_constraints(all_constraints, cel_ctx, attestation_id)
    children.append(c_result)
    if not c_result.valid:
        return _fail(label, c_result.detail, children), None
    return _ok(label, "remove accepted", children), {"deployment_id": output.deployment_id}


def _check_generation(
    verified_input: VerifiedInput,
    state: FulfillmentState | None,
    attestation_id: str,
    delivery_generation: int | None,
) -> VerificationResult:
    label = f"{attestation_id} generation"
    if delivery_generation is None:
        return _ok(label, "intermediate attestation")
    if delivery_generation <= 0:
        return _fail(label, "delivery generation must be positive")
    if (
        verified_input.expected_generation is not None
        and verified_input.expected_generation != delivery_generation
    ):
        return _fail(
            label,
            f"signed generation {verified_input.expected_generation} does not match "
            f"delivery generation {delivery_generation}",
        )
    if state is None:
        return _ok(
            label,
            f"delivery generation={delivery_generation}, no target state (stateless)",
        )
    cid = verified_input.content.content_id()
    if state.content_id != cid:
        return _fail(
            label,
            f"content state mismatch: state is for {state.content_id!r}, "
            f"attestation resolves to {cid!r}",
        )
    expected_current = state.generation + 1
    if delivery_generation != expected_current:
        return _fail(
            label,
            f"generation mismatch: delivery has {delivery_generation}, "
            f"target accepts {expected_current}",
        )
    return _ok(
        label,
        f"generation {delivery_generation} matches "
        f"target at {state.generation}",
    )


def _verify_attestation_core(
    att: Attestation,
    *,
    delivery: DeliveryPackage,
    trusted: TrustedMaterial,
    now: float,
    visited: frozenset[VerificationRef],
    target_identity: dict[str, Any],
    current_fulfillment_state: FulfillmentState | None,
    delivery_generation: int | None,
) -> tuple[VerificationResult, Any | None]:
    """Verify one attestation; return (result, verified output content or None)."""
    attestation_ref: VerificationRef = ("attestation", att.attestation_id)
    if len(visited) >= 64:
        return _fail(att.attestation_id, "delivery graph exceeds 64-node limit"), None
    if attestation_ref in visited:
        return _fail(att.attestation_id, "cycle detected in attestation graph"), None

    next_visited = visited | {attestation_ref}
    children: list[VerificationResult] = []

    in_result, verified_input = _verify_input(
        att.signed_input,
        attestation_id=att.attestation_id,
        delivery=delivery,
        trusted=trusted,
        now=now,
        visited=next_visited,
        check_generation_state=current_fulfillment_state,
        target_identity=target_identity,
    )
    children.append(in_result)
    if not in_result.valid or verified_input is None:
        return _fail(att.attestation_id, "input verification failed", children), None

    if isinstance(att.output, PutDelivery):
        out_result, out_content = _verify_put(
            att.attestation_id,
            verified_input,
            att.output,
            delivery=delivery,
            trusted=trusted,
            target_identity=target_identity,
        )
    else:
        out_result, out_content = _verify_remove(
            att.attestation_id,
            verified_input,
            att.output,
            delivery=delivery,
            trusted=trusted,
            target_identity=target_identity,
        )
    children.append(out_result)
    if not out_result.valid:
        return _fail(att.attestation_id, "output verification failed", children), None

    gen_result = _check_generation(
        verified_input,
        current_fulfillment_state,
        att.attestation_id,
        delivery_generation,
    )
    children.append(gen_result)
    if not gen_result.valid:
        return _fail(att.attestation_id, "generation check failed", children), None

    return _ok(att.attestation_id, "delivery accepted", children), out_content


@dataclass
class DeliveryAgent:
    """A delivery target with provisioned tenant identity and persistent TUF state."""

    tenant_id: str
    tuf_client: TUFClient

    def verify(
        self,
        delivery: DeliveryPackage,
        *,
        target_identity: dict[str, Any] | None = None,
        current_fulfillment_state: FulfillmentState | None = None,
        now: float | None = None,
    ) -> VerificationResult:
        """Verify a complete couriered package without network requests."""

        now = time.time() if now is None else now
        target_identity = target_identity or {}
        if delivery.tenant_id != self.tenant_id:
            return _fail(
                "tenant",
                f"package tenant {delivery.tenant_id!r} does not match "
                f"agent tenant {self.tenant_id!r}",
            )
        if delivery.delivery_id != delivery.attestation.attestation_id:
            return _fail("delivery", "delivery ID does not match root attestation ID")
        try:
            trusted = self.tuf_client.refresh(delivery.trust_update)
        except Exception as exc:  # noqa: BLE001
            return _fail("tuf", f"embedded trust update invalid: {exc}")
        tuf_result = _ok(
            "tuf",
            f"embedded targets version {trusted.targets_version} verified",
        )

        att = delivery.attestation
        result, _ = _verify_attestation_core(
            att,
            delivery=delivery,
            trusted=trusted,
            now=now,
            visited=frozenset(),
            target_identity=target_identity,
            current_fulfillment_state=current_fulfillment_state,
            delivery_generation=delivery.generation,
        )
        if result.children:
            return VerificationResult(
                valid=result.valid,
                label=result.label,
                detail=result.detail,
                children=[tuf_result, *result.children],
            )
        if not result.valid:
            return _fail(att.attestation_id, result.detail, [tuf_result, result])
        return _ok(att.attestation_id, result.detail, [tuf_result])

    def verify_or_raise(self, delivery: DeliveryPackage, **kwargs: Any) -> VerificationResult:
        result = self.verify(delivery, **kwargs)
        if not result.valid:
            raise VerificationError(result.pretty(), result)
        return result

    def explain(self, delivery: DeliveryPackage, **kwargs: Any) -> VerificationResult:
        return self.verify(delivery, **kwargs)


def verify_delivery(
    delivery: DeliveryPackage,
    *,
    agent: DeliveryAgent,
    **kwargs: Any,
) -> VerificationResult:
    """Functional wrapper that still requires explicit, provisioned agent state."""

    return agent.verify(delivery, **kwargs)


def verify_attestation_or_raise(
    delivery: DeliveryPackage,
    *,
    agent: DeliveryAgent,
    **kwargs: Any,
) -> VerificationResult:
    return agent.verify_or_raise(delivery, **kwargs)


def explain_verification(
    delivery: DeliveryPackage,
    *,
    agent: DeliveryAgent,
    **kwargs: Any,
) -> VerificationResult:
    return agent.explain(delivery, **kwargs)
