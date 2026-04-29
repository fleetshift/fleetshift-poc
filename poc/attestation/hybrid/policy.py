"""Constraint serialization and basic policy derivation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .model import OutputConstraint

if TYPE_CHECKING:
    from .model import (
        DeploymentContent,
        InputContent,
        ManagedResourceContent,
        RegisteredSelfTarget,
    )
    from .verify import VerificationContext


def constraint_to_document(constraint: OutputConstraint) -> dict[str, Any]:
    return {
        "expression": constraint.expression,
        "name": constraint.name,
    }


def constraints_to_documents(
    constraints: tuple[OutputConstraint, ...] | list[OutputConstraint],
) -> list[dict[str, Any]]:
    docs = [constraint_to_document(constraint) for constraint in constraints]
    return sorted(
        docs,
        key=lambda doc: json.dumps(doc, sort_keys=True, separators=(",", ":")),
    )


def constraint_from_document(doc: dict[str, Any]) -> OutputConstraint:
    name = doc.get("name")
    expression = doc.get("expression")
    if not isinstance(name, str) or not name:
        raise ValueError(f"constraint name must be a non-empty string: {doc!r}")
    if not isinstance(expression, str) or not expression:
        raise ValueError(
            f"constraint expression must be a non-empty string: {doc!r}"
        )
    return OutputConstraint(name=name, expression=expression)


def constraints_from_documents(documents: list[dict[str, Any]]) -> tuple[OutputConstraint, ...]:
    return tuple(constraint_from_document(document) for document in documents)


def signed_input_envelope(
    content: Any,
    valid_until: float,
    constraints: tuple[OutputConstraint, ...],
    expected_generation: int | None = None,
) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "content": content,
        "output_constraints": constraints_to_documents(constraints),
        "valid_until": valid_until,
    }
    if expected_generation is not None:
        envelope["expected_generation"] = expected_generation
    return envelope


# ---------------------------------------------------------------------------
# Strategy-implied constraint derivation for the delivery output model.
# ---------------------------------------------------------------------------


def derive_manifest_strategy_constraints(
    content: dict[str, Any],
) -> tuple[OutputConstraint, ...]:
    """Derive verification constraints implied by the manifest strategy."""
    strategy = content.get("manifest_strategy")
    if not isinstance(strategy, dict):
        return ()

    stype = strategy.get("type")

    if stype == "inline":
        return (
            OutputConstraint(
                name="manifests must match inline spec",
                expression=(
                    'action != "put" || '
                    "output.manifests == input.manifest_strategy.manifests"
                ),
            ),
        )

    if stype == "addon":
        addon_id = strategy.get("addon_id")
        if not addon_id:
            return ()
        return (
            OutputConstraint(
                name=f"manifests must be signed by {addon_id}",
                expression=(
                    f'action != "put" || '
                    f'(output.has_signature && '
                    f'output.signer_id == "{addon_id}")'
                ),
            ),
        )

    return (
        OutputConstraint(
            name=f"unknown manifest strategy type: {stype}",
            expression="false",
        ),
    )


def derive_placement_strategy_constraints(
    content: dict[str, Any],
) -> tuple[OutputConstraint, ...]:
    """Derive verification constraints implied by the placement strategy."""
    strategy = content.get("placement_strategy")
    if not isinstance(strategy, dict):
        return ()

    stype = strategy.get("type")

    if stype == "predicate":
        expression = strategy.get("expression")
        if not isinstance(expression, str) or not expression:
            return ()
        return (
            OutputConstraint(
                name="target matches placement predicate for put",
                expression=f'action != "put" || ({expression})',
            ),
            OutputConstraint(
                name="removal requires placement predicate non-match",
                expression=f'action != "remove" || !({expression})',
            ),
        )

    if stype == "addon":
        addon_id = strategy.get("addon_id")
        if not addon_id:
            return ()
        return (
            OutputConstraint(
                name=f"placement must be signed by {addon_id}",
                expression=(
                    f'placement.has_signature && '
                    f'placement.signer_id == "{addon_id}"'
                ),
            ),
            OutputConstraint(
                name="action consistent with placement decision",
                expression=(
                    '(action == "put" && target.id in placement.targets) || '
                    '(action == "remove" && !(target.id in placement.targets))'
                ),
            ),
        )

    return (
        OutputConstraint(
            name=f"unknown placement strategy type: {stype}",
            expression="false",
        ),
    )


def derive_strategy_constraints(
    content: InputContent,
    context: VerificationContext,
) -> tuple[OutputConstraint, ...]:
    """Derive all content-implied constraints from signed input content.

    Dispatches on content type.  [DeploymentContent] uses the existing
    strategy-implied constraint derivation.  [ManagedResourceContent]
    looks up its fulfillment relation from the verification bundle and
    derives constraints from it.
    """
    from .model import DeploymentContent as _DC
    from .model import ManagedResourceContent as _MRC

    if isinstance(content, _DC):
        d = content.to_dict()
        return (
            derive_manifest_strategy_constraints(d)
            + derive_placement_strategy_constraints(d)
        )

    if isinstance(content, _MRC):
        return derive_managed_resource_constraints(content, context)

    return (
        OutputConstraint(
            name=f"unknown content type: {content.content_type()}",
            expression="false",
        ),
    )


# ---------------------------------------------------------------------------
# Managed resource constraint derivation
# ---------------------------------------------------------------------------


def derive_managed_resource_constraints(
    content: ManagedResourceContent,
    context: VerificationContext,
) -> tuple[OutputConstraint, ...]:
    """Derive constraints from a managed resource's fulfillment relation.

    The relation is external evidence in the verification bundle, not
    part of the user-signed content.  The verifier looks it up by
    (addon_id, resource_type), verifies its signature against the trust
    store, and dispatches on the relation type.
    """
    from .model import RegisteredSelfTarget as _RST

    relation = context.bundle.find_fulfillment_relation(
        content.addon_id, content.resource_type,
    )
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

    if isinstance(relation, _RST):
        return _derive_registered_self_target_constraints(content, relation, context)

    return (
        OutputConstraint(
            name=f"unknown fulfillment relation type: {type(relation).__name__}",
            expression="false",
        ),
    )


def _derive_registered_self_target_constraints(
    content: ManagedResourceContent,
    relation: RegisteredSelfTarget,
    context: VerificationContext,
) -> tuple[OutputConstraint, ...]:
    """Derive constraints for a RegisteredSelfTarget relation.

    Verifies the relation signature cryptographically and against the
    trust store, then derives:
      - Placement is static to the addon (target.id == addon_id).
      - Manifests must match the user's signed spec (deterministic,
        like inline for deployments -- no addon signature required).
    """
    from .crypto import content_hash as _content_hash
    from .crypto import verify as _verify
    from .model import TrustAnchorSubject

    addon_id = content.addon_id

    relation_doc = {
        "relation_type": "registered_self_target",
        "resource_type": relation.resource_type,
    }
    relation_hash = _content_hash(relation_doc)
    sig = relation.signature.signature

    if sig.content_hash != relation_hash:
        return (
            OutputConstraint(
                name="fulfillment relation hash mismatch",
                expression="false",
            ),
        )
    if not _verify(sig.public_key, relation_hash, sig.signature_bytes):
        return (
            OutputConstraint(
                name="fulfillment relation signature invalid",
                expression="false",
            ),
        )

    if sig.signer_id != addon_id:
        return (
            OutputConstraint(
                name=(
                    f"relation signer mismatch: "
                    f"signed by {sig.signer_id!r}, "
                    f"expected addon {addon_id!r}"
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

    anchor = context.trust_store.get(relation.signature.trust_anchor_id)
    if anchor is None:
        return (
            OutputConstraint(
                name=(
                    f"trust anchor not found for relation: "
                    f"{relation.signature.trust_anchor_id!r}"
                ),
                expression="false",
            ),
        )
    known_key = anchor.known_keys.get(sig.signer_id)
    if known_key is None or known_key != sig.public_key:
        return (
            OutputConstraint(
                name=(
                    f"relation signer {sig.signer_id!r} not recognised "
                    f"by anchor {relation.signature.trust_anchor_id!r}"
                ),
                expression="false",
            ),
        )

    return (
        OutputConstraint(
            name=f"placement targets addon {addon_id}",
            expression=f'target.id == "{addon_id}"',
        ),
        OutputConstraint(
            name="manifests must match resource spec",
            expression=(
                'action != "put" || '
                'output.manifests == [{"resource_type": "managed_resource_spec", '
                '"content": input.spec}]'
            ),
        ),
    )


def describe_constraint(constraint: OutputConstraint) -> str:
    return constraint.name
