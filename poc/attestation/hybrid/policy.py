"""Constraint serialization and basic policy derivation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .model import OutputConstraint

if TYPE_CHECKING:
    from .model import DeploymentContent


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
        trust_anchor_id = strategy.get("trust_anchor_id", "fleet-addons")
        if not addon_id:
            return ()
        return (
            OutputConstraint(
                name=f"manifests must be signed by {addon_id} via {trust_anchor_id}",
                expression=(
                    f'action != "put" || '
                    f'(output.has_signature && '
                    f'output.signature.trust_anchor_id == "{trust_anchor_id}" && '
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
        trust_anchor_id = strategy.get("trust_anchor_id", "fleet-addons")
        if not addon_id:
            return ()
        return (
            OutputConstraint(
                name=f"placement must be signed by {addon_id} via {trust_anchor_id}",
                expression=(
                    f'placement.has_signature && '
                    f'placement.signature.trust_anchor_id == "{trust_anchor_id}" && '
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


def derive_strategy_constraints(content: DeploymentContent) -> tuple[OutputConstraint, ...]:
    """Derive all strategy-implied constraints from signed input content."""
    d = content.to_dict()
    return (
        derive_manifest_strategy_constraints(d)
        + derive_placement_strategy_constraints(d)
    )


def describe_constraint(constraint: OutputConstraint) -> str:
    return constraint.name
