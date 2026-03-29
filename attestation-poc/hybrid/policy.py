"""Constraint serialization and basic policy derivation."""

from __future__ import annotations

import json
from typing import Any

from .model import (
    AddonSignedConstraint,
    AllowedGVKsConstraint,
    NamespaceConstraint,
    NoClusterAdminConstraint,
    OutputConstraint,
)


def constraint_to_document(constraint: OutputConstraint) -> dict[str, Any]:
    match constraint:
        case AddonSignedConstraint(addon_id=addon_id, trust_anchor_id=trust_anchor_id):
            return {
                "type": "addon_signed",
                "addon_id": addon_id,
                "trust_anchor_id": trust_anchor_id,
            }
        case NamespaceConstraint(namespace=namespace):
            return {"type": "namespace", "namespace": namespace}
        case AllowedGVKsConstraint(allowed_gvks=allowed_gvks):
            return {
                "type": "allowed_gvks",
                "allowed_gvks": sorted(allowed_gvks),
            }
        case NoClusterAdminConstraint():
            return {"type": "no_cluster_admin"}
        case _:
            raise ValueError(f"unsupported constraint: {constraint!r}")


def constraints_to_documents(
    constraints: tuple[OutputConstraint, ...] | list[OutputConstraint],
) -> list[dict[str, Any]]:
    docs = [constraint_to_document(constraint) for constraint in constraints]
    return sorted(
        docs,
        key=lambda doc: json.dumps(doc, sort_keys=True, separators=(",", ":")),
    )


def constraint_from_document(doc: dict[str, Any]) -> OutputConstraint:
    doc_type = doc.get("type")
    if doc_type == "addon_signed":
        return AddonSignedConstraint(
            addon_id=doc["addon_id"],
            trust_anchor_id=doc.get("trust_anchor_id", "fleet-addons"),
        )
    if doc_type == "namespace":
        return NamespaceConstraint(namespace=doc["namespace"])
    if doc_type == "allowed_gvks":
        return AllowedGVKsConstraint(
            allowed_gvks=tuple(sorted(doc["allowed_gvks"])),
        )
    if doc_type == "no_cluster_admin":
        return NoClusterAdminConstraint()
    raise ValueError(f"unsupported constraint document: {doc!r}")


def constraints_from_documents(documents: list[dict[str, Any]]) -> tuple[OutputConstraint, ...]:
    return tuple(constraint_from_document(document) for document in documents)


def signed_input_envelope(
    content: Any,
    valid_until: float,
    constraints: tuple[OutputConstraint, ...],
) -> dict[str, Any]:
    return {
        "content": content,
        "output_constraints": constraints_to_documents(constraints),
        "valid_until": valid_until,
    }


def derive_output_constraints(content: Any) -> tuple[OutputConstraint, ...]:
    if not isinstance(content, dict):
        return ()

    strategy = content.get("manifest_strategy", {})
    if not isinstance(strategy, dict):
        return ()

    if strategy.get("type") != "addon":
        return ()

    addon_id = strategy.get("addon_id") or strategy.get("addon")
    if not addon_id:
        return ()

    trust_anchor_id = (
        strategy.get("trust_anchor_id")
        or strategy.get("trust_anchor")
        or "fleet-addons"
    )
    return (AddonSignedConstraint(addon_id=addon_id, trust_anchor_id=trust_anchor_id),)


def describe_constraint(constraint: OutputConstraint) -> str:
    match constraint:
        case AddonSignedConstraint(addon_id=addon_id, trust_anchor_id=trust_anchor_id):
            return f"output must be signed by {addon_id} via {trust_anchor_id}"
        case NamespaceConstraint(namespace=namespace):
            return f"all manifests must be in namespace {namespace}"
        case AllowedGVKsConstraint(allowed_gvks=allowed_gvks):
            return f"only GVKs in {sorted(allowed_gvks)}"
        case NoClusterAdminConstraint():
            return "no ClusterRoleBinding may grant cluster-admin"
        case _:
            return repr(constraint)
