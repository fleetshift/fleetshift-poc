"""Data-driven derivation for hybrid inputs."""

from __future__ import annotations

import copy
from typing import Any

from .model import OutputConstraint
from .policy import constraints_from_documents, derive_output_constraints


def apply_update(prior_content: Any, update_content: Any) -> Any:
    if not isinstance(prior_content, dict):
        raise ValueError("prior content must be a dict")
    if not isinstance(update_content, dict):
        raise ValueError("update content must be a dict")
    if update_content.get("type") != "spec_update":
        raise ValueError("update content must have type spec_update")

    mutation = update_content.get("mutation")
    if not isinstance(mutation, dict) or not mutation:
        raise ValueError("spec_update requires a non-empty mutation mapping")

    result = copy.deepcopy(prior_content)
    for path, value in mutation.items():
        if not isinstance(path, str) or not path:
            raise ValueError("mutation paths must be non-empty strings")
        _set_path(result, path.split("."), value)
    return result


def derive_constraints(update_content: Any, derived_content: Any) -> tuple[OutputConstraint, ...]:
    if not isinstance(update_content, dict):
        raise ValueError("update content must be a dict")

    output_constraints = update_content.get("output_constraints")
    if output_constraints is None:
        return derive_output_constraints(derived_content)
    if not isinstance(output_constraints, list):
        raise ValueError("output_constraints must be a list when provided")
    return constraints_from_documents(output_constraints)


def _set_path(obj: dict[str, Any], parts: list[str], value: Any) -> None:
    current = obj
    for part in parts[:-1]:
        current = current.setdefault(part, {})
        if not isinstance(current, dict):
            raise ValueError(f"cannot descend through non-object path segment {part!r}")
    current[parts[-1]] = value
