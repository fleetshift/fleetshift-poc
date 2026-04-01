"""Data-driven derivation for hybrid inputs."""

from __future__ import annotations

from typing import Any

from .cel_runtime import UPDATE_FUNCTIONS, evaluate_json
from .model import OutputConstraint
from .policy import constraints_from_documents, derive_strategy_constraints


def apply_update(prior_content: Any, update_content: Any) -> Any:
    if not isinstance(prior_content, dict):
        raise ValueError("prior content must be a dict")
    if not isinstance(update_content, dict):
        raise ValueError("update content must be a dict")
    if update_content.get("type") != "spec_update":
        raise ValueError("update content must have type spec_update")

    expression = update_content.get("derive_input_expression")
    if not isinstance(expression, str) or not expression:
        raise ValueError("spec_update requires a non-empty derive_input_expression")

    result = evaluate_json(
        expression,
        {
            "prior": prior_content,
            "update": update_content,
        },
        functions=UPDATE_FUNCTIONS,
    )
    if not isinstance(result, dict):
        raise ValueError("derive_input_expression must return an object")
    return result


def derive_constraints(update_content: Any, derived_content: Any) -> tuple[OutputConstraint, ...]:
    if not isinstance(update_content, dict):
        raise ValueError("update content must be a dict")

    output_constraints = update_content.get("output_constraints")
    if output_constraints is None:
        return derive_strategy_constraints(derived_content)
    if not isinstance(output_constraints, list):
        raise ValueError("output_constraints must be a list when provided")
    return constraints_from_documents(output_constraints)
