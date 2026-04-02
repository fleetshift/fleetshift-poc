"""Data-driven derivation for hybrid inputs."""

from __future__ import annotations

from typing import Any

from .cel_runtime import UPDATE_FUNCTIONS, evaluate_bool, evaluate_json
from .model import OutputConstraint
from .policy import constraints_from_documents


def check_preconditions(prior_content: Any, update_content: Any) -> None:
    """Evaluate signed preconditions against prior content.

    If any precondition evaluates false, raises ValueError to halt
    derivation (fail closed).  Updates that want conditional/no-op
    behavior should encode that inside the derive_input_expression.
    """
    from .model import DeploymentContent

    if not isinstance(update_content, dict):
        return
    preconditions = update_content.get("preconditions")
    if preconditions is None:
        return
    if not isinstance(preconditions, list):
        raise ValueError("preconditions must be a list when provided")

    if isinstance(prior_content, DeploymentContent):
        prior_content = prior_content.to_dict()

    for i, expr in enumerate(preconditions):
        if not isinstance(expr, str) or not expr:
            raise ValueError(f"precondition {i} must be a non-empty string")
        result = evaluate_bool(expr, {"prior": prior_content, "update": update_content})
        if not result:
            raise ValueError(f"precondition failed: {expr}")


def apply_update(prior_content: Any, update_content: Any) -> Any:
    """Apply a spec-update directive to prior input content.

    The caller is responsible for ensuring update_content comes from a
    manifest envelope whose resource_type identifies it as a spec update;
    this function does not re-check that discriminator.

    Returns a plain dict -- the caller reconstitutes typed content.
    """
    from .model import DeploymentContent

    if isinstance(prior_content, DeploymentContent):
        prior_content = prior_content.to_dict()
    if not isinstance(prior_content, dict):
        raise ValueError("prior content must be a dict")
    if not isinstance(update_content, dict):
        raise ValueError("update content must be a dict")

    expression = update_content.get("derive_input_expression")
    if not isinstance(expression, str) or not expression:
        raise ValueError("update content requires a non-empty derive_input_expression")

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


def derive_constraints(
    prior_constraints: tuple[OutputConstraint, ...],
    update_content: Any,
) -> tuple[OutputConstraint, ...]:
    """Derive the explicit output constraints for a derived input.

    The prior's explicit constraints carry forward unconditionally -- an
    update patches the spec, not the security policy.  If the update
    provides output_constraints they are additive, layered on top of the
    prior's.

    Strategy-implied constraints are not produced here; they are derived
    late from the final computed content at verification time.
    """
    if not isinstance(update_content, dict):
        raise ValueError("update content must be a dict")

    additional = update_content.get("output_constraints")
    if additional is None:
        return prior_constraints
    if not isinstance(additional, list):
        raise ValueError("output_constraints must be a list when provided")
    return prior_constraints + constraints_from_documents(additional)
