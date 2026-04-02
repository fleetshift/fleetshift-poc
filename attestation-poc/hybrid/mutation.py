"""Data-driven derivation for hybrid inputs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .cel_runtime import UPDATE_FUNCTIONS, evaluate_bool, evaluate_json
from .model import OutputConstraint
from .policy import constraints_from_documents

if TYPE_CHECKING:
    from .model import DeploymentContent


def check_preconditions(prior_content: DeploymentContent, update_content: dict[str, Any]) -> None:
    """Evaluate signed preconditions against prior content.

    If any precondition evaluates false, raises ValueError to halt
    derivation (fail closed).  Updates that want conditional/no-op
    behavior should encode that inside the derive_input_expression.
    """
    preconditions = update_content.get("preconditions")
    if preconditions is None:
        return
    if not isinstance(preconditions, list):
        raise ValueError("preconditions must be a list when provided")

    prior_dict = prior_content.to_dict()

    for i, expr in enumerate(preconditions):
        if not isinstance(expr, str) or not expr:
            raise ValueError(f"precondition {i} must be a non-empty string")
        result = evaluate_bool(expr, {"prior": prior_dict, "update": update_content})
        if not result:
            raise ValueError(f"precondition failed: {expr}")


def apply_update(prior_content: DeploymentContent, update_content: dict[str, Any]) -> dict[str, Any]:
    """Apply a spec-update directive to prior input content.

    The caller is responsible for ensuring update_content comes from a
    manifest envelope whose resource_type identifies it as a spec update;
    this function does not re-check that discriminator.

    Returns a plain dict -- the caller reconstitutes typed content.
    """
    prior_dict = prior_content.to_dict()

    expression = update_content.get("derive_input_expression")
    if not isinstance(expression, str) or not expression:
        raise ValueError("update content requires a non-empty derive_input_expression")

    result = evaluate_json(
        expression,
        {
            "prior": prior_dict,
            "update": update_content,
        },
        functions=UPDATE_FUNCTIONS,
    )
    if not isinstance(result, dict):
        raise ValueError("derive_input_expression must return an object")
    return result


def derive_constraints(
    prior_constraints: tuple[OutputConstraint, ...],
    update_content: dict[str, Any],
) -> tuple[OutputConstraint, ...]:
    """Derive the explicit output constraints for a derived input.

    The prior's explicit constraints carry forward unconditionally -- an
    update patches the spec, not the security policy.  If the update
    provides output_constraints they are additive, layered on top of the
    prior's.

    Strategy-implied constraints are not produced here; they are derived
    late from the final computed content at verification time.
    """

    additional = update_content.get("output_constraints")
    if additional is None:
        return prior_constraints
    if not isinstance(additional, list):
        raise ValueError("output_constraints must be a list when provided")
    return prior_constraints + constraints_from_documents(additional)
