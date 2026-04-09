"""CEL evaluation helpers for the hybrid attestation prototype."""

from __future__ import annotations

import copy
import json
from functools import lru_cache
from typing import Any, Callable

import celpy
from celpy import CELEvalError, json_to_cel
from celpy.adapter import CELJSONEncoder


class CelEvaluationError(Exception):
    """Raised when a CEL expression cannot be compiled or evaluated."""


def evaluate_bool(
    expression: str,
    context: dict[str, Any],
    *,
    functions: dict[str, Callable[..., Any]] | None = None,
) -> bool:
    value = evaluate_value(expression, context, functions=functions)
    if not isinstance(value, bool):
        raise CelEvaluationError(
            f"expression returned {type(value).__name__}, expected bool"
        )
    return value


def evaluate_json(
    expression: str,
    context: dict[str, Any],
    *,
    functions: dict[str, Callable[..., Any]] | None = None,
) -> Any:
    return evaluate_value(expression, context, functions=functions)


def evaluate_value(
    expression: str,
    context: dict[str, Any],
    *,
    functions: dict[str, Callable[..., Any]] | None = None,
) -> Any:
    try:
        env = celpy.Environment()
        ast = _compile(expression)
        program = env.program(ast, functions=functions)
        cel_context = {
            name: json_to_cel(value)
            for name, value in context.items()
        }
        result = program.evaluate(cel_context)
    except CELEvalError as exc:
        raise CelEvaluationError(str(exc)) from exc
    except Exception as exc:
        raise CelEvaluationError(str(exc)) from exc
    return to_python(result)


@lru_cache(maxsize=128)
def _compile(expression: str):
    env = celpy.Environment()
    return env.compile(expression)


def to_python(value: Any) -> Any:
    if value is None:
        return None
    try:
        return json.loads(json.dumps(value, cls=CELJSONEncoder))
    except TypeError:
        return value


def set_path(obj: Any, path: str, value: Any) -> Any:
    result = copy.deepcopy(to_python(obj))
    if not isinstance(result, dict):
        raise ValueError("set_path expects an object-like prior value")
    current = result
    parts = path.split(".")
    for part in parts[:-1]:
        next_value = current.setdefault(part, {})
        if not isinstance(next_value, dict):
            raise ValueError(f"cannot descend through non-object path segment {part!r}")
        current = next_value
    current[parts[-1]] = to_python(value)
    return json_to_cel(result)


def deep_merge(base: Any, patch: Any) -> Any:
    merged = _deep_merge(copy.deepcopy(to_python(base)), to_python(patch))
    return json_to_cel(merged)


def _deep_merge(base: Any, patch: Any) -> Any:
    if not isinstance(base, dict) or not isinstance(patch, dict):
        return patch

    result = dict(base)
    for key, value in patch.items():
        if key in result:
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


UPDATE_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "deep_merge": deep_merge,
    "set_path": set_path,
}
