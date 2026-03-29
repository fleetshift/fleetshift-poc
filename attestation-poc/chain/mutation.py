"""
Deterministic mutation expressions (simulating CEL for the prototype).

The critical property: given the same expression and the same input,
both the platform and the delivery agent produce the same output.
This makes the mutation independently re-executable and verifiable
without trusting the platform's evaluation.

Expression format: a JSON object whose keys are dot-separated field
paths and whose values are the values to set.

    '{"version": "1.30.2"}'                        → set top-level field
    '{"manifest_strategy.config.version": "1.30.2"}' → set nested field
"""

from __future__ import annotations

import copy
import json
from typing import Any


def apply_mutation(expression: str, spec: dict[str, Any]) -> dict[str, Any]:
    """Apply a mutation expression to a spec, returning the new spec."""
    result = copy.deepcopy(spec)
    mutations: dict[str, Any] = json.loads(expression)
    for path, value in mutations.items():
        _set_path(result, path.split("."), value)
    return result


def _set_path(obj: dict, parts: list[str], value: Any) -> None:
    for part in parts[:-1]:
        obj = obj.setdefault(part, {})
    obj[parts[-1]] = value
