"""Mechanical guardrails for semantic, not merely nominal, hybrid parity."""

from __future__ import annotations

import ast
from pathlib import Path
import unittest


ATTESTATION_DIR = Path(__file__).resolve().parents[1]
PORT_PAIRS = (
    (
        ATTESTATION_DIR / "hybrid" / "test_hybrid.py",
        ATTESTATION_DIR / "sigstore_tuf_bundle" / "test_hybrid_parity.py",
    ),
    (
        ATTESTATION_DIR / "hybrid" / "test_delivery.py",
        ATTESTATION_DIR / "sigstore_tuf_bundle" / "test_delivery_parity.py",
    ),
    (
        ATTESTATION_DIR / "hybrid" / "test_managed_resource.py",
        ATTESTATION_DIR / "sigstore_tuf_bundle" / "test_managed_resource_parity.py",
    ),
)


def _test_methods(path: Path) -> dict[str, ast.FunctionDef]:
    tree = ast.parse(path.read_text(), filename=str(path))
    return {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
    }


def _calls(method: ast.FunctionDef) -> set[str]:
    calls: set[str] = set()
    for node in ast.walk(method):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            calls.add(node.func.attr)
        elif isinstance(node.func, ast.Name):
            calls.add(node.func.id)
    return calls


class PortingContractTests(unittest.TestCase):
    def test_every_hybrid_test_has_exactly_one_named_port(self) -> None:
        for hybrid_path, peer_path in PORT_PAIRS:
            with self.subTest(suite=hybrid_path.name):
                hybrid = _test_methods(hybrid_path)
                peer = _test_methods(peer_path)
                self.assertEqual(set(hybrid), set(peer))

    def test_hybrid_negative_tests_remain_negative_and_pin_a_reason(self) -> None:
        for hybrid_path, peer_path in PORT_PAIRS:
            hybrid = _test_methods(hybrid_path)
            peer = _test_methods(peer_path)
            for name, hybrid_method in hybrid.items():
                hybrid_calls = _calls(hybrid_method)
                if "assertRaises" not in hybrid_calls:
                    continue
                peer_calls = _calls(peer[name])
                with self.subTest(suite=hybrid_path.name, test=name):
                    self.assertTrue(
                        {"assertRaises", "assert_rejected_with"} & peer_calls,
                        "hybrid rejection was replaced by a positive or soft assertion",
                    )
                    if "assertIn" in hybrid_calls:
                        self.assertTrue(
                            {"assertIn", "assert_rejected_with"} & peer_calls,
                            "hybrid failure-detail assertion was not retained",
                        )

    def test_parity_suites_do_not_use_soft_boolean_rejections(self) -> None:
        for _, peer_path in PORT_PAIRS:
            for name, method in _test_methods(peer_path).items():
                with self.subTest(suite=peer_path.name, test=name):
                    self.assertNotIn("assertFalse", _calls(method))


if __name__ == "__main__":
    unittest.main()
