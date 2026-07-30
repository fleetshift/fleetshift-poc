"""Minimal TUF repository for distributing Fulcio/TSA roots and trust anchors.

Follows the TUF role model (root → timestamp → snapshot → targets) with
ECDSA keys via securesystemslib. The management-plane "server" embeds a
verified snapshot of targets into each DeliveryBundle so delivery agents never
fetch trust material over HTTP at verify time.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from securesystemslib.signer import CryptoSigner

# TUF metadata versioning for this POC is intentionally simple: single root,
# incrementing targets/snapshot/timestamp versions on each publish.


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _keyid(signer: CryptoSigner) -> str:
    return signer.public_key.keyid


@dataclass(frozen=True)
class TrustedMaterial:
    """Resolved trust material after verifying an embedded TUF snapshot."""

    fulcio_roots_pem: tuple[bytes, ...]
    tsa_root_pem: bytes
    anchors: dict[str, Any]  # anchor_id -> anchor config
    bootstrap_root_keyids: tuple[str, ...]


@dataclass
class TUFRepository:
    """In-memory / on-disk TUF repo used by the POC server."""

    root_signer: CryptoSigner
    targets_signer: CryptoSigner
    snapshot_signer: CryptoSigner
    timestamp_signer: CryptoSigner
    targets: dict[str, bytes]
    version: int = 1

    @classmethod
    def create(cls) -> TUFRepository:
        return cls(
            root_signer=CryptoSigner.generate_ecdsa(),
            targets_signer=CryptoSigner.generate_ecdsa(),
            snapshot_signer=CryptoSigner.generate_ecdsa(),
            timestamp_signer=CryptoSigner.generate_ecdsa(),
            targets={},
            version=1,
        )

    def set_target(self, name: str, content: bytes) -> None:
        self.targets[name] = content

    def publish(self) -> dict[str, Any]:
        """Build a self-contained TUF snapshot dict for embedding in a delivery."""
        self.version += 1
        targets_meta: dict[str, Any] = {}
        for name, content in self.targets.items():
            targets_meta[name] = {
                "length": len(content),
                "hashes": {"sha256": _sha256_hex(content)},
            }

        targets_body = {
            "_type": "targets",
            "spec_version": "1.0",
            "version": self.version,
            "expires": "2030-01-01T00:00:00Z",
            "targets": targets_meta,
        }
        targets_signed = self._sign(targets_body, self.targets_signer)

        snapshot_body = {
            "_type": "snapshot",
            "spec_version": "1.0",
            "version": self.version,
            "expires": "2030-01-01T00:00:00Z",
            "meta": {
                "targets.json": {
                    "version": self.version,
                    "length": len(json.dumps(targets_signed).encode()),
                    "hashes": {
                        "sha256": _sha256_hex(
                            json.dumps(targets_signed, sort_keys=True).encode()
                        )
                    },
                }
            },
        }
        snapshot_signed = self._sign(snapshot_body, self.snapshot_signer)

        timestamp_body = {
            "_type": "timestamp",
            "spec_version": "1.0",
            "version": self.version,
            "expires": "2030-01-01T00:00:00Z",
            "meta": {
                "snapshot.json": {
                    "version": self.version,
                    "length": len(json.dumps(snapshot_signed).encode()),
                    "hashes": {
                        "sha256": _sha256_hex(
                            json.dumps(snapshot_signed, sort_keys=True).encode()
                        )
                    },
                }
            },
        }
        timestamp_signed = self._sign(timestamp_body, self.timestamp_signer)

        root_body = {
            "_type": "root",
            "spec_version": "1.0",
            "version": 1,
            "expires": "2030-01-01T00:00:00Z",
            "consistent_snapshot": False,
            "keys": {
                _keyid(self.root_signer): self.root_signer.public_key.to_dict(),
                _keyid(self.targets_signer): self.targets_signer.public_key.to_dict(),
                _keyid(self.snapshot_signer): self.snapshot_signer.public_key.to_dict(),
                _keyid(self.timestamp_signer): self.timestamp_signer.public_key.to_dict(),
            },
            "roles": {
                "root": {"keyids": [_keyid(self.root_signer)], "threshold": 1},
                "targets": {"keyids": [_keyid(self.targets_signer)], "threshold": 1},
                "snapshot": {"keyids": [_keyid(self.snapshot_signer)], "threshold": 1},
                "timestamp": {"keyids": [_keyid(self.timestamp_signer)], "threshold": 1},
            },
        }
        root_signed = self._sign(root_body, self.root_signer)

        return {
            "root.json": root_signed,
            "timestamp.json": timestamp_signed,
            "snapshot.json": snapshot_signed,
            "targets.json": targets_signed,
            "targets": {name: content.decode("utf-8") if _is_text(content) else
                        {"b64": __import__("base64").b64encode(content).decode("ascii")}
                        for name, content in self.targets.items()},
            # Binary targets are base64; PEM/JSON stored as text for readability.
            "_targets_raw": {name: content.hex() for name, content in self.targets.items()},
        }

    def bootstrap_root_keyids(self) -> tuple[str, ...]:
        return (_keyid(self.root_signer),)

    @staticmethod
    def _sign(body: dict[str, Any], signer: CryptoSigner) -> dict[str, Any]:
        payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        sig = signer.sign(payload)
        return {"signed": body, "signatures": [sig.to_dict()]}


def _is_text(content: bytes) -> bool:
    try:
        content.decode("utf-8")
        return b"\x00" not in content
    except UnicodeDecodeError:
        return False


def _verify_signed(
    signed_meta: dict[str, Any],
    *,
    public_keys: dict[str, Any],
    role: str,
    threshold: int,
) -> dict[str, Any]:
    import copy

    from securesystemslib.signer import Key, Signature

    body = signed_meta["signed"]
    if body.get("_type") != role:
        raise ValueError(f"expected {role} metadata")
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    verified = 0
    for sig_entry in signed_meta.get("signatures", []):
        keyid = sig_entry["keyid"]
        if keyid not in public_keys:
            continue
        # Key.from_dict / Signature.from_dict mutate their inputs.
        key = Key.from_dict(keyid, copy.deepcopy(public_keys[keyid]))
        signature = Signature.from_dict(copy.deepcopy(sig_entry))
        key.verify_signature(signature, payload)
        verified += 1
    if verified < threshold:
        raise ValueError(f"insufficient signatures for {role}: {verified}<{threshold}")
    return body


def verify_embedded_tuf(
    snapshot: dict[str, Any],
    *,
    bootstrap_root_keyids: tuple[str, ...],
) -> TrustedMaterial:
    """Verify embedded TUF metadata and return trusted targets.

    ``bootstrap_root_keyids`` is the only out-of-band trust (equivalent to a
    pinned TUF root distributed at agent provisioning). Everything else is
    checked against signatures inside the snapshot.
    """
    root = snapshot["root.json"]
    root_body = root["signed"]
    keys = root_body["keys"]
    # Bootstrap: root must be signed by an expected root keyid.
    root_role = root_body["roles"]["root"]
    if not set(bootstrap_root_keyids) & set(root_role["keyids"]):
        raise ValueError("embedded TUF root is not signed by bootstrap key")
    _verify_signed(
        root,
        public_keys={kid: keys[kid] for kid in root_role["keyids"]},
        role="root",
        threshold=root_role["threshold"],
    )

    def role_keys(role_name: str) -> dict[str, Any]:
        role = root_body["roles"][role_name]
        return {kid: keys[kid] for kid in role["keyids"]}, role["threshold"]

    ts_keys, ts_thresh = role_keys("timestamp")
    _verify_signed(snapshot["timestamp.json"], public_keys=ts_keys, role="timestamp", threshold=ts_thresh)

    sn_keys, sn_thresh = role_keys("snapshot")
    _verify_signed(snapshot["snapshot.json"], public_keys=sn_keys, role="snapshot", threshold=sn_thresh)

    tg_keys, tg_thresh = role_keys("targets")
    targets_body = _verify_signed(
        snapshot["targets.json"], public_keys=tg_keys, role="targets", threshold=tg_thresh
    )

    raw = snapshot.get("_targets_raw") or {}
    resolved: dict[str, bytes] = {}
    for name, meta in targets_body["targets"].items():
        if name not in raw:
            raise ValueError(f"missing target bytes for {name}")
        content = bytes.fromhex(raw[name])
        if len(content) != meta["length"]:
            raise ValueError(f"length mismatch for {name}")
        if _sha256_hex(content) != meta["hashes"]["sha256"]:
            raise ValueError(f"hash mismatch for {name}")
        resolved[name] = content

    anchors = json.loads(resolved["trust-anchors.json"].decode())
    fulcio_roots = [resolved["fulcio-root.pem"]]
    if "workload-fulcio-root.pem" in resolved:
        fulcio_roots.append(resolved["workload-fulcio-root.pem"])
    return TrustedMaterial(
        fulcio_roots_pem=tuple(fulcio_roots),
        tsa_root_pem=resolved["tsa-root.pem"],
        anchors=anchors,
        bootstrap_root_keyids=bootstrap_root_keyids,
    )


def publish_trust_repo(
    *,
    fulcio_root_pem: bytes,
    tsa_root_pem: bytes,
    anchors: dict[str, Any],
    workload_fulcio_root_pem: bytes | None = None,
    repo: TUFRepository | None = None,
) -> tuple[TUFRepository, dict[str, Any]]:
    """Convenience: load targets and publish an embeddable snapshot."""
    repo = repo or TUFRepository.create()
    repo.set_target("fulcio-root.pem", fulcio_root_pem)
    if workload_fulcio_root_pem is not None:
        repo.set_target("workload-fulcio-root.pem", workload_fulcio_root_pem)
    repo.set_target("tsa-root.pem", tsa_root_pem)
    repo.set_target(
        "trust-anchors.json",
        json.dumps(anchors, sort_keys=True, indent=2).encode(),
    )
    return repo, repo.publish()
