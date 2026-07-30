"""TUF repository and stateful delivery-agent client.

Unlike the sibling experiment, the courier never supplies the bootstrap trust
root.  A delivery agent is provisioned with a complete, pinned ``root.json``
and python-tuf persists its trusted metadata across deliveries.  The delivery
package only materializes the repository responses that the standard updater
would otherwise fetch over HTTP.
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from securesystemslib.signer import CryptoSigner
from sigstore_protobuf_specs.dev.sigstore.trustroot.v1 import TrustedRoot
from tuf.api.exceptions import DownloadHTTPError
from tuf.api.metadata import (
    MetaFile,
    Metadata,
    Root,
    Snapshot,
    TargetFile,
    Targets,
    Timestamp,
)
from tuf.ngclient import Updater
from tuf.ngclient.fetcher import FetcherInterface

SIGSTORE_TRUSTED_ROOT_TARGET = "sigstore-trusted-root.json"
FLEETSHIFT_POLICY_TARGET = "fleetshift-trust-policy.json"


@dataclass(frozen=True)
class TrustUpdate:
    """All TUF repository responses carried by one delivery package."""

    metadata: dict[str, bytes]
    targets: dict[str, bytes]


@dataclass(frozen=True)
class TrustedMaterial:
    """TUF-authenticated Sigstore roots and FleetShift identity policies."""

    sigstore_root: TrustedRoot
    anchors: dict[str, Any]
    targets_version: int


class TUFTrustRepository:
    """Small repository producer using python-tuf's real metadata types."""

    def __init__(self) -> None:
        self._signers = {
            role: CryptoSigner.generate_ecdsa()
            for role in ("root", "targets", "snapshot", "timestamp")
        }
        self.version = 0
        now = dt.datetime.now(dt.timezone.utc)
        root = Metadata(
            Root(
                version=1,
                expires=now + dt.timedelta(days=365),
                consistent_snapshot=False,
            )
        )
        for role, signer in self._signers.items():
            root.signed.add_key(signer.public_key, role)
        root.sign(self._signers["root"])
        self.trusted_root = root.to_bytes()

    def publish(
        self,
        targets: dict[str, bytes],
        *,
        timestamp_lifetime: dt.timedelta = dt.timedelta(hours=8),
        snapshot_lifetime: dt.timedelta = dt.timedelta(days=1),
        targets_lifetime: dt.timedelta = dt.timedelta(days=7),
    ) -> TrustUpdate:
        """Publish one coherent, incremented metadata set."""

        self.version += 1
        now = dt.datetime.now(dt.timezone.utc)
        targets_metadata = Metadata(
            Targets(
                version=self.version,
                expires=now + targets_lifetime,
                targets={
                    path: TargetFile.from_data(path, content)
                    for path, content in targets.items()
                },
            )
        )
        targets_metadata.sign(self._signers["targets"])
        targets_bytes = targets_metadata.to_bytes()

        snapshot_metadata = Metadata(
            Snapshot(
                version=self.version,
                expires=now + snapshot_lifetime,
                meta={
                    "targets.json": MetaFile.from_data(
                        self.version,
                        targets_bytes,
                        ["sha256"],
                    )
                },
            )
        )
        snapshot_metadata.sign(self._signers["snapshot"])
        snapshot_bytes = snapshot_metadata.to_bytes()

        timestamp_metadata = Metadata(
            Timestamp(
                version=self.version,
                expires=now + timestamp_lifetime,
                snapshot_meta=MetaFile.from_data(
                    self.version,
                    snapshot_bytes,
                    ["sha256"],
                ),
            )
        )
        timestamp_metadata.sign(self._signers["timestamp"])
        return TrustUpdate(
            metadata={
                "timestamp.json": timestamp_metadata.to_bytes(),
                "snapshot.json": snapshot_bytes,
                "targets.json": targets_bytes,
            },
            targets=dict(targets),
        )


class _EmbeddedFetcher(FetcherInterface):
    """Expose one delivery's bytes to python-tuf without network access."""

    def __init__(self, update: TrustUpdate) -> None:
        self.update = update

    def _fetch(self, url: str) -> Iterator[bytes]:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != "memory":
            raise DownloadHTTPError("delivery verifier forbids network fetches", 403)
        path = parsed.path.lstrip("/")
        if parsed.netloc == "metadata":
            content = self.update.metadata.get(path)
        elif parsed.netloc == "targets":
            content = self.update.targets.get(path)
        else:
            content = None
        if content is None:
            raise DownloadHTTPError(f"embedded TUF object not found: {url}", 404)
        yield content


class TUFClient:
    """Stateful, rollback-resistant TUF client owned by a delivery agent."""

    def __init__(
        self,
        trusted_root: bytes,
        *,
        state_directory: str | None = None,
    ) -> None:
        # Validate the provisioned trust root immediately.  Metadata.from_bytes
        # also checks the root's own signature when the Updater bootstraps.
        root = Metadata.from_bytes(trusted_root)
        if not isinstance(root.signed, Root):
            raise ValueError("provisioned TUF bootstrap is not root metadata")
        self._trusted_root = trusted_root
        self._owned_state = (
            tempfile.TemporaryDirectory(prefix="fleetshift-tuf-client-")
            if state_directory is None
            else None
        )
        self.state_directory = Path(
            self._owned_state.name if self._owned_state is not None else state_directory
        )
        self.state_directory.mkdir(parents=True, exist_ok=True)

    def refresh(self, update: TrustUpdate) -> TrustedMaterial:
        """Verify embedded metadata/targets and advance persistent client state."""

        bootstrap = (
            self._trusted_root
            if not (self.state_directory / "root.json").exists()
            else None
        )
        updater = Updater(
            str(self.state_directory),
            "memory://metadata/",
            target_dir=None,
            target_base_url="memory://targets/",
            fetcher=_EmbeddedFetcher(update),
            bootstrap=bootstrap,
        )
        updater.refresh()

        resolved: dict[str, bytes] = {}
        for target_path in (SIGSTORE_TRUSTED_ROOT_TARGET, FLEETSHIFT_POLICY_TARGET):
            target_info = updater.get_targetinfo(target_path)
            if target_info is None:
                raise ValueError(f"required TUF target is missing: {target_path}")
            try:
                content = update.targets[target_path]
            except KeyError as exc:
                raise ValueError(
                    f"delivery omitted required TUF target bytes: {target_path}"
                ) from exc
            target_info.verify_length_and_hashes(content)
            resolved[target_path] = content

        sigstore_root = TrustedRoot().from_json(
            resolved[SIGSTORE_TRUSTED_ROOT_TARGET]
        )
        if (
            sigstore_root.media_type
            != "application/vnd.dev.sigstore.trustedroot.v0.2+json"
        ):
            raise ValueError("unsupported Sigstore trusted-root media type")
        if sigstore_root.tlogs or sigstore_root.ctlogs:
            raise ValueError("this profile forbids Rekor and certificate transparency logs")
        if not sigstore_root.certificate_authorities:
            raise ValueError("Sigstore trusted root contains no Fulcio authorities")
        if not sigstore_root.timestamp_authorities:
            raise ValueError("Sigstore trusted root contains no timestamp authorities")
        anchors = json.loads(resolved[FLEETSHIFT_POLICY_TARGET])
        if not isinstance(anchors, dict):
            raise ValueError("FleetShift trust policy must be an anchor map")
        for anchor_id, anchor in anchors.items():
            if not isinstance(anchor_id, str) or not anchor_id:
                raise ValueError("FleetShift trust policy has an empty anchor ID")
            if not isinstance(anchor, dict):
                raise ValueError(f"trust anchor {anchor_id!r} must be an object")
            for field in ("kinds", "certificate_authority_uris", "allowed_identities"):
                if not isinstance(anchor.get(field), list) or not anchor[field]:
                    raise ValueError(
                        f"trust anchor {anchor_id!r} requires non-empty {field}"
                    )
            if any(
                not isinstance(identity, dict)
                for identity in anchor["allowed_identities"]
            ):
                raise ValueError(
                    f"trust anchor {anchor_id!r} contains a malformed identity rule"
                )
        trusted_targets = Metadata.from_file(self.state_directory / "targets.json")
        if not isinstance(trusted_targets.signed, Targets):
            raise ValueError("trusted targets metadata has the wrong signed type")
        return TrustedMaterial(
            sigstore_root=sigstore_root,
            anchors=anchors,
            targets_version=trusted_targets.signed.version,
        )
