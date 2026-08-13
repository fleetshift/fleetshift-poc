"""Security and Sigstore-profile tests beyond hybrid parity."""

from __future__ import annotations

import base64
import copy
import datetime as dt
import socket
import time

from cryptography import x509
from cryptography.x509.oid import ExtensionOID

from hybrid.model import DeploymentContent, StrategySpec

from .build import PoCEnvironment, make_put_attestation, make_signed_input


def _content() -> DeploymentContent:
    return DeploymentContent(
        deployment_id="tenant-a/deployment-1",
        manifest_strategy=StrategySpec(type="inline", attributes={"manifests": []}),
        placement_strategy=StrategySpec(
            type="predicate", attributes={"expression": "true"}
        ),
    )


def _delivery(env: PoCEnvironment, *, expected_generation: int | None = None):
    signed = make_signed_input(
        env.user_signer(),
        _content(),
        expected_generation=expected_generation,
    )
    return env.assemble(make_put_attestation("delivery-1", signed, ()))


def test_delivery_contains_complete_standard_logless_bundle() -> None:
    env = PoCEnvironment()
    delivery = _delivery(env)
    assert not hasattr(delivery, "trusted_root")
    assert not hasattr(delivery, "bootstrap_root_keyids")
    assert not hasattr(delivery, "tuf_root")

    document = delivery.sigstore_bundles()[0].to_dict()
    assert document["mediaType"] == "application/vnd.dev.sigstore.bundle.v0.3+json"
    material = document["verificationMaterial"]
    assert set(material) == {"certificate", "timestampVerificationData"}
    assert len(material["timestampVerificationData"]["rfc3161Timestamps"]) == 1
    assert "tlogEntries" not in material
    leaf = x509.load_der_x509_certificate(delivery.sigstore_bundles()[0].certificate_der)
    try:
        leaf.extensions.get_extension_for_oid(
            ExtensionOID.PRECERT_SIGNED_CERTIFICATE_TIMESTAMPS
        )
    except x509.ExtensionNotFound:
        pass
    else:  # pragma: no cover - explicit profile invariant
        raise AssertionError("logless profile must not contain an SCT")


def test_agent_rejects_tuf_rollback_after_advancing() -> None:
    env = PoCEnvironment()
    old_delivery = _delivery(env)
    agent = env.delivery_agent()
    assert agent.verify(old_delivery).valid

    env.republish()
    current_delivery = _delivery(env)
    assert agent.verify(current_delivery).valid
    rollback = agent.verify(old_delivery)
    assert not rollback.valid
    assert "rollback" in str(rollback).lower() or "version" in str(rollback).lower()


def test_agent_rejects_tuf_target_tampering() -> None:
    env = PoCEnvironment()
    delivery = _delivery(env)
    delivery.trust_update.targets["fleetshift-trust-policy.json"] = b"{}"
    result = env.delivery_agent().verify(delivery)
    assert not result.valid
    assert "tuf" in str(result).lower()


def test_expired_tuf_timestamp_is_rejected() -> None:
    env = PoCEnvironment()
    expired = env._repo.publish(
        env.trust_update.targets,
        timestamp_lifetime=dt.timedelta(seconds=-1),
    )
    package = _delivery(env)
    package.trust_update = expired
    result = env.delivery_agent().verify(package)
    assert not result.valid
    assert result.label == "tuf"


def test_cross_tenant_package_is_rejected_before_evidence() -> None:
    tenant_a = PoCEnvironment(tenant_id="tenant-a")
    tenant_b = PoCEnvironment(
        tenant_id="tenant-b",
        user_issuer="https://idp.fleetshift.local/tenants/tenant-b",
        user_subject_regexp=r".*@tenant-b\.example",
    )
    result = tenant_a.delivery_agent().verify(_delivery(tenant_b))
    assert not result.valid
    assert result.label == "tenant"


def test_cross_tenant_relabel_cannot_replace_provisioned_tuf_root() -> None:
    tenant_a = PoCEnvironment(tenant_id="tenant-a")
    tenant_b = PoCEnvironment(
        tenant_id="tenant-b",
        user_issuer="https://idp.fleetshift.local/tenants/tenant-b",
    )
    package = _delivery(tenant_b)
    package.tenant_id = "tenant-a"
    result = tenant_a.delivery_agent().verify(package)
    assert not result.valid
    assert result.label == "tuf"


def test_delivery_verification_does_not_open_network_connections(monkeypatch) -> None:
    def reject_network(*_args, **_kwargs):
        raise AssertionError("offline delivery verification attempted a network connection")

    monkeypatch.setattr(socket.socket, "connect", reject_network)
    env = PoCEnvironment()
    result = env.delivery_agent().verify(_delivery(env))
    assert result.valid, result


def test_signed_generation_cannot_be_changed_by_courier() -> None:
    env = PoCEnvironment()
    package = _delivery(env, expected_generation=3)
    package.generation = 4
    result = env.delivery_agent().verify(package)
    assert not result.valid
    assert "signed generation" in str(result)


def test_dsse_multi_signature_bundle_is_rejected() -> None:
    env = PoCEnvironment()
    package = _delivery(env)
    signed = package.attestation.signed_input
    document = signed.bundle.to_dict()
    document["dsseEnvelope"]["signatures"].append(
        copy.deepcopy(document["dsseEnvelope"]["signatures"][0])
    )
    object.__setattr__(signed, "bundle", signed.bundle.__class__(document))
    result = env.delivery_agent().verify(package)
    assert not result.valid
    assert "exactly one signature" in str(result)


def test_corrupted_rfc3161_response_is_rejected() -> None:
    env = PoCEnvironment()
    package = _delivery(env)
    signed = package.attestation.signed_input
    document = signed.bundle.to_dict()
    encoded = document["verificationMaterial"]["timestampVerificationData"][
        "rfc3161Timestamps"
    ][0]["signedTimestamp"]
    raw = bytearray(base64.b64decode(encoded))
    raw[-1] ^= 1
    document["verificationMaterial"]["timestampVerificationData"][
        "rfc3161Timestamps"
    ][0]["signedTimestamp"] = base64.b64encode(raw).decode()
    object.__setattr__(signed, "bundle", signed.bundle.__class__(document))
    result = env.delivery_agent().verify(package)
    assert not result.valid
    assert "timestamp" in str(result).lower()


def test_fulcio_leaf_expiry_after_signing_does_not_invalidate_timestamped_bundle() -> None:
    env = PoCEnvironment()
    package = _delivery(env)
    # Sigstore checks the short-lived certificate at trusted signing time. The
    # separate FleetShift authorization validity remains enforced at use time.
    result = env.delivery_agent().verify(package, now=time.time() + 15 * 60)
    assert result.valid, result
