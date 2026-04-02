"""Core data model for the hybrid attestation prototype."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .cel_runtime import CelEvaluationError, evaluate_bool
from .crypto import content_hash, verify

if TYPE_CHECKING:
    from .verify import VerificationContext, VerificationRef, VerificationResult


@dataclass(frozen=True)
class StrategySpec:
    """A strategy spec with a type discriminator and open attributes.

    Mirrors the Go domain's strategy specs (ManifestStrategySpec,
    PlacementStrategySpec): a typed discriminator plus variant-specific
    fields.  The type field is always present; strategy-specific fields
    live in attributes.
    """

    type: str
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, **self.attributes}

    @staticmethod
    def from_dict(d: dict[str, Any]) -> StrategySpec:
        return StrategySpec(
            type=d["type"],
            attributes={k: v for k, v in d.items() if k != "type"},
        )


@dataclass(frozen=True)
class DeploymentContent:
    """Typed input content for a deployment.

    Mirrors the Go domain's Deployment struct: deployment identity plus
    manifest and placement strategy specs.  Using a typed dataclass
    instead of an opaque dict makes the implied structure explicit and
    provides direct attribute access for verification consumers.
    """

    deployment_id: str
    manifest_strategy: StrategySpec
    placement_strategy: StrategySpec

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "manifest_strategy": self.manifest_strategy.to_dict(),
            "placement_strategy": self.placement_strategy.to_dict(),
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> DeploymentContent:
        return DeploymentContent(
            deployment_id=d["deployment_id"],
            manifest_strategy=StrategySpec.from_dict(d["manifest_strategy"]),
            placement_strategy=StrategySpec.from_dict(d["placement_strategy"]),
        )


@dataclass(frozen=True)
class TrustAnchor:
    """An external trust root that maps identities to public keys."""

    anchor_id: str
    known_keys: dict[str, bytes]
    attributes: dict[str, Any] = field(default_factory=dict)
    constraints: tuple[TrustAnchorConstraint, ...] = ()

    def verify_signer(
        self,
        *,
        signer_id: str,
        public_key: bytes,
        subject: TrustAnchorSubject,
        label: str,
        context: VerificationContext,
    ) -> VerificationResult:
        known_key = self.known_keys.get(signer_id)
        if known_key is None or known_key != public_key:
            return context.fail(label, f"key not recognised by anchor {self.anchor_id}")

        children: list[VerificationResult] = []
        for constraint in self.constraints:
            constraint_result = constraint.verify(
                anchor=self,
                subject=subject,
                label=label,
                context=context,
            )
            children.append(constraint_result)
            if not constraint_result.valid:
                return context.fail(
                    label,
                    f"trust anchor constraint failed: {constraint.name}",
                    children,
                )

        detail = f"recognised signer {signer_id}"
        if self.constraints:
            detail += f" with {len(self.constraints)} anchor constraints"
        return context.ok(label, detail, children)

    def to_context(self) -> dict[str, Any]:
        return {
            "anchor_id": self.anchor_id,
            "attributes": self.attributes,
        }


@dataclass(frozen=True)
class TrustAnchorConstraint:
    """A CEL predicate the trust anchor applies to signer subjects."""

    name: str
    expression: str

    def verify(
        self,
        *,
        anchor: TrustAnchor,
        subject: dict[str, Any],
        label: str,
        context: VerificationContext,
    ) -> VerificationResult:
        try:
            valid = evaluate_bool(
                self.expression,
                {
                    "anchor": anchor.to_context(),
                    "subject": subject.to_context(),
                },
            )
        except CelEvaluationError as exc:
            return context.fail(label, f"trust anchor constraint evaluation failed: {exc}")

        if not valid:
            return context.fail(label, f"predicate returned false: {self.name}")

        return context.ok(label, f"trust anchor constraint matched: {self.name}")


@dataclass(frozen=True)
class TrustAnchorSubject:
    """Authenticated subject fields a trust anchor may authorize against."""

    kind: str
    signer_id: str | None
    content: Any
    valid_until: float | None = None

    def to_context(self) -> dict[str, Any]:
        content = (
            self.content.to_dict()
            if isinstance(self.content, DeploymentContent) else self.content
        )
        ctx = {
            "content": content,
            "kind": self.kind,
            "signer_id": self.signer_id,
        }
        if self.valid_until is not None:
            ctx["valid_until"] = self.valid_until
        return ctx


@dataclass(frozen=True)
class Signature:
    """A detached signature over a canonical content hash."""

    signer_id: str
    public_key: bytes
    content_hash: bytes
    signature_bytes: bytes


@dataclass(frozen=True)
class OutputSignature:
    """Verification material for a signed output."""

    signature: Signature
    trust_anchor_id: str


@dataclass(frozen=True)
class KeyBinding:
    """Binds a signer's key to an identity via a trust anchor."""

    signer_id: str
    public_key: bytes
    trust_anchor_id: str
    binding_proof: bytes

    def verify(self, context: VerificationContext) -> VerificationResult:
        label = f"{self.signer_id} key binding"
        binding_doc = {
            "public_key": self.public_key.hex(),
            "signer_id": self.signer_id,
            "trust_anchor_id": self.trust_anchor_id,
        }
        binding_hash = content_hash(binding_doc)
        if not verify(self.public_key, binding_hash, self.binding_proof):
            return context.fail(label, "proof-of-possession failed")
        return context.ok(label, "proof-of-possession verified")


@dataclass(frozen=True)
class OutputConstraint:
    """A signed CEL predicate over the verified {input, output} pair."""

    name: str
    expression: str

    def evaluate(
        self,
        attestation_id: str,
        cel_context: dict[str, Any],
        context: VerificationContext,
    ) -> VerificationResult:
        """Evaluate this constraint against an arbitrary CEL context."""
        label = f"{attestation_id} constraint"
        try:
            valid = evaluate_bool(self.expression, cel_context)
        except CelEvaluationError as exc:
            return context.fail(label, f"constraint evaluation failed: {exc}")

        if not valid:
            return context.fail(label, f"predicate returned false: {self.name}")

        return context.ok(label, f"constraint matched: {self.name}")


@dataclass(frozen=True)
class VerifiedInput:
    content: Any
    content_hash: bytes
    output_constraints: tuple[OutputConstraint, ...]
    signer_id: str | None = None
    expected_generation: int | None = None


@dataclass(frozen=True)
class VerifiedOutput:
    content: Any
    content_hash: bytes
    signer_id: str | None = None


# ---------------------------------------------------------------------------
# Strategy types -- structured representations of the strategy fields within
# signed input content.  Used by strategy-implied constraint derivation.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InlineManifestStrategy:
    """Inline manifests: output must exactly match."""

    manifests: Any


@dataclass(frozen=True)
class AddonManifestStrategy:
    """Addon produces and signs manifests."""

    addon_id: str
    trust_anchor_id: str


@dataclass(frozen=True)
class PredicatePlacementStrategy:
    """CEL predicate over target identity for self-assessment."""

    expression: str


@dataclass(frozen=True)
class AddonPlacementStrategy:
    """Addon produces and signs placement decisions."""

    addon_id: str
    trust_anchor_id: str


# ---------------------------------------------------------------------------
# Manifest envelope -- typed payload items within a delivery.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ManifestEnvelope:
    """A single typed manifest/resource item within a delivery.

    Analogous to fleetshift-server's domain.Manifest: each item carries
    an explicit resource_type so receivers know how to parse the opaque
    content.  Order within a manifests list may be significant.
    """

    resource_type: str
    content: Any


# ---------------------------------------------------------------------------
# Delivery output types -- algebraic output variants for typed deliveries.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlacementEvidence:
    """Signed placement decision from a placement addon.

    The decision is a concrete list of target IDs -- placement answers
    "which targets," nothing more.  Unsigned placement decisions are
    meaningless (cannot be trusted), so the signature is required.

    deployment_id binds this evidence to a specific deployment so that
    valid evidence from one deployment cannot be replayed against another.
    The signature covers both deployment_id and targets.
    """

    deployment_id: str
    targets: tuple[str, ...]
    signature: OutputSignature


@dataclass(frozen=True)
class PutManifests:
    """Deliver manifests to a target.

    manifests is an ordered sequence of typed envelopes.  Order is
    preserved through signing and verification because some targets may
    treat multi-item deliveries as an ordered sequence.
    """

    manifests: tuple[ManifestEnvelope, ...]
    signature: OutputSignature | None = None
    placement: PlacementEvidence | None = None

    def verify(
        self,
        attestation_id: str,
        verified_input: VerifiedInput,
        context: VerificationContext,
    ) -> tuple[VerificationResult, VerifiedOutput | None]:
        from .policy import derive_strategy_constraints

        label = f"{attestation_id} output"
        children: list[VerificationResult] = []

        serialized = _serialize_manifests(self.manifests)

        manifest_signer_id: str | None = None
        if self.signature is not None:
            sig_result, manifest_signer_id = _verify_output_sig(
                serialized, self.signature,
                f"{attestation_id} manifest signature", context,
            )
            children.append(sig_result)
            if not sig_result.valid:
                return context.fail(label, "manifest signature invalid", children), None
        else:
            children.append(
                context.ok(f"{attestation_id} manifest signature", "unsigned"),
            )

        placement_signer_id: str | None = None
        if self.placement is not None:
            pe_result, placement_signer_id = _verify_placement_evidence(
                self.placement, verified_input, attestation_id, context,
            )
            children.append(pe_result)
            if not pe_result.valid:
                return context.fail(label, "placement evidence invalid", children), None

        implied = derive_strategy_constraints(verified_input.content)
        all_constraints = implied + verified_input.output_constraints

        cel_ctx = _delivery_cel_context(
            verified_input, "put", self.manifests, self.signature,
            manifest_signer_id, self.placement, placement_signer_id, context,
        )

        if not all_constraints:
            children.append(
                context.ok(f"{attestation_id} constraints", "no constraints"),
            )
        else:
            for c in all_constraints:
                r = c.evaluate(attestation_id, cel_ctx, context)
                children.append(r)
                if not r.valid:
                    return (
                        context.fail(label, f"constraint failed: {c.name}", children),
                        None,
                    )

        manifest_hash = content_hash(serialized)
        return (
            context.ok(label, "satisfies all constraints", children),
            VerifiedOutput(
                content=serialized,
                content_hash=manifest_hash,
                signer_id=manifest_signer_id,
            ),
        )


@dataclass(frozen=True)
class RemoveByDeploymentId:
    """Remove a deployment from a target by deployment ID.

    The deployment_id must match the attested deployment's identity,
    preventing confused-deputy attacks via opaque handles.
    """

    deployment_id: str
    placement: PlacementEvidence | None = None

    def verify(
        self,
        attestation_id: str,
        verified_input: VerifiedInput,
        context: VerificationContext,
    ) -> tuple[VerificationResult, VerifiedOutput | None]:
        from .policy import derive_strategy_constraints

        label = f"{attestation_id} output"
        children: list[VerificationResult] = []

        input_content = verified_input.content
        if isinstance(input_content, DeploymentContent):
            expected_id: str | None = input_content.deployment_id
        elif isinstance(input_content, dict):
            expected_id = input_content.get("deployment_id")
        else:
            expected_id = None
        if expected_id is not None and self.deployment_id != expected_id:
            return (
                context.fail(
                    label,
                    f"remove deployment_id mismatch: output targets {self.deployment_id!r}, "
                    f"input has {expected_id!r}",
                ),
                None,
            )

        placement_signer_id: str | None = None
        if self.placement is not None:
            pe_result, placement_signer_id = _verify_placement_evidence(
                self.placement, verified_input, attestation_id, context,
            )
            children.append(pe_result)
            if not pe_result.valid:
                return context.fail(label, "placement evidence invalid", children), None

        implied = derive_strategy_constraints(verified_input.content)
        all_constraints = implied + verified_input.output_constraints

        cel_ctx = _delivery_cel_context(
            verified_input, "remove", None, None,
            None, self.placement, placement_signer_id, context,
        )
        cel_ctx["output"] = {"deployment_id": self.deployment_id}

        if not all_constraints:
            children.append(
                context.ok(f"{attestation_id} constraints", "no constraints"),
            )
        else:
            for c in all_constraints:
                r = c.evaluate(attestation_id, cel_ctx, context)
                children.append(r)
                if not r.valid:
                    return (
                        context.fail(label, f"constraint failed: {c.name}", children),
                        None,
                    )

        remove_content: dict[str, Any] = {"deployment_id": self.deployment_id}
        return (
            context.ok(label, "satisfies all constraints", children),
            VerifiedOutput(
                content=remove_content,
                content_hash=content_hash(remove_content),
            ),
        )


DeliveryOutput = PutManifests | RemoveByDeploymentId


def _verify_output_sig(
    content: Any,
    sig: OutputSignature,
    label: str,
    context: VerificationContext,
) -> tuple[VerificationResult, str | None]:
    """Verify a signed output artifact against its trust anchor."""
    signature = sig.signature
    h = content_hash(content)
    if signature.content_hash != h:
        return context.fail(label, "hash mismatch"), None
    if not verify(signature.public_key, h, signature.signature_bytes):
        return context.fail(
            label, f"signature verification failed for {signature.signer_id}",
        ), None

    anchor = context.trust_store.get(sig.trust_anchor_id)
    if anchor is None:
        return context.fail(
            label, f"trust anchor not found: {sig.trust_anchor_id}",
        ), None

    subject = TrustAnchorSubject(
        kind="output", signer_id=signature.signer_id, content=content,
    )
    anchor_result = anchor.verify_signer(
        signer_id=signature.signer_id,
        public_key=signature.public_key,
        subject=subject,
        label=f"{label} anchor",
        context=context,
    )
    if not anchor_result.valid:
        return (
            context.fail(label, "trust anchor rejected signer", [anchor_result]),
            None,
        )

    return (
        context.ok(
            label,
            f"signed by {signature.signer_id}, verified against {sig.trust_anchor_id}",
            [anchor_result],
        ),
        signature.signer_id,
    )


def _placement_evidence_content(evidence: PlacementEvidence) -> dict[str, Any]:
    """Canonical document that a placement addon signs over."""
    return {
        "deployment_id": evidence.deployment_id,
        "targets": list(evidence.targets),
    }


def _verify_placement_evidence(
    evidence: PlacementEvidence,
    verified_input: VerifiedInput,
    attestation_id: str,
    context: VerificationContext,
) -> tuple[VerificationResult, str | None]:
    """Verify placement evidence signature and deployment binding."""
    label = f"{attestation_id} placement evidence"
    children: list[VerificationResult] = []

    sig_result, signer_id = _verify_output_sig(
        _placement_evidence_content(evidence), evidence.signature,
        label, context,
    )
    children.append(sig_result)
    if not sig_result.valid:
        return context.fail(label, "signature invalid", children), None

    input_content = verified_input.content
    if isinstance(input_content, DeploymentContent):
        expected_id: str | None = input_content.deployment_id
    elif isinstance(input_content, dict):
        expected_id = input_content.get("deployment_id")
    else:
        expected_id = None
    if expected_id is not None and evidence.deployment_id != expected_id:
        return (
            context.fail(
                label,
                f"deployment_id mismatch: evidence has {evidence.deployment_id!r}, "
                f"input has {expected_id!r}",
                children,
            ),
            None,
        )

    return (
        context.ok(label, f"verified for deployment {evidence.deployment_id!r}", children),
        signer_id,
    )


def _serialize_manifests(manifests: tuple[ManifestEnvelope, ...]) -> list[dict[str, Any]]:
    """Serialize manifest envelopes for the CEL evaluation context."""
    return [
        {"resource_type": m.resource_type, "content": m.content}
        for m in manifests
    ]


def _extract_update_content(serialized_manifests: Any) -> Any:
    """Extract the spec_update payload from a serialized manifest list.

    DerivedInput uses PutManifests whose envelope list contains a single
    spec_update item.  This helper finds that item and returns its content
    so that mutation.apply_update can consume it unchanged.
    """
    if not isinstance(serialized_manifests, list):
        raise ValueError("update output must be a list of manifest envelopes")
    for envelope in serialized_manifests:
        if isinstance(envelope, dict) and envelope.get("resource_type") == "spec_update":
            return envelope.get("content")
    raise ValueError("no spec_update manifest found in update output")


def _delivery_cel_context(
    verified_input: VerifiedInput,
    action: str,
    manifests: tuple[ManifestEnvelope, ...] | None,
    manifest_sig: OutputSignature | None,
    manifest_signer_id: str | None,
    placement: PlacementEvidence | None,
    placement_signer_id: str | None,
    context: VerificationContext,
) -> dict[str, Any]:
    """Build the CEL evaluation context for delivery constraint checks."""
    output_ctx: dict[str, Any] = {}
    if manifests is not None:
        sig_ctx: dict[str, Any] | None = None
        if manifest_sig is not None:
            sig_ctx = {
                "signer_id": manifest_sig.signature.signer_id,
                "trust_anchor_id": manifest_sig.trust_anchor_id,
            }
        output_ctx = {
            "manifests": _serialize_manifests(manifests),
            "has_signature": manifest_sig is not None,
            "signer_id": manifest_signer_id,
            "signature": sig_ctx,
        }

    placement_ctx: dict[str, Any]
    if placement is not None:
        placement_ctx = {
            "deployment_id": placement.deployment_id,
            "targets": list(placement.targets),
            "has_signature": True,
            "signer_id": placement_signer_id,
            "signature": {
                "signer_id": placement.signature.signature.signer_id,
                "trust_anchor_id": placement.signature.trust_anchor_id,
            },
        }
    else:
        placement_ctx = {
            "deployment_id": None,
            "targets": [],
            "has_signature": False,
            "signer_id": None,
        }

    input_ctx = (
        verified_input.content.to_dict()
        if isinstance(verified_input.content, DeploymentContent)
        else verified_input.content
    )
    return {
        "input": input_ctx,
        "output": output_ctx,
        "target": context.target_identity,
        "action": action,
        "placement": placement_ctx,
    }


class Input(ABC):
    """Common verification contract for all input variants."""

    @abstractmethod
    def verify(
        self,
        attestation_id: str,
        context: VerificationContext,
        visited: frozenset[VerificationRef],
    ) -> tuple[VerificationResult, VerifiedInput | None]:
        raise NotImplementedError


@dataclass(frozen=True)
class SignedInput(Input):
    """A signer's direct authorization of input content and CEL constraints."""

    content: Any
    signature: Signature
    key_binding: KeyBinding
    valid_until: float
    output_constraints: tuple[OutputConstraint, ...] = ()
    expected_generation: int | None = None

    def verify(
        self,
        attestation_id: str,
        context: VerificationContext,
        visited: frozenset[VerificationRef],
    ) -> tuple[VerificationResult, VerifiedInput | None]:
        del visited
        from .policy import signed_input_envelope

        label = f"{attestation_id} input"

        if self.key_binding.signer_id != self.signature.signer_id:
            return context.fail(label, "signature signer does not match key binding"), None

        anchor = context.trust_store.get(self.key_binding.trust_anchor_id)
        if anchor is None:
            return context.fail(
                label,
                f"trust anchor not found: {self.key_binding.trust_anchor_id}",
            ), None

        anchor_result = anchor.verify_signer(
            signer_id=self.key_binding.signer_id,
            public_key=self.key_binding.public_key,
            subject=self.to_trust_anchor_subject(),
            label=f"{attestation_id} input anchor",
            context=context,
        )
        if not anchor_result.valid:
            return context.fail(label, "trust anchor rejected signer or content", [anchor_result]), None

        binding_result = self.key_binding.verify(context)
        if not binding_result.valid:
            return context.fail(label, "key binding failed", [binding_result]), None

        if self.signature.public_key != self.key_binding.public_key:
            return context.fail(label, "signature key does not match key binding"), None

        envelope = signed_input_envelope(
            self.content,
            self.valid_until,
            self.output_constraints,
            self.expected_generation,
        )
        envelope_hash = content_hash(envelope)
        if self.signature.content_hash != envelope_hash:
            return context.fail(label, "signed input hash mismatch"), None
        if not verify(
            self.signature.public_key,
            envelope_hash,
            self.signature.signature_bytes,
        ):
            return context.fail(
                label,
                f"signature verification failed for {self.signature.signer_id}",
            ), None
        if context.now() > self.valid_until:
            return context.fail(label, f"expired: {self.signature.signer_id}"), None

        detail = (
            f"signed by {self.signature.signer_id}, "
            f"verified against {self.key_binding.trust_anchor_id}, "
            f"{len(self.output_constraints)} CEL constraints"
        )
        return (
            context.ok(label, detail, [anchor_result, binding_result]),
            VerifiedInput(
                content=self.content,
                content_hash=content_hash(self.content),
                output_constraints=self.output_constraints,
                signer_id=self.signature.signer_id,
                expected_generation=self.expected_generation,
            ),
        )

    def to_trust_anchor_subject(self) -> TrustAnchorSubject:
        return TrustAnchorSubject(
            kind="input",
            signer_id=self.signature.signer_id,
            content=self.content,
            valid_until=self.valid_until,
        )


@dataclass(frozen=True)
class DerivedInput(Input):
    """Input derived from a prior input and a verified update attestation.

    deployment_id is a first-class structural property: a derived input
    *is* a new version of a specific deployment.  It is grounded in the
    signed chain via the prior's deployment_id.

    prior_input_id references an Input (SignedInput | DerivedInput) in the
    verification bundle -- only the input side of the prior state is needed,
    since updates operate on the spec, not the prior's output.

    update_attestation_id references a full Attestation in the bundle --
    the update is a deployment whose verified output carries the CEL
    expression and constraints for derivation.  The update attestation
    is verified with the deployment as the target, so the update's
    placement strategy naturally gates which deployments it applies to.
    """

    deployment_id: str
    prior_input_id: str
    update_attestation_id: str

    def verify(
        self,
        attestation_id: str,
        context: VerificationContext,
        visited: frozenset[VerificationRef],
    ) -> tuple[VerificationResult, VerifiedInput | None]:
        from .mutation import apply_update, check_preconditions, derive_constraints

        label = f"{attestation_id} input"
        children: list[VerificationResult] = []

        prior_input = context.bundle.get_input(self.prior_input_id)
        if prior_input is None:
            return context.fail(
                label,
                f"prior input not found: {self.prior_input_id}",
            ), None

        update_attestation = context.bundle.get_attestation(self.update_attestation_id)
        if update_attestation is None:
            return context.fail(
                label,
                f"update attestation not found: {self.update_attestation_id}",
            ), None

        prior_ref = context.input_ref(self.prior_input_id)
        if prior_ref in visited:
            return context.fail(label, "cycle detected in input graph"), None

        next_visited = visited | {prior_ref}

        prior_result, verified_prior = prior_input.verify(
            self.prior_input_id,
            context,
            next_visited,
        )
        children.append(prior_result)
        if not prior_result.valid or verified_prior is None:
            return context.fail(label, "prior input verification failed", children), None

        # -- grounding check: deployment_id must match the signed prior --
        prior_dep_id: str | None = (
            verified_prior.content.deployment_id
            if isinstance(verified_prior.content, DeploymentContent)
            else None
        )
        if prior_dep_id is not None and self.deployment_id != prior_dep_id:
            return context.fail(
                label,
                f"deployment_id mismatch: input declares {self.deployment_id!r}, "
                f"prior has {prior_dep_id!r}",
            ), None

        # -- verify update with the deployment as the target --
        deployment_target = {"id": self.deployment_id}
        update_context = context.with_target_identity(deployment_target)

        update_result, _, verified_update_output = update_attestation.verify(
            update_context,
            next_visited,
        )
        children.append(update_result)
        if not update_result.valid or verified_update_output is None:
            return context.fail(
                label,
                "update attestation verification failed",
                children,
            ), None

        try:
            update_content = _extract_update_content(verified_update_output.content)
            check_preconditions(verified_prior.content, update_content)
            derived_content = apply_update(
                verified_prior.content,
                update_content,
            )
            if isinstance(verified_prior.content, DeploymentContent):
                derived_content = DeploymentContent.from_dict(derived_content)
                if derived_content.deployment_id != self.deployment_id:
                    raise ValueError(
                        f"update must not rewrite deployment identity: "
                        f"expected {self.deployment_id!r}, "
                        f"got {derived_content.deployment_id!r}"
                    )
            output_constraints = derive_constraints(
                verified_prior.output_constraints,
                update_content,
            )
            resolved_generation = (
                verified_prior.expected_generation + 1
                if verified_prior.expected_generation is not None
                else None
            )
        except Exception as exc:
            return context.fail(label, f"derivation failed: {exc}", children), None

        return (
            context.ok(
                label,
                (
                    f"derived from prior={self.prior_input_id} "
                    f"+ update={self.update_attestation_id}"
                ),
                children,
            ),
            VerifiedInput(
                content=derived_content,
                content_hash=content_hash(derived_content),
                output_constraints=output_constraints,
                expected_generation=resolved_generation,
            ),
        )


@dataclass(frozen=True)
class Attestation:
    """A single attested deployment: input plus output."""

    attestation_id: str
    input: Input
    output: PutManifests | RemoveByDeploymentId

    def verify(
        self,
        context: VerificationContext,
        visited: frozenset[VerificationRef],
    ) -> tuple[VerificationResult, VerifiedInput | None, VerifiedOutput | None]:
        attestation_ref = context.attestation_ref(self.attestation_id)
        if attestation_ref in visited:
            return context.fail(
                self.attestation_id,
                "cycle detected in attestation graph",
            ), None, None

        next_visited = visited | {attestation_ref}
        input_result, verified_input = self.input.verify(
            self.attestation_id,
            context,
            next_visited,
        )
        if not input_result.valid or verified_input is None:
            return (
                context.fail(
                    self.attestation_id,
                    "input verification failed",
                    [input_result],
                ),
                None,
                None,
            )

        output_result, verified_output = self.output.verify(
            self.attestation_id,
            verified_input,
            context,
        )
        if not output_result.valid or verified_output is None:
            return (
                context.fail(
                    self.attestation_id,
                    "output verification failed",
                    [input_result, output_result],
                ),
                verified_input,
                None,
            )

        gen_result = _check_generation(
            self.attestation_id, verified_input, context,
        )
        if not gen_result.valid:
            return (
                context.fail(
                    self.attestation_id,
                    "generation check failed",
                    [input_result, output_result, gen_result],
                ),
                verified_input,
                None,
            )

        return (
            context.ok(
                self.attestation_id,
                "fully verified",
                [input_result, output_result, gen_result],
            ),
            verified_input,
            verified_output,
        )


def _check_generation(
    attestation_id: str,
    verified_input: VerifiedInput,
    context: VerificationContext,
) -> VerificationResult:
    """Check expected_generation against optional target-side state."""
    label = f"{attestation_id} generation"
    state = context.current_deployment_state

    if verified_input.expected_generation is None:
        return context.ok(label, "no expected_generation signed")

    if state is None:
        return context.ok(
            label,
            f"expected_generation={verified_input.expected_generation}, "
            f"no target state (stateless)",
        )

    content = verified_input.content
    if isinstance(content, DeploymentContent):
        dep_id: str | None = content.deployment_id
    elif isinstance(content, dict):
        dep_id = content.get("deployment_id")
    else:
        dep_id = None

    if dep_id is not None and state.deployment_id != dep_id:
        return context.fail(
            label,
            f"deployment state mismatch: state is for {state.deployment_id!r}, "
            f"attestation resolves to {dep_id!r}",
        )

    expected_current = state.generation + 1
    if verified_input.expected_generation != expected_current:
        return context.fail(
            label,
            f"generation mismatch: attestation expects {verified_input.expected_generation}, "
            f"target accepts {expected_current}",
        )

    return context.ok(
        label,
        f"generation {verified_input.expected_generation} matches "
        f"target at {state.generation}",
    )
