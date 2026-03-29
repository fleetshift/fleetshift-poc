"""Core data model for the hybrid attestation prototype."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .cel_runtime import CelEvaluationError, evaluate_bool
from .crypto import content_hash, verify

if TYPE_CHECKING:
    from .verify import VerificationContext, VerificationResult


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
        subject: dict[str, Any],
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
                    "subject": subject,
                },
            )
        except CelEvaluationError as exc:
            return context.fail(label, f"trust anchor constraint evaluation failed: {exc}")

        if not valid:
            return context.fail(label, f"predicate returned false: {self.name}")

        return context.ok(label, f"trust anchor constraint matched: {self.name}")


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

    def verify(
        self,
        attestation_id: str,
        verified_input: VerifiedInput,
        output: Output,
        verified_output: VerifiedOutput,
        context: VerificationContext,
    ) -> VerificationResult:
        label = f"{attestation_id} constraint"
        try:
            valid = evaluate_bool(
                self.expression,
                {
                    "input": verified_input.content,
                    "output": output.to_context(verified_output),
                },
            )
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


@dataclass(frozen=True)
class VerifiedOutput:
    content: Any
    content_hash: bytes
    signer_id: str | None = None


@dataclass(frozen=True)
class Output:
    """Produced content, optionally signed."""

    content: Any
    signature: OutputSignature | None = None

    def verify(
        self,
        attestation_id: str,
        verified_input: VerifiedInput,
        context: VerificationContext,
    ) -> tuple[VerificationResult, VerifiedOutput | None]:
        label = f"{attestation_id} output"
        children: list[VerificationResult] = []

        signature_result, verified_output = self.verify_signature(attestation_id, context)
        children.append(signature_result)
        if not signature_result.valid or verified_output is None:
            return context.fail(label, "output signature invalid", children), None

        if not verified_input.output_constraints:
            children.append(
                context.ok(
                    f"{attestation_id} output constraints",
                    "no output constraints",
                )
            )
            return (
                context.ok(label, "satisfies all constraints", children),
                verified_output,
            )

        for constraint in verified_input.output_constraints:
            constraint_result = constraint.verify(
                attestation_id,
                verified_input,
                self,
                verified_output,
                context,
            )
            children.append(constraint_result)
            if not constraint_result.valid:
                return (
                    context.fail(
                        label,
                        f"constraint failed: {constraint.name}",
                        children,
                    ),
                    None,
                )

        return context.ok(label, "satisfies all constraints", children), verified_output

    def verify_signature(
        self,
        attestation_id: str,
        context: VerificationContext,
    ) -> tuple[VerificationResult, VerifiedOutput | None]:
        label = f"{attestation_id} output signature"
        output_hash = content_hash(self.content)

        if self.signature is None:
            return (
                context.ok(label, "unsigned output"),
                VerifiedOutput(content=self.content, content_hash=output_hash),
            )

        signature = self.signature.signature
        if signature.content_hash != output_hash:
            return context.fail(label, "output hash mismatch"), None
        if not verify(signature.public_key, output_hash, signature.signature_bytes):
            return context.fail(
                label,
                f"output signature verification failed for {signature.signer_id}",
            ), None

        anchor = context.trust_store.get(self.signature.trust_anchor_id)
        if anchor is None:
            return context.fail(
                label,
                f"trust anchor not found: {self.signature.trust_anchor_id}",
            ), None

        anchor_result = anchor.verify_signer(
            signer_id=signature.signer_id,
            public_key=signature.public_key,
            subject=self.to_trust_anchor_subject(attestation_id, signature.signer_id),
            label=f"{attestation_id} output anchor",
            context=context,
        )
        if not anchor_result.valid:
            return context.fail(label, "output trust anchor rejected signer", [anchor_result]), None

        return (
            context.ok(
                label,
                (
                    f"signed by {signature.signer_id}, "
                    f"verified against {self.signature.trust_anchor_id}"
                ),
                [anchor_result],
            ),
            VerifiedOutput(
                content=self.content,
                content_hash=output_hash,
                signer_id=signature.signer_id,
            ),
        )

    def to_context(self, verified_output: VerifiedOutput) -> dict[str, Any]:
        signature_context: dict[str, Any] | None = None
        if self.signature is not None:
            signature_context = {
                "signer_id": self.signature.signature.signer_id,
                "trust_anchor_id": self.signature.trust_anchor_id,
            }

        return {
            "content": verified_output.content,
            "content_hash": verified_output.content_hash.hex(),
            "has_signature": self.signature is not None,
            "signature": signature_context,
            "signer_id": verified_output.signer_id,
        }

    def to_trust_anchor_subject(
        self,
        attestation_id: str,
        signer_id: str | None,
    ) -> dict[str, Any]:
        return {
            "attestation_id": attestation_id,
            "content": self.content,
            "kind": "output",
            "signer_id": signer_id,
        }


class Input(ABC):
    """Common verification contract for all input variants."""

    @abstractmethod
    def verify(
        self,
        attestation_id: str,
        context: VerificationContext,
        visited: frozenset[str],
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

    def verify(
        self,
        attestation_id: str,
        context: VerificationContext,
        visited: frozenset[str],
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
            subject=self.to_trust_anchor_subject(attestation_id),
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
            ),
        )

    def to_trust_anchor_subject(self, attestation_id: str) -> dict[str, Any]:
        return {
            "attestation_id": attestation_id,
            "content": self.content,
            "kind": "input",
            "signer_id": self.signature.signer_id,
            "valid_until": self.valid_until,
        }


@dataclass(frozen=True)
class DerivedInput(Input):
    """Input derived from a prior attestation and a verified update.

    The referenced update attestation's signed output contains the CEL
    expression used to derive the next input.
    """

    prior_attestation_id: str
    update_attestation_id: str

    def verify(
        self,
        attestation_id: str,
        context: VerificationContext,
        visited: frozenset[str],
    ) -> tuple[VerificationResult, VerifiedInput | None]:
        from .mutation import apply_update, derive_constraints

        label = f"{attestation_id} input"
        children: list[VerificationResult] = []

        prior_attestation = context.attestation_store.get(self.prior_attestation_id)
        if prior_attestation is None:
            return context.fail(
                label,
                f"prior attestation not found: {self.prior_attestation_id}",
            ), None

        update_attestation = context.attestation_store.get(self.update_attestation_id)
        if update_attestation is None:
            return context.fail(
                label,
                f"update attestation not found: {self.update_attestation_id}",
            ), None

        prior_result, verified_prior = prior_attestation.verify_input_only(
            context,
            visited,
        )
        children.append(prior_result)
        if not prior_result.valid or verified_prior is None:
            return context.fail(label, "prior input verification failed", children), None

        update_result, _, verified_update_output = update_attestation.verify(
            context,
            visited,
        )
        children.append(update_result)
        if not update_result.valid or verified_update_output is None:
            return context.fail(
                label,
                "update attestation verification failed",
                children,
            ), None

        try:
            derived_content = apply_update(
                verified_prior.content,
                verified_update_output.content,
            )
            output_constraints = derive_constraints(
                verified_update_output.content,
                derived_content,
            )
        except Exception as exc:
            return context.fail(label, f"derivation failed: {exc}", children), None

        return (
            context.ok(
                label,
                (
                    f"derived from prior={self.prior_attestation_id} "
                    f"+ update={self.update_attestation_id}"
                ),
                children,
            ),
            VerifiedInput(
                content=derived_content,
                content_hash=content_hash(derived_content),
                output_constraints=output_constraints,
            ),
        )


@dataclass(frozen=True)
class Attestation:
    """A single attested deployment: input plus output."""

    attestation_id: str
    input: Input
    output: Output

    def verify(
        self,
        context: VerificationContext,
        visited: frozenset[str],
    ) -> tuple[VerificationResult, VerifiedInput | None, VerifiedOutput | None]:
        if self.attestation_id in visited:
            return context.fail(
                self.attestation_id,
                "cycle detected in attestation graph",
            ), None, None

        next_visited = visited | {self.attestation_id}
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

        return (
            context.ok(
                self.attestation_id,
                "fully verified",
                [input_result, output_result],
            ),
            verified_input,
            verified_output,
        )

    def verify_input_only(
        self,
        context: VerificationContext,
        visited: frozenset[str],
    ) -> tuple[VerificationResult, VerifiedInput | None]:
        if self.attestation_id in visited:
            return context.fail(
                f"{self.attestation_id} input",
                "cycle detected in attestation graph",
            ), None

        next_visited = visited | {self.attestation_id}
        return self.input.verify(self.attestation_id, context, next_visited)
