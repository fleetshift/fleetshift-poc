"""
Hybrid attestation prototype.

Combines:
  - a single Attestation(input, output) model
  - explicit, signed output constraints on inputs
  - ID-based provenance graph traversal
  - data-driven derivation from signed update outputs
  - explainable verification results
"""

from .build import make_key_binding, make_output, make_signed_input, sign_output
from .crypto import KeyPair, generate_keypair
from .model import (
    AddonSignedConstraint,
    AllowedGVKsConstraint,
    Attestation,
    DerivedInput,
    KeyBinding,
    NamespaceConstraint,
    NoClusterAdminConstraint,
    Output,
    OutputConstraint,
    OutputSignature,
    SignedInput,
    Signature,
    TrustAnchor,
    VerifiedOutput,
)
from .policy import derive_output_constraints
from .verify import (
    AttestationStore,
    TrustStore,
    VerificationError,
    VerificationResult,
    explain_verification,
    verify_attestation,
)

__all__ = [
    "AddonSignedConstraint",
    "AllowedGVKsConstraint",
    "Attestation",
    "AttestationStore",
    "DerivedInput",
    "KeyBinding",
    "KeyPair",
    "NamespaceConstraint",
    "NoClusterAdminConstraint",
    "Output",
    "OutputConstraint",
    "OutputSignature",
    "Signature",
    "SignedInput",
    "TrustAnchor",
    "TrustStore",
    "VerificationError",
    "VerificationResult",
    "VerifiedOutput",
    "derive_output_constraints",
    "explain_verification",
    "generate_keypair",
    "make_key_binding",
    "make_output",
    "make_signed_input",
    "sign_output",
    "verify_attestation",
]
