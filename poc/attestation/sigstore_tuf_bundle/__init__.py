"""Offline Sigstore + TUF delivery-attestation proof of concept.

This package is intentionally independent of ``sigstore_tuf``.  It reuses the
FleetShift-specific content and CEL policy vocabulary from ``hybrid`` while
replacing its signing and trust-distribution mechanisms.
"""

from .build import PoCEnvironment
from .model import DeliveryPackage
from .sigstore import SigstoreBundle
from .verify import DeliveryAgent, VerificationError, VerificationResult

__all__ = [
    "DeliveryAgent",
    "DeliveryPackage",
    "PoCEnvironment",
    "SigstoreBundle",
    "VerificationError",
    "VerificationResult",
]
