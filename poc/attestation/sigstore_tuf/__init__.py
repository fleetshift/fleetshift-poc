"""Sigstore + TUF attestation POC (Mode A: Fulcio + TSA, no transparency log).

See README.md for architecture and how this relates to ``hybrid/``.
"""

from .verify import verify_delivery

__all__ = ["verify_delivery"]
