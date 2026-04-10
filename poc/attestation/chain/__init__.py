"""
Provenance chain attestation prototype.

Models a directed graph of attestations where each node has an input
(what was authorized) and an output (what was produced). The input
determines how the output must be verified. Attestations reference
other attestations, forming a graph that is walked recursively
during verification.
"""
