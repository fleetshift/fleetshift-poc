# Retired UI Prototype: WebAuthn Grant Access

**Status: retired prototype reference.** The former passkey registration and Grant Access pages and mock-server endpoints are not part of the current console. This page preserves an existing inbound reference; it does not define a passkey implementation or delegation contract.

The current console provides [signing-key enrollment](../../../extensions/core/client/src/plugins/signing-plugin/SigningKeyEnrollment.tsx). Enrollment and signature verification must not be equated with blanket permission to act on a user's behalf.

For current security boundaries and design, see:

- [Authentication and credential presentation](../../design/authentication.md)
- [Delivery security model](../../design/security.md)
- [Signing UI trust boundary](../../design/addon-ui-architecture.md#security-sensitive-core-surfaces)

This retired prototype makes no commitment to WebAuthn adoption or a replacement step-up flow.
