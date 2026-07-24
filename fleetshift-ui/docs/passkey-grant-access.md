# Passkey & Grant Access Flow

This document describes an exploratory passkey-based identity verification flow. It covers what the approach can do and how it works at a high level.

## Overview

Instead of passwords, users can verify their identity using their device's built-in security (fingerprint, face scan, screen lock) via the Web Authentication API (WebAuthn). The browser handles all the biometric prompts natively — no custom UI is needed for the verification step itself.

This approach could be used to authorize FleetShift to perform actions on a user's behalf, with cryptographic proof that the user actually approved it.

## How It Works

### Registration (One-Time Setup)

1. The frontend requests a registration challenge from the server (`GET /passkey/challenge`). The server generates a random 32-byte challenge and returns it with relying party info, user details, and supported algorithms (ECDSA P-256 / RSA).
2. The frontend converts the server's JSON response into the native WebAuthn format (base64url → ArrayBuffer) and calls `navigator.credentials.create()`. The browser takes over with its native biometric prompt.
3. On success, the browser returns a `PublicKeyCredential` containing a newly generated public key and a credential ID. The private key is created and stored on the device — it's never exposed to the frontend or server.
4. The frontend extracts the public key from the attestation response, converts it from DER to PEM format, and sends it along with the credential ID to the server (`POST /passkey`).
5. The server stores the PEM public key and credential ID in the user's database record. A WebSocket event (`passkey-registered`) notifies the originating browser tab so the UI updates without polling.

### Verification (Grant Access)

1. The frontend requests an authentication challenge (`GET /passkey/auth-challenge`). The server generates a fresh random challenge and returns it with the user's stored credential ID.
2. The frontend calls `navigator.credentials.get()` with the challenge and allowed credential. The browser prompts for biometric verification again.
3. On success, the browser signs the challenge using the device-stored private key and returns an assertion containing `authenticatorData`, `clientDataJSON`, and the `signature`.
4. The frontend sends the full assertion to the server (`POST /passkey/verify-auth`).
5. The server verifies the assertion:
   - Decodes `clientDataJSON` and checks the challenge matches the one it issued (replay protection).
   - Looks up the stored public key by credential ID and username.
   - Reconstructs the signed payload (`authenticatorData` || SHA-256(`clientDataJSON`)).
   - Verifies the signature against the stored public key.
6. If the signature is valid, access is granted. The challenge is consumed so it can't be reused.

### Removal

Users can remove their passkey at any time (`DELETE /passkey`), which clears the stored public key and credential ID and returns them to the registration state. The server notifies via WebSocket so the UI reflects the change immediately.

## UI States

The Grant Access page walks the user through the flow with four states, each in its own component:

| State | Component | What the user sees |
|-------|-----------|-------------------|
| Loading | (spinner) | Checking passkey status |
| No passkey | `SetupPasskey` | CTA to set up a passkey |
| Awaiting grant | `AuthorizeAccess` | Permissions list + "Grant Access" button |
| Granted | `AccessGranted` | Confirmation with granted permissions |

## Key Files

| File | Purpose |
|------|---------|
| `packages/gui/src/utils/webauthn.ts` | Buffer encoding utilities, WebAuthn options parsing |
| `packages/gui/src/api/passkey.ts` | Passkey API calls (create, grant, delete, status check) |
| `packages/gui/src/hooks/usePasskey.ts` | React hook for passkey state + WebSocket updates |
| `packages/gui/src/pages/GrantAccessPage/` | UI components per state |
| `packages/mock-servers/src/routes/passkey.ts` | Server routes — challenge generation, signature verification |

## Concepts and Standards

- **WebAuthn (Web Authentication API)** — a W3C standard that lets websites create and use public-key credentials via the browser's `navigator.credentials` API. The browser handles all biometric/PIN prompts natively. See [webauthn.io](https://webauthn.io) for an interactive overview.
- **FIDO2 / Passkeys** — the broader ecosystem around WebAuthn. "Passkey" is the user-facing term for a FIDO2 credential that can sync across devices (e.g. via iCloud Keychain or Google Password Manager) or stay bound to a single hardware key.
- **Public-key cryptography** — each registration creates an asymmetric key pair. The private key stays on the device; the server only stores the public key. Verification works by signing a challenge with the private key and checking the signature with the public key.
- **Challenge-response protocol** — the server issues a random single-use challenge for each operation. The client signs it and returns the signature. This prevents replay attacks — a captured signature is useless because the challenge won't match next time.
- **Relying Party (RP)** — WebAuthn term for the server/application requesting authentication. The RP ID (typically the domain) is bound into the credential so it can't be phished across domains.
- **Attestation vs. Assertion** — registration produces an *attestation* (proof that a credential was created). Verification produces an *assertion* (proof the user possesses the credential). The server uses `attestation: "none"` since we don't need to verify the authenticator's manufacturer.
- **DER / PEM encoding** — the public key comes from the browser in DER (binary) format. It's converted to PEM (base64 with header/footer lines) for storage and use with Node.js `crypto.createVerify()`.
- **Base64URL** — a URL-safe variant of base64 (uses `-_` instead of `+/`, no padding). WebAuthn uses this encoding for all binary data in JSON payloads.

## Worth Noting

- Challenges are single-use random values (replay protection).
- The server performs real cryptographic signature verification, not a stub.
- WebSocket events notify the UI when passkey state changes so it updates without polling.
