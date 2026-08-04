# SPIKE: WebAuthn for Delegated Actions

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (Web UI)
**Status**: Done
**Related**: FM-22 (Security Model), FM-37 (End-to-End Identity for Control Plane Delivery)

## Problem

FleetShift is a management layer that connects to clusters it does not own. It also does not own the identity providers (IdPs) that users use to authenticate against those clusters. When a user wants FleetShift to do something on their behalf — create a cluster, scale a deployment, change configuration — the platform needs a way to confirm that the user is actually authorizing the action.

The regular login (OIDC via Keycloak or similar) proves who you are to FleetShift. But that is not enough when FleetShift needs to act on your behalf against external clusters with their own identity systems. There needs to be a separate, stronger signal that says "I, the human(agent), am authorizing this specific action right now."

## What was explored

### WebAuthn / Passkeys as step-up authentication

WebAuthn is a W3C standard that lets users authenticate using device-bound credentials — biometrics (fingerprint, face), security keys, or platform authenticators. Unlike passwords or tokens, passkeys cannot be phished, replayed, or stolen from a server because the private key never leaves the user's device.

The idea is to use WebAuthn as a step-up authentication layer on top of the regular login. The user is already logged in. When they want to authorize a sensitive action, they verify their identity with a passkey. This gives FleetShift a cryptographic proof that the person behind the browser actually approved the action — independent of any external IdP.

### How it works

**Registration (one-time setup):**
- Before the user can authorize the system to act on their behalf, they need to generate and store a public key. This is a one-time step (though the key can be re-generated if needed). The key generation happens in the background — the browser and the authenticator (biometric sensor, security key, etc.) handle everything. The user just confirms with a fingerprint or PIN, they do not need to manually create or manage any keys.

**Authorization (per action or per session):**
- When the user wants to authorize delegated actions, they are prompted for their passkey.
- The browser signs a challenge with the private key. The server verifies the signature against the stored public key.
- If it checks out, FleetShift knows the actual user is present and approving.

### Why this matters for FleetShift

- **FleetShift does not control the IdP.** The clusters being managed may use their own IdPs (corporate SSO, cloud provider IAM, etc.). FleetShift cannot ask those IdPs to verify the user for delegation purposes.
- **Tokens can be stolen or replayed.** A bearer token sitting in a session can be used by anything that has access to it. A passkey requires the actual user to be physically present.
- **It is phishing-resistant.** The credential is bound to the origin (domain). Even if someone tricks the user into visiting a fake site, the passkey will not work there.
- **It works across IdP boundaries.** No matter what IdP the target cluster uses, the passkey verification happens between the user and FleetShift directly.

### UI implementation details

A key part of the spike was figuring out how WebAuthn works on the frontend. The Web Authentication API deals in binary data (ArrayBuffers) but the server communicates in JSON. The main challenge is the transformation between JSON-serialized options (base64url-encoded strings) and the actual binary data the browser API expects.

The POC implements this transformation from scratch — parsing challenge and credential IDs from base64url to ArrayBuffer, converting DER-encoded public keys to PEM, encoding assertion responses back to base64url for the server. It works, but for a real implementation we would want a library like SimpleWebAuthn that handles all of this reliably and stays current with spec changes.

### Current state (proof of concept)

A working POC was built to validate the flow end to end:

- Registration and verification flows work using the Web Authentication API.
- The server stores public keys and verifies signatures using standard crypto (ES256/RS256).
- Challenge-response prevents replay attacks.
- The UI walks the user through setup, authorization, and confirmation.

Limitations of the POC:
- The JSON-to-binary transformations are hand-rolled. A production implementation should use a maintained library like SimpleWebAuthn.
- The permissions shown after authorization are display-only. Nothing on the backend enforces them yet.
- There is no per-action authorization — it is a one-time "grant access" flow, not tied to specific operations.
- The delegation model (how FleetShift actually acts on behalf of the user against a cluster) is not wired up. That is part of the broader security model work (FM-22, FM-37).

### Open questions

- How does the passkey authorization connect to the actual delegation mechanism? When FleetShift acts against a cluster, what credential does it present? This is being explored in the security model spikes.
- Should authorization be per-session or per-action? Per-action is more secure but adds friction.
- How do we handle multiple devices? A user may want passkeys on their laptop and phone.
- What happens if the user loses their device? Recovery flow needs design.

## Key files

- Passkey API client: `packages/gui/src/api/passkey.ts`
- Passkey lifecycle hook: `packages/gui/src/hooks/usePasskey.ts`
- WebAuthn utilities: `packages/gui/src/utils/webauthn.ts`
- Grant Access UI: `packages/gui/src/pages/GrantAccessPage/`
- Server-side passkey routes: `packages/mock-servers/src/routes/passkey.ts`
