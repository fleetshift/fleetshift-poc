# SPIKE: CLI Authentication and Delegated Actions

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (CLI)
**Status**: Done
**Related**: [004 - WebAuthn for Delegated Actions](004-webauthn-delegation.md)

## Problem

The CLI needs to authenticate the user against the same backend as the GUI. It also needs a way to authorize delegated actions — when the user asks FleetShift to act on their behalf against a cluster. In the GUI, this is handled with WebAuthn/passkeys (spike 004), but WebAuthn is a browser API. It requires a browser and a platform authenticator (biometric sensor, security key via the Web Authentication API). None of that is available in a terminal.

The CLI needs both regular authentication (who are you) and step-up authentication (prove you are authorizing this specific action), using mechanisms that work in a Node.js environment.

**Note:** Authentication and security are being explored in other spikes and by other teams (FM-22, FM-37). This spike focuses specifically on what was needed to get a working CLI with auth, and what alternatives exist for delegated actions in a terminal environment where WebAuthn is not available. The final authentication design will depend on decisions made across those efforts.

## What was explored

### Regular authentication — OIDC with PKCE (POC)

The POC implements one possible approach for CLI authentication. The actual auth mechanism will depend on decisions made by the auth team — the IdP, the flow, and the token format may all change. What follows is what was built to have a working auth flow to develop against.

The CLI uses the same identity provider as the GUI (Keycloak) but with a flow designed for CLI tools. Since the CLI cannot render a login page, it opens the user's default browser to the Keycloak login URL. The user authenticates in the browser (with whatever method the IdP supports — password, passkey, SSO, etc.) and Keycloak redirects back to a temporary local HTTP server that the CLI starts on a known port. The CLI captures the authorization code from the redirect and exchanges it for tokens.

The flow uses PKCE (Proof Key for Code Exchange) to protect the authorization code exchange. The CLI generates a random code verifier and its SHA-256 hash (the code challenge) before starting the flow. The verifier is sent during the token exchange, proving that the same client that initiated the flow is the one completing it.

Tokens are stored in the operating system's keyring — macOS Keychain, Windows Credential Store, or Linux Secret Service. This is more secure than writing tokens to a file on disk because the OS manages access control and encryption. The CLI reads tokens from the keyring on startup and refreshes them automatically when they are close to expiry.

A fetch interceptor injects the Bearer token into all API requests transparently. Commands do not need to handle authentication — they just make API calls and the token is added automatically.

### Delegated actions — alternatives to WebAuthn

WebAuthn cannot work in a CLI. The Web Authentication API is only available in browsers, and platform authenticators (Touch ID, Windows Hello) expose their APIs through OS frameworks that are designed for GUI applications, not terminal processes. There is no standard way to access a passkey from a CLI.

Several alternatives were explored:

**OAuth Device Authorization Grant.** This is the industry standard for authenticating on devices that do not have a browser or have limited input. The CLI requests a device code from the authorization server and displays a short user code along with a URL. The user opens the URL in any browser (on their phone, another computer, etc.), enters the code, and authenticates. The CLI polls the token endpoint until the authentication is complete. This delegates the hard part (passkeys, MFA, biometrics) to the browser where these mechanisms actually work. The downside is that it requires the user to switch to a browser for every step-up action.

**SSH key signing.** The CLI signs a server-provided challenge with the user's SSH private key. The server verifies the signature against a registered public key. This is a well-established pattern — SSH keys are ubiquitous in developer and operations workflows. Node.js can interact with SSH keys either directly via the crypto module or through the ssh-agent. The user does not need to set up anything new if they already have SSH keys. This provides strong cryptographic proof that the person with the private key is authorizing the action.

**Local key pair generation.** The CLI generates its own asymmetric key pair, stores the private key securely (in the OS keyring or encrypted on disk), and registers the public key with the server. When a delegated action is needed, the CLI signs a challenge with the private key. This follows the same cryptographic model as WebAuthn — registration stores a public key, authentication signs a challenge with the private key. The difference is that WebAuthn locks the private key inside a hardware authenticator (Secure Enclave, TPM) and requires user presence verification (fingerprint, PIN) to sign anything. A local key pair lives in software and lacks those hardware-bound guarantees. However, the server-side verification is essentially the same — challenge-response with a registered public key. If we chose this approach for the CLI, the backend could share the same verification logic used for WebAuthn in the GUI, reducing complexity. The tradeoff is weaker security guarantees around key protection and user presence.

**FIDO2 direct access via USB HID.** It is technically possible to communicate with a FIDO2 security key (like a YubiKey) directly from Node.js without going through a browser. This uses the CTAP2 protocol over USB HID. However, the ecosystem is very limited — there is only one pure Node.js CTAP2 client and it is not actively maintained. A more reliable option would be wrapping Yubico's C library (libfido2) or calling its CLI tools as subprocesses. This is the most secure option (hardware-bound keys, user presence verification via physical touch) but the most complex to implement and maintain.

**PKCS#11 and smart cards.** For enterprise environments that already use smart cards or hardware security modules, Node.js has mature PKCS#11 bindings. The CLI loads the PKCS#11 module, opens a session with the user's smart card, prompts for a PIN, and uses the on-device private key to sign a challenge. This is well-suited for environments with existing PKI infrastructure but too heavy for general use.

### Current state (proof of concept)

The regular OIDC authentication works end to end:

- Browser-based login with PKCE code exchange.
- Tokens stored securely in the OS keyring.
- Automatic token refresh.
- Transparent token injection on all API requests.
- Auth-exempt commands (login, logout, whoami) work without a token.

The delegation mechanism is not implemented in the CLI. The GUI uses WebAuthn, but no CLI equivalent has been wired up yet.

### Open questions

- Which delegation mechanism should we use? The OAuth Device Authorization Grant is the most portable but adds friction (browser switch). SSH key signing is the most natural for CLI users but requires key management.
- Can we support multiple delegation mechanisms and let the user choose?
- Should the CLI support the same per-action vs per-session authorization model discussed in the WebAuthn spike (004)?
- How do we handle the case where the user has both the GUI and CLI open — should delegated access granted in one carry over to the other?

## Key files

- OIDC login flow: `packages/cli/src/auth/login.ts`
- PKCE helpers: `packages/cli/src/auth/pkce.ts`
- Token storage (OS keyring): `packages/cli/src/auth/tokenStore.ts`
- Fetch interceptor: `packages/cli/src/auth/fetchInterceptor.ts`
- Login/logout/whoami commands: `packages/cli/src/commands/login.tsx`
