# Rules for Agents

For how to...
- generate and lint gRPC / proto, see docs/buf.md
- design APIs, see docs/api-design.md
- understand the domain, API, and architectural context, see docs/design/architecture.md
- understand the authentication model, see docs/design/authentication.md and poc/attestation/hybrid/README.md

Rules:
- Never remove TODO comments unless it is truly no longer relevant (e.g. implemented or obsolete)
- Always follow project specific AGENTS.md where present for THAT project (e.g. fleetshift-server/AGENTS.md)
- Prefer modern stdlib abstractions and utilities where relevant (especially around crypto or low level encoding / decoding)
- Follow test-driven development. When at all possible, write failing tests **first**, then write the code to make the test pass.