# fleetshift-poc

This repository represents both a **prototype** for a next generation k8s/OpenShift cluster management vision, alongside **individual POCs** for exploration of isolated concepts.

## Full-stack local development

See [docs/fullstack-setup.md](docs/fullstack-setup.md) for the unified Docker Compose setup that runs the entire stack (Keycloak, FleetShift server, mock servers, plugins, GUI) with one command.

## Security considerations

### OCP callback token

The OCP delivery agent passes a short-lived callback JWT to the ocp-engine subprocess via the `OCP_CALLBACK_TOKEN` environment variable. On Linux, environment variables of a running process are readable via `/proc/{pid}/environ` by any process running as the same OS user. This is mitigated by:

- The token is an ephemeral ED25519-signed JWT scoped to a single cluster ID
- The token has a 2-hour expiry matching the STS session duration
- The signing key is generated in-memory at server startup and never persisted
- The callback endpoint validates both the token signature and cluster ID match
