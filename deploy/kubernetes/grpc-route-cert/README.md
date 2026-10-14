# FleetShift gRPC Route Certificate

Scripted, repeatable workflow for attaching a trusted, unique certificate to the FleetShift `grpc` OpenShift Route while keeping the working `edge + h2c` transport model.

The resulting path is:

```text
fleetctl -- TLS + ALPN h2 --> OpenShift Route :443 -- h2c --> fleetshift-server :50051
```

## What this does

`scripts/deploy.mjs`:

1. Enables HTTP/2 on the default ingress controller if needed
2. Installs cert-manager if needed
3. Creates or updates a FleetShift-specific `ClusterIssuer`
4. Grants `openshift-ingress:router` permission to read the TLS Secret
5. Restores a backed-up TLS Secret first, unless `--fresh-cert` is requested
6. Requests or reconciles a certificate for the **existing** gRPC Route hostname
7. Patches the Route to use `spec.tls.externalCertificate`
8. Verifies ALPN `h2`, HTTP/2, and gRPC access
9. Refreshes the TLS Secret backup

`scripts/teardown.mjs`:

1. Removes the Route certificate integration
2. Backs up the Route TLS Secret to avoid unnecessary ACME reissuance on redeploy
3. Deletes the namespace-scoped certificate resources
4. Deletes the FleetShift-specific `ClusterIssuer`
5. Asks interactively whether to uninstall cert-manager
6. Leaves the ingress HTTP/2 annotation unchanged by default

## Prerequisites

- `oc` CLI installed
- Logged into the target OpenShift cluster
- FleetShift already deployed (`route/grpc` and `service/fleetshift-server` must exist)
- `appProtocol: kubernetes.io/h2c` already present on the Service `grpc` port
- `openssl` and `curl` in PATH
- `grpcurl` in PATH if you want the built-in gRPC verification step

## Cluster prerequisite

This workflow also depends on HTTP/2 being enabled on the default ingress controller. The Route certificate alone is not enough.

The deploy script will ensure this for you when possible. The underlying command is:

```bash
oc -n openshift-ingress-operator annotate ingresscontrollers/default \
  ingress.operator.openshift.io/default-enable-http2=true --overwrite
```

Why this matters:

- `appProtocol: kubernetes.io/h2c` fixes the backend Route -> pod leg
- the ingress controller HTTP/2 annotation enables the frontend/client-facing HTTP/2 capability
- both were required in the live investigation before the gRPC Route would negotiate ALPN `h2` correctly

## Important note about normal FleetShift deploys

The base Kubernetes deploy still applies the plain `route-grpc.yaml` from `deploy/kubernetes/`.

That means this workflow should be treated as a repeatable **post-deploy** step:

```bash
npx nx run k8s:deploy
npx nx run k8s:grpc-cert:deploy -- ACME_EMAIL=you@example.com
```

If a future base deploy overwrites the Route fields, just rerun the Route-cert deploy script.

## Deploy

### Direct script

```bash
node ./scripts/deploy.mjs --acme-email you@example.com
```

Optional flags:

```bash
node ./scripts/deploy.mjs \
  --acme-email you@example.com \
  --namespace fleetshift \
  --route-name grpc \
  --issuer-name fleetshift-grpc-letsencrypt-prod \
  --tls-secret-name fleetshift-grpc-route-tls
```

Force a fresh certificate request instead of restoring the saved backup:

```bash
node ./scripts/deploy.mjs --acme-email you@example.com --fresh-cert
```

The script auto-detects the current Route host from:

```bash
oc get route grpc -n fleetshift -o jsonpath='{.spec.host}'
```

For this POC, that host is expected to be:

```text
grpc-fleetshift.apps.sno-1-6c5z7.aws-acm-cluster-virt.devcluster.openshift.com
```

### Task wrapper

From the repo root:

```bash
npx nx run k8s:grpc-cert:deploy -- ACME_EMAIL=you@example.com
```

## Expected success state

After a successful deploy:

- the Route uses `spec.tls.externalCertificate`
- ALPN negotiation selects `h2`
- `curl --http2` reports HTTP/2
- `grpcurl <host>:443 list` works without `-insecure`
- `fleetctl --server <host>:443 --server-tls deployment list` works without `--server-ca-file`

## Teardown

### Direct script

```bash
node ./scripts/teardown.mjs
```

Optional cleanup flags:

```bash
node ./scripts/teardown.mjs --namespace fleetshift --route-name grpc
```

### Task wrapper

```bash
npx nx run k8s:grpc-cert:teardown
```

## Safe cleanup defaults

By default, teardown:

- backs up the current TLS Secret to `cert-manager-operator/<tls-secret-name>-backup`
- removes the Route certificate integration
- deletes the FleetShift namespace-scoped cert resources
- deletes the FleetShift-specific `ClusterIssuer`
- asks interactively before uninstalling cert-manager
- **does not** revert the ingress HTTP/2 annotation

The backup Secret allows a later redeploy to restore the previous certificate instead of immediately creating a fresh ACME order, unless you choose to uninstall cert-manager as well.
