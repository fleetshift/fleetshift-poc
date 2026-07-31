# POC

This folder represents standalone proofs of concepts for concepts explored as part of the fleetshift vision. These are primarily intended for learning purposes. They may or may not be integrated into the main prototype. As such, they can be implemented in any language.

Current POCs include:

- `attestation/`: attestation chain and hybrid verification experiments
- `attestation/sigstore_tuf/`: Sigstore (Fulcio+TSA+DSSE/in-toto) + TUF trust
  distribution POC with offline, preassembled delivery bundles (Mode A)
- `trust-model-v3/`: three-role OIDC enrollment, client-held continuity keys,
  signed delivery, rotation cutoff, and compromised-resource-manager POC
- `inventory-identity-reconciliation/`: inventory write-path experiment where
  reported identity assertions are accepted asynchronously into a platform
    identity read model
- `ocm-work-agent-adapter/`: a standalone Go prototype showing how FleetShift-style delivery could be materialized as OCM `ManifestWork` and consumed through a simplified spoke-side reconcile loop
- `fleetshift-controller-runtime/`: adapt sig-multicluster controller-runtime so reconcilers run against FleetShift delivery targets (via a custom `cluster.Cluster` + Provider) instead of kube-apiserver, reporting through the delivery contract
