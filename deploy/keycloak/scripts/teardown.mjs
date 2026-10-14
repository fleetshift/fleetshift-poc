#!/usr/bin/env node
import { $, question } from "zx";
import { requireOcLogin } from "../../scripts/common.mjs";

const namespace = "keycloak-prod";
const backupNamespace = "cert-manager-operator";

await requireOcLogin();

// Confirm before deleting persistent Keycloak and PostgreSQL data.
console.log(
  `This will remove Keycloak deployment from namespace '${namespace}'.`,
);
console.log("All data (including PostgreSQL) will be PERMANENTLY DELETED.");
if (!/^y$/i.test(await question("Are you sure? (y/N): "))) {
  console.log("Aborted.");
  process.exit(0);
}

// Delete dependents before operators and namespace-scoped resources.
console.log("==> Deleting Keycloak realm import...");
await $`oc delete keycloakrealmimport fleetshift-realm -n ${namespace} --ignore-not-found`;
console.log("==> Deleting Keycloak custom resource...");
await $`oc delete keycloak keycloak -n ${namespace} --ignore-not-found`;
console.log("==> Waiting for Keycloak pods to terminate...");
await $`oc wait --for=delete pod -l app=keycloak -n ${namespace} --timeout=120s`.nothrow();

console.log("==> Deleting PostgreSQL resources...");
await $`oc delete statefulset postgres -n ${namespace} --ignore-not-found`;
await $`oc delete service postgres -n ${namespace} --ignore-not-found`;
await $`oc delete pvc postgres-data-postgres-0 -n ${namespace} --ignore-not-found`;

// Preserve the certificate to avoid an unnecessary Let's Encrypt order.
const tlsSecret =
  await $`oc get secret keycloak-tls -n ${namespace} -o json`.nothrow();
if (tlsSecret.ok) {
  const secret = JSON.parse(tlsSecret.stdout);
  for (const field of [
    "namespace",
    "resourceVersion",
    "uid",
    "creationTimestamp",
    "ownerReferences",
    "managedFields",
  ])
    delete secret.metadata[field];
  delete secret.metadata.annotations?.[
    "kubectl.kubernetes.io/last-applied-configuration"
  ];
  secret.metadata.name = "keycloak-tls-backup";
  console.log(
    `==> Backing up TLS certificate to ${backupNamespace}/keycloak-tls-backup...`,
  );
  await $({
    input: JSON.stringify(secret),
  })`oc apply -n ${backupNamespace} -f -`;
  console.log("==> TLS certificate backed up.");
}

console.log("==> Deleting secrets...");
for (const [kind, name, ns] of [
  ["secret", "keycloak-db-credentials", namespace],
  ["secret", "keycloak-initial-admin", namespace],
  ["secret", "keycloak-tls", namespace],
  ["certificate", "keycloak-tls", namespace],
])
  await $`oc delete ${kind} ${name} -n ${ns} --ignore-not-found`;
const issuer = await $`oc get clusterissuer letsencrypt-prod -o json`.nothrow();
if (issuer.ok) {
  const owned =
    JSON.parse(issuer.stdout).metadata?.labels?.[
      "app.kubernetes.io/managed-by"
    ] === "fleetshift-keycloak";
  if (
    owned ||
    /^y$/i.test(
      await question(
        "ClusterIssuer letsencrypt-prod is not marked as Keycloak-owned. Delete it? (y/N): ",
      ),
    )
  ) {
    await $`oc delete clusterissuer letsencrypt-prod`;
  } else {
    console.log("==> Preserving unowned ClusterIssuer letsencrypt-prod.");
  }
}

console.log(`==> Deleting namespace ${namespace}...`);
await $`oc delete namespace ${namespace} --ignore-not-found`;
console.log("==> Waiting for namespace deletion...");
await $`oc wait --for=delete namespace/${namespace} --timeout=120s`.nothrow();

// Operators are shared cluster resources; remove only with explicit consent.
console.log(
  "\nOperators are shared cluster resources and slow to reinstall (~5 min each).",
);
console.log("Skip unless fully decommissioning Keycloak from this cluster.\n");
if (/^y$/i.test(await question("Uninstall cert-manager operator? (y/N): "))) {
  console.log("==> Removing cert-manager operator...");
  await $`oc delete subscription openshift-cert-manager-operator -n cert-manager-operator --ignore-not-found`;
  const csv = (
    await $`oc get csv -n cert-manager-operator -l operators.coreos.com/openshift-cert-manager-operator.cert-manager-operator -o jsonpath={.items[0].metadata.name}`.nothrow()
  ).stdout.trim();
  if (csv)
    await $`oc delete csv ${csv} -n cert-manager-operator --ignore-not-found`;
  await $`oc delete namespace cert-manager-operator --ignore-not-found`;
}
if (/^y$/i.test(await question("Uninstall RHBK operator? (y/N): "))) {
  console.log("==> Removing RHBK operator...");
  await $`oc delete subscription rhbk-operator -n rhbk-operator --ignore-not-found`;
  const csv = (
    await $`oc get csv -n rhbk-operator -l operators.coreos.com/rhbk-operator.rhbk-operator -o jsonpath={.items[0].metadata.name}`.nothrow()
  ).stdout.trim();
  if (csv) await $`oc delete csv ${csv} -n rhbk-operator --ignore-not-found`;
  await $`oc delete namespace rhbk-operator --ignore-not-found`;
}
console.log("\n==> Teardown complete.");
