#!/usr/bin/env node
import { $, argv, question } from "zx";
import { requireOcLogin } from "../../../../deploy/scripts/common.mjs";

const value = (flag, fallback) => {
  const index = argv.indexOf(flag);
  if (index < 0) return fallback;
  const result = argv[index + 1];
  if (!result || result.startsWith("-"))
    throw new Error(`${flag} requires a value`);
  return result;
};

if (argv.includes("--help") || argv.includes("-h")) {
  console.log(`Usage: teardown.mjs [options]

Options:
  --namespace <namespace>         FleetShift namespace (default: fleetshift)
  --route-name <name>             gRPC Route name (default: grpc)
  --issuer-name <name>            ClusterIssuer name (default: fleetshift-grpc-letsencrypt-prod)
  --tls-secret-name <name>        TLS Secret name (default: fleetshift-grpc-route-tls)
  --backup-namespace <name>       Backup Secret namespace (default: cert-manager-operator)
  -h, --help                      Show this help`);
  process.exit(0);
}

const optionsWithValues = new Set([
  "--namespace",
  "--route-name",
  "--issuer-name",
  "--tls-secret-name",
  "--backup-namespace",
]);
for (let index = 0; index < argv.length; index++) {
  const argument = argv[index];
  if (!optionsWithValues.has(argument))
    throw new Error(`Unknown argument: ${argument}`);
  index++;
}

const namespace = value("--namespace", "fleetshift");
const routeName = value("--route-name", "grpc");
const issuerName = value("--issuer-name", "fleetshift-grpc-letsencrypt-prod");
const tlsSecretName = value("--tls-secret-name", "fleetshift-grpc-route-tls");
const backupNamespace = value("--backup-namespace", "cert-manager-operator");

await requireOcLogin();

// Remove Route certificate integration before deleting its Secret.
const route = await $`oc get route ${routeName} -n ${namespace}`.nothrow();
if (route.ok) {
  console.log(
    `==> Removing certificate integration from ${namespace}/${routeName}...`,
  );
  await $`oc patch route ${routeName} -n ${namespace} --type=merge -p ${'{"spec":{"tls":{"externalCertificate":null,"certificate":null,"key":null,"caCertificate":null}}}'}`;
}

// Preserve certificate for redeploy, avoiding unnecessary ACME reissuance.
const tlsSecret =
  await $`oc get secret ${tlsSecretName} -n ${namespace} -o json`.nothrow();
const backupNamespaceExists =
  await $`oc get namespace ${backupNamespace}`.nothrow();
if (tlsSecret.ok && backupNamespaceExists.ok) {
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
  secret.metadata.name = `${tlsSecretName}-backup`;
  console.log(
    `==> Backing up TLS Secret to ${backupNamespace}/${secret.metadata.name}...`,
  );
  await $({
    input: JSON.stringify(secret),
  })`oc apply -n ${backupNamespace} -f -`;
} else if (!tlsSecret.ok) {
  console.log(
    `WARNING: TLS Secret '${tlsSecretName}' not found; skipping backup.`,
  );
} else {
  console.log(
    `WARNING: Backup namespace '${backupNamespace}' not found; skipping backup.`,
  );
}

// Delete namespace-scoped certificate resources, then FleetShift issuer.
console.log("==> Deleting certificate resources...");
for (const [kind, name] of [
  ["certificate", "fleetshift-grpc-route"],
  ["secret", tlsSecretName],
  ["role", "fleetshift-grpc-route-cert-reader"],
  ["rolebinding", "fleetshift-grpc-route-cert-reader"],
])
  await $`oc delete ${kind} ${name} -n ${namespace} --ignore-not-found=true`;
const issuer = await $`oc get clusterissuer ${issuerName} -o json`.nothrow();
if (issuer.ok) {
  const owned =
    JSON.parse(issuer.stdout).metadata?.labels?.[
      "app.kubernetes.io/managed-by"
    ] === "fleetshift-grpc-route-cert";
  if (
    owned ||
    /^y$/i.test(
      await question(
        `ClusterIssuer ${issuerName} is not marked as FleetShift-owned. Delete it? (y/N): `,
      ),
    )
  ) {
    console.log(`==> Deleting ClusterIssuer ${issuerName}...`);
    await $`oc delete clusterissuer ${issuerName}`;
  } else {
    console.log(`==> Preserving unowned ClusterIssuer ${issuerName}.`);
  }
}

// cert-manager is shared; uninstall only after explicit confirmation.
console.log(
  "\nOperators are shared cluster resources and slow to reinstall (~5 min each).",
);
console.log(
  `Skip unless fully decommissioning Route certs from this cluster.\n`,
);
if (/^y$/i.test(await question("Uninstall cert-manager operator? (y/N): "))) {
  console.log(
    `WARNING: uninstalling cert-manager also removes backup Secret in '${backupNamespace}'.`,
  );
  console.log("==> Removing cert-manager operator...");
  await $`oc delete subscription openshift-cert-manager-operator -n cert-manager-operator --ignore-not-found=true`;
  const csv = (
    await $`oc get csv -n cert-manager-operator -l operators.coreos.com/openshift-cert-manager-operator.cert-manager-operator -o jsonpath={.items[0].metadata.name}`.nothrow()
  ).stdout.trim();
  if (csv)
    await $`oc delete csv ${csv} -n cert-manager-operator --ignore-not-found=true`;
  await $`oc delete namespace cert-manager --ignore-not-found=true`;
  await $`oc delete namespace cert-manager-operator --ignore-not-found=true`;
} else {
  console.log("==> Leaving cert-manager installed.");
}
console.log("\n=== gRPC Route Certificate Teardown Complete ===");
console.log(`  Route integration removed from ${namespace}/${routeName}`);
console.log("  Namespace-scoped certificate resources deleted");
console.log(
  `  Backup Secret stored at ${backupNamespace}/${tlsSecretName}-backup unless cert-manager was uninstalled`,
);
console.log("  Ingress HTTP/2 annotation left unchanged");
