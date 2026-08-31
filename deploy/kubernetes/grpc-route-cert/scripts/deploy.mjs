#!/usr/bin/env node
import { $, argv, sleep } from "zx";
import { resolve } from "node:path";
import { requireOcLogin } from "../../../../deploy/scripts/common.mjs";

const value = (flag, fallback) => {
  const index = argv.indexOf(flag);
  return index < 0 ? fallback : argv[index + 1];
};
if (argv.includes("--help") || argv.includes("-h")) {
  console.log(
    "Usage: deploy.mjs --acme-email EMAIL [--namespace NS] [--route-name NAME] [--issuer-name NAME] [--tls-secret-name NAME] [--route-host HOST] [--fresh-cert]",
  );
  process.exit(0);
}
const namespace = value("--namespace", "fleetshift");
const routeName = value("--route-name", "grpc");
const issuerName = value("--issuer-name", "fleetshift-grpc-letsencrypt-prod");
const secretName = value("--tls-secret-name", "fleetshift-grpc-route-tls");
const backupNamespace = "cert-manager-operator";
const email = value("--acme-email", process.env.ACME_EMAIL);
const fresh = argv.includes("--fresh-cert");
const routeHost = value("--route-host", "");
if (!email) throw new Error("--acme-email is required");
const fail = (message) => {
  throw new Error(message);
};
const waitFor = async (predicate, seconds) => {
  for (let elapsed = 0; elapsed < seconds; elapsed += 5) {
    if (await predicate()) return true;
    await sleep(5000);
  }
  return false;
};
const dir = resolve(import.meta.dirname, "..");
await requireOcLogin();
for (const command of ["oc", "openssl", "curl", "jq"])
  await $`command -v ${command}`;
const route =
  await $`oc get route ${routeName} -n ${namespace} -o jsonpath={.spec.host}`.nothrow();
const host = route.stdout.trim();
if (!host)
  fail(
    `Could not determine host for route '${routeName}' in namespace '${namespace}'.`,
  );
if (routeHost && routeHost !== host)
  fail(`--route-host (${routeHost}) does not match live Route host (${host}).`);
await $`oc get svc fleetshift-server -n ${namespace}`;
const h2c = (
  await $`oc get svc fleetshift-server -n ${namespace} -o jsonpath={.spec.ports[?(@.name=="grpc")].appProtocol}`.nothrow()
).stdout.trim();
if (h2c !== "kubernetes.io/h2c")
  fail(
    "Service fleetshift-server grpc port must have appProtocol=kubernetes.io/h2c.",
  );
const ingress =
  await $`oc get ingresscontrollers.operator.openshift.io/default -n openshift-ingress-operator -o jsonpath={.metadata.annotations.ingress\.operator\.openshift\.io/default-enable-http2}`.nothrow();
if (ingress.stdout.trim() !== "true")
  await $`oc -n openshift-ingress-operator annotate ingresscontrollers/default ingress.operator.openshift.io/default-enable-http2=true --overwrite`;
const render = async (file) => {
  const source = resolve(dir, "manifests", file);
  const substitutions = {
    __NAMESPACE__: namespace,
    __ISSUER_NAME__: issuerName,
    __ACME_EMAIL__: email,
    __GRPC_ROUTE_HOST__: host,
    __TLS_SECRET_NAME__: secretName,
  };
  let text = await (await import("node:fs/promises")).readFile(source, "utf8");
  for (const [key, replacement] of Object.entries(substitutions))
    text = text.replaceAll(key, replacement);
  return text;
};
const apply = async (file) =>
  await $({ input: await render(file) })`oc apply -f -`;
const subscription =
  await $`oc get subscription openshift-cert-manager-operator -n cert-manager-operator`.nothrow();
if (!subscription.ok) {
  await $`oc auth can-i create namespaces`;
  await $`oc auth can-i create subscriptions.operators.coreos.com -n cert-manager-operator`;
  await apply("cert-manager-sub.yaml");
}
const csvReady = await waitFor(
  async () =>
    (
      await $`oc get csv -n cert-manager-operator -l operators.coreos.com/openshift-cert-manager-operator.cert-manager-operator -o jsonpath={.items[0].status.phase}`.nothrow()
    ).stdout.trim() === "Succeeded",
  300,
);
if (!csvReady) fail("Timed out waiting for cert-manager operator CSV.");
for (const deployment of [
  "cert-manager",
  "cert-manager-webhook",
  "cert-manager-cainjector",
]) {
  if (
    !(await waitFor(
      async () =>
        (await $`oc get deployment ${deployment} -n cert-manager`.nothrow()).ok,
      180,
    ))
  )
    fail(`Timed out waiting for deployment/${deployment}.`);
  await $`oc wait --for=condition=Available deployment/${deployment} -n cert-manager --timeout=180s`;
}
await apply("cluster-issuer.yaml");
await apply("router-secret-reader-role.yaml");
await apply("router-secret-reader-binding.yaml");
const backup =
  await $`oc get secret ${secretName}-backup -n ${backupNamespace} -o json`.nothrow();
const existing = await $`oc get secret ${secretName} -n ${namespace}`.nothrow();
if (!fresh && !existing.ok && backup.ok) {
  const secret = JSON.parse(backup.stdout);
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
  secret.metadata.name = secretName;
  await $({ input: JSON.stringify(secret) })`oc apply -n ${namespace} -f -`;
}
await apply("certificate.yaml");
await $`oc wait --for=condition=Ready certificate/fleetshift-grpc-route -n ${namespace} --timeout=600s`;
await $`oc get secret ${secretName} -n ${namespace}`;
await $`oc patch route ${routeName} -n ${namespace} --type=merge -p ${'{"spec":{"tls":{"certificate":null,"key":null,"caCertificate":null,"externalCertificate":{"name":"' + secretName + '"}}}}'}`;
await sleep(5000);
const alpn =
  await $`openssl s_client -alpn h2 -connect ${host}:443 -servername ${host}`.nothrow();
if (!alpn.stdout.includes("ALPN protocol: h2"))
  fail(`Route ${host} is not negotiating ALPN h2 yet.`);
const http =
  await $`curl --http2 -sS -o /dev/null -w %{http_version} https://${host}/`.nothrow();
if (http.stdout.trim() !== "2")
  fail(`Route ${host} did not negotiate HTTP/2 with curl.`);
const grpc = await $`command -v grpcurl`.nothrow();
if (grpc.ok) await $`grpcurl ${host}:443 list`;
else
  console.warn(
    "WARNING: grpcurl not found in PATH. Skipping grpcurl verification.",
  );
const current =
  await $`oc get secret ${secretName} -n ${namespace} -o json`.nothrow();
if (current.ok) {
  const secret = JSON.parse(current.stdout);
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
  secret.metadata.name = `${secretName}-backup`;
  await $({
    input: JSON.stringify(secret),
  })`oc apply -n ${backupNamespace} -f -`;
}
console.log(
  `\n=== gRPC Route Certificate Ready ===\n  Route host: ${host}\n  TLS Secret: ${secretName}\n  Backup Secret: ${backupNamespace}/${secretName}-backup\n  Issuer: ${issuerName}`,
);
