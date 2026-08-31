#!/usr/bin/env node
import { $, argv, sleep } from "zx";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { randomBytes } from "node:crypto";
import { requireOcLogin } from "../../scripts/common.mjs";

const arg = (name, fallback = "") => {
  const i = argv.indexOf(name);
  return i < 0 ? fallback : argv[i + 1] || "";
};
const dir = resolve(import.meta.dirname, "..");
const ns = "keycloak-prod";
const email = arg("--acme-email", process.env.ACME_EMAIL);
const fresh = argv.includes("--fresh-cert");
const domains = argv
  .flatMap((v, i) => (v === "--base-domain" ? [argv[i + 1]] : []))
  .filter(Boolean);
const fail = (message) => {
  throw new Error(message);
};
const wait = async (fn, seconds, interval = 10) => {
  for (let elapsed = 0; elapsed < seconds; elapsed += interval) {
    if (await fn()) return true;
    await sleep(interval * 1000);
  }
  return false;
};
const apply = async (file, namespace) =>
  await $`oc apply ${namespace ? ["-n", namespace] : []} -f ${resolve(dir, "manifests", file)}`;
const template = async (file, replacements, namespace) => {
  let text = await readFile(resolve(dir, "manifests", file), "utf8");
  for (const [key, value] of Object.entries(replacements))
    text = text.replaceAll(key, value);
  await $({ input: text })`oc apply ${namespace ? ["-n", namespace] : []} -f -`;
};

await requireOcLogin();
for (const command of ["oc", "jq", "openssl"]) await $`command -v ${command}`;
const domain = (
  await $`oc get ingresses.config/cluster -o jsonpath={.spec.domain}`
).stdout.trim();
if (!domain) fail("Could not determine cluster apps domain.");
const host = `keycloak-${ns}.${domain}`;
console.log(`==> Keycloak will be available at: https://${host}`);
await apply("namespace.yaml");
if (
  !(
    await $`oc get subscription openshift-cert-manager-operator -n cert-manager-operator`.nothrow()
  ).ok
)
  await apply("cert-manager-sub.yaml");
if (
  !(await wait(
    async () =>
      (
        await $`oc get csv -n cert-manager-operator -l operators.coreos.com/openshift-cert-manager-operator.cert-manager-operator -o jsonpath={.items[0].status.phase}`.nothrow()
      ).stdout.trim() === "Succeeded",
    300,
  ))
)
  fail("Timed out waiting for cert-manager operator.");
if (
  !fresh &&
  (
    await $`oc get secret keycloak-tls-backup -n cert-manager-operator`.nothrow()
  ).ok
) {
  const secret = JSON.parse(
    (
      await $`oc get secret keycloak-tls-backup -n cert-manager-operator -o json`
    ).stdout,
  );
  for (const field of [
    "namespace",
    "resourceVersion",
    "uid",
    "creationTimestamp",
    "ownerReferences",
    "managedFields",
  ])
    delete secret.metadata[field];
  secret.metadata.name = "keycloak-tls";
  await $({ input: JSON.stringify(secret) })`oc apply -n ${ns} -f -`;
}
if (
  !(await $`oc get secret keycloak-tls -n ${ns}`.nothrow().then((r) => r.ok))
) {
  if (email) {
    await template("cluster-issuer.yaml", {
      ACME_EMAIL: email,
      KEYCLOAK_HOST: host,
    });
    await template("certificate.yaml", { KEYCLOAK_HOST: host }, ns);
    await $`oc wait --for=condition=Ready certificate/keycloak-tls -n ${ns} --timeout=180s`.nothrow();
  }
  if (!(await $`oc get secret keycloak-tls -n ${ns}`.nothrow()).ok) {
    const tmp = `/tmp/keycloak-tls-${process.pid}`;
    await $`mkdir -p ${tmp}`;
    await $`openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout ${tmp}/tls.key -out ${tmp}/tls.crt -days 365 -nodes -subj /CN=keycloak -addext subjectAltName=DNS:${host}`;
    await $`oc create secret tls keycloak-tls --cert=${tmp}/tls.crt --key=${tmp}/tls.key -n ${ns}`;
    await $`rm -rf ${tmp}`;
  }
}
if (!(await $`oc get subscription rhbk-operator -n rhbk-operator`.nothrow()).ok)
  await apply("rhbk-sub.yaml");
if (
  !(await wait(
    async () =>
      (
        await $`oc get csv -n rhbk-operator -l operators.coreos.com/rhbk-operator.rhbk-operator -o jsonpath={.items[0].status.phase}`.nothrow()
      ).stdout.trim() === "Succeeded",
    300,
  ))
)
  fail("Timed out waiting for RHBK operator.");
if (!(await $`oc get secret keycloak-db-credentials -n ${ns}`.nothrow()).ok) {
  const password = (await $`openssl rand -base64 48`).stdout
    .replace(/[^a-zA-Z0-9]/g, "")
    .slice(0, 24);
  await $`oc create secret generic keycloak-db-credentials --from-literal=username=keycloak --from-literal=password=${password} --from-literal=database=keycloak -n ${ns}`;
}
await apply("postgres-statefulset.yaml", ns);
await $`oc wait --for=condition=Ready pod/postgres-0 -n ${ns} --timeout=180s`;
await template("keycloak.yaml", { KEYCLOAK_HOST: host }, ns);
await $`oc wait --for=condition=Ready keycloak/keycloak -n ${ns} --timeout=300s`;
const realm = JSON.parse(
  await readFile(resolve(dir, "fleetshift-realm.json"), "utf8"),
);
const random = (length) =>
  randomBytes(length).toString("base64url").slice(0, length);
const passwords = { ops: random(16), dev: random(16) };
realm.users?.forEach((user) => {
  if (user.username === "ops-user") user.credentials[0].value = passwords.ops;
  if (user.username === "dev-user") user.credentials[0].value = passwords.dev;
});
await $({
  input: JSON.stringify({
    apiVersion: "k8s.keycloak.org/v2alpha1",
    kind: "KeycloakRealmImport",
    metadata: { name: "fleetshift-realm" },
    spec: { keycloakCRName: "keycloak", realm },
  }),
})`oc apply -n ${ns} -f -`;
await $`oc wait --for=condition=Done keycloakrealmimport/fleetshift-realm -n ${ns} --timeout=120s`.nothrow();
await $`oc wait --for=condition=Ready keycloak/keycloak -n ${ns} --timeout=300s`;
const adminUser = (
  await $`oc get secret keycloak-initial-admin -n ${ns} -o jsonpath={.data.username} | base64 -d`
).stdout.trim();
const adminPassword = (
  await $`oc get secret keycloak-initial-admin -n ${ns} -o jsonpath={.data.password} | base64 -d`
).stdout.trim();
if (
  !(await wait(
    async () =>
      (
        await $`curl -sk --connect-timeout 5 ${`https://${host}/realms/master`}`.nothrow()
      ).ok,
    120,
    5,
  ))
)
  fail("Keycloak API not reachable after 120s");
const access = JSON.parse(
  (
    await $`curl -sk -X POST ${`https://${host}/realms/master/protocol/openid-connect/token`} -d ${`grant_type=password&client_id=admin-cli&username=${adminUser}&password=${adminPassword}`}`
  ).stdout,
).access_token;
if (!access) fail("Failed to obtain admin token");
const profileUrl = `https://${host}/admin/realms/fleetshift/users/profile`;
const profile = JSON.parse(
  (await $`curl -sk ${profileUrl} -H ${`Authorization: Bearer ${access}`}`)
    .stdout,
);
if (!profile.attributes?.some((entry) => entry.name === "github_username")) {
  profile.attributes = [
    ...(profile.attributes || []),
    {
      name: "github_username",
      displayName: "GitHub Username",
      validations: {},
      annotations: {},
      permissions: { view: ["admin", "user"], edit: ["admin"] },
      multivalued: false,
    },
  ];
  await $`curl -sk -X PUT ${profileUrl} -H ${`Authorization: Bearer ${access}`} -H Content-Type:application/json -d ${JSON.stringify(profile)}`;
}
const clientsUrl = `https://${host}/admin/realms/fleetshift/clients`;
const consoleId = JSON.parse(
  (
    await $`curl -sk ${clientsUrl}?clientId=ocp-console -H ${`Authorization: Bearer ${access}`}`
  ).stdout,
)[0]?.id;
if (consoleId) {
  await $`curl -sk -X POST ${clientsUrl}/${consoleId}/client-secret -H ${`Authorization: Bearer ${access}`}`;
  const consoleSecret = JSON.parse(
    (
      await $`curl -sk ${clientsUrl}/${consoleId}/client-secret -H ${`Authorization: Bearer ${access}`}`
    ).stdout,
  ).value;
  await $`oc create secret generic ocp-console-client-secret -n ${ns} --from-literal=clientSecret=${consoleSecret} --dry-run=client -o yaml | oc apply -f -`;
}
for (const d of domains)
  await $`node ${resolve(import.meta.dirname, "add-base-domain.mjs")} --base-domain ${d} --cluster-name ${process.env.CLUSTER_NAME || ""}`.nothrow();
console.log(
  `\n==========================================\n  Keycloak Deployment Complete\n==========================================\n\n  URL: https://${host}\n  FleetShift Realm Users:\n    ops-user / ${passwords.ops}\n    dev-user / ${passwords.dev}\n\n  Run 'npx nx run kc:add-user' to create personal dev accounts.\n==========================================`,
);
