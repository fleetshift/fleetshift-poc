#!/usr/bin/env node
import { $, question } from "zx";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { randomBytes } from "node:crypto";
import { requireOcLogin } from "../../scripts/common.mjs";

await requireOcLogin();
for (const command of ["oc", "jq", "openssl", "curl"])
  await $`command -v ${command}`;
if (!(await $`oc get keycloak/keycloak -n keycloak-prod`.nothrow()).ok)
  throw new Error(
    "Keycloak CR 'keycloak' not found in namespace 'keycloak-prod'. Is Keycloak deployed?",
  );
console.log("This will delete and re-import FleetShift realm state.");
if (!/^y$/i.test(await question("Continue? (y/N): "))) process.exit(0);
const ns = "keycloak-prod";
const domain = (
  await $`oc get ingresses.config/cluster -o jsonpath={.spec.domain}`
).stdout.trim();
const host = `https://keycloak-${ns}.${domain}`;
const user = (
  await $`oc get secret keycloak-initial-admin -n ${ns} -o jsonpath={.data.username} | base64 -d`
).stdout.trim();
const pass = (
  await $`oc get secret keycloak-initial-admin -n ${ns} -o jsonpath={.data.password} | base64 -d`
).stdout.trim();
const token = JSON.parse(
  (
    await $`curl -sk -X POST ${host}/realms/master/protocol/openid-connect/token -d ${`grant_type=password&client_id=admin-cli&username=${user}&password=${pass}`}`
  ).stdout,
).access_token;
if (!token) throw new Error("Failed to obtain admin token");
await $`oc delete keycloakrealmimport fleetshift-realm -n ${ns} --ignore-not-found`;
await $`oc wait --for=delete keycloakrealmimport/fleetshift-realm -n ${ns} --timeout=60s`.nothrow();
const deleted =
  await $`curl -sk -o /dev/null -w %{http_code} -X DELETE ${host}/admin/realms/fleetshift -H ${`Authorization: Bearer ${token}`}`;
if (!/^2|404$/.test(deleted.stdout.trim()))
  throw new Error(`Failed to delete realm (HTTP ${deleted.stdout.trim()})`);
await $`oc wait --for=condition=Ready keycloak/keycloak -n ${ns} --timeout=300s`;
const realm = JSON.parse(
  await readFile(
    resolve(import.meta.dirname, "../fleetshift-realm.json"),
    "utf8",
  ),
);
const makePassword = () => randomBytes(16).toString("base64url").slice(0, 16);
const opsPassword = makePassword();
const devPassword = makePassword();
realm.users?.forEach((entry) => {
  if (entry.username === "ops-user") entry.credentials[0].value = opsPassword;
  if (entry.username === "dev-user") entry.credentials[0].value = devPassword;
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
const refreshed = JSON.parse(
  (
    await $`curl -sk -X POST ${host}/realms/master/protocol/openid-connect/token -d ${`grant_type=password&client_id=admin-cli&username=${user}&password=${pass}`}`
  ).stdout,
).access_token;
const profileUrl = `${host}/admin/realms/fleetshift/users/profile`;
const profile = JSON.parse(
  (await $`curl -sk ${profileUrl} -H ${`Authorization: Bearer ${refreshed}`}`)
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
  await $`curl -sk -X PUT ${profileUrl} -H ${`Authorization: Bearer ${refreshed}`} -H Content-Type:application/json -d ${JSON.stringify(profile)}`;
}
const clientUrl = `${host}/admin/realms/fleetshift/clients`;
const clientId = JSON.parse(
  (
    await $`curl -sk ${clientUrl}?clientId=ocp-console -H ${`Authorization: Bearer ${refreshed}`}`
  ).stdout,
)[0]?.id;
if (clientId) {
  await $`curl -sk -X POST ${clientUrl}/${clientId}/client-secret -H ${`Authorization: Bearer ${refreshed}`}`;
  const consoleSecret = JSON.parse(
    (
      await $`curl -sk ${clientUrl}/${clientId}/client-secret -H ${`Authorization: Bearer ${refreshed}`}`
    ).stdout,
  ).value;
  await $`oc create secret generic ocp-console-client-secret -n ${ns} --from-literal=clientSecret=${consoleSecret} --dry-run=client -o yaml | oc apply -f -`;
}
console.log(
  `Realm reset submitted.\n  ops-user / ${opsPassword}\n  dev-user / ${devPassword}`,
);
