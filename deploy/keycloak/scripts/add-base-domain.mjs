#!/usr/bin/env node
import { $, argv } from "zx";
const options = new Set(["--base-domain", "--cluster-name"]);
for (let index = 0; index < argv.length; index++) {
  if (!options.has(argv[index]))
    throw new Error(`Unknown argument: ${argv[index]}`);
  const next = argv[++index];
  if (!next || next.startsWith("-"))
    throw new Error(`${argv[index - 1]} requires a value`);
}
const value = (name) => {
  const index = argv.indexOf(name);
  return index < 0 ? "" : argv[index + 1] || "";
};
const domain = value("--base-domain"),
  cluster = value("--cluster-name");
if (!domain || !cluster)
  throw new Error("Usage: --base-domain <domain> --cluster-name <name>");
const host = (
  await $`oc get route keycloak -n keycloak-prod -o jsonpath={.spec.host}`
).stdout.trim();
if (!host)
  throw new Error("Route 'keycloak' not found in namespace 'keycloak-prod'.");
const token = JSON.parse(
  (
    await $`curl -s -X POST https://${host}/realms/master/protocol/openid-connect/token -d ${`grant_type=password&client_id=admin-cli&username=${(await $`oc get secret keycloak-initial-admin -n keycloak-prod -o jsonpath={.data.username} | base64 -d`).stdout.trim()}&password=${(await $`oc get secret keycloak-initial-admin -n keycloak-prod -o jsonpath={.data.password} | base64 -d`).stdout.trim()}`}`
  ).stdout,
).access_token;
if (!token) throw new Error("Failed to obtain Keycloak admin token");
const endpoint = `https://${host}/admin/realms/fleetshift/clients`;
const found = JSON.parse(
  (
    await $`curl -s ${endpoint}?clientId=ocp-console -H ${`Authorization: Bearer ${token}`}`
  ).stdout,
)[0];
if (!found?.id)
  throw new Error(
    "Client 'ocp-console' not found in realm 'fleetshift'. Run deploy.sh first.",
  );
const client = JSON.parse(
  (
    await $`curl -s ${endpoint}/${found.id} -H ${`Authorization: Bearer ${token}`}`
  ).stdout,
);
const uri = `https://console-openshift-console.apps.${cluster}.${domain}/*`;
client.redirectUris = Array.isArray(client.redirectUris)
  ? client.redirectUris
  : [];
if (!client.redirectUris.includes(uri)) {
  client.redirectUris.push(uri);
  const update =
    await $`curl -sf -X PUT ${endpoint}/${found.id} -H ${`Authorization: Bearer ${token}`} -H Content-Type:application/json -d ${JSON.stringify(client)}`.nothrow();
  if (!update.ok)
    throw new Error("Failed to update ocp-console redirect URIs.");
}
console.log(
  `Done. Redirect URI for cluster '${cluster}' added to ocp-console.`,
);
