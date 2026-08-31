#!/usr/bin/env node
import { $, argv } from "zx";
const value = (name) => {
  const index = argv.indexOf(name);
  return index < 0 ? "" : argv[index + 1] || "";
};
const domain = value("--base-domain"),
  cluster = value("--cluster-name");
if (!domain || !cluster)
  throw new Error("Usage: --base-domain <domain> --cluster-name <name>");
const host = (
  await $`oc get route -n keycloak-prod -o jsonpath={.items[0].spec.host}`
).stdout.trim();
const token = JSON.parse(
  (
    await $`curl -s -X POST https://${host}/realms/master/protocol/openid-connect/token -d ${`grant_type=password&client_id=admin-cli&username=${(await $`oc get secret keycloak-initial-admin -n keycloak-prod -o jsonpath={.data.username} | base64 -d`).stdout.trim()}&password=${(await $`oc get secret keycloak-initial-admin -n keycloak-prod -o jsonpath={.data.password} | base64 -d`).stdout.trim()}`}`
  ).stdout,
).access_token;
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
if (!client.redirectUris.includes(uri)) {
  client.redirectUris.push(uri);
  await $`curl -s -X PUT ${endpoint}/${found.id} -H ${`Authorization: Bearer ${token}`} -H Content-Type:application/json -d ${JSON.stringify(client)}`;
}
console.log(
  `Done. Redirect URI for cluster '${cluster}' added to ocp-console.`,
);
