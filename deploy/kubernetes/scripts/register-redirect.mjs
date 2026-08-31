#!/usr/bin/env node
import { $ } from "zx";
const [user, password] = process.argv.slice(2);
if (!user || !password)
  throw new Error(`Usage: ${process.argv[1]} <admin-user> <admin-password>`);
const host = (
  await $`oc get route web -n fleetshift -o jsonpath={.spec.host}`
).stdout.trim();
const issuer = (
  await $`oc get configmap fleetshift-server-config -n fleetshift -o jsonpath={.data.OIDC_ISSUER_URL}`
).stdout.trim();
const kc = issuer.replace(/\/realms\/.*$/, "");
const token = (
  await $`curl -sf -X POST ${kc}/realms/master/protocol/openid-connect/token -d ${`grant_type=password&client_id=admin-cli&username=${user}&password=${password}`}`
).stdout;
const access = JSON.parse(token).access_token;
const client = `${kc}/admin/realms/fleetshift/clients`;
const clients = JSON.parse(
  (
    await $`curl -sf ${client}?clientId=fleetshift-ui -H ${`Authorization: Bearer ${access}`}`
  ).stdout,
);
if (!clients[0]?.id)
  throw new Error("Client 'fleetshift-ui' not found in realm 'fleetshift'.");
const id = clients[0].id;
const body = JSON.parse(
  (await $`curl -sf ${client}/${id} -H ${`Authorization: Bearer ${access}`}`)
    .stdout,
);
const uri = `https://${host}/*`;
if (body.redirectUris.includes(uri)) {
  console.log("Redirect URI already registered (skipping).");
  process.exit(0);
}
body.redirectUris.push(uri);
body.webOrigins = [...new Set([...(body.webOrigins || []), `https://${host}`])];
await $`curl -sf -X PUT ${client}/${id} -H ${`Authorization: Bearer ${access}`} -H Content-Type:application/json -d ${JSON.stringify(body)}`;
console.log(`Done. Redirect URI registered for https://${host}.`);
