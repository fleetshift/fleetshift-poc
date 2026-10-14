#!/usr/bin/env node
import { $, argv } from "zx";

const option = (flag, env, fallback = "") => {
  const index = argv.indexOf(flag);
  return index >= 0 ? argv[index + 1] || "" : process.env[env] || fallback;
};
const urlArg = option("--keycloak-url", "KC_ADMIN_URL");
let admin = option("--admin-user", "KC_ADMIN_USER", "admin");
let adminPassword = option("--admin-password", "KC_ADMIN_PASSWORD");
const username = option("--username", "KC_NEW_USERNAME");
const password = option("--password", "KC_NEW_PASSWORD");
const github = option("--github", "KC_NEW_GITHUB");
const roles = option("--roles", "KC_NEW_ROLES");
const error = (message) => {
  throw new Error(message);
};

if (!username) error("Username required.");
if (!password) error("Password required.");
if (!github) error("GitHub username required.");
if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(username))
  error(`Username must be an email address. Got: '${username}'`);
for (const flag of [
  "--keycloak-url",
  "--admin-user",
  "--admin-password",
  "--username",
  "--password",
  "--github",
  "--roles",
]) {
  const index = argv.indexOf(flag);
  if (index >= 0 && (!argv[index + 1] || argv[index + 1].startsWith("-")))
    error(`${flag} requires a value`);
}

let url = urlArg;
if (adminPassword && !url)
  error("Admin password requires --keycloak-url (or KC_ADMIN_URL).");
if (url && !adminPassword)
  error("--keycloak-url requires --admin-password (or KC_ADMIN_PASSWORD).");
if (!url) {
  await (await import("../../scripts/common.mjs")).requireOcLogin();
  const ns = process.env.OC_NAMESPACE || "keycloak-prod";
  const cr = process.env.OC_CR_NAME || "keycloak";
  const domain = (
    await $`oc get ingresses.config/cluster -o jsonpath={.spec.domain}`
  ).stdout.trim();
  url = `https://${cr}-${ns}.${domain}`;
  admin = (
    await $`oc get secret ${cr}-initial-admin -n ${ns} -o jsonpath={.data.username} | base64 -d`
  ).stdout.trim();
  adminPassword = (
    await $`oc get secret ${cr}-initial-admin -n ${ns} -o jsonpath={.data.password} | base64 -d`
  ).stdout.trim();
}
await $`command -v jq`;
const token = JSON.parse(
  (
    await $`curl -sk -X POST ${url}/realms/master/protocol/openid-connect/token -d ${`grant_type=password&client_id=admin-cli&username=${admin}&password=${adminPassword}`}`
  ).stdout,
).access_token;
if (!token)
  error(
    "Failed to obtain admin token. Check Keycloak URL and admin credentials.",
  );
const body = {
  username,
  email: username,
  enabled: true,
  emailVerified: true,
  credentials: [{ type: "password", value: password, temporary: false }],
  attributes: { github_username: [github] },
  realmRoles: roles ? roles.split(",") : [],
};
const response =
  await $`curl -sk -w '\n%{http_code}' -X POST ${url}/admin/realms/fleetshift/users -H ${`Authorization: Bearer ${token}`} -H Content-Type:application/json -d ${JSON.stringify(body)}`;
const status = response.stdout.trim().split("\n").pop();
let userId;
if (/^2/.test(status)) console.log("User created successfully.");
else if (status === "409") {
  userId = (
    await $`curl -sk ${url}/admin/realms/fleetshift/users?username=${username}&exact=true -H ${`Authorization: Bearer ${token}`}`
  ).stdout;
  userId = JSON.parse(userId)[0]?.id;
  if (!userId) error("Could not find user ID");
  await $`curl -sk -X PUT ${url}/admin/realms/fleetshift/users/${userId} -H ${`Authorization: Bearer ${token}`} -H Content-Type:application/json -d ${JSON.stringify({ attributes: { github_username: [github] } })}`;
  await $`curl -sk -X PUT ${url}/admin/realms/fleetshift/users/${userId}/reset-password -H ${`Authorization: Bearer ${token}`} -H Content-Type:application/json -d ${JSON.stringify({ type: "password", value: password, temporary: false })}`;
  console.log(`User '${username}' already exists. Updated.`);
} else error(`Failed to create user (HTTP ${status}).`);

if (roles) {
  userId ||= JSON.parse(
    (
      await $`curl -sk ${url}/admin/realms/fleetshift/users?username=${username}&exact=true -H ${`Authorization: Bearer ${token}`}`
    ).stdout,
  )[0]?.id;
  for (const role of roles.split(",")) {
    const roleResult =
      await $`curl -sk ${url}/admin/realms/fleetshift/roles/${role} -H ${`Authorization: Bearer ${token}`}`;
    if (JSON.parse(roleResult.stdout).name === role)
      await $`curl -sk -X POST ${url}/admin/realms/fleetshift/users/${userId}/role-mappings/realm -H ${`Authorization: Bearer ${token}`} -H Content-Type:application/json -d ${`[${roleResult.stdout}]`}`;
    else console.warn(`WARNING: Role '${role}' not found, skipping.`);
  }
}
console.log(
  `\n  User: ${username}\n  Password: ${password}\n  GitHub: ${github}\n  Roles: ${roles || "none"}\n  Keycloak: ${url}/realms/fleetshift/account`,
);
