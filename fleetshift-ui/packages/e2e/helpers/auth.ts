const KEYCLOAK_URL = process.env.KEYCLOAK_URL ?? "http://localhost:8180";
const TOKEN_ENDPOINT = `${KEYCLOAK_URL}/realms/fleetshift/protocol/openid-connect/token`;

export async function getTestToken(
  username: string,
  password: string,
): Promise<string> {
  const body = new URLSearchParams({
    grant_type: "password",
    client_id: "fleetshift-ui",
    username,
    password,
  });

  const res = await fetch(TOKEN_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  if (!res.ok) {
    throw new Error(
      `Failed to get test token for ${username}: ${res.status} ${res.statusText}`,
    );
  }

  const data = (await res.json()) as { access_token: string };
  return data.access_token;
}
