import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { createServer } from "node:http";

import { flagString, hasFlag, parseArgs } from "../argv";
import { loadAuthConfig } from "../config";
import { oidcRequest, saveTokenResponse, type TokenResponse } from "./helpers";

const LOGIN_TIMEOUT_MS = 60_000;

export async function runAuthLogin(
  args: ReturnType<typeof parseArgs>,
): Promise<string> {
  const directory = flagString(args, "config-dir") || undefined;
  const config = await loadAuthConfig(directory);
  const token = await runOIDCFlow(args, config.client_id, config.scopes);
  await saveTokenResponse(directory, token);
  return "Login successful!";
}

export async function runOIDCFlow(
  args: ReturnType<typeof parseArgs>,
  clientID: string,
  scopes: string[],
): Promise<TokenResponse> {
  const directory = flagString(args, "config-dir") || undefined;
  const config = await loadAuthConfig(directory);
  const verifier = randomBytes(32).toString("base64url");
  const challenge = createHash("sha256").update(verifier).digest("base64url");
  const state = randomBytes(32).toString("base64url");
  const listener = createServer();
  await new Promise<void>((resolve) =>
    listener.listen(0, "127.0.0.1", resolve),
  );
  const address = listener.address();
  if (!address || typeof address === "string")
    throw new Error("callback listener failed");
  const redirectURI = `http://127.0.0.1:${address.port}/callback`;
  const authorizationURL = new URL(config.authorization_endpoint);
  authorizationURL.search = new URLSearchParams({
    client_id: clientID,
    redirect_uri: redirectURI,
    response_type: "code",
    scope: scopes.join(" "),
    state,
    code_challenge: challenge,
    code_challenge_method: "S256",
  }).toString();
  let rejectCallback: (error: Error) => void = () => undefined;
  const callback = new Promise<string>((resolve, reject) => {
    rejectCallback = reject;
    listener.on("request", (request, response) => {
      const url = new URL(request.url ?? "/", "http://127.0.0.1");
      if (url.pathname !== "/callback") return;
      if (url.searchParams.get("state") !== state) {
        response.writeHead(400).end("Authentication failed");
        reject(new Error("callback state mismatch"));
        return;
      }
      const code = url.searchParams.get("code");
      if (!code) {
        response.writeHead(400).end("Authentication failed");
        reject(
          new Error(
            url.searchParams.get("error") ?? "missing authorization code",
          ),
        );
        return;
      }
      response
        .writeHead(200, { "Content-Type": "text/html" })
        .end("<p>Authentication successful. You can close this window.</p>");
      resolve(code);
    });
  });
  const callbackTimeout = setTimeout(() => {
    listener.close();
    rejectCallback(new Error("authentication callback timed out"));
  }, LOGIN_TIMEOUT_MS);
  process.stdout.write(
    `AUTH_URL ${authorizationURL}\nWaiting for callback...\n`,
  );
  if (!hasFlag(args, "no-browser")) {
    const command = process.platform === "darwin" ? "open" : "xdg-open";
    spawn(command, [authorizationURL.toString()], {
      detached: true,
      stdio: "ignore",
    }).unref();
  }
  const code = await callback.finally(() => {
    clearTimeout(callbackTimeout);
    listener.close();
  });
  const token = await oidcRequest<TokenResponse>(
    config.token_endpoint,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: clientID,
        grant_type: "authorization_code",
        code,
        redirect_uri: redirectURI,
        code_verifier: verifier,
      }),
    },
    config.oidc_ca_file,
  );
  return token;
}
