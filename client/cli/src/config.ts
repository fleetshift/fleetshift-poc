import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";

export interface AuthConfig {
  issuer_url: string;
  client_id: string;
  scopes: string[];
  authorization_endpoint: string;
  token_endpoint: string;
  oidc_ca_file?: string;
  key_enrollment_client_id?: string;
}

export interface StoredTokens {
  access_token: string;
  refresh_token?: string;
  id_token?: string;
  expiry: string;
  token_type: string;
}

export function configDirectory(value?: string): string {
  return (
    value ||
    process.env.FLEETCTL_CONFIG_DIR ||
    join(homedir(), ".config", "fleetshift")
  );
}

export function configPath(value?: string): string {
  return join(configDirectory(value), "auth.json");
}

export async function loadAuthConfig(directory?: string): Promise<AuthConfig> {
  return JSON.parse(
    await readFile(configPath(directory), "utf8"),
  ) as AuthConfig;
}

export async function saveAuthConfig(
  directory: string | undefined,
  config: AuthConfig,
): Promise<void> {
  const path = configPath(directory);
  await mkdir(resolve(path, ".."), { recursive: true, mode: 0o700 });
  await writeFile(path, `${JSON.stringify(config, null, 2)}\n`, {
    mode: 0o600,
  });
}

export async function clearStoredTokens(directory?: string): Promise<void> {
  await rm(join(configDirectory(directory), "credentials.json"), {
    force: true,
  });
}

export async function loadStoredTokens(
  directory?: string,
): Promise<StoredTokens> {
  return JSON.parse(
    await readFile(
      join(configDirectory(directory), "credentials.json"),
      "utf8",
    ),
  ) as StoredTokens;
}

export async function saveStoredTokens(
  directory: string | undefined,
  tokens: StoredTokens,
): Promise<void> {
  const directoryPath = configDirectory(directory);
  await mkdir(directoryPath, { recursive: true, mode: 0o700 });
  await writeFile(
    join(directoryPath, "credentials.json"),
    `${JSON.stringify(tokens, null, 2)}\n`,
    { mode: 0o600 },
  );
}

export async function saveSigningKey(
  directory: string | undefined,
  key: string,
): Promise<void> {
  const directoryPath = configDirectory(directory);
  await mkdir(directoryPath, { recursive: true, mode: 0o700 });
  await writeFile(join(directoryPath, "signing_key.pem"), key, { mode: 0o600 });
}

export async function loadSigningKey(directory?: string): Promise<string> {
  return readFile(join(configDirectory(directory), "signing_key.pem"), "utf8");
}

export function absolutePath(value: string): string {
  return isAbsolute(value) ? value : resolve(value);
}
