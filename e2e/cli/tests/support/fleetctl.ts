import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { copyFile, mkdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";

import { type Browser, type BrowserContext } from "@playwright/test";

import { completeDexCliLogin } from "../../../shared/dex-login";
import { type Persona } from "../../../shared/personas";
import { commandFailure, type CommandResult, runCommand } from "./command";
import { type Sandbox } from "./sandbox";

const CLIENT_ID = "fleetshift-cli";
const SCOPES = "openid,profile,email,audience:server:client_id:fleetshift";
const AUTH_FILE = "auth.json";
const CREDENTIALS_FILE = "credentials.json";
const LOGIN_TIMEOUT_MS = 60_000;

export function buildFleetctlArgs(
  configDir: string,
  grpcTarget: string,
  args: readonly string[],
): string[] {
  return [
    "--config-dir",
    configDir,
    "--insecure-storage",
    "--server",
    grpcTarget,
    "--output",
    "json",
    ...args,
  ];
}

export function parseAuthURLLine(line: string): string | null {
  const match = /^AUTH_URL\s+(.+)$/.exec(line.trim());
  return match?.[1]?.trim() || null;
}

export function parseAccessToken(raw: string): string {
  const parsed: unknown = JSON.parse(raw);
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("access_token" in parsed) ||
    typeof parsed.access_token !== "string" ||
    !parsed.access_token.trim()
  ) {
    throw new Error("credentials.json has no access_token");
  }
  return parsed.access_token;
}

export function sanitizeSecretText(value: string): string {
  return value
    .replace(/Bearer\s+\S+/gi, "Bearer [REDACTED]")
    .replace(/https?:\/\/\S+/gi, "[LOGIN URL REDACTED]")
    .replace(/([?&](?:code|state|code_challenge)=)[^&\s]+/gi, "$1[REDACTED]");
}

function safeFailure(label: string, result: CommandResult): Error {
  const sanitized: CommandResult = {
    ...result,
    stderr: sanitizeSecretText(result.stderr),
    // Fleetctl stdout can contain inspect-token output. Never include it in
    // process errors, even when the command itself looks harmless.
    stdout: "",
  };
  return commandFailure(label, sanitized);
}

interface FleetctlRunOptions {
  configDir?: string;
}

export class FleetctlClient {
  readonly #binary: string;
  readonly configDir: string;
  readonly #browser: Browser;
  readonly #sandbox: Sandbox;

  constructor(options: {
    binary: string;
    browser: Browser;
    configDir: string;
    sandbox: Sandbox;
  }) {
    this.#binary = options.binary;
    this.#browser = options.browser;
    this.configDir = options.configDir;
    this.#sandbox = options.sandbox;
  }

  async run(
    args: readonly string[],
    options: FleetctlRunOptions = {},
  ): Promise<CommandResult> {
    return runCommand(
      this.#binary,
      buildFleetctlArgs(
        options.configDir ?? this.configDir,
        this.#sandbox.grpcTarget,
        args,
      ),
    );
  }

  async succeed(
    args: readonly string[],
    options: FleetctlRunOptions = {},
  ): Promise<CommandResult> {
    const result = await this.run(args, options);
    if (result.exitCode !== 0 || result.timedOut)
      throw safeFailure("fleetctl", result);
    return result;
  }

  async login(persona: Persona, configDir = this.configDir): Promise<void> {
    await mkdir(configDir, { mode: 0o700, recursive: true });
    await this.#authSetup(configDir);
    await this.#loginNoBrowser(configDir, persona);
  }

  async loginAs(persona: Persona): Promise<string> {
    const dir = path.join(
      this.#sandbox.workDir,
      `fleetctl-${persona.id}-${randomUUID()}`,
    );
    await mkdir(dir, { mode: 0o700, recursive: true });
    await copyFile(
      path.join(this.configDir, AUTH_FILE),
      path.join(dir, AUTH_FILE),
    );
    await this.#loginNoBrowser(dir, persona);
    return dir;
  }

  async accessToken(configDir = this.configDir): Promise<string> {
    const raw = await readFile(path.join(configDir, CREDENTIALS_FILE), "utf8");
    return parseAccessToken(raw);
  }

  async #authSetup(configDir: string): Promise<void> {
    const deadline = Date.now() + LOGIN_TIMEOUT_MS;
    let last: CommandResult | undefined;
    while (Date.now() < deadline) {
      last = await this.run(
        [
          "auth",
          "setup",
          "--issuer-url",
          this.#sandbox.issuer,
          "--client-id",
          CLIENT_ID,
          "--oidc-ca-file",
          this.#sandbox.caFile,
          "--scopes",
          SCOPES,
        ],
        { configDir },
      );
      if (last.exitCode === 0 && !last.timedOut) return;
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
    if (last === undefined) {
      throw new Error("fleetctl auth setup produced no result");
    }
    throw safeFailure("fleetctl auth setup", last);
  }

  async #loginNoBrowser(configDir: string, persona: Persona): Promise<void> {
    const args = buildFleetctlArgs(configDir, this.#sandbox.grpcTarget, [
      "auth",
      "login",
      "--no-browser",
    ]);
    const child = spawn(this.#binary, args, {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    let exited = false;
    let context: BrowserContext | undefined;
    child.stderr.on("data", (chunk: Buffer) => {
      if (stderr.length < 64 * 1024) stderr += chunk.toString("utf8");
    });

    const exit = new Promise<number>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code) => {
        exited = true;
        resolve(code ?? -1);
      });
    });
    // Spawn failures reject `exit` and also close stdout, so `authURL` rejects
    // first. Keep a handler on `exit` so that rejection is not unhandled.
    void exit.catch(() => undefined);
    const authURL = new Promise<string>((resolve, reject) => {
      const lines = readline.createInterface({ input: child.stdout });
      let found = false;
      lines.on("line", (line) => {
        const url = parseAuthURLLine(line);
        if (url && !found) {
          found = true;
          resolve(url);
        }
      });
      lines.once("close", () => {
        if (!found) reject(new Error("fleetctl output ended before AUTH_URL"));
      });
    });
    const timer = setTimeout(() => child.kill("SIGKILL"), LOGIN_TIMEOUT_MS);

    try {
      const url = await authURL;
      context = await this.#browser.newContext({
        ignoreHTTPSErrors: true,
        recordVideo: undefined,
      });
      const page = await context.newPage();
      await page.goto(url);
      await completeDexCliLogin(page, persona);
      const code = await exit;
      if (code !== 0) {
        throw new Error(
          `fleetctl auth login exited with code ${code}${stderr.trim() ? `\n${sanitizeSecretText(stderr.trim())}` : ""}`,
        );
      }
      await stat(path.join(configDir, CREDENTIALS_FILE));
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(
        `fleetctl auth login for ${persona.id} failed: ${sanitizeSecretText(detail)}`,
      );
    } finally {
      clearTimeout(timer);
      await context?.close();
      if (!exited) child.kill("SIGKILL");
    }
  }
}
