export interface Sandbox {
  caFile: string;
  grpcTarget: string;
  issuer: string;
  kindIdPrefix: string;
  uiOrigin: string;
  workDir: string;
}

function requireEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(
      `${name} is unset; run this suite through e2e/sandbox/run.mjs`,
    );
  }
  return value;
}

export function readSandboxEnvironment(): Sandbox {
  const uiOrigin = requireEnvironment("BASE_URL");
  return {
    caFile: requireEnvironment("FLEETSHIFT_CA_FILE"),
    grpcTarget: requireEnvironment("FLEETSHIFT_GRPC_TARGET"),
    issuer: `${uiOrigin}/idp`,
    kindIdPrefix: requireEnvironment("FLEETSHIFT_KIND_PREFIX"),
    uiOrigin,
    workDir: requireEnvironment("FLEETSHIFT_E2E_WORK_DIR"),
  };
}
