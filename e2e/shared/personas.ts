/**
 * Fixed sandbox personas served by the bundled Dex identity provider
 * (deploy/aio/internal/aioinit/dexconfig.go, deploy/aio/README.md). These are
 * public sandbox fixtures, not production credentials.
 *
 * Note: the console cannot yet surface an "ops" vs "dev" role under Dex — role
 * is derived from a Keycloak-only `realm_access.roles` claim, so both personas
 * currently resolve to the same role and identical navigation. The only
 * user-visible persona signal today is the masthead user-menu label, which is
 * the capitalized `preferred_username` (see AuthContext.display_name). Proper
 * group-to-role mapping is tracked separately (OME-206).
 */
export type PersonaId = "ops" | "dev";

export interface Persona {
  /** Stable id used to name saved auth artifacts. */
  id: PersonaId;
  email: string;
  password: string;
  /** Masthead user-menu toggle label = capitalized preferred_username. */
  usernameLabel: string;
}

export const OPERATOR: Persona = {
  id: "ops",
  email: "ops@fleetshift.local",
  password: "fleetshift-ops",
  usernameLabel: "Ops-user",
};

export const DEVELOPER: Persona = {
  id: "dev",
  email: "dev@fleetshift.local",
  password: "fleetshift-dev",
  usernameLabel: "Dev-user",
};

export const PERSONAS: Persona[] = [OPERATOR, DEVELOPER];
