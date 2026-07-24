/**
 * TypeScript types for the FleetShift management API.
 *
 * Derived from the finalized OpenAPI spec:
 * `docs/openapi/fleetshift.swagger.yaml` in fleetshift-poc.
 *
 * Use with {@link createApiClient}:
 * ```ts
 * import { createApiClient, type Deployment } from "@fleetshift/common";
 * const mgmt = createApiClient("/v1");
 * const { deployments } = await mgmt.get<ListDeploymentsResponse>("/deployments");
 * ```
 */

import type { OutputConstraint } from "./canonical.js";

// ---------------------------------------------------------------------------
// Shared / cross-service
// ---------------------------------------------------------------------------

/** Federated identity: issuer-scoped subject. */
export interface FederatedIdentity {
  subject: string;
  issuer: string;
}

// ---------------------------------------------------------------------------
// AuthMethodService
// ---------------------------------------------------------------------------

export type AuthMethodType = "TYPE_UNSPECIFIED" | "TYPE_OIDC";

/** Maps ID token claims to a key registry subject. */
export interface RegistrySubjectMapping {
  /** External key registry identifier (e.g. "github.com"). */
  registryId: string;
  /** CEL expression over claims map producing a string. */
  expression: string;
}

/** OIDC-specific configuration for an authentication method. */
export interface OIDCConfig {
  issuerUrl: string;
  audience: string;
  /** Expected audience for signing key enrollment ID tokens. */
  keyEnrollmentAudience?: string;
  /**
   * CEL expression to extract signer's base64-encoded SPKI/PKIX public key
   * from ID token claims.
   */
  publicKeyClaimExpression?: string;
  registrySubjectMapping?: RegistrySubjectMapping;
  /** Server-resolved from OIDC discovery (output only). */
  readonly authorizationEndpoint?: string;
  /** Server-resolved from OIDC discovery (output only). */
  readonly tokenEndpoint?: string;
  /** Server-resolved from OIDC discovery (output only). */
  readonly jwksUri?: string;
}

/** A configured authentication method. */
export interface AuthMethod {
  /** Resource name. Format: authMethods/{auth_method} */
  name: string;
  type: AuthMethodType;
  oidcConfig?: OIDCConfig;
}

// ---------------------------------------------------------------------------
// DeploymentService
// ---------------------------------------------------------------------------

export type DeploymentState =
  | "STATE_UNSPECIFIED"
  | "STATE_CREATING"
  | "STATE_ACTIVE"
  | "STATE_DELETING"
  | "STATE_FAILED";

export type ManifestStrategyType = "TYPE_UNSPECIFIED" | "TYPE_INLINE";

export interface DeploymentManifest {
  resourceType: string;
  /** Opaque content (base64). */
  raw: string;
}

export type DeploymentManifestStrategy =
  | { type: "TYPE_UNSPECIFIED"; manifests?: DeploymentManifest[] }
  | { type: "TYPE_INLINE"; manifests: DeploymentManifest[] };

export type PlacementStrategyType =
  | "TYPE_UNSPECIFIED"
  | "TYPE_STATIC"
  | "TYPE_ALL"
  | "TYPE_SELECTOR";

export interface TargetSelector {
  matchLabels: Record<string, string>;
}

export interface DeploymentPlacementStrategy {
  type: PlacementStrategyType;
  targetIds?: string[];
  targetSelector?: TargetSelector;
}

export type RolloutStrategyType = "TYPE_UNSPECIFIED" | "TYPE_IMMEDIATE";

export interface RolloutStrategy {
  type: RolloutStrategyType;
}

/** Detached signature over a canonical content hash. */
export interface Signature {
  signer: FederatedIdentity;
  contentHash: string;
  signatureBytes: string;
}

/** Cryptographic proof that a user authorized a deployment. */
export interface Provenance {
  signature: Signature;
  validUntil: string;
  expectedGeneration: string;
  outputConstraints: OutputConstraint[];
}

/**
 * Deployment resource (AIP-128 declarative-friendly).
 *
 * Composition of manifest, placement, and rollout strategies.
 */
export interface Deployment {
  /** Resource name. Format: deployments/{deployment} */
  name: string;
  /** System-assigned UUID. */
  readonly uid: string;
  manifestStrategy: DeploymentManifestStrategy;
  placementStrategy: DeploymentPlacementStrategy;
  rolloutStrategy?: RolloutStrategy;
  /** Target IDs resolved by placement. */
  readonly resolvedTargetIds: string[];
  readonly state: DeploymentState;
  /** True while the resource is reconciling toward desired state. */
  readonly reconciling: boolean;
  readonly createTime: string;
  readonly updateTime: string;
  /** Weak domain-state concurrency token (AIP-154). */
  etag: string;
  /** Cryptographic proof of user authorization. */
  readonly provenance?: Provenance;
  /**
   * Monotonically increasing version counter. Increments on
   * generation-advancing mutations (create, resume, delete).
   */
  readonly generation: string;
  /** Non-empty when reconciliation is paused. */
  readonly pauseReason?: string;
}

/** Paginated list response for deployments. */
export interface ListDeploymentsResponse {
  deployments: Deployment[];
  /** Empty when there are no more pages. */
  nextPageToken: string;
}

/** Body for DeploymentService.ResumeDeployment. */
export interface ResumeDeploymentRequest {
  /** ECDSA-P256-SHA256 signature (base64). Required when re-signing. */
  userSignature?: string;
  /** Attestation expiry for envelope reconstruction. */
  validUntil?: string;
  /** Concurrency control token from the most recent GET. */
  etag?: string;
  /** Expected generation (current + 1). Required when signing. */
  expectedGeneration?: string;
}

// ---------------------------------------------------------------------------
// SignerEnrollmentService
// ---------------------------------------------------------------------------

/** Signer enrollment resource. */
export interface SignerEnrollment {
  /** Resource name. Format: signerEnrollments/{signer_enrollment} */
  name: string;
  /** Subject from the identity token (jwt.sub). */
  readonly subject: string;
  /** OIDC issuer URL (jwt.iss). */
  readonly issuer: string;
  /** Purpose-scoped ID token for delivery agent verification. */
  readonly identityToken: string;
  /** Registry subject derived from CEL claim mapping. */
  readonly registrySubject: string;
  /** External key registry identifier. */
  readonly registryId: string;
  readonly createTime: string;
  readonly expireTime: string;
}

/** Body for SignerEnrollmentService.CreateSignerEnrollment. */
export interface CreateSignerEnrollmentRequest {
  /** Client-assigned ID for the enrollment. */
  signerEnrollmentId: string;
  /** Purpose-scoped ID token from the tenant IdP. */
  identityToken: string;
}
