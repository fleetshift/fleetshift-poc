import { describe, expect, it } from "vitest";

import type { ClusterResource } from "../clusterTypes";
import {
  buildAddonBasePath,
  deriveClusterState,
  extractClusterId,
  extractService,
  isFailureState,
  isHealthyState,
  isTransientState,
  serviceLabel,
  stateLabel,
} from "../clusterTypes";

function resource(overrides: Partial<ClusterResource> = {}): ClusterResource {
  return { name: "clusters/test", uid: "u1", ...overrides };
}

describe("deriveClusterState", () => {
  it("returns PAUSED_AUTH when pauseReason is set", () => {
    expect(
      deriveClusterState(
        resource({ state: "CREATING", pauseReason: "auth failed" }),
      ),
    ).toBe("PAUSED_AUTH");
  });

  it("returns PAUSED_AUTH even when state is ACTIVE", () => {
    expect(
      deriveClusterState(
        resource({ state: "ACTIVE", pauseReason: "token expired" }),
      ),
    ).toBe("PAUSED_AUTH");
  });

  it("pauseReason takes precedence over conditions", () => {
    expect(
      deriveClusterState(
        resource({
          pauseReason: "cred issue",
          conditions: { Ready: { status: "True" } },
        }),
      ),
    ).toBe("PAUSED_AUTH");
  });

  it("returns resource.state when no pauseReason", () => {
    expect(deriveClusterState(resource({ state: "CREATING" }))).toBe(
      "CREATING",
    );
    expect(deriveClusterState(resource({ state: "ACTIVE" }))).toBe("ACTIVE");
    expect(deriveClusterState(resource({ state: "DELETING" }))).toBe(
      "DELETING",
    );
  });

  it("falls back to RUNNING when Ready condition is True", () => {
    expect(
      deriveClusterState(
        resource({ conditions: { Ready: { status: "True" } } }),
      ),
    ).toBe("RUNNING");
  });

  it("falls back to ERROR when Ready condition is False", () => {
    expect(
      deriveClusterState(
        resource({ conditions: { Ready: { status: "False" } } }),
      ),
    ).toBe("ERROR");
  });

  it("returns undefined when no state, no pauseReason, no conditions", () => {
    expect(deriveClusterState(resource())).toBeUndefined();
  });

  it("ignores empty string pauseReason", () => {
    expect(
      deriveClusterState(resource({ state: "ACTIVE", pauseReason: "" })),
    ).toBe("ACTIVE");
  });
});

describe("stateLabel", () => {
  it("returns label for known states", () => {
    expect(stateLabel("PAUSED_AUTH")).toEqual({
      text: "Paused",
      color: "orange",
    });
    expect(stateLabel("ACTIVE")).toEqual({ text: "Active", color: "green" });
  });

  it("composes underlying state with pause suffix", () => {
    expect(stateLabel("PAUSED_AUTH", "CREATING")).toEqual({
      text: "Creating (Paused)",
      color: "orange",
    });
    expect(stateLabel("PAUSED_AUTH", "ACTIVE")).toEqual({
      text: "Active (Paused)",
      color: "orange",
    });
  });

  it("falls back to plain Paused when underlying state is unknown", () => {
    expect(stateLabel("PAUSED_AUTH", "BOGUS")).toEqual({
      text: "Paused",
      color: "orange",
    });
  });

  it("returns Unknown for undefined or unrecognized state", () => {
    expect(stateLabel(undefined)).toEqual({ text: "Unknown", color: "grey" });
    expect(stateLabel("BOGUS")).toEqual({ text: "Unknown", color: "grey" });
  });
});

describe("state category helpers", () => {
  it("isHealthyState", () => {
    expect(isHealthyState("ACTIVE")).toBe(true);
    expect(isHealthyState("RUNNING")).toBe(true);
    expect(isHealthyState("PAUSED_AUTH")).toBe(false);
  });

  it("isFailureState", () => {
    expect(isFailureState("FAILED")).toBe(true);
    expect(isFailureState("ERROR")).toBe(true);
    expect(isFailureState("PAUSED_AUTH")).toBe(false);
  });

  it("isTransientState", () => {
    expect(isTransientState("CREATING")).toBe(true);
    expect(isTransientState("PAUSED_AUTH")).toBe(false);
  });
});

describe("extractClusterId", () => {
  it("strips clusters/ prefix", () => {
    expect(extractClusterId("clusters/my-cluster")).toBe("my-cluster");
  });

  it("returns unchanged if no prefix", () => {
    expect(extractClusterId("my-cluster")).toBe("my-cluster");
  });
});

describe("extractService", () => {
  it("extracts service from canonical name", () => {
    expect(extractService("//gcphcp.fleetshift.io/clusters/c1")).toBe(
      "gcphcp.fleetshift.io",
    );
  });

  it("returns empty for malformed name", () => {
    expect(extractService("no-match")).toBe("");
  });
});

describe("serviceLabel", () => {
  it("maps known services", () => {
    expect(serviceLabel("gcphcp.fleetshift.io")).toBe("GCP HCP");
    expect(serviceLabel("kind.fleetshift.io")).toBe("Kind");
  });

  it("falls back to raw service string", () => {
    expect(serviceLabel("custom.io")).toBe("custom.io");
  });
});

describe("buildAddonBasePath", () => {
  it("builds path for known services", () => {
    expect(buildAddonBasePath("gcphcp.fleetshift.io")).toBe(
      "/apis/gcphcp.fleetshift.io/v1",
    );
  });

  it("throws for unknown service", () => {
    expect(() => buildAddonBasePath("unknown.io")).toThrow(
      "Unsupported cluster service",
    );
  });
});
