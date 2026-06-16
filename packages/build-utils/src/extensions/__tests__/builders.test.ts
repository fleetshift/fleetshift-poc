import { describe, expect, it } from "vitest";

import {
  createClusterProvider,
  createModule,
  createOnboardingAction,
  createSetup,
} from "../builders";

describe("createModule", () => {
  it("returns extension with correct type", () => {
    const ext = createModule({
      id: "clusters",
      label: "Clusters",
      component: { $codeRef: "ClustersModule.default" },
      icon: { $codeRef: "ClustersIcon.default" },
    });
    expect(ext.type).toBe("fleetshift.module");
    expect(ext.properties.id).toBe("clusters");
    expect(ext.properties.component).toEqual({
      $codeRef: "ClustersModule.default",
    });
    expect(ext.properties.icon).toEqual({
      $codeRef: "ClustersIcon.default",
    });
  });

  it("preserves optional fields", () => {
    const ext = createModule({
      id: "clusters",
      label: "Clusters",
      component: { $codeRef: "ClustersModule.default" },
      icon: { $codeRef: "ClustersIcon.default" },
      description: "Manage clusters",
      keywords: ["cluster", "fleet"],
      searchResult: { $codeRef: "SearchResult.default" },
      extensionPoints: {
        providers: {
          description: "Cluster providers",
          type: "fleetshift.cluster-provider",
        },
      },
    });
    expect(ext.properties.description).toBe("Manage clusters");
    expect(ext.properties.keywords).toEqual(["cluster", "fleet"]);
    expect(ext.properties.searchResult).toEqual({
      $codeRef: "SearchResult.default",
    });
    expect(ext.properties.extensionPoints?.providers.type).toBe(
      "fleetshift.cluster-provider",
    );
  });

  it("throws on invalid properties", () => {
    expect(() =>
      createModule({
        id: "",
        label: "X",
        component: { $codeRef: "M.default" },
        icon: { $codeRef: "I.default" },
      }),
    ).toThrow(/id/);
  });

  it("throws on invalid CodeRef", () => {
    expect(() =>
      createModule({
        id: "test",
        label: "Test",
        component: { $codeRef: "bad" },
        icon: { $codeRef: "I.default" },
      }),
    ).toThrow(/component/);
  });

  it("throws on invalid icon CodeRef", () => {
    expect(() =>
      createModule({
        id: "test",
        label: "Test",
        component: { $codeRef: "M.default" },
        icon: { $codeRef: "bad" },
      }),
    ).toThrow(/icon/);
  });
});

describe("createSetup", () => {
  it("returns extension with correct type", () => {
    const ext = createSetup({
      id: "auth-setup",
      label: "Authentication",
      path: "auth",
      component: { $codeRef: "AuthForm.default" },
      requires: [],
      requiresAuth: false,
    });
    expect(ext.type).toBe("fleetshift.setup");
    expect(ext.properties.path).toBe("auth");
    expect(ext.properties.requires).toEqual([]);
    expect(ext.properties.requiresAuth).toBe(false);
  });

  it("throws on missing path", () => {
    expect(() =>
      createSetup({
        id: "bad",
        label: "Bad",
        path: "",
        component: { $codeRef: "M.default" },
        requires: [],
        requiresAuth: false,
      }),
    ).toThrow(/path/);
  });

  it("throws on non-array requires", () => {
    expect(() =>
      createSetup({
        id: "bad",
        label: "Bad",
        path: "x",
        component: { $codeRef: "M.default" },
        requires: "oops" as unknown as string[],
        requiresAuth: false,
      }),
    ).toThrow(/requires/);
  });
});

describe("createClusterProvider", () => {
  it("returns extension with correct type", () => {
    const ext = createClusterProvider({
      id: "gcphcp",
      label: "GCP HCP",
      description: "Create a GCP cluster",
      icon: { $codeRef: "Card.GcpIcon" },
      card: { $codeRef: "Card.default" },
      wizard: { $codeRef: "Wizard.default" },
    });
    expect(ext.type).toBe("fleetshift.cluster-provider");
    expect(ext.properties.icon).toEqual({ $codeRef: "Card.GcpIcon" });
  });

  it("throws on missing description", () => {
    expect(() =>
      createClusterProvider({
        id: "bad",
        label: "Bad",
        description: "",
        icon: { $codeRef: "I.default" },
        card: { $codeRef: "C.default" },
        wizard: { $codeRef: "W.default" },
      }),
    ).toThrow(/description/);
  });

  it("throws on invalid icon CodeRef", () => {
    expect(() =>
      createClusterProvider({
        id: "bad",
        label: "Bad",
        description: "A provider",
        icon: { $codeRef: "nope" },
        card: { $codeRef: "C.default" },
        wizard: { $codeRef: "W.default" },
      }),
    ).toThrow(/icon/);
  });
});

describe("createOnboardingAction", () => {
  it("returns extension with correct type", () => {
    const ext = createOnboardingAction({
      id: "gcphcp-connect",
      label: "GCP HCP",
      icon: { $codeRef: "Icon.default" },
      card: { $codeRef: "Card.default" },
      form: { $codeRef: "Form.default" },
    });
    expect(ext.type).toBe("fleetshift.onboarding-action");
    expect(ext.properties.id).toBe("gcphcp-connect");
    expect(ext.properties.card).toEqual({ $codeRef: "Card.default" });
    expect(ext.properties.form).toEqual({ $codeRef: "Form.default" });
  });

  it("preserves optional overviewCta", () => {
    const ext = createOnboardingAction({
      id: "gcphcp-connect",
      label: "GCP HCP",
      icon: { $codeRef: "Icon.default" },
      card: { $codeRef: "Card.default" },
      form: { $codeRef: "Form.default" },
      overviewCta: "Integrate your first addon",
    });
    expect(ext.properties.overviewCta).toBe("Integrate your first addon");
  });

  it("omits overviewCta when not provided", () => {
    const ext = createOnboardingAction({
      id: "gcphcp-connect",
      label: "GCP HCP",
      icon: { $codeRef: "Icon.default" },
      card: { $codeRef: "Card.default" },
      form: { $codeRef: "Form.default" },
    });
    expect(ext.properties.overviewCta).toBeUndefined();
  });

  it("throws on invalid form CodeRef", () => {
    expect(() =>
      createOnboardingAction({
        id: "bad",
        label: "Bad",
        icon: { $codeRef: "I.default" },
        card: { $codeRef: "C.default" },
        form: { $codeRef: "nope" },
      }),
    ).toThrow(/form/);
  });
});
