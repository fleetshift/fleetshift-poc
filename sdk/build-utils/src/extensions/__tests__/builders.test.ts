import { describe, expect, it } from "vitest";

import {
  createClusterProvider,
  createOnboardingAction,
  createRoute,
  createRouteGroup,
  createSetup,
} from "../builders";

describe("createRouteGroup", () => {
  it("returns extension with correct type", () => {
    const ext = createRouteGroup({
      id: "settings",
      label: "Settings",
    });
    expect(ext.type).toBe("ocm-fleet.route-group");
    expect(ext.properties.id).toBe("settings");
  });

  it("preserves optional fields", () => {
    const ext = createRouteGroup({
      id: "settings",
      label: "Settings",
      description: "Workspace settings",
      keywords: ["settings", "preferences"],
    });
    expect(ext.properties.description).toBe("Workspace settings");
    expect(ext.properties.keywords).toEqual(["settings", "preferences"]);
  });

  it("throws on invalid id", () => {
    expect(() =>
      createRouteGroup({
        id: "BadId",
        label: "Bad",
      }),
    ).toThrow(/id/);
  });
});

describe("createRoute", () => {
  it("returns extension with correct type", () => {
    const ext = createRoute({
      id: "clusters",
      label: "Clusters",
      component: { $codeRef: "ClustersModule.default" },
      icon: { $codeRef: "ClustersIcon.default" },
    });
    expect(ext.type).toBe("ocm-fleet.route");
    expect(ext.properties.id).toBe("clusters");
    expect(ext.properties.component).toEqual({
      $codeRef: "ClustersModule.default",
    });
    expect(ext.properties.icon).toEqual({
      $codeRef: "ClustersIcon.default",
    });
  });

  it("preserves optional fields", () => {
    const ext = createRoute({
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
          type: "ocm-fleet.cluster-provider",
        },
      },
    });
    expect(ext.properties.description).toBe("Manage clusters");
    expect(ext.properties.keywords).toEqual(["cluster", "fleet"]);
    expect(ext.properties.searchResult).toEqual({
      $codeRef: "SearchResult.default",
    });
    expect(ext.properties.extensionPoints?.providers.type).toBe(
      "ocm-fleet.cluster-provider",
    );
  });

  it("throws on invalid properties", () => {
    expect(() =>
      createRoute({
        id: "",
        label: "X",
        component: { $codeRef: "M.default" },
        icon: { $codeRef: "I.default" },
      }),
    ).toThrow(/id/);
  });

  it("preserves optional group field", () => {
    const ext = createRoute({
      id: "navigation",
      label: "Navigation",
      component: { $codeRef: "ClustersModule.default" },
      icon: { $codeRef: "ClustersIcon.default" },
      group: "settings",
    });
    expect(ext.properties.group).toBe("settings");
  });

  it("omits group when not provided", () => {
    const ext = createRoute({
      id: "clusters",
      label: "Clusters",
      component: { $codeRef: "ClustersModule.default" },
      icon: { $codeRef: "ClustersIcon.default" },
    });
    expect(ext.properties.group).toBeUndefined();
  });

  it("throws on invalid CodeRef", () => {
    expect(() =>
      createRoute({
        id: "test",
        label: "Test",
        component: { $codeRef: "bad" },
        icon: { $codeRef: "I.default" },
      }),
    ).toThrow(/component/);
  });

  it("throws on invalid icon CodeRef", () => {
    expect(() =>
      createRoute({
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
    expect(ext.type).toBe("ocm-fleet.setup");
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
      id: "kind",
      label: "Kind",
      description: "Create a local Kind cluster",
      icon: { $codeRef: "Card.KindIcon" },
      card: { $codeRef: "Card.default" },
      wizard: { $codeRef: "Wizard.default" },
    });
    expect(ext.type).toBe("ocm-fleet.cluster-provider");
    expect(ext.properties.icon).toEqual({ $codeRef: "Card.KindIcon" });
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
      id: "kind-connect",
      label: "Kind",
      icon: { $codeRef: "Icon.default" },
      card: { $codeRef: "Card.default" },
      form: { $codeRef: "Form.default" },
    });
    expect(ext.type).toBe("ocm-fleet.onboarding-action");
    expect(ext.properties.id).toBe("kind-connect");
    expect(ext.properties.card).toEqual({ $codeRef: "Card.default" });
    expect(ext.properties.form).toEqual({ $codeRef: "Form.default" });
  });

  it("preserves optional overviewCta", () => {
    const ext = createOnboardingAction({
      id: "kind-connect",
      label: "Kind",
      icon: { $codeRef: "Icon.default" },
      card: { $codeRef: "Card.default" },
      form: { $codeRef: "Form.default" },
      overviewCta: "Integrate your first addon",
    });
    expect(ext.properties.overviewCta).toBe("Integrate your first addon");
  });

  it("omits overviewCta when not provided", () => {
    const ext = createOnboardingAction({
      id: "kind-connect",
      label: "Kind",
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
