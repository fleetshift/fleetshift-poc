import { describe, expect, it } from "vitest";

import type {
  ClusterProviderProperties,
  FleetshiftExtension,
  ModuleGroupProperties,
  ModuleProperties,
  OnboardingActionProperties,
  SetupProperties,
} from "../types";
import {
  validateClusterProviderProperties,
  validateCodeRef,
  validateExtensionSet,
  validateModuleGroupProperties,
  validateModuleProperties,
  validateOnboardingActionProperties,
  validateSetupProperties,
} from "../validate";

const validCodeRef = { $codeRef: "SomeModule.default" };

const validModule: ModuleProperties = {
  id: "clusters",
  label: "Clusters",
  component: validCodeRef,
  icon: validCodeRef,
};

const validSetup: SetupProperties = {
  id: "auth-setup",
  label: "Authentication",
  path: "auth",
  component: validCodeRef,
  requires: [],
  requiresAuth: false,
};

const validClusterProvider: ClusterProviderProperties = {
  id: "gcphcp",
  label: "GCP HCP",
  description: "Create a GCP cluster",
  icon: { $codeRef: "Card.Icon" },
  card: { $codeRef: "Card.default" },
  wizard: { $codeRef: "Wizard.default" },
};

describe("validateCodeRef", () => {
  it("accepts valid CodeRef", () => {
    expect(validateCodeRef(validCodeRef, "component", "test")).toEqual([]);
  });

  it("accepts named export CodeRef", () => {
    expect(
      validateCodeRef({ $codeRef: "MyModule.MyExport" }, "icon", "test"),
    ).toEqual([]);
  });

  it("rejects non-object", () => {
    const errors = validateCodeRef("not-a-ref", "component", "test");
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("must be a CodeRef");
  });

  it("rejects missing $codeRef", () => {
    const errors = validateCodeRef({ foo: "bar" }, "component", "test");
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("must be a CodeRef");
  });

  it("rejects CodeRef without dot separator", () => {
    const errors = validateCodeRef(
      { $codeRef: "NoDotHere" },
      "component",
      "test",
    );
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("invalid $codeRef");
  });

  it("rejects CodeRef with empty module name", () => {
    const errors = validateCodeRef(
      { $codeRef: ".default" },
      "component",
      "test",
    );
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("invalid $codeRef");
  });

  it("rejects CodeRef with empty export name", () => {
    const errors = validateCodeRef(
      { $codeRef: "Module." },
      "component",
      "test",
    );
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain("invalid $codeRef");
  });
});

const validModuleGroup: ModuleGroupProperties = {
  id: "settings",
  label: "Settings",
};

describe("validateModuleGroupProperties", () => {
  it("accepts valid module group", () => {
    expect(validateModuleGroupProperties(validModuleGroup)).toEqual([]);
  });

  it("rejects empty id", () => {
    const errors = validateModuleGroupProperties({
      ...validModuleGroup,
      id: "",
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("id");
  });

  it("rejects uppercase id", () => {
    const errors = validateModuleGroupProperties({
      ...validModuleGroup,
      id: "BadId",
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("lowercase");
  });
});

describe("validateModuleProperties", () => {
  it("accepts valid module", () => {
    expect(validateModuleProperties(validModule)).toEqual([]);
  });

  it("accepts module with optional search fields", () => {
    const errors = validateModuleProperties({
      ...validModule,
      searchResult: { $codeRef: "SearchResult.default" },
      searchIcon: { $codeRef: "SearchIcon.default" },
    });
    expect(errors).toEqual([]);
  });

  it("rejects empty id", () => {
    const errors = validateModuleProperties({ ...validModule, id: "" });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("id");
  });

  it("rejects uppercase id", () => {
    const errors = validateModuleProperties({
      ...validModule,
      id: "BadId",
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("lowercase");
  });

  it("rejects empty label", () => {
    const errors = validateModuleProperties({ ...validModule, label: "" });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("label");
  });

  it("rejects invalid component CodeRef", () => {
    const errors = validateModuleProperties({
      ...validModule,
      component: { $codeRef: "invalid" },
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("component");
  });

  it("rejects invalid icon CodeRef", () => {
    const errors = validateModuleProperties({
      ...validModule,
      icon: { $codeRef: "invalid" },
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("icon");
  });

  it("validates searchResult CodeRef format", () => {
    const errors = validateModuleProperties({
      ...validModule,
      searchResult: { $codeRef: "bad" },
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("searchResult");
  });
});

describe("validateSetupProperties", () => {
  it("accepts valid setup", () => {
    expect(validateSetupProperties(validSetup)).toEqual([]);
  });

  it("rejects missing path", () => {
    const errors = validateSetupProperties({
      ...validSetup,
      path: "",
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("path");
  });

  it("rejects non-array requires", () => {
    const errors = validateSetupProperties({
      ...validSetup,
      requires: "not-an-array" as unknown as string[],
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("requires");
  });

  it("rejects invalid requires entries", () => {
    const errors = validateSetupProperties({
      ...validSetup,
      requires: ["valid-id", "BAD_ID"],
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("requires");
  });

  it("rejects non-boolean requiresAuth", () => {
    const errors = validateSetupProperties({
      ...validSetup,
      requiresAuth: "yes" as unknown as boolean,
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("requiresAuth");
  });

  it("collects multiple errors at once", () => {
    const errors = validateSetupProperties({
      ...validSetup,
      id: "",
      label: "",
      path: "",
    });
    expect(errors.length).toBeGreaterThan(2);
  });
});

describe("validateClusterProviderProperties", () => {
  it("accepts valid cluster provider", () => {
    expect(validateClusterProviderProperties(validClusterProvider)).toEqual([]);
  });

  it("rejects missing description", () => {
    const errors = validateClusterProviderProperties({
      ...validClusterProvider,
      description: "",
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("description");
  });

  it("validates all three required CodeRefs", () => {
    const errors = validateClusterProviderProperties({
      ...validClusterProvider,
      icon: { $codeRef: "bad" },
      card: { $codeRef: "bad" },
      wizard: { $codeRef: "bad" },
    });
    expect(errors.length).toBe(3);
    expect(errors[0]).toContain("icon");
    expect(errors[1]).toContain("card");
    expect(errors[2]).toContain("wizard");
  });
});

const validOnboardingAction: OnboardingActionProperties = {
  id: "gcphcp-connect",
  label: "GCP HCP",
  icon: { $codeRef: "Icon.default" },
  card: { $codeRef: "Card.default" },
  form: { $codeRef: "Form.default" },
};

describe("validateOnboardingActionProperties", () => {
  it("accepts valid onboarding action", () => {
    expect(validateOnboardingActionProperties(validOnboardingAction)).toEqual(
      [],
    );
  });

  it("accepts onboarding action with overviewCta", () => {
    expect(
      validateOnboardingActionProperties({
        ...validOnboardingAction,
        overviewCta: "Integrate your first addon",
      }),
    ).toEqual([]);
  });

  it("accepts onboarding action without overviewCta", () => {
    const { ...withoutCta } = validOnboardingAction;
    expect(validateOnboardingActionProperties(withoutCta)).toEqual([]);
  });

  it("validates all three required CodeRefs", () => {
    const errors = validateOnboardingActionProperties({
      ...validOnboardingAction,
      icon: { $codeRef: "bad" },
      card: { $codeRef: "bad" },
      form: { $codeRef: "bad" },
    });
    expect(errors.length).toBe(3);
    expect(errors[0]).toContain("icon");
    expect(errors[1]).toContain("card");
    expect(errors[2]).toContain("form");
  });
});

describe("validateExtensionSet", () => {
  const exposedModules = {
    SomeModule: "./src/SomeModule.tsx",
    Card: "./src/Card.tsx",
    Wizard: "./src/Wizard.tsx",
  };

  it("accepts valid extension set", () => {
    expect(() =>
      validateExtensionSet(
        [{ type: "fleetshift.module", properties: validModule }],
        exposedModules,
      ),
    ).not.toThrow();
  });

  it("detects duplicate extension IDs", () => {
    expect(() =>
      validateExtensionSet(
        [
          { type: "fleetshift.module", properties: validModule },
          {
            type: "fleetshift.module",
            properties: { ...validModule, label: "Different" },
          },
        ],
        exposedModules,
      ),
    ).toThrow(/Duplicate extension ID "clusters"/);
  });

  it("detects CodeRef referencing missing exposed module", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              component: { $codeRef: "MissingModule.default" },
            },
          },
        ],
        exposedModules,
      ),
    ).toThrow(/MissingModule.*not in exposedModules/);
  });

  it("allows custom extension types with base validation", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "acme.custom-widget" as unknown as FleetshiftExtension["type"],
            properties: { id: "my-widget", label: "My Widget" },
          },
        ],
        {},
      ),
    ).not.toThrow();
  });

  it("validates base properties on custom extension types", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "acme.custom" as unknown as FleetshiftExtension["type"],
            properties: { id: "", label: "" },
          },
        ],
        {},
      ),
    ).toThrow(/id.*required/);
  });

  it("handles null properties gracefully", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "fleetshift.module",
            properties: null as unknown as FleetshiftExtension["properties"],
          },
        ],
        {},
      ),
    ).toThrow(/properties.*must be an object/);
  });

  it("accepts module group with grouped modules", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "fleetshift.module-group",
            properties: validModuleGroup,
          },
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              id: "navigation",
              group: "settings",
            },
          },
        ],
        exposedModules,
      ),
    ).not.toThrow();
  });

  it("rejects module referencing non-existent group", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              id: "navigation",
              group: "nonexistent",
            },
          },
        ],
        exposedModules,
      ),
    ).toThrow(/references group "nonexistent" which does not exist/);
  });

  it("rejects duplicate module ids within the same group", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "fleetshift.module-group",
            properties: validModuleGroup,
          },
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              id: "nav",
              label: "First",
              group: "settings",
            },
          },
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              id: "nav",
              label: "Second",
              group: "settings",
            },
          },
        ],
        exposedModules,
      ),
    ).toThrow(/Duplicate module id "nav" within group "settings"/);
  });

  it("allows same module id in different groups", () => {
    expect(() =>
      validateExtensionSet(
        [
          {
            type: "fleetshift.module-group",
            properties: { ...validModuleGroup, id: "group-a" },
          },
          {
            type: "fleetshift.module-group",
            properties: {
              ...validModuleGroup,
              id: "group-b",
              label: "Group B",
            },
          },
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              id: "overview",
              label: "Overview A",
              group: "group-a",
            },
          },
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              id: "overview",
              label: "Overview B",
              group: "group-b",
            },
          },
        ],
        exposedModules,
      ),
    ).not.toThrow();
  });

  it("reports all errors in a single throw", () => {
    try {
      validateExtensionSet(
        [
          {
            type: "fleetshift.module",
            properties: {
              ...validModule,
              id: "",
              label: "",
              component: { $codeRef: "Missing.default" },
            },
          },
        ],
        exposedModules,
      );
      expect.unreachable("should have thrown");
    } catch (e) {
      const msg = (e as Error).message;
      expect(msg).toContain("id");
      expect(msg).toContain("label");
      expect(msg).toContain("Missing");
    }
  });
});
