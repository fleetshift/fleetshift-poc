import { describe, expect, it } from "vitest";

import {
  createSearchResultRenderer,
  RENDER_SEARCH_TYPE,
} from "../searchResultRenderer";
import { validateExtensionSet } from "../validate";

describe("createSearchResultRenderer", () => {
  it("returns extension with static type and resourceType in properties", () => {
    const ext = createSearchResultRenderer({
      id: "kind-cluster-renderer",
      label: "Kind Cluster",
      resourceType: "kind.fleetshift.io/Cluster",
      resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
    });

    expect(ext.type).toBe(RENDER_SEARCH_TYPE);
    expect(ext.properties.id).toBe("kind-cluster-renderer");
    expect(ext.properties.label).toBe("Kind Cluster");
    expect(ext.properties.resourceType).toBe("kind.fleetshift.io/Cluster");
    expect(ext.properties.resolve).toEqual({
      $codeRef: "KindSearchResult.resolveKindCluster",
    });
  });

  it("includes optional component and icon CodeRefs", () => {
    const ext = createSearchResultRenderer({
      id: "kind-cluster-renderer",
      label: "Kind Cluster",
      resourceType: "kind.fleetshift.io/Cluster",
      resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
      component: { $codeRef: "KindSearchResult.KindResultComponent" },
      icon: { $codeRef: "KindSearchResult.KindClusterIcon" },
    });
    expect(ext.properties.component).toEqual({
      $codeRef: "KindSearchResult.KindResultComponent",
    });
    expect(ext.properties.icon).toEqual({
      $codeRef: "KindSearchResult.KindClusterIcon",
    });
  });

  it("omits optional fields when not provided", () => {
    const ext = createSearchResultRenderer({
      id: "kind-cluster-renderer",
      label: "Kind Cluster",
      resourceType: "kind.fleetshift.io/Cluster",
      resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
    });
    expect(ext.properties.component).toBeUndefined();
    expect(ext.properties.icon).toBeUndefined();
  });

  it("throws on invalid id", () => {
    expect(() =>
      createSearchResultRenderer({
        id: "BadId",
        label: "Bad",
        resourceType: "kind.fleetshift.io/Cluster",
        resolve: { $codeRef: "Mod.fn" },
      }),
    ).toThrow(/id/);
  });

  it("throws on missing label", () => {
    expect(() =>
      createSearchResultRenderer({
        id: "good-id",
        label: "",
        resourceType: "kind.fleetshift.io/Cluster",
        resolve: { $codeRef: "Mod.fn" },
      }),
    ).toThrow(/label/);
  });

  it("throws on invalid resolve CodeRef", () => {
    expect(() =>
      createSearchResultRenderer({
        id: "good-id",
        label: "Good",
        resourceType: "kind.fleetshift.io/Cluster",
        resolve: { $codeRef: "bad-coderef" },
      }),
    ).toThrow(/resolve/);
  });

  it("throws on missing resourceType", () => {
    expect(() =>
      createSearchResultRenderer({
        id: "good-id",
        label: "Good",
        resourceType: "",
        resolve: { $codeRef: "Mod.fn" },
      }),
    ).toThrow(/resourceType/);
  });
});

describe("validateExtensionSet with search result renderer", () => {
  it("passes for valid renderer alongside other extensions", () => {
    const ext = createSearchResultRenderer({
      id: "kind-cluster-renderer",
      label: "Kind Cluster",
      resourceType: "kind.fleetshift.io/Cluster",
      resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
      icon: { $codeRef: "KindSearchResult.KindClusterIcon" },
    });

    expect(() =>
      validateExtensionSet([ext], {
        KindSearchResult: "./src/KindSearchResult.tsx",
      }),
    ).not.toThrow();
  });

  it("catches unreferenced module in CodeRef", () => {
    const ext = createSearchResultRenderer({
      id: "kind-cluster-renderer",
      label: "Kind Cluster",
      resourceType: "kind.fleetshift.io/Cluster",
      resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
    });

    expect(() =>
      validateExtensionSet([ext], {
        SomeOtherModule: "./src/Other.tsx",
      }),
    ).toThrow(/KindSearchResult/);
  });
});
