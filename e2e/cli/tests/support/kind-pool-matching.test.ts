/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  type AvailableKindCluster,
  type KindClusterCondition,
  type KindClusterRequest,
  matchKindClusterRequests,
  nextKindClusterCondition,
} from "./kind-pool";
import { type KindClusterCreateSpec } from "./kind-spec";

const MULTI_NODE: KindClusterCreateSpec = {
  nodes: [{ role: "control-plane" }, { role: "worker" }],
};

describe("kind cluster matching policy", () => {
  it.each([
    {
      name: "read-only + clean matches clean only and stays clean",
      available: [
        { id: "modified", condition: "modified" as const, spec: {} },
        { id: "clean", condition: "clean" as const, spec: {} },
      ],
      requests: [{ access: "read-only" as const, state: "clean" as const }],
      assigned: ["clean"],
      after: ["clean"],
    },
    {
      name: "read-only + any prefers modified and leaves it unmodified",
      available: [
        { id: "clean", condition: "clean" as const, spec: {} },
        { id: "modified", condition: "modified" as const, spec: {} },
      ],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["modified"],
      after: ["modified"],
    },
    {
      name: "read-only + any falls back to clean and leaves it clean",
      available: [{ id: "clean", condition: "clean" as const, spec: {} }],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["clean"],
      after: ["clean"],
    },
    {
      name: "modifiable + clean matches clean only and becomes modified",
      available: [
        { id: "modified", condition: "modified" as const, spec: {} },
        { id: "clean", condition: "clean" as const, spec: {} },
      ],
      requests: [{ access: "modifiable" as const, state: "clean" as const }],
      assigned: ["clean"],
      after: ["modified"],
    },
    {
      name: "modifiable + any prefers modified and stays modified",
      available: [
        { id: "clean", condition: "clean" as const, spec: {} },
        { id: "modified", condition: "modified" as const, spec: {} },
      ],
      requests: [{ access: "modifiable" as const, state: "any" as const }],
      assigned: ["modified"],
      after: ["modified"],
    },
    {
      name: "modifiable + any falls back to clean and becomes modified",
      available: [{ id: "clean", condition: "clean" as const, spec: {} }],
      requests: [{ access: "modifiable" as const, state: "any" as const }],
      assigned: ["clean"],
      after: ["modified"],
    },
    {
      name: "clean-state requests are satisfied before any-state requests",
      available: [{ id: "only-clean", condition: "clean" as const, spec: {} }],
      requests: [
        { access: "read-only" as const, state: "any" as const },
        { access: "read-only" as const, state: "clean" as const },
      ],
      assigned: [undefined, "only-clean"],
      after: [undefined, "clean"],
    },
    {
      name: "any-state requests consume modified clusters before remaining clean ones",
      available: [
        { id: "clean-a", condition: "clean" as const, spec: {} },
        { id: "modified", condition: "modified" as const, spec: {} },
        { id: "clean-b", condition: "clean" as const, spec: {} },
      ],
      requests: [
        { access: "read-only" as const, state: "any" as const },
        { access: "modifiable" as const, state: "any" as const },
      ],
      assigned: ["modified", "clean-a"],
      after: ["modified", "modified"],
    },
    {
      name: "clean-only requests do not take modified clusters",
      available: [{ id: "modified", condition: "modified" as const, spec: {} }],
      requests: [{ access: "read-only" as const, state: "clean" as const }],
      assigned: [undefined],
      after: [undefined],
    },
  ] satisfies Array<{
    name: string;
    available: AvailableKindCluster[];
    requests: KindClusterRequest[];
    assigned: Array<string | undefined>;
    after: Array<KindClusterCondition | undefined>;
  }>)("$name", ({ available, requests, assigned, after }) => {
    const matched = matchKindClusterRequests(available, requests);
    expect([...matched]).toEqual(assigned);
    expect(
      matched.map((id, index) => {
        if (id === undefined) return undefined;
        const cluster = available.find((item) => item.id === id);
        if (!cluster) throw new Error(`matched unknown cluster ${id}`);
        return nextKindClusterCondition(cluster.condition, requests[index]);
      }),
    ).toEqual(after);
  });
});

describe("kind cluster spec matching", () => {
  it.each([
    {
      name: "a pinned spec only matches a cluster created with that spec",
      available: [
        { id: "default", condition: "clean" as const, spec: {} },
        { id: "multi", condition: "clean" as const, spec: MULTI_NODE },
      ],
      requests: [
        {
          access: "read-only" as const,
          state: "any" as const,
          spec: MULTI_NODE,
        },
      ],
      assigned: ["multi"],
    },
    {
      name: "spec {} pins default and does not take a non-default cluster",
      available: [
        { id: "multi", condition: "clean" as const, spec: MULTI_NODE },
      ],
      requests: [
        { access: "read-only" as const, state: "any" as const, spec: {} },
      ],
      assigned: [undefined],
    },
    {
      name: "omitted spec matches a non-default cluster",
      available: [
        { id: "multi", condition: "clean" as const, spec: MULTI_NODE },
      ],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["multi"],
    },
    {
      name: "omitted spec prefers non-default when condition is equal",
      available: [
        { id: "default", condition: "clean" as const, spec: {} },
        { id: "multi", condition: "clean" as const, spec: MULTI_NODE },
      ],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["multi"],
    },
    {
      name: "omitted spec still prefers modified default over clean non-default",
      available: [
        { id: "default-mod", condition: "modified" as const, spec: {} },
        { id: "multi", condition: "clean" as const, spec: MULTI_NODE },
      ],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["default-mod"],
    },
    {
      name: "omitted spec prefers modified non-default over modified default",
      available: [
        { id: "default", condition: "modified" as const, spec: {} },
        { id: "multi", condition: "modified" as const, spec: MULTI_NODE },
      ],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["multi"],
    },
    {
      name: "specific-spec slots are filled before omitted-spec slots",
      available: [
        { id: "multi", condition: "modified" as const, spec: MULTI_NODE },
      ],
      requests: [
        { access: "read-only" as const, state: "any" as const },
        {
          access: "read-only" as const,
          state: "any" as const,
          spec: MULTI_NODE,
        },
      ],
      assigned: [undefined, "multi"],
    },
    {
      name: "specific-spec clean is filled before omitted clean",
      available: [
        { id: "multi", condition: "clean" as const, spec: MULTI_NODE },
      ],
      requests: [
        { access: "read-only" as const, state: "clean" as const },
        {
          access: "read-only" as const,
          state: "clean" as const,
          spec: MULTI_NODE,
        },
      ],
      assigned: [undefined, "multi"],
    },
  ] satisfies Array<{
    name: string;
    available: AvailableKindCluster[];
    requests: KindClusterRequest[];
    assigned: Array<string | undefined>;
  }>)("$name", ({ available, requests, assigned }) => {
    expect([...matchKindClusterRequests(available, requests)]).toEqual(
      assigned,
    );
  });
});
