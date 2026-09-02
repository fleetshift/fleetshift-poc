/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  type KindClusterCondition,
  type KindClusterRequest,
  matchKindClusterRequests,
  nextKindClusterCondition,
} from "./kind-pool";

interface AvailableCluster {
  id: string;
  condition: KindClusterCondition;
}

describe("kind cluster matching policy", () => {
  it.each([
    {
      name: "read-only + clean matches clean only and stays clean",
      available: [
        { id: "modified", condition: "modified" as const },
        { id: "clean", condition: "clean" as const },
      ],
      requests: [{ access: "read-only" as const, state: "clean" as const }],
      assigned: ["clean"],
      after: ["clean"],
    },
    {
      name: "read-only + any prefers modified and leaves it unmodified",
      available: [
        { id: "clean", condition: "clean" as const },
        { id: "modified", condition: "modified" as const },
      ],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["modified"],
      after: ["modified"],
    },
    {
      name: "read-only + any falls back to clean and leaves it clean",
      available: [{ id: "clean", condition: "clean" as const }],
      requests: [{ access: "read-only" as const, state: "any" as const }],
      assigned: ["clean"],
      after: ["clean"],
    },
    {
      name: "modifiable + clean matches clean only and becomes modified",
      available: [
        { id: "modified", condition: "modified" as const },
        { id: "clean", condition: "clean" as const },
      ],
      requests: [{ access: "modifiable" as const, state: "clean" as const }],
      assigned: ["clean"],
      after: ["modified"],
    },
    {
      name: "modifiable + any prefers modified and stays modified",
      available: [
        { id: "clean", condition: "clean" as const },
        { id: "modified", condition: "modified" as const },
      ],
      requests: [{ access: "modifiable" as const, state: "any" as const }],
      assigned: ["modified"],
      after: ["modified"],
    },
    {
      name: "modifiable + any falls back to clean and becomes modified",
      available: [{ id: "clean", condition: "clean" as const }],
      requests: [{ access: "modifiable" as const, state: "any" as const }],
      assigned: ["clean"],
      after: ["modified"],
    },
    {
      name: "clean-state requests are satisfied before any-state requests",
      available: [{ id: "only-clean", condition: "clean" as const }],
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
        { id: "clean-a", condition: "clean" as const },
        { id: "modified", condition: "modified" as const },
        { id: "clean-b", condition: "clean" as const },
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
      available: [{ id: "modified", condition: "modified" as const }],
      requests: [{ access: "read-only" as const, state: "clean" as const }],
      assigned: [undefined],
      after: [undefined],
    },
  ] satisfies Array<{
    name: string;
    available: AvailableCluster[];
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
