/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";

test(
  "resource types and Kind cluster describe expose stable schema identifiers",
  { kindClusters: [] },
  async ({ cli }) => {
    const { resources } = cli;
    await test.step("list resource types and find Kind Cluster and Kind Node", () =>
      resources.typesIncludeKindClusterAndNode());
    await test.step("describe clusters and verify methods, spec type, and nested fields", () =>
      resources.expectKindClusterSchema());
  },
);
