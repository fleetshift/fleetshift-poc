/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";

test(
  "resource query scopes, paginates, and returns both node types",
  {
    kindClusters: [
      { access: "read-only", state: "any" },
      { access: "read-only", state: "any" },
    ],
  },
  async ({ cli, kindClusters: [first, second] }) => {
    const { query } = cli;
    await test.step("find indexed Kubernetes objects on both clusters", () =>
      Promise.all([
        query.indexedKubernetesObjectsExist(first.id),
        query.indexedKubernetesObjectsExist(second.id),
      ]));
    await test.step("match Kind cluster query output to resource get", () =>
      query.kindClusterQueryMatchesGet(first.id));
    await test.step("paginate Kubernetes objects without duplicates", () =>
      query.kubernetesObjectQueryPaginates(first.id));
    await test.step("find both Kind and Kubernetes node types", () =>
      query.bothNodeTypesAreIndexed(first.id));
  },
);
