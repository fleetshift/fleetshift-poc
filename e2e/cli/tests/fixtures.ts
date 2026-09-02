/* eslint-disable react-hooks/rules-of-hooks -- Playwright fixture `use` is not a React hook. */
import { mkdir } from "node:fs/promises";
import path from "node:path";

import {
  test as base,
  type TestDetails,
  type TestInfo,
} from "@playwright/test";

import { OPERATOR, type Persona } from "../../shared/personas";
import { DeploymentSteps } from "./steps/deployments";
import { GatewaySteps } from "./steps/gateway";
import { KindSteps } from "./steps/kind";
import { LoginSteps } from "./steps/login";
import { QuerySteps } from "./steps/query";
import { CleanupStack } from "./support/cleanup";
import { FleetctlClient } from "./support/fleetctl";
import {
  encodeKindClusterDetails,
  type KindClusterTestDetails,
  readKindClusterRequests,
} from "./support/kind-cluster-declaration";
import {
  type KindCluster,
  KindClusterPool,
  pooledKindClusterIdPrefix,
} from "./support/kind-pool";
import { readSandboxEnvironment, type Sandbox } from "./support/sandbox";

interface Suite {
  fleetctl: FleetctlClient;
  gateway: GatewaySteps;
  kind: KindSteps;
  kindPool: KindClusterPool;
  login: LoginSteps;
  loginAs: (persona: Persona) => Promise<string>;
  query: QuerySteps;
  sandbox: Sandbox;
}

interface KindResources {
  cleanup: CleanupStack;
  clusters: readonly KindCluster[];
}

interface Cli {
  deployments: DeploymentSteps;
  gateway: GatewaySteps;
  kind: KindSteps;
  login: LoginSteps;
  loginAs: (persona: Persona) => Promise<string>;
  query: QuerySteps;
}

type TestArgs = {
  cli: Cli;
  kindClusters: readonly KindCluster[];
  kindResources: KindResources;
};

type KindClusterTestBody = (
  args: { cli: Cli; kindClusters: readonly KindCluster[] },
  testInfo: TestInfo,
) => Promise<unknown> | unknown;

type KindClusterTestCallable = (
  title: string,
  details: KindClusterTestDetails,
  body: KindClusterTestBody,
) => void;

type PlaywrightDeclare = (
  title: string,
  details: TestDetails,
  body: KindClusterTestBody,
) => void;

const fixtures = base.extend<TestArgs, { suite: Suite }>({
  suite: [
    async ({ browser }, use, workerInfo) => {
      const shared = readSandboxEnvironment();
      const suffix = `worker-${workerInfo.parallelIndex}`;
      const workDir = path.join(shared.workDir, suffix);

      await mkdir(workDir, { mode: 0o700, recursive: true });

      const sandbox = {
        ...shared,
        workDir,
      };

      const configDir = path.join(workDir, "fleetctl-ops");
      await mkdir(configDir, { mode: 0o700, recursive: true });
      const fleetctl = new FleetctlClient({
        binary: path.resolve(process.cwd(), "../../bin/fleetctl"),
        browser,
        configDir,
        sandbox,
      });
      await fleetctl.login(OPERATOR);
      const kind = new KindSteps(fleetctl, sandbox);
      const kindPool = new KindClusterPool({
        directory: path.join(shared.workDir, "kind-pool"),
        idPrefix: pooledKindClusterIdPrefix(shared.kindIdPrefix),
      });
      await use({
        fleetctl,
        gateway: new GatewaySteps(fleetctl, sandbox),
        kind,
        kindPool,
        login: new LoginSteps(fleetctl, sandbox),
        loginAs: (persona) => fleetctl.loginAs(persona),
        query: new QuerySteps(fleetctl),
        sandbox,
      });
    },
    { scope: "worker", timeout: 25 * 60_000 },
  ],

  kindResources: [
    async ({ suite }, use, testInfo) => {
      const requests = readKindClusterRequests(testInfo.annotations);
      const cleanup = new CleanupStack();
      const reservation = await suite.kindPool.reserve(requests);
      let claimed = false;
      let testError: unknown;
      try {
        const pending = reservation.allocations.filter(
          (allocation) => allocation.needsProvisioning,
        );
        await Promise.all(
          pending.map(async ({ cluster }) => {
            await suite.kind.create(cluster.id);
            await suite.kind.waitUntilReady(cluster.id);
            await suite.kind.waitUntilAPIAcceptsToken(cluster.id);
          }),
        );
        const clusters = await suite.kindPool.activate(reservation);
        claimed = true;
        await use({ cleanup, clusters });
      } catch (error) {
        testError = error;
      } finally {
        let cleanupError: unknown;
        try {
          await cleanup.run();
        } catch (error) {
          cleanupError = error;
        }
        try {
          if (!claimed) {
            await suite.kindPool.release(reservation, "unused");
          } else {
            const reusable =
              testInfo.status === "passed" && cleanupError === undefined;
            await suite.kindPool.release(
              reservation,
              reusable ? "reusable" : "discarded",
            );
          }
        } catch (error) {
          cleanupError =
            cleanupError === undefined
              ? error
              : new AggregateError([cleanupError, error]);
        }
        if (testError && cleanupError) {
          throw new AggregateError([testError, cleanupError]);
        }
        if (cleanupError) throw cleanupError;
      }
      if (testError) throw testError;
    },
    { auto: true, timeout: 15 * 60_000 },
  ],

  kindClusters: async ({ kindResources }, use) => {
    await use(kindResources.clusters);
  },

  cli: async ({ suite, kindResources }, use) => {
    await use({
      deployments: new DeploymentSteps(
        suite.fleetctl,
        suite.sandbox,
        kindResources.cleanup,
      ),
      gateway: suite.gateway,
      kind: new KindSteps(suite.fleetctl, suite.sandbox, kindResources.cleanup),
      login: suite.login,
      loginAs: suite.loginAs,
      query: suite.query,
    });
  },
});

function wrapDeclare(declare: PlaywrightDeclare): KindClusterTestCallable {
  return (title, details, body) => {
    if (!("kindClusters" in details)) {
      throw new Error("test is missing a kindClusters declaration");
    }
    declare(title, encodeKindClusterDetails(details), body);
  };
}

type KindClusterTest = KindClusterTestCallable & {
  describe: typeof fixtures.describe;
  fixme: KindClusterTestCallable;
  only: KindClusterTestCallable;
  skip: KindClusterTestCallable;
  step: typeof fixtures.step;
};

export const test = Object.assign(
  wrapDeclare(fixtures as unknown as PlaywrightDeclare),
  fixtures,
  {
    fixme: wrapDeclare(fixtures.fixme as unknown as PlaywrightDeclare),
    only: wrapDeclare(fixtures.only as unknown as PlaywrightDeclare),
    skip: wrapDeclare(fixtures.skip as unknown as PlaywrightDeclare),
  },
) as KindClusterTest;

export { expect } from "@playwright/test";
