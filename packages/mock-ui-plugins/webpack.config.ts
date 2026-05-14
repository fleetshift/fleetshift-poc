/* eslint-disable @typescript-eslint/ban-ts-comment */
import path from "path";
import {
  ModuleFederationPlugin as BaseMFPlugin,
  ContainerPlugin,
} from "@module-federation/enhanced";
import { DynamicRemotePlugin } from "@openshift/dynamic-plugin-sdk-webpack";
import { getDynamicModules, createTsLoaderRule } from "@fleetshift/build-utils";
import type { Configuration } from "webpack";
import { PluginRegistryPlugin } from "./src/PluginRegistryPlugin";

const monorepoRoot = path.resolve(__dirname, "../..");
const nodeModulesRoot = path.resolve(monorepoRoot, "node_modules");
const pfSharedModules = getDynamicModules(__dirname, monorepoRoot);
const tsLoaderRule = {
  ...createTsLoaderRule({ nodeModulesRoot }),
  exclude: [/node_modules/, /packages\/common\/dist/],
};

const sharedModules = {
  react: { singleton: true, requiredVersion: "*" },
  "@fleetshift/common": {
    requiredVersion: "*",
    version: "*",
  },
  "react-dom": { singleton: true, requiredVersion: "*" },
  "@scalprum/core": { singleton: true, requiredVersion: "*" },
  "@scalprum/react-core": { singleton: true, requiredVersion: "*" },
  "@openshift/dynamic-plugin-sdk": {
    singleton: true,
    requiredVersion: "*",
    version: "*",
  },
  "react-router-dom": { singleton: true, requiredVersion: "*" },
  ...pfSharedModules,
};

// Wrap MF plugin to disable federated type generation (dts-plugin crashes in Docker)
class ModuleFederationPlugin extends BaseMFPlugin {
  constructor(options: ConstructorParameters<typeof BaseMFPlugin>[0]) {
    super({ ...options, dts: false });
  }
}

// @ts-ignore — @module-federation/enhanced types differ from SDK expectations
const mfOverride = {
  libraryType: "global",
  pluginOverride: {
    ModuleFederationPlugin,
    ContainerPlugin,
  },
};

const p = (rel: string) => path.resolve(__dirname, rel);

const ManagementPlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        label: "Targets",
        component: { $codeRef: "TargetsPage.default" },
      },
    },
    {
      type: "fleetshift.module",
      properties: {
        label: "Orchestration",
        component: { $codeRef: "DeploymentsPage.default" },
      },
    },
    {
      type: "fleetshift.module",
      properties: {
        label: "Signing Keys",
        component: { $codeRef: "SigningKeyPage.default" },
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/management/management-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/management/management-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "management-plugin",
    version: "1.0.0",
    exposedModules: {
      TargetsPage: p("./src/plugins/management-plugin/TargetsPage.tsx"),
      DeploymentsPage: p("./src/plugins/management-plugin/DeploymentsPage.tsx"),
      DeploymentDetailPage: p(
        "./src/plugins/management-plugin/DeploymentDetailPage.tsx",
      ),
      SigningKeyPage: p("./src/plugins/management-plugin/SigningKeyPage.tsx"),
      useSigningKeyStore: p(
        "./src/plugins/management-plugin/signingKeyStore.ts",
      ),
    },
  },
});

const DayOnePlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        label: "Day One",
        component: { $codeRef: "DayOnePage.default" },
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/day-one/day-one-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/day-one/day-one-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "day-one-plugin",
    version: "1.0.0",
    exposedModules: {
      DayOnePage: p("./src/plugins/day-one-plugin/DayOnePage.tsx"),
    },
  },
});

const RoutingPlugin = new DynamicRemotePlugin({
  extensions: [],
  sharedModules,
  entryScriptFilename: "plugins/routing/routing-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/routing/routing-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "routing-plugin",
    version: "1.0.0",
    exposedModules: {
      PluginLink: p("./src/plugins/routing-plugin/PluginLink.tsx"),
      usePluginNavigate: p(
        "./src/plugins/routing-plugin/usePluginNavigate.tsx",
      ),
      usePluginLinks: p("./src/plugins/routing-plugin/usePluginLinks.tsx"),
    },
  },
});

const config: Configuration = {
  entry: {
    mock: path.resolve(__dirname, "./src/index.ts"),
  },
  output: {
    publicPath: "auto",
    chunkFilename: "plugins/[name].js",
  },
  mode: "development",
  cache: {
    type: "filesystem",
  },
  plugins: [
    ManagementPlugin,
    DayOnePlugin,
    RoutingPlugin,
    new PluginRegistryPlugin({
      assetsHost: "",
      plugins: [
        {
          name: "management-plugin",
          key: "management",
          label: "Management",
          persona: "ops",
        },
        {
          name: "day-one-plugin",
          key: "day-one",
          label: "Day One",
          persona: "ops",
        },
        {
          name: "routing-plugin",
          key: "routing",
          label: "Routing",
          persona: "ops",
        },
      ],
    }),
  ],
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
  },
  module: {
    rules: [
      tsLoaderRule,
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
      },
      {
        test: /\.scss$/,
        use: ["style-loader", "css-loader", "sass-loader"],
      },
    ],
  },
};

export default config;
