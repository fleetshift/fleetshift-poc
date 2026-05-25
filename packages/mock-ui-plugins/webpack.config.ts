/* eslint-disable @typescript-eslint/ban-ts-comment */
import path from "path";
import {
  ModuleFederationPlugin as BaseMFPlugin,
  ContainerPlugin,
} from "@module-federation/enhanced";
import { DynamicRemotePlugin } from "@openshift/dynamic-plugin-sdk-webpack";
import { getDynamicModules, createTsLoaderRule } from "@fleetshift/build-utils";
import type { Configuration } from "webpack";

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
  "react/jsx-runtime": { singleton: true, requiredVersion: "^18" },
  "oidc-client-ts": { singleton: true, requiredVersion: "*" },
  "react-oidc-context": { singleton: true, requiredVersion: "*" },
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
      InitialSetupForm: p("./src/plugins/day-one-plugin/InitialSetupForm.tsx"),
      CreateClusterWizard: p(
        "./src/plugins/day-one-plugin/CreateClusterWizard.tsx",
      ),
    },
  },
});

const CorePlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        label: "Core Plugin",
        component: { $codeRef: "CorePluginPage.default" },
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/core/core-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/core/core-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "core-plugin",
    version: "1.0.0",
    exposedModules: {
      CorePluginPage: p("./src/plugins/core-plugin/CorePluginPage.tsx"),
    },
  },
});

const SigningPlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        label: "Signing Keys",
        component: { $codeRef: "SigningKeyEnrollment.default" },
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/signing/signing-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/signing/signing-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "signing-plugin",
    version: "1.0.0",
    exposedModules: {
      SigningKeyEnrollment: p(
        "./src/plugins/signing-plugin/SigningKeyEnrollment.tsx",
      ),
      useSigningKey: p("./src/plugins/signing-plugin/useSigningKey.ts"),
      signingKeyApi: p("./src/plugins/signing-plugin/signingKeyApi.ts"),
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

const pluginConfigs = [
  { plugin: ManagementPlugin, key: "management" },
  { plugin: DayOnePlugin, key: "day-one" },
  { plugin: CorePlugin, key: "core" },
  { plugin: SigningPlugin, key: "signing" },
  { plugin: RoutingPlugin, key: "routing" },
] as const;

const configs: Configuration[] = pluginConfigs.map(({ plugin, key }) => ({
  name: key,
  entry: {
    mock: path.resolve(__dirname, "./src/index.ts"),
  },
  output: {
    publicPath: "auto",
    chunkFilename: `plugins/${key}/[name].js`,
    uniqueName: key,
  },
  mode: "development",
  cache: {
    type: "filesystem",
    name: key,
  },
  plugins: [plugin],
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
      {
        test: /\.(png|jpe?g|gif|svg)$/i,
        type: "asset/resource",
      },
    ],
  },
}));

export default configs;
