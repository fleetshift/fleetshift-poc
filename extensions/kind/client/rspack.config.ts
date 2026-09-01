import {
  createClusterProvider,
  createCommonModuleReplacementPlugin,
  createCommonTransformImport,
  createPfModuleReplacementPlugin,
  createPfTransformImport,
  createSearchResultRenderer,
  FleetshiftPlugin,
  getCommonDynamicModules,
  getDynamicModules,
} from "@fleetshift/build-utils";
import { ModuleFederationPlugin as BaseMFPlugin } from "@module-federation/enhanced/rspack";
import type { Configuration } from "@rspack/core";
import rspack from "@rspack/core";
import path from "path";
import { fileURLToPath } from "url";

const configDir = path.dirname(fileURLToPath(import.meta.url));
const uiRoot = path.resolve(configDir, "..");
const pfSharedModules = getDynamicModules(configDir, uiRoot);
const commonSharedModules = getCommonDynamicModules();
const pfTransformImport = createPfTransformImport();
const commonTransformImport = createCommonTransformImport();

const swcLoaderRule = {
  test: /\.tsx?$/,
  exclude: [/node_modules/, /packages\/common\/dist/],
  loader: "builtin:swc-loader" as const,
  options: {
    jsc: {
      parser: { syntax: "typescript" as const, tsx: true },
      // TODO: enable reactCompiler: true when rspack >= 2.1.0
      transform: { react: { runtime: "automatic" as const } },
    },
    transformImport: [...pfTransformImport, commonTransformImport],
  },
  type: "javascript/auto" as const,
};

const sharedModules = {
  react: { singleton: true, requiredVersion: "*" },
  "react-dom": { singleton: true, requiredVersion: "*" },
  "react/jsx-dev-runtime": { singleton: true, requiredVersion: "^19" },
  "@scalprum/core": { singleton: true, requiredVersion: "*" },
  "@scalprum/react-core": { singleton: true, requiredVersion: "*" },
  "@openshift/dynamic-plugin-sdk": {
    singleton: true,
    requiredVersion: "*",
    version: "*",
  },
  "react-router-dom": { singleton: true, requiredVersion: "*" },
  "react/jsx-runtime": { singleton: true, requiredVersion: "^19" },
  ...pfSharedModules,
  ...commonSharedModules,
};

class ModuleFederationPlugin extends BaseMFPlugin {
  constructor(options: ConstructorParameters<typeof BaseMFPlugin>[0]) {
    super({ ...options, dts: false, manifest: false });
  }
}

const mfOverride = {
  libraryType: "global",
  pluginOverride: {
    ModuleFederationPlugin,
  },
};

const p = (rel: string) => path.resolve(configDir, rel);

const KindPlugin = new FleetshiftPlugin({
  extensions: [
    createClusterProvider({
      id: "kind",
      label: "Kind",
      description: "Create a local Kind cluster for development and testing.",
      keywords: ["kind", "local", "development", "testing"],
      to: { search: "?create=kind" },
      icon: { $codeRef: "KindProviderCard.KindIcon" },
      card: { $codeRef: "KindProviderCard.default" },
      wizard: { $codeRef: "CreateClusterWizard.default" },
      searchIcon: { $codeRef: "KindIcon.default" },
    }),
    createSearchResultRenderer({
      id: "kind-cluster-renderer",
      label: "Kind Cluster",
      resourceType: "kind.fleetshift.io/Cluster",
      resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
      icon: { $codeRef: "KindSearchResult.KindClusterIcon" },
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/kind/kind-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/kind/kind-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "kind-plugin",
    version: "1.0.0",
    exposedModules: {
      KindProviderCard: p("./src/KindProviderCard.tsx"),
      CreateClusterWizard: p("./src/CreateClusterWizard.tsx"),
      KindIcon: p("./src/KindIcon.tsx"),
      KindSearchResult: p("./src/KindSearchResult.tsx"),
    },
  },
});

const pluginConfigs = [{ plugin: KindPlugin, key: "kind" }] as const;

const configs: Configuration[] = pluginConfigs.map(({ plugin, key }) => ({
  name: key,
  cache: {
    type: "persistent" as const,
    version: key,
    buildDependencies: [fileURLToPath(import.meta.url)],
  },
  entry: {
    mock: path.resolve(configDir, "./src/index.ts"),
  },
  output: {
    publicPath: "auto",
    chunkFilename: `plugins/${key}/[name].js`,
    assetModuleFilename: `plugins/${key}/assets/[hash][ext]`,
    uniqueName: key,
  },
  mode: (process.env.NODE_ENV === "development" ? "development" : "production") as Configuration["mode"],
  ignoreWarnings: [/Plugin base URL/, /Plugin has no extensions/],
  stats: {
    preset: "normal",
    colors: true,
    timings: true,
    modules: false,
  },
  plugins: [
    plugin,
    new rspack.DefinePlugin({
      "process.env.DRAGGABLE_DEBUG": "false",
    }),
    createPfModuleReplacementPlugin(uiRoot),
    createCommonModuleReplacementPlugin(),
    new rspack.NormalModuleReplacementPlugin(
      /^@patternfly\/react-core\/dist\/esm\/(components|helpers|layouts)(\/|$)/,
      (resource) => {
        const compMatch = resource.request.match(
          /^(@patternfly\/react-core\/)dist\/esm\/((?:components|layouts)\/[^/]+)/,
        );
        if (compMatch) {
          resource.request = `${compMatch[1]}dist/dynamic/${compMatch[2]}`;
          return;
        }
        resource.request = resource.request.replace(
          "/dist/esm/",
          "/dist/dynamic/",
        );
        resource.request = resource.request.replace(/\.(js|mjs)$/, "");
      },
    ),
  ],
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
    fallback: {
      cookie: false,
      "set-cookie-parser": false,
    },
  },
  module: {
    rules: [
      swcLoaderRule,
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
      },
      {
        test: /\.scss$/,
        use: ["style-loader", "css-loader", "sass-loader"],
      },
      {
        test: /\.(png|jpe?g|gif|svg|webp)$/i,
        type: "asset/resource",
      },
    ],
  },
}));

export default configs;
