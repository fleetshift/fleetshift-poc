import * as mf from "@module-federation/enhanced/rspack";
import type { Configuration } from "@rspack/core";
import rspack from "@rspack/core";
import path from "path";
import { fileURLToPath } from "url";

const { ModuleFederationPlugin: BaseMFPlugin } = mf;

class ModuleFederationPlugin extends BaseMFPlugin {
  constructor(options: ConstructorParameters<typeof BaseMFPlugin>[0]) {
    super({ ...options, dts: false, manifest: false });
  }
}

import * as buildUtils from "@fleetshift/build-utils";

const {
  createPfModuleReplacementPlugin,
  createPfTransformImport,
  getDynamicModules,
} = buildUtils;

const configDir = path.dirname(fileURLToPath(import.meta.url));
const monorepoRoot = path.resolve(configDir, "../..");
const pfSharedModules = getDynamicModules(configDir, monorepoRoot);
const pfTransformImport = createPfTransformImport();

const config: Configuration = {
  entry: "./src/index.ts",
  output: {
    publicPath: "/",
    path: path.resolve(configDir, "dist"),
    chunkFilename: "shell/[name].js",
    clean: true,
  },
  mode: "development",
  stats: {
    preset: "normal",
    colors: true,
    timings: true,
    modules: false,
  },
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
    symlinks: false,
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        exclude: [/node_modules/, /__tests__/],
        loader: "builtin:swc-loader",
        options: {
          jsc: {
            parser: { syntax: "typescript", tsx: true },
            // TODO: enable reactCompiler: true when rspack >= 2.1.0
            transform: { react: { runtime: "automatic" } },
          },
          transformImport: pfTransformImport,
        },
        type: "javascript/auto",
      },
      {
        test: /\.css$/,
        use: [rspack.CssExtractRspackPlugin.loader, "css-loader"],
      },
      {
        test: /\.s[ac]ss$/,
        use: [
          rspack.CssExtractRspackPlugin.loader,
          "css-loader",
          "sass-loader",
        ],
      },
      {
        test: /\.(png|jpe?g|gif|svg|ico)$/,
        type: "asset/resource",
      },
    ],
  },
  plugins: [
    new ModuleFederationPlugin({
      name: "fleetshift_shell",
      remotes: {},
      shared: {
        react: { singleton: true, requiredVersion: "^19" },
        "react/jsx-runtime": { singleton: true, requiredVersion: "^19" },
        "react-dom": { singleton: true, requiredVersion: "^19" },
        "react-router-dom": { singleton: true, requiredVersion: "^7" },
        "@scalprum/core": { singleton: true },
        "@scalprum/react-core": { singleton: true },
        "@openshift/dynamic-plugin-sdk": { singleton: true },
        "@fleetshift/common": { requiredVersion: "*", version: "*" },
        "oidc-client-ts": { singleton: true, requiredVersion: "*" },
        "react-oidc-context": { singleton: true, requiredVersion: "*" },
        ...pfSharedModules,
      },
    }),
    new rspack.CssExtractRspackPlugin({ chunkFilename: "shell/[name].css" }),
    new rspack.HtmlRspackPlugin({
      template: "./src/index.html",
      favicon: "./src/assets/masthead.ico",
    }),
    new rspack.HtmlRspackPlugin({
      template: "./src/silent-renew.html",
      filename: "silent-renew.html",
      inject: false,
    }),
    new rspack.DefinePlugin({
      "process.env.DRAGGABLE_DEBUG": "false",
    }),
    createPfModuleReplacementPlugin(monorepoRoot),
  ],
};

export default config;
