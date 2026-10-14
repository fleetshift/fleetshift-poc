import path from "node:path";
import { fileURLToPath } from "node:url";

import rspack, { type Configuration } from "@rspack/core";

const configDirectory = path.dirname(fileURLToPath(import.meta.url));

const config: Configuration = {
  mode: "production",
  target: "node",
  entry: "./src/main.tsx",
  output: {
    clean: true,
    filename: "fleetctl",
    module: true,
    path: path.resolve(configDirectory, "bin"),
  },
  resolve: {
    extensions: [".tsx", ".ts", ".jsx", ".js"],
  },
  externalsPresets: { node: true },
  externalsType: "module",
  externals: ["ink", "react", "react/jsx-runtime", "undici"],
  optimization: {
    minimize: true,
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
            transform: { react: { runtime: "automatic" } },
          },
        },
      },
    ],
  },
  plugins: [
    new rspack.BannerPlugin({
      banner: "#!/usr/bin/env node",
      entryOnly: true,
      raw: true,
    }),
  ],
};

export default config;
