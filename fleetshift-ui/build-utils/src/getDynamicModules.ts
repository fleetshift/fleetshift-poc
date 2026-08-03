import chalk from "chalk";
import fs from "fs";
import * as glob from "glob";
import { createRequire } from "module";
import path from "path";

const checkPfVersion = (version: string) => {
  const number = version?.replace(/[^0-9]/g, "");
  try {
    const versionInt = Number(number);
    return versionInt >= 500;
  } catch (error) {
    console.error(error);
    console.log(
      chalk.yellow(`Unable to parse PF package version: ${version}.`),
    );
    return false;
  }
};

// Resolve package root regardless of hoisting
const resolvePkgDir = (pkg: string) => {
  const req = createRequire(import.meta.url);
  return path.dirname(req.resolve(`${pkg}/package.json`));
};

const getDynamicModules = (root: string, _nodeModulesRoot?: string) => {
  if (!root) {
    throw new Error(
      "Provide a directory of your node_modules to find dynamic modules",
    );
  }

  const packageFile = fs.readFileSync(path.resolve(root, "package.json"), {
    encoding: "utf-8",
  });
  const packageJSON = JSON.parse(packageFile);
  const coreVersion =
    packageJSON.dependencies["@patternfly/react-core"] ||
    packageJSON.devDependencies["@patternfly/react-core"];
  const iconsVersion =
    packageJSON.dependencies["@patternfly/react-icons"] ||
    packageJSON.devDependencies["@patternfly/react-icons"];

  const coreValid = checkPfVersion(coreVersion);
  const iconsValid = checkPfVersion(iconsVersion);
  if (!coreValid) {
    console.log(
      chalk.yellow("[fec]"),
      `Unsupported @patternfly packages version. Dynamic modules require version ^5.0.0. Got ${coreVersion}.`,
    );
    return {};
  }
  if (!iconsValid) {
    console.log(
      chalk.yellow("[fec]"),
      `Unsupported @patternfly packages version. Dynamic modules require version ^5.0.0. Got ${iconsVersion}.`,
    );
    return {};
  }

  const corePkgDir = resolvePkgDir("@patternfly/react-core");
  const iconsPkgDir = resolvePkgDir("@patternfly/react-icons");

  const componentsGlob = path.resolve(
    corePkgDir,
    "dist/dynamic/*/**/package.json",
  );
  const iconsGlob = path.resolve(iconsPkgDir, "dist/dynamic/*/**/package.json");

  const readInstalledVersion = (pkgDir: string) => {
    const pkgJson = JSON.parse(
      fs.readFileSync(path.resolve(pkgDir, "package.json"), {
        encoding: "utf-8",
      }),
    );
    return pkgJson.version as string;
  };

  const coreInstalledVersion = readInstalledVersion(corePkgDir);
  const iconsInstalledVersion = readInstalledVersion(iconsPkgDir);

  const files = [
    {
      requiredVersion: coreVersion,
      version: coreInstalledVersion,
      files: glob.sync(componentsGlob),
    },
    {
      requiredVersion: iconsVersion,
      version: iconsInstalledVersion,
      files: glob.sync(iconsGlob),
    },
  ];
  const modules: {
    [moduleName: string]: {
      requiredVersion: string;
      version: string;
    };
  } = files
    .map(({ files, requiredVersion, version }) =>
      files.reduce(
        (acc, curr) => {
          const moduleName = curr
            .replace(/\/package.json$/, "")
            .split("/node_modules/")
            .pop();
          if (!moduleName) {
            throw new Error(`Unable to get module name from: ${curr}`);
          }
          return {
            ...acc,
            [moduleName]: {
              requiredVersion,
              version,
            },
          };
        },
        {} as Record<string, { requiredVersion: string; version: string }>,
      ),
    )
    .reduce(
      (acc, curr) => ({
        ...acc,
        ...curr,
      }),
      {},
    );

  return modules;
};

export default getDynamicModules;
