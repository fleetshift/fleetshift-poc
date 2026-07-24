import chalk from "chalk";
import fs from "fs";
import * as glob from "glob";
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

const getDynamicModules = (root: string, nodeModulesRoot?: string) => {
  if (!root) {
    throw new Error(
      "Provide a directory of your node_modules to find dynamic modules",
    );
  }

  const modulesRoot = nodeModulesRoot || root;
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

  const componentsGlob = path.resolve(
    modulesRoot,
    "node_modules/@patternfly/react-core/dist/dynamic/*/**/package.json",
  );
  const iconsGlob = path.resolve(
    modulesRoot,
    "node_modules/@patternfly/react-icons/dist/dynamic/*/**/package.json",
  );

  const readInstalledVersion = (pkg: string) => {
    const pkgJson = JSON.parse(
      fs.readFileSync(
        path.resolve(modulesRoot, "node_modules", pkg, "package.json"),
        { encoding: "utf-8" },
      ),
    );
    return pkgJson.version as string;
  };

  const coreInstalledVersion = readInstalledVersion("@patternfly/react-core");
  const iconsInstalledVersion = readInstalledVersion("@patternfly/react-icons");

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
