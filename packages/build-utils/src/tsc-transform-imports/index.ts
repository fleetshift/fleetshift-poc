import path from "path";
import * as ts from "typescript";
import * as fs from "fs";
import * as glob from "glob";

export interface TransformImportsConfig {
  /** Absolute path to the node_modules directory where PF packages are installed. */
  nodeModulesRoot: string;
}

// Icons names that do not match the filename pattern
const ICONS_NAME_FIX: Record<string, string> = {
  AnsibeTowerIcon: "ansibeTower-icon",
  ChartSpikeIcon: "chartSpike-icon",
};

const DYNAMIC_OUTPUTS = [
  ts.ModuleKind.ES2015,
  ts.ModuleKind.ES2020,
  ts.ModuleKind.ES2022,
  ts.ModuleKind.ESNext,
];

function findFirstGlob(
  roots: string[],
  suffix: string,
  filter?: (p: string) => boolean,
): string | undefined {
  const adjusted = suffix.startsWith("/") ? suffix.substring(1) : suffix;
  return roots.flatMap((root) => {
    const found = glob.sync(`${root}/${adjusted}`);
    return filter ? found.filter(filter) : found;
  })[0];
}

function filterNonStable(location: string) {
  return !location.includes("next") && !location.includes("deprecated");
}

function loadModuleMap(roots: string[]): Map<string, string> | undefined {
  const mapPath = findFirstGlob(roots, "dist/dynamic-modules.json");
  if (!mapPath) return undefined;

  const loaded: unknown = JSON.parse(fs.readFileSync(mapPath, "utf-8"));
  if (typeof loaded !== "object" || loaded == null) {
    throw new Error(
      `Expected dynamic-modules.json to contain an object, got ${loaded}`,
    );
  }

  const map = new Map<string, string>();
  const prefix = "dist/dynamic/";
  for (const [key, value] of Object.entries(loaded)) {
    if (typeof value !== "string")
      throw new Error(
        `Expected value of ${key} in dynamic-modules.json to be string, got ${value}`,
      );
    map.set(
      key,
      value.startsWith(prefix) ? value.substring(prefix.length) : value,
    );
  }
  return map;
}

function getModuleExplicitLocation(roots: string[], relativePath: string) {
  const loc = findFirstGlob(
    roots,
    `dist/dynamic/**/${relativePath}`,
    filterNonStable,
  )
    ?.split("/dynamic/")
    .pop();
  if (loc) return loc;
  throw new Error(
    `Could not find source file for ${relativePath} in any of ${roots}!`,
  );
}

function buildHardcodedComponents(coreRoots: string[]): Record<string, string> {
  if (coreRoots.length === 0) return {};
  const g = (p: string) => getModuleExplicitLocation(coreRoots, p);
  return {
    getResizeObserver: g("helpers/resizeObserver"),
    useOUIAProps: g("helpers/OUIA/ouia"),
    OUIAProps: g("helpers/OUIA/ouia"),
    getDefaultOUIAId: g("helpers/OUIA/ouia"),
    useOUIAId: g("helpers/OUIA/ouia"),
    handleArrows: g("helpers/KeyboardHandler"),
    setTabIndex: g("helpers/KeyboardHandler"),
    IconComponentProps: g("components/Icon"),
    TreeViewDataItem: g("components/TreeView"),
    Popper: g("helpers/Popper/Popper"),
    clipboardCopyFunc: g("components/ClipboardCopy"),
    ToolbarChipGroup: g("components/Toolbar"),
    DatePickerRef: g("components/DatePicker"),
    ButtonType: g("components/Button"),
    PaginationTitles: g("components/Pagination"),
    ProgressMeasureLocation: g("components/Progress"),
    isValidDate: g("helpers/datetimeUtils"),
    ValidatedOptions: g("helpers/constants"),
    capitalize: g("helpers/util"),
    WizardFooterWrapper: g("components/Wizard"),
    WizardFooter: g("components/Wizard"),
    WizardContextProvider: g("components/Wizard"),
    useWizardContext: g("components/Wizard"),
    DataListWrapModifier: g("components/DataList"),
    MenuToggleElement: g("components/MenuToggle"),
    TimestampFormat: g("components/Timestamp"),
  };
}

function getPossibleLocations(roots: string[], nameBinding: string) {
  const patterns = [
    nameBinding,
    nameBinding.replace(/Props$/, ""),
    nameBinding.replace(/Variants?$/, ""),
    nameBinding.replace(/Position$/, ""),
    nameBinding.replace(/Sizes?$/, ""),
  ];
  for (const pattern of patterns) {
    if (pattern === nameBinding || pattern !== nameBinding) {
      const loc = findFirstGlob(
        roots,
        `dist/esm/**/${pattern}.js`,
        filterNonStable,
      );
      if (loc) return loc;
    }
  }
  return undefined;
}

/**
 * Creates a PF dynamic import transformer configured for a specific
 * node_modules root. Use this in webpack configs via ts-loader's
 * getCustomTransformers option.
 */
export function createTransformer(
  config: TransformImportsConfig,
): ts.TransformerFactory<ts.Node> {
  const coreRoots = glob.sync(
    path.join(config.nodeModulesRoot, "@patternfly/react-core"),
  );
  const iconsRoots = glob.sync(
    path.join(config.nodeModulesRoot, "@patternfly/react-icons"),
  );

  const coreModuleMap = loadModuleMap(coreRoots);
  const hardcoded = buildHardcodedComponents(coreRoots);
  const componentCache = new Map<string, string>();
  const iconsCache: Record<string, string> = {};

  function findComponentModule(nameBinding: string): string {
    const cached = componentCache.get(nameBinding);
    if (cached) return cached;

    // Try module map first
    if (coreModuleMap) {
      const mapPath = coreModuleMap.get(nameBinding);
      if (mapPath) {
        const found = findFirstGlob(coreRoots, `dist/dynamic/${mapPath}`);
        if (!found) {
          throw new Error(
            `dynamic-modules.json contains path "${mapPath}" for "${nameBinding}", but no such file exists.`,
          );
        }
        componentCache.set(nameBinding, mapPath);
        return mapPath;
      }
    }

    // Try hardcoded
    if (hardcoded[nameBinding]) {
      componentCache.set(nameBinding, hardcoded[nameBinding]);
      return hardcoded[nameBinding];
    }

    // Guess from filesystem
    const sourceGlob = getPossibleLocations(coreRoots, nameBinding);
    const sourceFile = sourceGlob ? glob.sync(sourceGlob) : [];
    if (sourceFile.length < 1) {
      throw new Error(`Unable to find source file for module ${nameBinding}!`);
    }
    const moduleSource: string[] =
      sourceFile[0].split("esm").pop()?.split("/") || [];
    moduleSource.pop();
    const modulePath = moduleSource.join("/").replace(/^\//, "");
    componentCache.set(nameBinding, modulePath);
    return modulePath;
  }

  function camelToDash(str: string) {
    return str
      .replace(/([A-Z])/g, (g) => `-${g[0].toLowerCase()}`)
      .replace(/^-/, "");
  }

  function iconImportLiteral(icon: string) {
    if (iconsCache[icon]) return iconsCache[icon];

    const assumed = camelToDash(icon);
    const fallback = ICONS_NAME_FIX[icon];

    if (
      iconsRoots.flatMap((root) => glob.sync(`${root}/**/${assumed}.js`))
        .length > 0
    ) {
      iconsCache[icon] =
        `@patternfly/react-icons/dist/dynamic/icons/${assumed}`;
    }

    if (
      !iconsCache[icon] &&
      fallback &&
      iconsRoots.flatMap((root) => glob.sync(`${root}/**/${fallback}.js`))
        .length > 0
    ) {
      iconsCache[icon] =
        `@patternfly/react-icons/dist/dynamic/icons/${fallback}`;
    }

    if (iconsCache[icon]) return iconsCache[icon];
    throw new Error(
      `Cannot find source files for the ${icon} icon. Expected filename ${assumed}.`,
    );
  }

  function createIconDynamicImports(
    factory: ts.NodeFactory,
    iconNames: string[],
  ) {
    return iconNames.map((icon) =>
      factory.createImportDeclaration(
        undefined,
        factory.createImportClause(
          false,
          factory.createIdentifier(icon),
          undefined,
        ),
        factory.createStringLiteral(iconImportLiteral(icon)),
      ),
    );
  }

  function createDynamicReactCoreImports(
    factory: ts.NodeFactory,
    node: ts.ImportDeclaration,
  ) {
    const importNames: (string | [string, string])[] = [];
    node.importClause?.namedBindings?.forEachChild((child) => {
      if (child.getChildCount() > 1) {
        importNames.push([
          child.getChildAt(0).getText().trim(),
          child.getText().trim(),
        ]);
      } else {
        importNames.push(child.getText().trim());
      }
    });

    const groups = importNames.reduce<Record<string, string[]>>(
      (acc, nameBinding) => {
        const name =
          typeof nameBinding === "string" ? nameBinding : nameBinding[0];
        const alias =
          typeof nameBinding === "string" ? nameBinding : nameBinding[1];
        const importPartial = findComponentModule(name).replace(
          "/esm/",
          "/dynamic/",
        );
        if (acc[importPartial]) {
          acc[importPartial].push(alias);
        } else {
          acc[importPartial] = [alias];
        }
        return acc;
      },
      {},
    );

    return Object.entries(groups).map(([importPartial, nameBindings]) =>
      factory.createImportDeclaration(
        node.modifiers,
        factory.createImportClause(
          false,
          undefined,
          factory.createNamedImports(
            nameBindings.map((n) =>
              factory.createImportSpecifier(
                false,
                undefined,
                factory.createIdentifier(n),
              ),
            ),
          ),
        ),
        factory.createStringLiteral(
          `@patternfly/react-core/dist/dynamic/${importPartial}`,
        ),
        node.assertClause,
      ),
    );
  }

  return (context) => (rootNode) => {
    if (coreRoots.length === 0 || iconsRoots.length === 0) {
      return rootNode;
    }

    const opts = context.getCompilerOptions();
    const isDynamic = opts.module && DYNAMIC_OUTPUTS.includes(opts.module);

    function visitor(node: ts.Node): ts.VisitResult<ts.Node> {
      const { factory } = context;

      if (
        isDynamic &&
        ts.isImportDeclaration(node) &&
        (/@patternfly\/react-(core|icons|tokens)['"]$/.test(
          node.moduleSpecifier.getText(),
        ) ||
          /@patternfly\/react-(core|icons|tokens)\/['"]$/.test(
            node.moduleSpecifier.getText(),
          ))
      ) {
        if (node.moduleSpecifier.getText().includes("react-icons")) {
          const importNames: string[] = [];
          node.importClause?.namedBindings?.forEachChild((child) => {
            importNames.push(child.getText().trim());
          });
          return createIconDynamicImports(factory, importNames);
        }

        if (node.moduleSpecifier.getText().includes("react-core")) {
          return createDynamicReactCoreImports(factory, node);
        }

        return node;
      }

      if (
        isDynamic &&
        ts.isImportDeclaration(node) &&
        /@patternfly\/react-(icons|tokens)/.test(node.moduleSpecifier.getText())
      ) {
        if (
          ts.isImportDeclaration(node) &&
          /@patternfly\/.*\/dist\/esm/.test(node.moduleSpecifier.getText())
        ) {
          const moduleSpecifier = node.moduleSpecifier
            .getFullText()
            .replace(/"/g, "")
            .replace(/'/g, "");

          if (!node.moduleSpecifier.getText().includes("react-tokens")) {
            moduleSpecifier.replace(/dist\/esm/, "dist/dynamic");
          }

          return factory.updateImportDeclaration(
            node,
            node.modifiers,
            node.importClause,
            factory.createStringLiteral(moduleSpecifier.trim(), true),
            undefined,
          );
        }
      }

      return ts.visitEachChild(node, visitor, context);
    }

    return ts.visitNode(rootNode, visitor);
  };
}

export default createTransformer;
