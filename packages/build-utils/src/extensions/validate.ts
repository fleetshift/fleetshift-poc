import chalk from "chalk";

import {
  type BaseExtensionProperties,
  type ClusterProviderProperties,
  type EncodedCodeRef,
  type FleetshiftExtension,
  type ModuleGroupProperties,
  type ModuleProperties,
  type OnboardingActionProperties,
  type SetupProperties,
} from "./types";

const TAG = chalk.red("[FleetshiftPlugin]");

function isCodeRef(value: unknown): value is EncodedCodeRef {
  return (
    typeof value === "object" &&
    value !== null &&
    "$codeRef" in value &&
    typeof (value as EncodedCodeRef).$codeRef === "string"
  );
}

const CODEREF_PATTERN = /^[A-Za-z_]\w*\.[A-Za-z_]\w*$/;

export function validateCodeRef(
  ref: unknown,
  field: string,
  extensionId: string,
): string[] {
  if (!isCodeRef(ref)) {
    return [
      `${extensionId}: "${field}" must be a CodeRef ({ $codeRef: "ModuleName.exportName" })`,
    ];
  }
  if (!CODEREF_PATTERN.test(ref.$codeRef)) {
    return [
      `${extensionId}: "${field}" has invalid $codeRef "${ref.$codeRef}" — expected "ModuleName.exportName"`,
    ];
  }
  return [];
}

function validateId(id: unknown, context: string): string[] {
  if (typeof id !== "string" || id.length === 0) {
    return [`${context}: "id" is required and must be a non-empty string`];
  }
  if (!/^[a-z][a-z0-9-]*$/.test(id)) {
    return [
      `${context}: "id" must start with a lowercase letter and contain only a-z, 0-9, hyphens (got "${id}")`,
    ];
  }
  return [];
}

function validateLabel(label: unknown, context: string): string[] {
  if (typeof label !== "string" || label.length === 0) {
    return [`${context}: "label" is required and must be a non-empty string`];
  }
  return [];
}

function validateBaseProperties(
  props: BaseExtensionProperties,
  ctx: string,
): string[] {
  return [
    ...validateId(props.id, ctx),
    ...validateLabel(props.label, ctx),
    ...(props.searchResult
      ? validateCodeRef(props.searchResult, "searchResult", ctx)
      : []),
    ...(props.searchIcon
      ? validateCodeRef(props.searchIcon, "searchIcon", ctx)
      : []),
  ];
}

export function validateModuleProperties(props: ModuleProperties): string[] {
  const ctx = `fleetshift.module "${props.id || "(no id)"}"`;
  return [
    ...validateBaseProperties(props, ctx),
    ...validateCodeRef(props.component, "component", ctx),
    ...validateCodeRef(props.icon, "icon", ctx),
  ];
}

export function validateModuleGroupProperties(
  props: ModuleGroupProperties,
): string[] {
  const ctx = `fleetshift.module-group "${props.id || "(no id)"}"`;
  return [...validateBaseProperties(props, ctx)];
}

export function validateSetupProperties(props: SetupProperties): string[] {
  const ctx = `fleetshift.setup "${props.id || "(no id)"}"`;
  const errors = [
    ...validateBaseProperties(props, ctx),
    ...validateCodeRef(props.component, "component", ctx),
  ];

  if (typeof props.path !== "string" || props.path.length === 0) {
    errors.push(`${ctx}: "path" is required and must be a non-empty string`);
  }
  if (!Array.isArray(props.requires)) {
    errors.push(`${ctx}: "requires" must be an array of dependency IDs`);
  } else if (
    props.requires.some(
      (dep) =>
        typeof dep !== "string" ||
        dep.length === 0 ||
        !/^[a-z][a-z0-9-]*$/.test(dep),
    )
  ) {
    errors.push(
      `${ctx}: "requires" entries must be non-empty dependency IDs matching /^[a-z][a-z0-9-]*$/`,
    );
  }
  if (typeof props.requiresAuth !== "boolean") {
    errors.push(`${ctx}: "requiresAuth" must be a boolean`);
  }

  return errors;
}

export function validateClusterProviderProperties(
  props: ClusterProviderProperties,
): string[] {
  const ctx = `fleetshift.cluster-provider "${props.id || "(no id)"}"`;
  const errors = [
    ...validateBaseProperties(props, ctx),
    ...validateCodeRef(props.icon, "icon", ctx),
    ...validateCodeRef(props.card, "card", ctx),
    ...validateCodeRef(props.wizard, "wizard", ctx),
  ];

  if (typeof props.description !== "string" || props.description.length === 0) {
    errors.push(
      `${ctx}: "description" is required and must be a non-empty string`,
    );
  }

  return errors;
}

export function validateOnboardingActionProperties(
  props: OnboardingActionProperties,
): string[] {
  const ctx = `fleetshift.onboarding-action "${props.id || "(no id)"}"`;
  return [
    ...validateBaseProperties(props, ctx),
    ...validateCodeRef(props.icon, "icon", ctx),
    ...validateCodeRef(props.card, "card", ctx),
    ...validateCodeRef(props.form, "form", ctx),
  ];
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}

function validateSingleExtension(ext: FleetshiftExtension): string[] {
  const props = asRecord(ext.properties);
  if (!props) {
    return [`${ext.type}: "properties" must be an object`];
  }
  switch (ext.type) {
    case "fleetshift.module-group":
      return validateModuleGroupProperties(
        props as unknown as ModuleGroupProperties,
      );
    case "fleetshift.module":
      return validateModuleProperties(props as unknown as ModuleProperties);
    case "fleetshift.setup":
      return validateSetupProperties(props as unknown as SetupProperties);
    case "fleetshift.cluster-provider":
      return validateClusterProviderProperties(
        props as unknown as ClusterProviderProperties,
      );
    case "fleetshift.onboarding-action":
      return validateOnboardingActionProperties(
        props as unknown as OnboardingActionProperties,
      );
    default: {
      const id = typeof props.id === "string" ? props.id : "(no id)";
      return validateBaseProperties(
        props as BaseExtensionProperties,
        `${ext.type} "${id}"`,
      );
    }
  }
}

function extractCodeRefs(
  properties: Record<string, unknown>,
): EncodedCodeRef[] {
  const refs: EncodedCodeRef[] = [];
  for (const value of Object.values(properties)) {
    if (isCodeRef(value)) {
      refs.push(value);
    }
  }
  return refs;
}

export function validateExtensionSet(
  extensions: FleetshiftExtension[],
  exposedModules: Record<string, string>,
): void {
  const errors: string[] = [];
  const seenIds = new Map<string, string>();
  const exposedKeys = new Set(Object.keys(exposedModules));

  for (const ext of extensions) {
    errors.push(...validateSingleExtension(ext));

    const props = asRecord(ext.properties);
    if (!props) {
      continue;
    }

    if (typeof props.id === "string" && props.id.length > 0) {
      const group =
        ext.type === "fleetshift.module"
          ? (props.group as string | undefined)
          : undefined;
      const dedupKey = group ? `${group}.${props.id}` : props.id;
      const existing = seenIds.get(dedupKey);
      if (existing) {
        errors.push(
          `Duplicate extension ID "${props.id}" — first declared as ${existing}, also declared as ${ext.type}`,
        );
      } else {
        seenIds.set(dedupKey, ext.type);
      }
    }

    for (const ref of extractCodeRefs(props)) {
      const moduleName = ref.$codeRef.split(".")[0];
      if (!exposedKeys.has(moduleName)) {
        errors.push(
          `CodeRef "${ref.$codeRef}" references module "${moduleName}" which is not in exposedModules. Available: ${[...exposedKeys].join(", ")}`,
        );
      }
    }
  }

  const groupIds = new Set<string>();
  for (const ext of extensions) {
    if (ext.type === "fleetshift.module-group") {
      const id = (ext.properties as { id?: string }).id;
      if (typeof id === "string" && id.length > 0) {
        groupIds.add(id);
      }
    }
  }

  const moduleIdsPerGroup = new Map<string, string[]>();
  for (const ext of extensions) {
    if (ext.type !== "fleetshift.module") continue;
    const props = asRecord(ext.properties);
    if (!props) continue;
    const group = props.group as string | undefined;
    if (typeof group !== "string") continue;

    if (!groupIds.has(group)) {
      errors.push(
        `Module "${props.id}" references group "${group}" which does not exist in this plugin's extensions`,
      );
    }

    const existing = moduleIdsPerGroup.get(group);
    const moduleId = props.id as string;
    if (existing) {
      if (existing.includes(moduleId)) {
        errors.push(
          `Duplicate module id "${moduleId}" within group "${group}"`,
        );
      }
      existing.push(moduleId);
    } else {
      moduleIdsPerGroup.set(group, [moduleId]);
    }
  }

  if (errors.length > 0) {
    const message = [
      `${TAG} Extension validation failed with ${errors.length} error(s):`,
      ...errors.map((e) => `  - ${e}`),
    ].join("\n");
    throw new Error(message);
  }
}
