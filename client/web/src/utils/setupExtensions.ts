import type { LoadedAndResolvedExtension } from "@openshift/dynamic-plugin-sdk";

import type { SetupExtension } from "../extensions/isSetupExtension";

export type ResolvedSetup = LoadedAndResolvedExtension<SetupExtension>;
type ExtMap = Record<string, ResolvedSetup>;

function topoSort(subset: ResolvedSetup[], allMap: ExtMap): ResolvedSetup[] {
  const subsetIds = new Set(subset.map((e) => e.properties.id));
  const placed = new Set<string>();
  const result: ResolvedSetup[] = [];

  function visit(ext: ResolvedSetup) {
    if (placed.has(ext.properties.id)) return;

    for (const reqId of ext.properties.requires) {
      const dep = allMap[reqId];
      if (!dep) {
        console.warn(
          `Setup extension "${ext.properties.id}" requires "${reqId}" which was not found`,
        );
        continue;
      }
      if (subsetIds.has(reqId)) {
        visit(dep);
      }
    }

    placed.add(ext.properties.id);
    result.push(ext);
  }

  const sorted = [...subset].sort((a, b) => {
    const pa = a.properties.priority ?? Infinity;
    const pb = b.properties.priority ?? Infinity;
    if (pa !== pb) return pa - pb;
    return a.properties.label.localeCompare(b.properties.label);
  });

  for (const ext of sorted) {
    visit(ext);
  }

  return result;
}

function filterValid(
  extensions: ResolvedSetup[],
  allMap: ExtMap,
): ResolvedSetup[] {
  const status = new Map<string, "valid" | "invalid" | "visiting">();

  function isValid(id: string): boolean {
    const cached = status.get(id);
    if (cached === "valid") return true;
    if (cached === "invalid" || cached === "visiting") return false;

    const ext = allMap[id];
    if (!ext) return false;

    status.set(id, "visiting");

    for (const reqId of ext.properties.requires) {
      if (!allMap[reqId]) {
        console.warn(
          `Setup extension "${id}" requires "${reqId}" which was not found`,
        );
        status.set(id, "invalid");
        return false;
      }
      if (!isValid(reqId)) {
        console.warn(
          `Setup extension "${id}" excluded: dependency "${reqId}" is invalid`,
        );
        status.set(id, "invalid");
        return false;
      }
    }

    status.set(id, "valid");
    return true;
  }

  return extensions.filter((e) => isValid(e.properties.id));
}

export function resolveSetupExtensions(
  extensionsSet: ResolvedSetup[],
  allExtensions: ResolvedSetup[],
): ResolvedSetup[] {
  const allMap: ExtMap = {};
  for (const ext of allExtensions) {
    const id = ext.properties.id;
    if (allMap[id]) {
      console.warn(
        `Duplicate setup extension id "${id}" detected; ignoring later definition`,
      );
      continue;
    }
    allMap[id] = ext;
  }

  const valid = filterValid(extensionsSet, allMap);
  return topoSort(valid, allMap);
}
