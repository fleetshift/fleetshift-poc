import type { ComponentType } from "react";

/**
 * Module-level cache for dynamically loaded PF icon components.
 * Shared across all consumers to prevent duplicate imports.
 */
const iconCache = new Map<string, ComponentType>();

/**
 * In-flight import promises to deduplicate concurrent loads.
 */
const pendingLoads = new Map<string, Promise<ComponentType | null>>();

/**
 * Convert a PascalCase icon name to kebab-case file stem.
 * E.g. "CogIcon" → "cog-icon", "FolderOpenIcon" → "folder-open-icon"
 */
export function iconNameToFile(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
    .replace(/([A-Z])([A-Z][a-z])/g, "$1-$2")
    .toLowerCase();
}

/**
 * Derive search keywords from a PascalCase icon name.
 * E.g. "FolderOpenIcon" → ["folder", "open"]
 */
export function iconNameToKeywords(name: string): string[] {
  return name
    .replace(/Icon$/, "")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z])([A-Z][a-z])/g, "$1 $2")
    .toLowerCase()
    .split(/\s+/)
    .filter((w) => w.length > 0);
}

/**
 * Dynamically load a PatternFly icon component by name.
 *
 * Returns the cached component immediately if available, otherwise
 * triggers a dynamic `import()` and caches the result. Returns `null`
 * if the icon cannot be loaded.
 *
 * @example
 * const Icon = await loadPfIcon("CogIcon");
 * if (Icon) return <Icon />;
 */
export async function loadPfIcon(name: string): Promise<ComponentType | null> {
  const cached = iconCache.get(name);
  if (cached) return cached;

  const pending = pendingLoads.get(name);
  if (pending) return pending;

  const file = iconNameToFile(name);
  const promise = import(
    /* webpackChunkName: "pf-icon-[request]" */
    `@patternfly/react-icons/dist/esm/icons/${file}.js`
  )
    .then((mod: Record<string, unknown>) => {
      const Component = (mod[name] ?? mod.default) as ComponentType | undefined;
      if (Component) {
        iconCache.set(name, Component);
        return Component;
      }
      return null;
    })
    .catch(() => null)
    .finally(() => {
      pendingLoads.delete(name);
    });

  pendingLoads.set(name, promise);
  return promise;
}

/**
 * Get a cached PF icon component synchronously.
 * Returns `undefined` if not yet loaded — use `loadPfIcon` to trigger loading.
 */
export function getCachedPfIcon(name: string): ComponentType | undefined {
  return iconCache.get(name);
}

/**
 * Convert a kebab-case icon slug to PascalCase icon component name.
 * E.g. "folder-open" → "FolderOpenIcon", "cog" → "CogIcon"
 */
export function iconSlugToName(slug: string): string {
  return (
    slug
      .split("-")
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join("") + "Icon"
  );
}
