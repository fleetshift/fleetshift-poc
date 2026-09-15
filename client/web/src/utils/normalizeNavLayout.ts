import type { NavLayoutEntry as ApiNavLayoutEntry } from "@fleetshift/common/dynamic/client/generated/types.gen";
import type {
  NavLayoutEntry,
  NavLayoutGroup,
  NavLayoutPage,
  NavLayoutSection,
} from "@fleetshift/common/dynamic/navLayout";

/** Convert permissive UI API nodes into canonical navigation entries. */
export function normalizeNavLayout(
  entries: ApiNavLayoutEntry[],
): NavLayoutEntry[] {
  const normalizePage = (
    entry: ApiNavLayoutEntry,
  ): NavLayoutPage | undefined =>
    entry.type === "page" && typeof entry.pageId === "string"
      ? { type: "page", pageId: entry.pageId, iconOverride: entry.iconOverride }
      : undefined;

  const normalize = (entry: ApiNavLayoutEntry): NavLayoutEntry | undefined => {
    if (entry.type === "page") return normalizePage(entry);
    if (
      entry.type === "group" &&
      typeof entry.groupId === "string" &&
      typeof entry.pluginKey === "string" &&
      typeof entry.label === "string"
    ) {
      const children = (entry.children ?? [])
        .map(normalizePage)
        .filter((child): child is NavLayoutPage => child !== undefined);
      const group: NavLayoutGroup = {
        type: "group",
        groupId: entry.groupId,
        pluginKey: entry.pluginKey,
        label: entry.label,
        children,
      };
      if (entry.description !== undefined) group.description = entry.description;
      if (entry.keywords !== undefined) group.keywords = entry.keywords;
      if (entry.icon !== undefined) group.icon = entry.icon;
      return group;
    }
    if (
      entry.type === "section" &&
      typeof entry.id === "string" &&
      typeof entry.label === "string"
    ) {
      return {
        type: "section",
        id: entry.id,
        label: entry.label,
        children: (entry.children ?? [])
          .filter((child) => typeof child.pageId === "string")
          .map((child) => ({ pageId: child.pageId! })),
      } satisfies NavLayoutSection;
    }
    if (entry.type === "more") {
      return {
        type: "more",
        children: (entry.children ?? [])
          .map(normalize)
          .filter((child): child is NavLayoutEntry => child !== undefined),
      };
    }
    return undefined;
  };

  return entries
    .map(normalize)
    .filter((entry): entry is NavLayoutEntry => entry !== undefined);
}
