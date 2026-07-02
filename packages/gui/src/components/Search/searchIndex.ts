import type { SearchEntry } from "@fleetshift/common";
import type { Orama } from "@orama/orama";
import { create, insert, search } from "@orama/orama";

import { highlightText } from "./highlightUtils";

export type { SearchEntry };

export type SearchCategory = "nav" | "nav-group" | "cluster" | "setting";

export interface SearchResultItem {
  id: string;
  title: string;
  description: string;
  category: string;
  pathname: string;
  icon: string;
  status: string;
  feature?: string;
  IconComponent?: React.ComponentType;
  Component?: React.ComponentType<{ title: string; description: string }>;
}

export type GroupedResults = Record<string, SearchResultItem[]>;

const searchSchema = {
  title: "string",
  description: "string",
  category: "string",
  pathname: "string",
  icon: "string",
  status: "string",
  meta: "string",
  feature: "string",
} as const;

export type SearchDB = Orama<typeof searchSchema>;

export function createSearchDB(): SearchDB {
  return create({ schema: searchSchema });
}

export function insertEntry(db: SearchDB, entry: SearchEntry) {
  return insert(db, {
    id: entry.id,
    title: entry.title,
    description: entry.description,
    category: entry.category,
    pathname: entry.pathname,
    icon: entry.icon,
    status: entry.status,
    meta: entry.meta,
    feature: entry.feature ?? "",
  });
}

const MAX_PER_CATEGORY = 5;

export async function queryIndex(
  db: SearchDB,
  term: string,
): Promise<GroupedResults> {
  if (!term.trim()) return {};

  const result = await search(db, {
    term,
    threshold: 0.5,
    tolerance: 1.5,
    properties: ["title", "description", "meta"],
    boost: { title: 10, description: 3, meta: 2 },
  });

  const grouped: GroupedResults = {};

  for (const hit of result.hits) {
    const doc = hit.document as unknown as SearchEntry;
    const cat = doc.category;
    if (!grouped[cat]) grouped[cat] = [];
    if (grouped[cat].length >= MAX_PER_CATEGORY) continue;

    grouped[cat].push({
      id: String(doc.id),
      title: highlightText(term, doc.title),
      description: highlightText(term, doc.description),
      category: cat,
      pathname: doc.pathname,
      icon: doc.icon,
      status: doc.status,
      feature: doc.feature || undefined,
    });
  }

  return grouped;
}
