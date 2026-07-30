import type { Orama } from "@orama/orama";
import { create, insert, search } from "@orama/orama";

import type { FieldDef } from "./types";

const fieldSchema = {
  name: "string",
  label: "string",
  fieldType: "string",
} as const;

type FieldDB = Orama<typeof fieldSchema>;

export function createFieldIndex(fields: FieldDef[]): FieldDB {
  const db = create({ schema: fieldSchema });
  for (const field of fields) {
    insert(db, {
      id: field.name,
      name: field.name,
      label: field.label,
      fieldType: field.type,
    });
  }
  return db;
}

export async function queryFields(
  db: FieldDB,
  partial: string,
  fields: FieldDef[],
): Promise<FieldDef[]> {
  if (!partial.trim()) {
    return fields;
  }

  const result = await search(db, {
    term: partial,
    threshold: 0.3,
    tolerance: 1,
    properties: ["name", "label"],
    boost: { name: 10, label: 3 },
  });

  const byName = new Map(fields.map((f) => [f.name, f]));
  return result.hits
    .map((hit) => {
      const doc = hit.document as unknown as { name: string };
      return byName.get(doc.name);
    })
    .filter((f): f is FieldDef => f !== undefined);
}
