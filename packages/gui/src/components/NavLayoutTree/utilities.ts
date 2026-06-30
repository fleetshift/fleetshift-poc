/**
 * Re-export tree utilities from @fleetshift/common.
 * Canonical source: packages/common/src/navLayout.ts
 */
export type { FlatNode } from "@fleetshift/common";
export {
  arrayMove,
  buildLayout,
  flattenLayout,
  getDescendantIds,
  getProjection,
  INDENTATION,
  normalizeOrder,
} from "@fleetshift/common";
