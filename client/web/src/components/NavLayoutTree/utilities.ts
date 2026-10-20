/**
 * Re-export tree utilities from @ocm-fleet/common.
 * Canonical source: common/src/navLayout.ts
 */
export type { FlatNode } from "@ocm-fleet/common";
export {
  arrayMove,
  buildLayout,
  flattenLayout,
  getDescendantIds,
  getProjection,
  INDENTATION,
  normalizeOrder,
} from "@ocm-fleet/common";
