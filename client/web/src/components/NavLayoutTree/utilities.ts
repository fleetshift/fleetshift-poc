/**
 * Re-export tree utilities from @ocm-resource-manager/common.
 * Canonical source: common/src/navLayout.ts
 */
export type { FlatNode } from "@ocm-resource-manager/common";
export {
  arrayMove,
  buildLayout,
  flattenLayout,
  getDescendantIds,
  getProjection,
  INDENTATION,
  normalizeOrder,
} from "@ocm-resource-manager/common";
