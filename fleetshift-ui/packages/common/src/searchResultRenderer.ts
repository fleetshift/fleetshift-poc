import type { ReactNode } from "react";

import type { ResourceResult } from "./resourceApi.js";

export interface SearchResultRender {
  scope: string;
  module: string;
  to?: string;
  search?: string;
  title?: string;
  description: ReactNode;
  navigable?: boolean;
}

export type InventoryResource = ResourceResult;

export type SearchResultResolve = (
  resource: InventoryResource,
) => SearchResultRender;
