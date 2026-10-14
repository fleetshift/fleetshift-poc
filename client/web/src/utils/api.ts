import type { CanvasModule, CanvasPage, NavLayoutEntry } from "./extensions";

export type { CanvasModule, CanvasPage, NavLayoutEntry };

export interface User {
  id: string;
  username: string;
  display_name: string;
  role: string;
  navLayout: NavLayoutEntry[];
}
