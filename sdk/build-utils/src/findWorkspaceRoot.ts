import { existsSync } from "fs";
import path from "path";

export function findWorkspaceRoot(start: string): string {
  let dir = start;
  while (!existsSync(path.join(dir, "nx.json"))) {
    const parent = path.dirname(dir);
    if (parent === dir) throw new Error("workspace root not found");
    dir = parent;
  }
  return dir;
}
