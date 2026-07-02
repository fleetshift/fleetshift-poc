import { describe, expect, it } from "vitest";

import {
  iconNameToFile,
  iconNameToKeywords,
  iconSlugToName,
} from "../pfIconLoader";

describe("iconNameToFile", () => {
  it("converts PascalCase to kebab-case", () => {
    expect(iconNameToFile("CogIcon")).toBe("cog-icon");
    expect(iconNameToFile("FolderOpenIcon")).toBe("folder-open-icon");
    expect(iconNameToFile("ShieldAltIcon")).toBe("shield-alt-icon");
  });
});

describe("iconNameToKeywords", () => {
  it("extracts search keywords from icon name", () => {
    expect(iconNameToKeywords("FolderOpenIcon")).toEqual(["folder", "open"]);
    expect(iconNameToKeywords("CogIcon")).toEqual(["cog"]);
    expect(iconNameToKeywords("ShieldAltIcon")).toEqual(["shield", "alt"]);
  });
});

describe("iconSlugToName", () => {
  it("converts kebab-case slug to PascalCase icon name", () => {
    expect(iconSlugToName("cog")).toBe("CogIcon");
    expect(iconSlugToName("folder-open")).toBe("FolderOpenIcon");
    expect(iconSlugToName("shield-alt")).toBe("ShieldAltIcon");
  });

  it("handles single-word slugs", () => {
    expect(iconSlugToName("cubes")).toBe("CubesIcon");
    expect(iconSlugToName("globe")).toBe("GlobeIcon");
    expect(iconSlugToName("key")).toBe("KeyIcon");
  });

  it("handles multi-segment slugs", () => {
    expect(iconSlugToName("layer-group")).toBe("LayerGroupIcon");
    expect(iconSlugToName("puzzle-piece")).toBe("PuzzlePieceIcon");
  });

  it("roundtrips with iconNameToFile (minus trailing -icon)", () => {
    const names = [
      "CogIcon",
      "FolderOpenIcon",
      "CubesIcon",
      "ShieldAltIcon",
      "LayerGroupIcon",
    ];
    for (const name of names) {
      // iconNameToFile("CogIcon") → "cog-icon"
      // strip trailing "-icon" → "cog"
      // iconSlugToName("cog") → "CogIcon"
      const file = iconNameToFile(name);
      const slug = file.replace(/-icon$/, "");
      expect(iconSlugToName(slug)).toBe(name);
    }
  });
});
