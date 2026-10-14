import { describe, expect, it } from "vitest";

import type { NavLayoutEntry, NavLayoutGroup } from "../navLayout";
import { CUSTOM_GROUP_PREFIX, isCustomGroup, slugify } from "../navLayout";

describe("slugify", () => {
  it("converts a simple name to a slug", () => {
    expect(slugify("My Custom Group")).toBe("my-custom-group");
  });

  it("handles special characters", () => {
    expect(slugify("Hello, World!")).toBe("hello-world");
  });

  it("collapses multiple spaces and hyphens", () => {
    expect(slugify("  spaced   out--thing  ")).toBe("spaced-out-thing");
  });

  it("returns empty string for non-alphanumeric input", () => {
    expect(slugify("!!!")).toBe("");
  });

  it("handles underscores", () => {
    expect(slugify("my_cool_group")).toBe("my-cool-group");
  });

  it("preserves numbers", () => {
    expect(slugify("Group 42")).toBe("group-42");
  });
});

describe("CUSTOM_GROUP_PREFIX", () => {
  it("is 'user-'", () => {
    expect(CUSTOM_GROUP_PREFIX).toBe("user-");
  });
});

describe("isCustomGroup", () => {
  it("returns true for groups with user- prefix", () => {
    const group: NavLayoutGroup = {
      type: "group",
      groupId: "user-my-group",
      pluginKey: "",
      label: "My Group",
      children: [],
    };
    expect(isCustomGroup(group)).toBe(true);
  });

  it("returns false for plugin-defined groups", () => {
    const group: NavLayoutGroup = {
      type: "group",
      groupId: "core-group",
      pluginKey: "core",
      label: "Core",
      children: [],
    };
    expect(isCustomGroup(group)).toBe(false);
  });

  it("returns false for groups with user in the middle", () => {
    const group: NavLayoutGroup = {
      type: "group",
      groupId: "power-user-tools",
      pluginKey: "tools",
      label: "Power User Tools",
      children: [],
    };
    expect(isCustomGroup(group)).toBe(false);
  });
});

describe("custom group layout operations", () => {
  const baseLayout: NavLayoutEntry[] = [
    { type: "page", pageId: "overview" },
    {
      type: "group",
      groupId: "user-my-tools",
      pluginKey: "",
      label: "My Tools",
      children: [
        { type: "page", pageId: "tool-a" },
        { type: "page", pageId: "tool-b" },
      ],
    },
    {
      type: "group",
      groupId: "core-group",
      pluginKey: "core",
      label: "Core",
      children: [{ type: "page", pageId: "clusters" }],
    },
    { type: "page", pageId: "settings" },
  ];

  it("deleting a custom group promotes children to top level at group position", () => {
    const result: NavLayoutEntry[] = [];
    for (const entry of baseLayout) {
      if (entry.type === "group" && entry.groupId === "user-my-tools") {
        for (const child of entry.children) {
          result.push({ type: "page", pageId: child.pageId });
        }
      } else {
        result.push(entry);
      }
    }

    expect(result).toHaveLength(5);
    // overview stays first
    expect(result[0]).toEqual({ type: "page", pageId: "overview" });
    // promoted children at group's position
    expect(result[1]).toEqual({ type: "page", pageId: "tool-a" });
    expect(result[2]).toEqual({ type: "page", pageId: "tool-b" });
    // rest unchanged
    expect(result[3]).toMatchObject({
      type: "group",
      groupId: "core-group",
    });
    expect(result[4]).toEqual({ type: "page", pageId: "settings" });
  });

  it("creating a custom group generates correct ID", () => {
    const name = "DevOps Tooling";
    const groupId = `${CUSTOM_GROUP_PREFIX}${slugify(name)}`;
    expect(groupId).toBe("user-devops-tooling");

    const newGroup: NavLayoutGroup = {
      type: "group",
      groupId,
      pluginKey: "",
      label: name,
      children: [],
      description: "Tools for DevOps",
      keywords: ["cicd", "deploy"],
      icon: "wrench",
    };

    expect(isCustomGroup(newGroup)).toBe(true);
    expect(newGroup.description).toBe("Tools for DevOps");
    expect(newGroup.keywords).toEqual(["cicd", "deploy"]);
    expect(newGroup.icon).toBe("wrench");
  });

  it("editing a custom group preserves children", () => {
    const group = baseLayout[1] as NavLayoutGroup;
    const edited: NavLayoutGroup = {
      ...group,
      label: "Renamed Tools",
      groupId: `${CUSTOM_GROUP_PREFIX}${slugify("Renamed Tools")}`,
      description: "Updated description",
    };

    expect(edited.groupId).toBe("user-renamed-tools");
    expect(edited.label).toBe("Renamed Tools");
    expect(edited.children).toHaveLength(2);
    expect(edited.children[0].pageId).toBe("tool-a");
  });
});
