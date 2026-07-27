import type { FlatNode } from "@fleetshift/common";
import { MORE_ENTRY_ID, NodeKind } from "@fleetshift/common";
import { expect, test } from "@playwright/experimental-ct-react";

import type { DragState } from "../../useDragTree";
import { TreeItem } from "../TreeItem";
import { TestSortableSection } from "./TestSortableSection";

type PageEntry = [string, { title: string; scope: string }];

// ---------------------------------------------------------------------------
// Factories
// ---------------------------------------------------------------------------

function page(id: string, parentId?: string): FlatNode {
  return {
    id,
    kind: NodeKind.Page,
    depth: parentId ? 1 : 0,
    parentId: parentId ?? null,
    pageId: id,
  };
}

function groupNode(id: string, opts?: { custom?: boolean }): FlatNode {
  const groupId = opts?.custom ? `user-${id}` : id;
  return {
    id,
    kind: NodeKind.Group,
    depth: 0,
    parentId: null,
    label: id,
    groupMeta: {
      type: "group",
      groupId,
      pluginKey: "test",
      label: id,
      children: [],
    },
  };
}

function sectionNode(id: string): FlatNode {
  return {
    id,
    kind: NodeKind.Section,
    depth: 0,
    parentId: null,
    label: id,
  };
}

function divider(): FlatNode {
  return {
    id: MORE_ENTRY_ID,
    kind: NodeKind.Section,
    depth: 0,
    parentId: null,
    label: "Hidden",
  };
}

function noop() {}

function pages(entries: [string, string][]): PageEntry[] {
  return entries.map(([id, title]) => [id, { title, scope: "test" }]);
}

// ---------------------------------------------------------------------------
// TreeItem tests
// ---------------------------------------------------------------------------

test.describe("TreeItem", () => {
  test("page node renders label and drag handle", async ({ mount }) => {
    const node = page("dashboard");
    const component = await mount(
      <TreeItem
        node={node}
        label="Dashboard"
        isElevated={false}
        isGhost={false}
        isHidden={false}
        isDropTarget={false}
        isDragActive={false}
        isKbDrag={false}
        displacementY={0}
      />,
    );
    await expect(component.getByText("Dashboard")).toBeVisible();
    await expect(
      component.getByRole("button", { name: "Reorder Dashboard" }),
    ).toBeVisible();
  });

  test("section node renders with section row class", async ({ mount }) => {
    const node = sectionNode("infra");
    const component = await mount(
      <TreeItem
        node={node}
        label="Infrastructure"
        isElevated={false}
        isGhost={false}
        isHidden={false}
        isDropTarget={false}
        isDragActive={false}
        isKbDrag={false}
        displacementY={0}
      />,
    );
    await expect(
      component.locator(".ome-settings-tree-item__row--section"),
    ).toBeVisible();
    await expect(
      component.locator(".ome-settings-tree-item__label--section"),
    ).toBeVisible();
  });

  test("divider renders Hidden text with no drag handle", async ({ mount }) => {
    const node = { ...divider() };
    const component = await mount(
      <TreeItem
        node={node}
        label="Hidden"
        isElevated={false}
        isGhost={false}
        isHidden={false}
        isDropTarget={false}
        isDragActive={false}
        isKbDrag={false}
        displacementY={0}
      />,
    );
    await expect(
      component.locator(".ome-settings-nav-editor__hidden-divider"),
    ).toContainText("Hidden");
    await expect(component.locator("[data-drag-handle]")).toHaveCount(0);
  });

  test("page action menu shows Set icon and Hide", async ({
    mount,
    page: p,
  }) => {
    const node = page("clusters");
    let iconClicked = false;
    const component = await mount(
      <TreeItem
        node={node}
        label="Clusters"
        isElevated={false}
        isGhost={false}
        isHidden={false}
        isDropTarget={false}
        isDragActive={false}
        isKbDrag={false}
        displacementY={0}
        onSetIcon={() => {
          iconClicked = true;
        }}
        onHideItem={noop}
      />,
    );
    await component
      .getByRole("button", { name: "Actions for Clusters" })
      .click();
    // PF Dropdown renders in a portal outside the component root
    await expect(p.getByRole("menuitem", { name: "Set icon" })).toBeVisible();
    await expect(p.getByRole("menuitem", { name: "Hide" })).toBeVisible();
    await p.getByRole("menuitem", { name: "Set icon" }).click();
    expect(iconClicked).toBe(true);
  });

  test("custom group shows Edit group and Delete group", async ({
    mount,
    page: p,
  }) => {
    const node = groupNode("mygroup", { custom: true });
    let editClicked = false;
    const component = await mount(
      <TreeItem
        node={node}
        label="My Group"
        isElevated={false}
        isGhost={false}
        isHidden={false}
        isDropTarget={false}
        isDragActive={false}
        isKbDrag={false}
        displacementY={0}
        onEditGroup={() => {
          editClicked = true;
        }}
        onDeleteGroup={noop}
      />,
    );
    await component
      .getByRole("button", { name: "Actions for My Group" })
      .click();
    await expect(p.getByRole("menuitem", { name: "Edit group" })).toBeVisible();
    await expect(
      p.getByRole("menuitem", { name: "Delete group" }),
    ).toBeVisible();
    await p.getByRole("menuitem", { name: "Edit group" }).click();
    expect(editClicked).toBe(true);
  });

  test("isHidden adds hidden class", async ({ mount, page: p }) => {
    const node = page("hidden-page");
    await mount(
      <TreeItem
        node={node}
        label="Hidden Page"
        isElevated={false}
        isGhost={false}
        isHidden={true}
        isDropTarget={false}
        isDragActive={false}
        isKbDrag={false}
        displacementY={0}
      />,
    );
    await expect(p.locator(".ome-settings-tree-item--hidden")).toBeVisible();
  });

  test("isDropTarget adds drop-target class", async ({ mount, page: p }) => {
    const node = groupNode("target-group");
    await mount(
      <TreeItem
        node={node}
        label="Target Group"
        isElevated={false}
        isGhost={false}
        isHidden={false}
        isDropTarget={true}
        isDragActive={false}
        isKbDrag={false}
        displacementY={0}
      />,
    );
    await expect(
      p.locator(".ome-settings-tree-item--drop-target"),
    ).toBeVisible();
  });

  test("isElevated adds elevated class", async ({ mount, page: p }) => {
    const node = page("elevated-page");
    await mount(
      <TreeItem
        node={node}
        label="Elevated Page"
        isElevated={true}
        isGhost={false}
        isHidden={false}
        isDropTarget={false}
        isDragActive={true}
        isKbDrag={false}
        displacementY={0}
      />,
    );
    await expect(p.locator(".ome-settings-tree-item--elevated")).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// SortableSection tests
// ---------------------------------------------------------------------------

test.describe("SortableSection", () => {
  test("renders all nodes with correct labels", async ({ mount, page: p }) => {
    const nodes = [page("a"), page("b"), page("c")];
    const pm = pages([
      ["a", "Alpha"],
      ["b", "Bravo"],
      ["c", "Charlie"],
    ]);
    await mount(
      <TestSortableSection
        sectionLabel="Main"
        nodes={nodes}
        pages={pm}
        dragState={null}
        isKbDrag={false}
        onPointerDown={noop}
        onPointerMove={noop}
        onPointerUp={noop}
        onPointerCancel={noop}
        onKeyDown={noop}
        onBlur={noop}
      />,
    );
    await expect(p.getByText("Alpha")).toBeVisible();
    await expect(p.getByText("Bravo")).toBeVisible();
    await expect(p.getByText("Charlie")).toBeVisible();
  });

  test("empty group shows placeholder", async ({ mount, page: p }) => {
    const nodes = [groupNode("g"), page("x")];
    const pm = pages([["x", "Page X"]]);
    await mount(
      <TestSortableSection
        sectionLabel="Main"
        nodes={nodes}
        pages={pm}
        dragState={null}
        isKbDrag={false}
        onPointerDown={noop}
        onPointerMove={noop}
        onPointerUp={noop}
        onPointerCancel={noop}
        onKeyDown={noop}
        onBlur={noop}
      />,
    );
    await expect(
      p.locator(".ome-settings-tree-item__empty-group"),
    ).toContainText("Drop items here to add to this group");
  });

  test("hidden divider does NOT show empty placeholder", async ({
    mount,
    page: p,
  }) => {
    const nodes = [page("a"), divider(), page("b")];
    const pm = pages([
      ["a", "Visible"],
      ["b", "Hidden One"],
    ]);
    await mount(
      <TestSortableSection
        sectionLabel="Main"
        nodes={nodes}
        pages={pm}
        dragState={null}
        isKbDrag={false}
        hiddenDividerIndex={1}
        onPointerDown={noop}
        onPointerMove={noop}
        onPointerUp={noop}
        onPointerCancel={noop}
        onKeyDown={noop}
        onBlur={noop}
      />,
    );
    await expect(p.locator(".ome-settings-tree-item__empty-group")).toHaveCount(
      0,
    );
  });

  test("items after hidden divider get hidden styling", async ({
    mount,
    page: p,
  }) => {
    const nodes = [page("vis"), divider(), page("hid")];
    const pm = pages([
      ["vis", "Visible"],
      ["hid", "Hidden"],
    ]);
    await mount(
      <TestSortableSection
        sectionLabel="Main"
        nodes={nodes}
        pages={pm}
        dragState={null}
        isKbDrag={false}
        hiddenDividerIndex={1}
        onPointerDown={noop}
        onPointerMove={noop}
        onPointerUp={noop}
        onPointerCancel={noop}
        onKeyDown={noop}
        onBlur={noop}
      />,
    );
    const hiddenItem = p.locator(".ome-settings-tree-item--hidden");
    await expect(hiddenItem).toHaveCount(1);
    await expect(hiddenItem).toContainText("Hidden");
  });

  test("section label renders above tree list", async ({ mount, page: p }) => {
    const nodes = [page("a")];
    const pm = pages([["a", "Alpha"]]);
    await mount(
      <TestSortableSection
        sectionLabel="Navigation"
        nodes={nodes}
        pages={pm}
        dragState={null}
        isKbDrag={false}
        onPointerDown={noop}
        onPointerMove={noop}
        onPointerUp={noop}
        onPointerCancel={noop}
        onKeyDown={noop}
        onBlur={noop}
      />,
    );
    await expect(
      p.locator(".ome-settings-nav-editor__section-label"),
    ).toContainText("Navigation");
  });

  test("empty group as drop target gets drop-target class", async ({
    mount,
    page: p,
  }) => {
    const g = groupNode("g");
    const nodes = [g, page("dragged")];
    const pm = pages([["dragged", "Dragged"]]);
    const dragState: DragState = {
      dragId: "dragged",
      dragParentId: null,
      dropIndex: 0,
      dropDepth: 1,
      dropParentId: "g",
      isBlock: false,
      blockLength: 1,
      sourceTopIndex: 1,
      blockHeight: 40,
      nestGap: 0,
    };
    await mount(
      <TestSortableSection
        sectionLabel="Main"
        nodes={nodes}
        pages={pm}
        dragState={dragState}
        isKbDrag={false}
        onPointerDown={noop}
        onPointerMove={noop}
        onPointerUp={noop}
        onPointerCancel={noop}
        onKeyDown={noop}
        onBlur={noop}
      />,
    );
    const emptyPlaceholder = p.locator(
      ".ome-settings-tree-item--drop-target .ome-settings-tree-item__empty-group",
    );
    await expect(emptyPlaceholder).toBeVisible();
    await expect(emptyPlaceholder).toContainText(
      "Drop items here to add to this group",
    );
  });

  test("drop target group gets drop-target class via dragState", async ({
    mount,
    page: p,
  }) => {
    const g = groupNode("target");
    const nodes = [g, page("child", "target"), page("other")];
    const pm = pages([
      ["child", "Child"],
      ["other", "Other"],
    ]);
    const dragState: DragState = {
      dragId: "other",
      dragParentId: null,
      dropIndex: 0,
      dropDepth: 1,
      dropParentId: "target",
      isBlock: false,
      blockLength: 1,
      sourceTopIndex: 2,
      blockHeight: 40,
      nestGap: 0,
    };
    await mount(
      <TestSortableSection
        sectionLabel="Main"
        nodes={nodes}
        pages={pm}
        dragState={dragState}
        isKbDrag={false}
        onPointerDown={noop}
        onPointerMove={noop}
        onPointerUp={noop}
        onPointerCancel={noop}
        onKeyDown={noop}
        onBlur={noop}
      />,
    );
    const dropTarget = p.locator(".ome-settings-tree-item--drop-target");
    await expect(dropTarget).toHaveCount(1);
    await expect(dropTarget).toContainText("target");
  });
});
