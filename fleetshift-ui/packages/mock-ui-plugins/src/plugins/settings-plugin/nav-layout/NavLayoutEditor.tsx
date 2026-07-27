import "./NavLayoutEditor.scss";

import type {
  FlatNode,
  FleetShiftApi,
  NavLayoutEntry,
  NavLayoutGroup,
} from "@fleetshift/common";
import {
  buildLayout,
  CUSTOM_GROUP_PREFIX,
  extractMore,
  flattenLayout,
  mergeLayout,
  MORE_ENTRY_ID,
  NodeKind,
  normalizeOrder,
  slugify,
  useNavLayout,
} from "@fleetshift/common";
import {
  Button,
  Content,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Title,
} from "@patternfly/react-core";
import { PlusCircleIcon } from "@patternfly/react-icons";
import { useScalprum } from "@scalprum/react-core";
import { useCallback, useMemo, useState } from "react";

import type { GroupFormData } from "../GroupFormModal";
import GroupFormModal from "../GroupFormModal";
import IconGalleryModal from "../IconGalleryModal";
import { useDragTree } from "../useDragTree";
import {
  collectGroupIds,
  deleteGroupFromLayout,
  findGroup,
  resolveLabel,
  splitNodes,
} from "./navLayoutHelpers";
import { SortableSection } from "./SortableSection";

const NavLayoutEditor = () => {
  const { api } = useScalprum<{ api: FleetShiftApi }>();
  const { override, loaded, setOverride, clearOverride } = useNavLayout(
    api.fleetshift.extensionStore,
  );
  const [resetItemId, setResetItemId] = useState<string | null>(null);
  const [showFullReset, setShowFullReset] = useState(false);
  const [showGroupForm, setShowGroupForm] = useState(false);
  const [editGroupId, setEditGroupId] = useState<string | null>(null);
  const [deleteGroupId, setDeleteGroupId] = useState<string | null>(null);
  const [iconTarget, setIconTarget] = useState<{
    id: string;
    kind: NodeKind.Page | NodeKind.Group;
  } | null>(null);

  const backendLayout = useMemo(() => api.fleetshift.getBackendLayout(), [api]);

  const pageMap = useMemo(() => {
    const map = new Map<string, { title: string; scope: string }>();
    const pages = api.fleetshift.getNavPages();
    for (const p of pages) {
      map.set(p.id, { title: p.title, scope: p.scope });
    }
    return map;
  }, [api]);

  const effectiveLayout = useMemo(
    () => mergeLayout(backendLayout, override),
    [backendLayout, override],
  );

  const existingGroupIds = useMemo(
    () => collectGroupIds(effectiveLayout),
    [effectiveLayout],
  );

  const { mainNodes, bottomNodes, hiddenDividerIndex } = useMemo(() => {
    const { active, more } = extractMore(effectiveLayout);
    const allNodes = flattenLayout(active);
    const { main, bottom } = splitNodes(allNodes, pageMap);

    // Append hidden divider + hidden items to main nodes
    const dividerNode: FlatNode = {
      id: MORE_ENTRY_ID,
      kind: NodeKind.Section,
      depth: 0,
      parentId: null,
      label: "Hidden",
    };
    const hiddenNodes = flattenLayout(more);
    const dividerIdx = main.length;
    const combined = [...main, dividerNode, ...hiddenNodes];

    return {
      mainNodes: combined,
      bottomNodes: bottom,
      hiddenDividerIndex: dividerIdx,
    };
  }, [effectiveLayout, pageMap]);

  const persistLayout = useCallback(
    (layout: NavLayoutEntry[]) => {
      setOverride({ version: 1, layout });
    },
    [setOverride],
  );

  const handleMainReorder = useCallback(
    (newNodes: FlatNode[]) => {
      // Split at the hidden divider to separate active from hidden items
      const divIdx = newNodes.findIndex((n) => n.id === MORE_ENTRY_ID);
      let activeNodes: FlatNode[];
      let hiddenNodes: FlatNode[];
      if (divIdx === -1) {
        activeNodes = newNodes;
        hiddenNodes = [];
      } else {
        activeNodes = newNodes.slice(0, divIdx);
        hiddenNodes = newNodes.slice(divIdx + 1);
      }

      const normalizedActive = normalizeOrder(activeNodes);
      const normalizedHidden = normalizeOrder(hiddenNodes);
      const activeLayout = buildLayout([...normalizedActive, ...bottomNodes]);
      const hiddenLayout = buildLayout(normalizedHidden);
      if (hiddenLayout.length > 0) {
        activeLayout.push({ type: "more", children: hiddenLayout });
      }
      persistLayout(activeLayout);
    },
    [bottomNodes, persistLayout],
  );

  const handleBottomReorder = useCallback(
    (newBottom: FlatNode[]) => {
      // Reconstruct active main nodes (before divider) from current mainNodes
      const divIdx = mainNodes.findIndex((n) => n.id === MORE_ENTRY_ID);
      const activeMain = divIdx === -1 ? mainNodes : mainNodes.slice(0, divIdx);
      const hiddenMain = divIdx === -1 ? [] : mainNodes.slice(divIdx + 1);

      const normalized = normalizeOrder(newBottom);
      const activeLayout = buildLayout([...activeMain, ...normalized]);
      const hiddenLayout = buildLayout(hiddenMain);
      if (hiddenLayout.length > 0) {
        activeLayout.push({ type: "more", children: hiddenLayout });
      }
      persistLayout(activeLayout);
    },
    [mainNodes, persistLayout],
  );

  const mainDrag = useDragTree(mainNodes, handleMainReorder);
  const bottomDrag = useDragTree(bottomNodes, handleBottomReorder);

  // --- Hide / Restore ---

  const handleHideItem = useCallback(
    (nodeId: string) => {
      const currentLayout = effectiveLayout;
      const { active, more } = extractMore(currentLayout);

      // Find the entry to hide (could be a page or group at any depth)
      let hiddenEntry: NavLayoutEntry | null = null;
      const updatedActive: NavLayoutEntry[] = [];
      for (const entry of active) {
        if (
          (entry.type === "page" && entry.pageId === nodeId) ||
          (entry.type === "group" && entry.groupId === nodeId)
        ) {
          hiddenEntry = entry;
        } else if (entry.type === "group") {
          // Check if a child page is being hidden
          const childIdx = entry.children.findIndex((c) => c.pageId === nodeId);
          if (childIdx !== -1) {
            hiddenEntry = {
              type: "page",
              pageId: entry.children[childIdx].pageId,
              iconOverride: entry.children[childIdx].iconOverride,
            };
            updatedActive.push({
              ...entry,
              children: entry.children.filter((c) => c.pageId !== nodeId),
            });
          } else {
            updatedActive.push(entry);
          }
        } else {
          updatedActive.push(entry);
        }
      }

      if (!hiddenEntry) return;

      const updatedMore: NavLayoutEntry[] = [...more, hiddenEntry];
      const layout: NavLayoutEntry[] = [
        ...updatedActive,
        { type: "more" as const, children: updatedMore },
      ];
      persistLayout(layout);
    },
    [effectiveLayout, persistLayout],
  );

  const handleRestoreItem = useCallback(
    (nodeId: string) => {
      const currentLayout = effectiveLayout;
      const { active, more } = extractMore(currentLayout);

      // Find the entry to restore from hidden items
      let restoredEntry: NavLayoutEntry | null = null;
      const updatedMore: NavLayoutEntry[] = [];
      for (const entry of more) {
        if (
          (entry.type === "page" && entry.pageId === nodeId) ||
          (entry.type === "group" && entry.groupId === nodeId)
        ) {
          restoredEntry = entry;
        } else {
          updatedMore.push(entry);
        }
      }

      if (!restoredEntry) return;

      const layout: NavLayoutEntry[] = [...active, restoredEntry];
      if (updatedMore.length > 0) {
        layout.push({ type: "more" as const, children: updatedMore });
      }
      persistLayout(layout);
    },
    [effectiveLayout, persistLayout],
  );

  const handleResetItem = useCallback((pageId: string) => {
    setResetItemId(pageId);
  }, []);

  const confirmResetItem = useCallback(() => {
    if (!resetItemId || !override) return;

    const removeFromEntries = (entries: NavLayoutEntry[]): NavLayoutEntry[] =>
      entries
        .map((entry) => {
          if (entry.type === "page" && entry.pageId === resetItemId) {
            return null;
          }
          if (entry.type === "group") {
            return {
              ...entry,
              children: entry.children.filter((c) => c.pageId !== resetItemId),
            };
          }
          if (entry.type === "section") {
            return {
              ...entry,
              children: entry.children.filter((c) => c.pageId !== resetItemId),
            };
          }
          if (entry.type === "more") {
            const filtered = removeFromEntries(entry.children);
            return filtered.length > 0
              ? { ...entry, children: filtered }
              : null;
          }
          return entry;
        })
        .filter(Boolean) as NavLayoutEntry[];

    const filtered = removeFromEntries(override.layout);

    const reconciled = mergeLayout(backendLayout, {
      version: 1,
      layout: filtered,
    });
    persistLayout(reconciled);
    setResetItemId(null);
  }, [resetItemId, override, backendLayout, persistLayout]);

  const confirmFullReset = useCallback(() => {
    clearOverride();
    setShowFullReset(false);
  }, [clearOverride]);

  // --- Custom group CRUD ---

  const handleOpenAddGroup = useCallback(() => {
    setEditGroupId(null);
    setShowGroupForm(true);
  }, []);

  const handleOpenEditGroup = useCallback((groupId: string) => {
    setEditGroupId(groupId);
    setShowGroupForm(true);
  }, []);

  const handleCloseGroupForm = useCallback(() => {
    setShowGroupForm(false);
    setEditGroupId(null);
  }, []);

  const editGroup = useMemo(
    () =>
      editGroupId ? (findGroup(effectiveLayout, editGroupId) ?? null) : null,
    [editGroupId, effectiveLayout],
  );

  const handleSaveGroup = useCallback(
    (data: GroupFormData) => {
      const groupId = `${CUSTOM_GROUP_PREFIX}${slugify(data.name)}`;
      const currentLayout = override?.layout ?? effectiveLayout;

      if (editGroupId) {
        // Edit existing group — update metadata, preserve children + position
        const updated = currentLayout.map((entry) => {
          if (entry.type === "group" && entry.groupId === editGroupId) {
            return {
              ...entry,
              groupId,
              label: data.name,
              description: data.description || undefined,
              keywords: data.keywords.length > 0 ? data.keywords : undefined,
              icon: data.icon || undefined,
            };
          }
          return entry;
        });
        persistLayout(updated);
      } else {
        // Create new group — insert before "more" entry (if any)
        const newGroup: NavLayoutGroup = {
          type: "group",
          groupId,
          pluginKey: "",
          label: data.name,
          children: [],
          description: data.description || undefined,
          keywords: data.keywords.length > 0 ? data.keywords : undefined,
          icon: data.icon || undefined,
        };
        const { active, more } = extractMore(currentLayout);
        const updated: NavLayoutEntry[] = [...active, newGroup];
        if (more.length > 0) {
          updated.push({ type: "more", children: more });
        }
        persistLayout(updated);
      }

      handleCloseGroupForm();
    },
    [
      editGroupId,
      override,
      effectiveLayout,
      persistLayout,
      handleCloseGroupForm,
    ],
  );

  const handleRequestDeleteGroup = useCallback((groupId: string) => {
    setDeleteGroupId(groupId);
  }, []);

  const confirmDeleteGroup = useCallback(() => {
    if (!deleteGroupId) return;
    const currentLayout = override?.layout ?? effectiveLayout;
    const updated = deleteGroupFromLayout(currentLayout, deleteGroupId);
    persistLayout(updated);
    setDeleteGroupId(null);
  }, [deleteGroupId, override, effectiveLayout, persistLayout]);

  // --- Icon override ---

  const handleOpenIconGallery = useCallback(
    (nodeId: string, kind: NodeKind.Page | NodeKind.Group) => {
      setIconTarget({ id: nodeId, kind });
    },
    [],
  );

  const iconTargetCurrentIcon = useMemo(() => {
    if (!iconTarget) return null;
    const currentLayout = override?.layout ?? effectiveLayout;

    const searchEntries = (entries: NavLayoutEntry[]): string | null => {
      if (iconTarget.kind === NodeKind.Group) {
        for (const entry of entries) {
          if (entry.type === "group" && entry.groupId === iconTarget.id) {
            return entry.icon ?? null;
          }
          if (entry.type === "more") {
            const found = searchEntries(entry.children);
            if (found !== null) return found;
          }
        }
        return null;
      }
      for (const entry of entries) {
        if (entry.type === "page" && entry.pageId === iconTarget.id) {
          return entry.iconOverride ?? null;
        }
        if (entry.type === "group") {
          for (const child of entry.children) {
            if (child.pageId === iconTarget.id) {
              return child.iconOverride ?? null;
            }
          }
        }
        if (entry.type === "more") {
          const found = searchEntries(entry.children);
          if (found !== null) return found;
        }
      }
      return null;
    };

    return searchEntries(currentLayout);
  }, [iconTarget, override, effectiveLayout]);

  const handleSetIcon = useCallback(
    (iconName: string | null) => {
      if (!iconTarget) return;
      const currentLayout = override?.layout ?? effectiveLayout;

      const applyIcon = (entries: NavLayoutEntry[]): NavLayoutEntry[] =>
        entries.map((entry) => {
          if (iconTarget.kind === NodeKind.Group) {
            if (entry.type === "group" && entry.groupId === iconTarget.id) {
              return { ...entry, icon: iconName || undefined };
            }
          } else {
            if (entry.type === "page" && entry.pageId === iconTarget.id) {
              return { ...entry, iconOverride: iconName || undefined };
            }
            if (entry.type === "group") {
              const updatedChildren = entry.children.map((child) =>
                child.pageId === iconTarget.id
                  ? { ...child, iconOverride: iconName || undefined }
                  : child,
              );
              return { ...entry, children: updatedChildren };
            }
          }
          if (entry.type === "more") {
            return { ...entry, children: applyIcon(entry.children) };
          }
          return entry;
        });

      persistLayout(applyIcon(currentLayout));
      setIconTarget(null);
    },
    [iconTarget, override, effectiveLayout, persistLayout],
  );

  const deleteGroupLabel = useMemo(() => {
    if (!deleteGroupId) return "";
    const group = findGroup(effectiveLayout, deleteGroupId);
    return group?.label ?? deleteGroupId;
  }, [deleteGroupId, effectiveLayout]);

  const deleteGroupChildCount = useMemo(() => {
    if (!deleteGroupId) return 0;
    const group = findGroup(effectiveLayout, deleteGroupId);
    return group?.children.length ?? 0;
  }, [deleteGroupId, effectiveLayout]);

  if (!loaded) return null;

  const resetItemLabel = resetItemId ? resolveLabel(resetItemId, pageMap) : "";

  return (
    <div className="ome-settings-nav-editor">
      <div className="ome-settings-nav-editor__header">
        <Title headingLevel="h2">Navigation layout</Title>
        <div className="ome-settings-nav-editor__header-actions">
          <Button
            variant="link"
            icon={<PlusCircleIcon />}
            onClick={handleOpenAddGroup}
          >
            Add group
          </Button>
          <Button variant="link" onClick={() => setShowFullReset(true)}>
            Reset all to default
          </Button>
        </div>
      </div>
      <Content component="p">
        Drag items to reorder the navigation sidebar. Groups move with their
        children. Drag an item left to pull it out of a group, or right to nest
        it. Drag items below the Hidden divider to hide them, or use the menu.
      </Content>

      <SortableSection
        sectionLabel="Main"
        nodes={mainDrag.resolvedNodes}
        pageMap={pageMap}
        dragState={mainDrag.dragState}
        isKbDrag={mainDrag.isKbDrag}
        dragX={mainDrag.dragX}
        dragY={mainDrag.dragY}
        containerRef={mainDrag.containerRef}
        onPointerDown={mainDrag.handlePointerDown}
        onPointerMove={mainDrag.handlePointerMove}
        onPointerUp={mainDrag.handlePointerUp}
        onPointerCancel={mainDrag.handlePointerCancel}
        onKeyDown={mainDrag.handleKeyDown}
        onBlur={mainDrag.handleBlur}
        onResetItem={override ? handleResetItem : undefined}
        onEditGroup={handleOpenEditGroup}
        onDeleteGroup={handleRequestDeleteGroup}
        onSetIcon={handleOpenIconGallery}
        onHideItem={handleHideItem}
        onRestoreItem={handleRestoreItem}
        hiddenDividerIndex={hiddenDividerIndex}
      />

      {bottomNodes.length > 0 && (
        <SortableSection
          sectionLabel="Bottom"
          nodes={bottomDrag.resolvedNodes}
          pageMap={pageMap}
          dragState={bottomDrag.dragState}
          isKbDrag={bottomDrag.isKbDrag}
          dragX={bottomDrag.dragX}
          dragY={bottomDrag.dragY}
          containerRef={bottomDrag.containerRef}
          onPointerDown={bottomDrag.handlePointerDown}
          onPointerMove={bottomDrag.handlePointerMove}
          onPointerUp={bottomDrag.handlePointerUp}
          onPointerCancel={bottomDrag.handlePointerCancel}
          onKeyDown={bottomDrag.handleKeyDown}
          onBlur={bottomDrag.handleBlur}
          onResetItem={override ? handleResetItem : undefined}
          onEditGroup={handleOpenEditGroup}
          onDeleteGroup={handleRequestDeleteGroup}
          onSetIcon={handleOpenIconGallery}
        />
      )}

      <GroupFormModal
        isOpen={showGroupForm}
        editGroup={editGroup}
        existingGroupIds={existingGroupIds}
        onSave={handleSaveGroup}
        onClose={handleCloseGroupForm}
      />

      <IconGalleryModal
        isOpen={iconTarget !== null}
        selected={iconTargetCurrentIcon}
        onSelect={handleSetIcon}
        onClose={() => setIconTarget(null)}
      />

      <Modal
        variant="small"
        isOpen={deleteGroupId !== null}
        onClose={() => setDeleteGroupId(null)}
      >
        <ModalHeader title="Delete group" />
        <ModalBody>
          Delete group <strong>{deleteGroupLabel}</strong>?
          {deleteGroupChildCount > 0 && (
            <>
              {" "}
              Its {deleteGroupChildCount}{" "}
              {deleteGroupChildCount === 1 ? "child" : "children"} will be moved
              to the top level.
            </>
          )}
        </ModalBody>
        <ModalFooter>
          <Button variant="danger" onClick={confirmDeleteGroup}>
            Delete
          </Button>
          <Button variant="link" onClick={() => setDeleteGroupId(null)}>
            Cancel
          </Button>
        </ModalFooter>
      </Modal>

      <Modal
        variant="small"
        isOpen={resetItemId !== null}
        onClose={() => setResetItemId(null)}
      >
        <ModalHeader title="Reset item position" />
        <ModalBody>
          Reset <strong>{resetItemLabel}</strong> to its default position in the
          navigation? This cannot be undone.
        </ModalBody>
        <ModalFooter>
          <Button variant="primary" onClick={confirmResetItem}>
            Reset
          </Button>
          <Button variant="link" onClick={() => setResetItemId(null)}>
            Cancel
          </Button>
        </ModalFooter>
      </Modal>

      <Modal
        variant="small"
        isOpen={showFullReset}
        onClose={() => setShowFullReset(false)}
      >
        <ModalHeader title="Reset navigation layout" />
        <ModalBody>
          Reset the entire navigation layout to its default order? This will
          clear all your customizations and cannot be undone.
        </ModalBody>
        <ModalFooter>
          <Button variant="danger" onClick={confirmFullReset}>
            Reset all
          </Button>
          <Button variant="link" onClick={() => setShowFullReset(false)}>
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </div>
  );
};

export default NavLayoutEditor;
