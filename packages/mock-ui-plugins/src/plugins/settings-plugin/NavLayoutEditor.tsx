import "./NavLayoutEditor.scss";

import type { DragEndEvent, DragStartEvent } from "@dnd-kit/dom";
import { DragDropProvider } from "@dnd-kit/react";
import { useSortable } from "@dnd-kit/react/sortable";
import type {
  FlatNode,
  FleetShiftApi,
  NavLayoutEntry,
} from "@fleetshift/common";
import {
  arrayMove,
  buildLayout,
  CORE_EXTENSION_META,
  flattenLayout,
  getProjection,
  INDENTATION,
  mergeLayout,
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
import { GripVerticalIcon, UndoIcon } from "@patternfly/react-icons";
import { useScalprum } from "@scalprum/react-core";
import { useCallback, useMemo, useRef, useState } from "react";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function resolveLabel(
  pageId: string,
  pageMap: Map<string, { title: string }>,
): string {
  return pageMap.get(pageId)?.title ?? pageId;
}

function splitNodes(
  nodes: FlatNode[],
  pageMap: Map<string, { scope: string }>,
): { main: FlatNode[]; bottom: FlatNode[] } {
  const main: FlatNode[] = [];
  const bottom: FlatNode[] = [];

  const bottomContainerIds = new Set<string>();
  for (const node of nodes) {
    if (node.depth !== 0) continue;
    if (node.kind === "page" && node.pageId) {
      const scope = pageMap.get(node.pageId)?.scope;
      if (scope && CORE_EXTENSION_META[scope]?.navSection === "bottom") {
        bottomContainerIds.add(node.id);
      }
    } else if (node.kind === "group" && node.groupMeta) {
      const scope = `${node.groupMeta.pluginKey}-plugin`;
      if (CORE_EXTENSION_META[scope]?.navSection === "bottom") {
        bottomContainerIds.add(node.id);
      }
    }
  }

  for (const node of nodes) {
    const isBottomNode =
      bottomContainerIds.has(node.id) ||
      (node.parentId !== null && bottomContainerIds.has(node.parentId));
    (isBottomNode ? bottom : main).push(node);
  }

  return { main, bottom };
}

// ---------------------------------------------------------------------------
// TreeItem
// ---------------------------------------------------------------------------

interface TreeItemProps {
  node: FlatNode;
  index: number;
  label: string;
  onResetItem?: () => void;
}

function TreeItem({ node, index, label, onResetItem }: TreeItemProps) {
  const { ref, handleRef, isDragSource } = useSortable({
    id: node.id,
    index,
    data: { depth: node.depth, parentId: node.parentId, kind: node.kind },
    transition: { idle: true },
  });

  const isContainer = node.kind === "group" || node.kind === "section";
  const kindClass = isContainer ? "section" : "page";

  return (
    <li
      ref={ref}
      className={`ome-settings-tree-item${isDragSource ? " ome-settings-tree-item--dragging" : ""}`}
      // eslint-disable-next-line no-restricted-syntax -- dynamic: depth-based indentation
      style={{ marginLeft: node.depth * INDENTATION }}
    >
      <div
        className={`ome-settings-tree-item__row ome-settings-tree-item__row--${kindClass}`}
      >
        <span ref={handleRef} className="ome-settings-tree-item__handle">
          <GripVerticalIcon className="pf-v6-u-icon-color-subtle" />
        </span>

        <span
          className={`ome-settings-tree-item__label ome-settings-tree-item__label--${kindClass}`}
        >
          {label}
        </span>

        {!isContainer && onResetItem && (
          <Button
            variant="plain"
            size="sm"
            aria-label={`Reset ${label} to default position`}
            onClick={onResetItem}
            icon={<UndoIcon />}
          />
        )}
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------------
// SortableSection — one dnd-kit sortable list for main or bottom
// ---------------------------------------------------------------------------

interface SortableSectionProps {
  sectionLabel: string;
  nodes: FlatNode[];
  pageMap: Map<string, { title: string; scope: string }>;
  onReorder: (nodes: FlatNode[]) => void;
  onResetItem: (pageId: string) => void;
}

function SortableSection({
  sectionLabel,
  nodes,
  pageMap,
  onReorder,
  onResetItem,
}: SortableSectionProps) {
  const initialDepthRef = useRef(0);

  const handleDragStart = useCallback(
    (event: DragStartEvent) => {
      const id = String(event.operation.source?.id ?? "");
      const node = nodes.find((n) => n.id === id);
      initialDepthRef.current = node?.depth ?? 0;
    },
    [nodes],
  );

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { source, target } = event.operation;
      if (!target || !source) return;

      const sourceIndex =
        "index" in source.sortable ? (source.sortable.index as number) : -1;
      const targetIndex =
        "index" in target.sortable ? (target.sortable.index as number) : -1;

      if (sourceIndex === -1 || targetIndex === -1) return;

      let reordered = arrayMove(nodes, sourceIndex, targetIndex);

      // Apply depth projection based on drag offset
      const draggedId = String(source.id);
      const offsetX =
        (event.operation.position?.current?.x ?? 0) -
        (event.operation.position?.initial?.x ?? 0);
      const projection = getProjection(
        reordered,
        draggedId,
        offsetX,
        initialDepthRef.current,
      );
      reordered = reordered.map((n) =>
        n.id === draggedId
          ? { ...n, depth: projection.depth, parentId: projection.parentId }
          : n,
      );

      onReorder(reordered);
    },
    [nodes, onReorder],
  );

  return (
    <div>
      <div className="ome-settings-nav-editor__section-label">
        {sectionLabel}
      </div>
      <DragDropProvider onDragStart={handleDragStart} onDragEnd={handleDragEnd}>
        <ul className="ome-settings-nav-editor__tree-list">
          {nodes.map((node, index) => {
            const label =
              node.label ??
              (node.pageId ? resolveLabel(node.pageId, pageMap) : node.id);
            return (
              <TreeItem
                key={node.id}
                node={node}
                index={index}
                label={label}
                onResetItem={
                  node.kind === "page" && node.pageId
                    ? () => onResetItem(node.pageId!)
                    : undefined
                }
              />
            );
          })}
        </ul>
      </DragDropProvider>
    </div>
  );
}

// ---------------------------------------------------------------------------
// NavLayoutEditor (main export)
// ---------------------------------------------------------------------------

const NavLayoutEditor = () => {
  const { api } = useScalprum<{ api: FleetShiftApi }>();
  const { override, loaded, setOverride, clearOverride } = useNavLayout();
  const [resetItemId, setResetItemId] = useState<string | null>(null);
  const [showFullReset, setShowFullReset] = useState(false);

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

  const { mainNodes, bottomNodes } = useMemo(() => {
    const allNodes = flattenLayout(effectiveLayout);
    const { main, bottom } = splitNodes(allNodes, pageMap);
    return { mainNodes: main, bottomNodes: bottom };
  }, [effectiveLayout, pageMap]);

  const persistLayout = useCallback(
    (layout: NavLayoutEntry[]) => {
      setOverride({ version: 1, layout });
    },
    [setOverride],
  );

  const handleMainReorder = useCallback(
    (newMain: FlatNode[]) => {
      const layout = buildLayout([...newMain, ...bottomNodes]);
      persistLayout(layout);
    },
    [bottomNodes, persistLayout],
  );

  const handleBottomReorder = useCallback(
    (newBottom: FlatNode[]) => {
      const layout = buildLayout([...mainNodes, ...newBottom]);
      persistLayout(layout);
    },
    [mainNodes, persistLayout],
  );

  const handleResetItem = useCallback((pageId: string) => {
    setResetItemId(pageId);
  }, []);

  const confirmResetItem = useCallback(() => {
    if (!resetItemId || !override) return;

    const filtered = override.layout
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
        return entry;
      })
      .filter(Boolean) as NavLayoutEntry[];

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

  if (!loaded) return null;

  const resetItemLabel = resetItemId ? resolveLabel(resetItemId, pageMap) : "";

  return (
    <div className="ome-settings-nav-editor">
      <div className="ome-settings-nav-editor__header">
        <Title headingLevel="h2">Navigation layout</Title>
        <Button variant="link" onClick={() => setShowFullReset(true)}>
          Reset all to default
        </Button>
      </div>
      <Content component="p">
        Drag items to reorder the navigation sidebar. Items can be nested inside
        groups by dragging them to the right.
      </Content>

      <SortableSection
        sectionLabel="Main"
        nodes={mainNodes}
        pageMap={pageMap}
        onReorder={handleMainReorder}
        onResetItem={handleResetItem}
      />

      {bottomNodes.length > 0 && (
        <SortableSection
          sectionLabel="Bottom"
          nodes={bottomNodes}
          pageMap={pageMap}
          onReorder={handleBottomReorder}
          onResetItem={handleResetItem}
        />
      )}

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
