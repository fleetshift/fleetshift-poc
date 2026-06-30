import "./NavLayoutEditor.scss";

import type {
  FlatNode,
  FleetShiftApi,
  NavLayoutEntry,
} from "@fleetshift/common";
import {
  buildLayout,
  CORE_EXTENSION_META,
  flattenLayout,
  mergeLayout,
  normalizeOrder,
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
import { RhUiGripVerticalFillIcon, UndoIcon } from "@patternfly/react-icons";
import { useScalprum } from "@scalprum/react-core";
import clsx from "clsx";
import { motion, type MotionValue } from "motion/react";
import { useCallback, useMemo, useState } from "react";

import type { DragState } from "./useDragTree";
import { useDragTree } from "./useDragTree";

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

function computeDisplacement(
  topIdx: number,
  dragState: DragState | null,
): number {
  if (!dragState) return 0;
  const S = dragState.sourceTopIndex;
  const D = dragState.dropIndex;
  const h = dragState.blockHeight;

  if (D < S && topIdx >= D && topIdx < S) return h;
  if (D > S + 1 && topIdx >= S + 1 && topIdx < D) return -h;
  return 0;
}

// ---------------------------------------------------------------------------
// TreeItem
// ---------------------------------------------------------------------------

interface TreeItemProps {
  node: FlatNode;
  label: string;
  isElevated: boolean;
  isGhost: boolean;
  isDragActive: boolean;
  isKbDrag: boolean;
  displacementY: number;
  dragX?: MotionValue<number>;
  dragY?: MotionValue<number>;
  onResetItem?: () => void;
}

function TreeItem({
  node,
  label,
  isElevated,
  isGhost,
  isDragActive,
  isKbDrag,
  displacementY,
  dragX,
  dragY,
  onResetItem,
}: TreeItemProps) {
  const isContainer = node.kind === "group" || node.kind === "section";
  const kindClass = isContainer ? "section" : "page";

  return (
    <motion.li
      data-node-id={node.id}
      className={clsx(
        "ome-settings-tree-item",
        node.depth === 1 && "ome-settings-tree-item--nested",
        isElevated && "ome-settings-tree-item--elevated",
        isGhost && !isElevated && "ome-settings-tree-item--ghost",
      )}
      layout={isKbDrag}
      initial={false}
      animate={isElevated && !isKbDrag ? undefined : { y: displacementY }}
      style={isElevated && !isKbDrag ? { x: dragX, y: dragY } : undefined}
      transition={
        isDragActive
          ? { type: "tween", duration: 0.15, ease: "easeInOut" }
          : { duration: 0 }
      }
    >
      <div
        className={`ome-settings-tree-item__row ome-settings-tree-item__row--${kindClass}`}
      >
        <button
          type="button"
          data-drag-handle
          className="ome-settings-tree-item__handle"
          aria-label={`Reorder ${label}`}
          aria-roledescription="sortable"
        >
          <RhUiGripVerticalFillIcon />
        </button>

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
    </motion.li>
  );
}

// ---------------------------------------------------------------------------
// SortableSection
// ---------------------------------------------------------------------------

interface SortableSectionProps {
  sectionLabel: string;
  nodes: FlatNode[];
  pageMap: Map<string, { title: string; scope: string }>;
  dragState: DragState | null;
  isKbDrag: boolean;
  dragX: MotionValue<number>;
  dragY: MotionValue<number>;
  containerRef: React.RefObject<HTMLUListElement | null>;
  onPointerDown: (e: React.PointerEvent<HTMLElement>) => void;
  onPointerMove: (e: React.PointerEvent<HTMLElement>) => void;
  onPointerUp: (e: React.PointerEvent<HTMLElement>) => void;
  onPointerCancel: () => void;
  onKeyDown: (e: React.KeyboardEvent<HTMLElement>) => void;
  onBlur: (e: React.FocusEvent<HTMLElement>) => void;
  onResetItem?: (pageId: string) => void;
}

function SortableSection({
  sectionLabel,
  nodes,
  pageMap,
  dragState,
  isKbDrag,
  dragX,
  dragY,
  containerRef,
  onPointerDown,
  onPointerMove,
  onPointerUp,
  onPointerCancel,
  onKeyDown,
  onBlur,
  onResetItem,
}: SortableSectionProps) {
  const parentTopIdxMap = new Map<string, number>();
  const intraGroup =
    dragState &&
    dragState.dragParentId !== null &&
    dragState.dropParentId === dragState.dragParentId
      ? dragState.dragParentId
      : null;
  const nestingTarget =
    dragState &&
    dragState.dropParentId &&
    dragState.dropParentId !== dragState.dragParentId
      ? dragState.dropParentId
      : null;
  let topIdx = 0;
  let siblingIdx = 0;
  let nestChildIdx = 0;

  const items: React.ReactNode[] = [];

  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    const label =
      node.label ??
      (node.pageId ? resolveLabel(node.pageId, pageMap) : node.id);

    if (node.depth === 0) {
      parentTopIdxMap.set(node.id, topIdx);
    }

    let effectiveIdx: number;
    if (intraGroup) {
      if (node.parentId === intraGroup) {
        effectiveIdx = siblingIdx;
        siblingIdx++;
      } else {
        effectiveIdx = -1;
      }
    } else {
      effectiveIdx =
        node.depth === 0
          ? topIdx
          : (parentTopIdxMap.get(node.parentId!) ?? topIdx);
    }

    const isInDragBlock =
      dragState?.dragId === node.id ||
      (!!dragState?.isBlock && node.parentId === dragState.dragId);

    let displacementY: number;
    if (isInDragBlock) {
      displacementY = 0;
    } else if (nestingTarget && node.parentId === nestingTarget) {
      displacementY =
        nestChildIdx >= dragState!.nestGap ? dragState!.blockHeight : 0;
      nestChildIdx++;
    } else if (effectiveIdx === -1) {
      displacementY = 0;
    } else {
      displacementY = computeDisplacement(effectiveIdx, dragState);
    }

    items.push(
      <TreeItem
        key={node.id}
        node={node}
        label={label}
        isElevated={isInDragBlock}
        isGhost={isInDragBlock}
        isDragActive={!!dragState}
        isKbDrag={isKbDrag}
        displacementY={displacementY}
        dragX={isInDragBlock ? dragX : undefined}
        dragY={isInDragBlock ? dragY : undefined}
        onResetItem={
          onResetItem && node.kind === "page" && node.pageId
            ? () => onResetItem(node.pageId!)
            : undefined
        }
      />,
    );

    if (node.depth === 0) topIdx++;
  }

  return (
    <div>
      <div className="ome-settings-nav-editor__section-label">
        {sectionLabel}
      </div>
      <ul
        ref={containerRef}
        className="ome-settings-nav-editor__tree-list"
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerCancel}
        onKeyDown={onKeyDown}
        onBlur={onBlur}
      >
        {items}
      </ul>
    </div>
  );
}

// ---------------------------------------------------------------------------
// NavLayoutEditor (main export)
// ---------------------------------------------------------------------------

const NavLayoutEditor = () => {
  const { api } = useScalprum<{ api: FleetShiftApi }>();
  const { override, loaded, setOverride, clearOverride } = useNavLayout(
    api.fleetshift.extensionStore,
  );
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
      const normalized = normalizeOrder(newMain);
      const layout = buildLayout([...normalized, ...bottomNodes]);
      persistLayout(layout);
    },
    [bottomNodes, persistLayout],
  );

  const handleBottomReorder = useCallback(
    (newBottom: FlatNode[]) => {
      const normalized = normalizeOrder(newBottom);
      const layout = buildLayout([...mainNodes, ...normalized]);
      persistLayout(layout);
    },
    [mainNodes, persistLayout],
  );

  const mainDrag = useDragTree(mainNodes, handleMainReorder);
  const bottomDrag = useDragTree(bottomNodes, handleBottomReorder);

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
        Drag items to reorder the navigation sidebar. Groups move with their
        children. Drag an item left to pull it out of a group, or right to nest
        it.
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
