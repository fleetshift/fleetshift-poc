import type { FlatNode } from "@fleetshift/common";
import { MORE_ENTRY_ID, NodeKind } from "@fleetshift/common";
import type { MotionValue } from "motion/react";

import type { DragState } from "../useDragTree";
import { computeDisplacement, resolveLabel } from "./navLayoutHelpers";
import { TreeItem } from "./TreeItem";

export interface SortableSectionProps {
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
  onEditGroup?: (groupId: string) => void;
  onDeleteGroup?: (groupId: string) => void;
  onSetIcon?: (nodeId: string, kind: NodeKind.Page | NodeKind.Group) => void;
  onHideItem?: (nodeId: string) => void;
  onRestoreItem?: (nodeId: string) => void;
  /** Index of the hidden divider in the nodes array (-1 = no divider). */
  hiddenDividerIndex?: number;
}

export function SortableSection({
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
  onEditGroup,
  onDeleteGroup,
  onSetIcon,
  onHideItem,
  onRestoreItem,
  hiddenDividerIndex = -1,
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

    const isAfterDivider = hiddenDividerIndex >= 0 && i > hiddenDividerIndex;

    // Hide/restore only supported for top-level pages, top-level groups,
    // and group children. Sections and section children are not supported.
    const isSection = node.kind === NodeKind.Section;
    const isSectionChild =
      node.depth === 1 &&
      node.parentId !== null &&
      nodes.some((n) => n.id === node.parentId && n.kind === NodeKind.Section);
    const canHideRestore = !isSection && !isSectionChild;

    const isDropTarget =
      !isInDragBlock &&
      !!dragState &&
      dragState.dropParentId === node.id &&
      dragState.dragId !== node.id;

    items.push(
      <TreeItem
        key={node.id}
        node={node}
        label={label}
        isElevated={isInDragBlock}
        isGhost={isInDragBlock}
        isHidden={isAfterDivider && !isInDragBlock}
        isDropTarget={isDropTarget}
        isDragActive={!!dragState}
        isKbDrag={isKbDrag}
        displacementY={displacementY}
        dragX={isInDragBlock ? dragX : undefined}
        dragY={isInDragBlock ? dragY : undefined}
        onResetItem={
          onResetItem &&
          node.kind === NodeKind.Page &&
          node.pageId &&
          !isAfterDivider
            ? () => onResetItem(node.pageId!)
            : undefined
        }
        onEditGroup={
          onEditGroup && node.kind === NodeKind.Group && !isAfterDivider
            ? () => onEditGroup(node.id)
            : undefined
        }
        onDeleteGroup={
          onDeleteGroup && node.kind === NodeKind.Group && !isAfterDivider
            ? () => onDeleteGroup(node.id)
            : undefined
        }
        onSetIcon={
          onSetIcon && !isAfterDivider
            ? () =>
                onSetIcon(
                  node.id,
                  node.kind === NodeKind.Group ? NodeKind.Group : NodeKind.Page,
                )
            : undefined
        }
        onHideItem={
          onHideItem &&
          !isAfterDivider &&
          node.id !== MORE_ENTRY_ID &&
          canHideRestore
            ? () => onHideItem(node.id)
            : undefined
        }
        onRestoreItem={
          onRestoreItem && isAfterDivider && canHideRestore
            ? () => onRestoreItem(node.id)
            : undefined
        }
      />,
    );

    const isContainer =
      node.kind === NodeKind.Group || node.kind === NodeKind.Section;
    if (isContainer && !isAfterDivider && node.id !== MORE_ENTRY_ID) {
      const hasChildren = nodes.some((n) => n.parentId === node.id);
      if (!hasChildren) {
        items.push(
          <li
            key={`${node.id}-empty`}
            className="ome-settings-tree-item ome-settings-tree-item--nested pf-v6-u-mr-2xl"
          >
            <div className="ome-settings-tree-item__empty-group">
              Drop items here to add to this group
            </div>
          </li>,
        );
      }
    }

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
