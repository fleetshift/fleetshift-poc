import type { FlatNode } from "@fleetshift/common";
import { INDENTATION, NodeKind } from "@fleetshift/common";
import { useMotionValue } from "motion/react";
import { useCallback, useEffect, useRef, useState } from "react";

import type { Slot } from "./dragTreeHelpers";
import {
  computeKbMove,
  computeNestGap,
  computeSlots,
  findDropGap,
  gapToFlatIndex,
  getBlockLength,
  measureBlockHeight,
  resolveDepthAndParent,
} from "./dragTreeHelpers";

export type { DragState } from "./dragTreeHelpers";

interface DragData {
  dragId: string;
  dragParentId: string | null;
  isBlock: boolean;
  blockLength: number;
  startX: number;
  startY: number;
  siblingSlots: Slot[];
  topLevelSlots: Slot[];
  containerLeft: number;
  siblingSourceIndex: number;
  topLevelSourceIndex: number;
  blockHeight: number;
  allMidYs: number[];
}

interface KbDragData {
  dragId: string;
  isBlock: boolean;
  blockLength: number;
  originalNodes: FlatNode[];
}

export function useDragTree(
  nodes: FlatNode[],
  onReorder: (nodes: FlatNode[]) => void,
) {
  const containerRef = useRef<HTMLUListElement>(null);
  const [dragState, setDragState] = useState<DragState | null>(null);
  const dragDataRef = useRef<DragData | null>(null);
  const kbDragRef = useRef<KbDragData | null>(null);
  const dragX = useMotionValue(0);
  const dragY = useMotionValue(0);

  const [previewNodes, setPreviewNodes] = useState<FlatNode[] | null>(null);
  const resolvedNodes = previewNodes ?? nodes;

  useEffect(() => {
    if (previewNodes) setPreviewNodes(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodes]);

  const handlePointerDown = useCallback(
    (e: React.PointerEvent<HTMLElement>) => {
      const handle = (e.target as HTMLElement).closest<HTMLElement>(
        "[data-drag-handle]",
      );
      if (!handle) return;

      const row = handle.closest<HTMLElement>("[data-node-id]");
      if (!row) return;

      const nodeId = row.dataset.nodeId!;
      const container = containerRef.current;
      if (!container) return;

      const idx = resolvedNodes.findIndex((n) => n.id === nodeId);
      if (idx === -1) return;

      const isBlock =
        resolvedNodes[idx].kind === NodeKind.Group ||
        resolvedNodes[idx].kind === NodeKind.Section;
      const blockLength = getBlockLength(resolvedNodes, idx);
      const dragParentId = resolvedNodes[idx].parentId;

      handle.setPointerCapture(e.pointerId);
      e.preventDefault();

      const containerRect = container.getBoundingClientRect();
      const blockHeight = measureBlockHeight(container, idx, blockLength);

      const allItems =
        container.querySelectorAll<HTMLElement>("[data-node-id]");
      const allMidYs: number[] = [];
      for (let i = 0; i < allItems.length; i++) {
        const rect = allItems[i].getBoundingClientRect();
        allMidYs.push(rect.top + rect.height / 2);
      }

      const siblingSlots = dragParentId
        ? computeSlots(
            container,
            resolvedNodes,
            nodeId,
            isBlock,
            blockLength,
            dragParentId,
          )
        : [];
      const topLevelSlots = computeSlots(
        container,
        resolvedNodes,
        nodeId,
        isBlock,
        blockLength,
        null,
      );

      let siblingSourceIndex = 0;
      if (dragParentId) {
        for (let i = 0; i < idx; i++) {
          if (resolvedNodes[i].parentId === dragParentId) siblingSourceIndex++;
        }
      }
      let topLevelSourceIndex = 0;
      for (let i = 0; i < idx; i++) {
        if (resolvedNodes[i].depth === 0) topLevelSourceIndex++;
      }

      const relX = e.clientX - containerRect.left;
      const intraGroup =
        !!dragParentId && !isBlock && relX >= INDENTATION * 0.5;
      const activeSlots = intraGroup ? siblingSlots : topLevelSlots;
      const activeSourceIndex = intraGroup
        ? siblingSourceIndex
        : topLevelSourceIndex;

      dragX.set(0);
      dragY.set(0);

      dragDataRef.current = {
        dragId: nodeId,
        dragParentId,
        isBlock,
        blockLength,
        startX: e.clientX,
        startY: e.clientY,
        siblingSlots,
        topLevelSlots,
        containerLeft: containerRect.left,
        siblingSourceIndex,
        topLevelSourceIndex,
        blockHeight,
        allMidYs,
      };

      const dropGap = findDropGap(e.clientY, activeSlots);
      const { depth, parentId } = resolveDepthAndParent(
        e.clientX,
        containerRect.left,
        dropGap,
        resolvedNodes,
        isBlock,
        activeSlots,
        dragParentId,
        INDENTATION,
      );

      let nestGap = 0;
      if (parentId && parentId !== dragParentId) {
        nestGap = computeNestGap(
          e.clientY,
          resolvedNodes,
          parentId,
          nodeId,
          allMidYs,
        );
      }

      setDragState({
        dragId: nodeId,
        dragParentId,
        dropIndex: dropGap,
        dropDepth: depth,
        dropParentId: parentId,
        isBlock,
        blockLength,
        sourceTopIndex: activeSourceIndex,
        blockHeight,
        nestGap,
      });
    },
    [resolvedNodes, dragX, dragY],
  );

  const handlePointerMove = useCallback(
    (e: React.PointerEvent<HTMLElement>) => {
      const data = dragDataRef.current;
      if (!data) return;

      dragX.set(e.clientX - data.startX);
      dragY.set(e.clientY - data.startY);

      const relX = e.clientX - data.containerLeft;
      const intraGroup =
        !!data.dragParentId && !data.isBlock && relX >= INDENTATION * 0.5;
      const activeSlots = intraGroup ? data.siblingSlots : data.topLevelSlots;
      const activeSourceIndex = intraGroup
        ? data.siblingSourceIndex
        : data.topLevelSourceIndex;

      const dropGap = findDropGap(e.clientY, activeSlots);
      const { depth, parentId } = resolveDepthAndParent(
        e.clientX,
        data.containerLeft,
        dropGap,
        resolvedNodes,
        data.isBlock,
        activeSlots,
        data.dragParentId,
        INDENTATION,
      );

      let nestGap = 0;
      if (parentId && parentId !== data.dragParentId) {
        nestGap = computeNestGap(
          e.clientY,
          resolvedNodes,
          parentId,
          data.dragId,
          data.allMidYs,
        );
      }

      setDragState((prev) => {
        if (
          prev &&
          prev.dropIndex === dropGap &&
          prev.dropDepth === depth &&
          prev.dropParentId === parentId &&
          prev.sourceTopIndex === activeSourceIndex &&
          prev.nestGap === nestGap
        )
          return prev;
        return {
          dragId: data.dragId,
          dragParentId: data.dragParentId,
          dropIndex: dropGap,
          dropDepth: depth,
          dropParentId: parentId,
          isBlock: data.isBlock,
          blockLength: data.blockLength,
          sourceTopIndex: activeSourceIndex,
          blockHeight: data.blockHeight,
          nestGap,
        };
      });
    },
    [resolvedNodes, dragX, dragY],
  );

  const applyDrop = useCallback(
    (
      state: DragState,
      dragParentId: string | null,
      dragId: string,
      isBlock: boolean,
      blockLength: number,
    ) => {
      const sourceIdx = resolvedNodes.findIndex((n) => n.id === dragId);
      if (sourceIdx === -1) return;

      const sameGroup = state.dropParentId === dragParentId;
      const modeParentId =
        state.dropParentId !== null && sameGroup ? dragParentId : null;

      const targetFlatIdx = gapToFlatIndex(
        state.dropIndex,
        resolvedNodes,
        dragId,
        isBlock,
        blockLength,
        modeParentId,
      );

      let updated: FlatNode[];
      if (isBlock) {
        const result = [...resolvedNodes];
        const block = result.splice(sourceIdx, blockLength);
        const adjustedTarget =
          targetFlatIdx > sourceIdx
            ? targetFlatIdx - blockLength
            : targetFlatIdx;
        const insertAt = Math.max(0, Math.min(result.length, adjustedTarget));
        result.splice(insertAt, 0, ...block);
        updated = result;
      } else {
        const result = [...resolvedNodes];
        const [item] = result.splice(sourceIdx, 1);

        const newItem = {
          ...item,
          depth: state.dropDepth,
          parentId: state.dropParentId,
        };

        if (state.dropParentId && state.dropParentId !== dragParentId) {
          const parentIdx = result.findIndex(
            (n) => n.id === state.dropParentId,
          );
          if (parentIdx !== -1) {
            let insertPos = parentIdx + 1;
            let childCount = 0;
            while (
              insertPos < result.length &&
              result[insertPos].parentId === state.dropParentId
            ) {
              if (childCount === state.nestGap) break;
              childCount++;
              insertPos++;
            }
            result.splice(insertPos, 0, newItem);
            updated = result;
          } else {
            const adjustedTarget =
              targetFlatIdx > sourceIdx ? targetFlatIdx - 1 : targetFlatIdx;
            const insertAt = Math.max(
              0,
              Math.min(result.length, adjustedTarget),
            );
            result.splice(insertAt, 0, newItem);
            updated = result;
          }
        } else {
          const adjustedTarget =
            targetFlatIdx > sourceIdx ? targetFlatIdx - 1 : targetFlatIdx;
          const insertAt = Math.max(0, Math.min(result.length, adjustedTarget));
          result.splice(insertAt, 0, newItem);
          updated = result;
        }
      }

      setPreviewNodes(updated);
      setDragState(null);
      onReorder(updated);
    },
    [resolvedNodes, onReorder],
  );

  const handlePointerUp = useCallback(
    (_e: React.PointerEvent<HTMLElement>) => {
      const data = dragDataRef.current;
      if (!data) return;
      dragDataRef.current = null;

      const state = dragState;
      if (!state) {
        setDragState(null);
        return;
      }

      dragX.set(0);
      dragY.set(0);
      applyDrop(
        state,
        data.dragParentId,
        data.dragId,
        data.isBlock,
        data.blockLength,
      );
    },
    [dragState, dragX, dragY, applyDrop],
  );

  const handlePointerCancel = useCallback(() => {
    dragDataRef.current = null;
    dragX.set(0);
    dragY.set(0);
    setDragState(null);
  }, [dragX, dragY]);

  // -------------------------------------------------------------------------
  // Keyboard drag
  // -------------------------------------------------------------------------

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLElement>) => {
      const kb = kbDragRef.current;

      // Space on a focused handle → pick up
      if (e.key === " " && !kb) {
        const handle = (e.target as HTMLElement).closest<HTMLElement>(
          "[data-drag-handle]",
        );
        if (!handle) return;
        const row = handle.closest<HTMLElement>("[data-node-id]");
        if (!row) return;

        const nodeId = row.dataset.nodeId!;
        const idx = resolvedNodes.findIndex((n) => n.id === nodeId);
        if (idx === -1) return;

        e.preventDefault();

        const node = resolvedNodes[idx];
        const isBlock =
          node.kind === NodeKind.Group || node.kind === NodeKind.Section;
        const blockLength = getBlockLength(resolvedNodes, idx);

        kbDragRef.current = {
          dragId: nodeId,
          isBlock,
          blockLength,
          originalNodes: resolvedNodes,
        };

        dragX.set(0);
        dragY.set(0);

        setDragState({
          dragId: nodeId,
          dragParentId: node.parentId,
          dropIndex: 0,
          dropDepth: node.depth,
          dropParentId: node.parentId,
          isBlock,
          blockLength,
          sourceTopIndex: 0,
          blockHeight: 0,
          nestGap: 0,
        });
        return;
      }

      if (!kb) return;

      // Arrow Up/Down → preview reorder
      if (e.key === "ArrowUp" || e.key === "ArrowDown") {
        e.preventDefault();
        const delta = e.key === "ArrowUp" ? -1 : 1;
        const newNodes = computeKbMove(
          resolvedNodes,
          kb.dragId,
          kb.isBlock,
          kb.blockLength,
          delta,
        );
        if (!newNodes) return;

        setPreviewNodes(newNodes);
        const moved = newNodes.find((n) => n.id === kb.dragId);
        if (moved) {
          setDragState((prev) =>
            prev
              ? {
                  ...prev,
                  dragParentId: moved.parentId,
                  dropParentId: moved.parentId,
                  dropDepth: moved.depth,
                }
              : prev,
          );
        }
        return;
      }

      // Space / Enter → commit
      if (e.key === " " || e.key === "Enter") {
        e.preventDefault();
        kbDragRef.current = null;
        dragX.set(0);
        dragY.set(0);
        const final = resolvedNodes;
        setDragState(null);
        onReorder(final);
        return;
      }

      // Escape → cancel
      if (e.key === "Escape") {
        e.preventDefault();
        kbDragRef.current = null;
        dragX.set(0);
        dragY.set(0);
        setPreviewNodes(null);
        setDragState(null);
        return;
      }
    },
    [resolvedNodes, dragX, dragY, onReorder],
  );

  const handleBlur = useCallback(
    (e: React.FocusEvent<HTMLElement>) => {
      if (!kbDragRef.current) return;
      if (e.currentTarget.contains(e.relatedTarget)) return;
      kbDragRef.current = null;
      dragX.set(0);
      dragY.set(0);
      setPreviewNodes(null);
      setDragState(null);
    },
    [dragX, dragY],
  );

  return {
    resolvedNodes,
    dragState,
    isKbDrag: kbDragRef.current !== null,
    dragX,
    dragY,
    containerRef,
    handlePointerDown,
    handlePointerMove,
    handlePointerUp,
    handlePointerCancel,
    handleKeyDown,
    handleBlur,
  };
}
