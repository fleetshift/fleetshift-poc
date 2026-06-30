import type { FlatNode } from "@fleetshift/common";

export interface DragState {
  dragId: string;
  dragParentId: string | null;
  dropIndex: number;
  dropDepth: number;
  dropParentId: string | null;
  isBlock: boolean;
  blockLength: number;
  sourceTopIndex: number;
  blockHeight: number;
  nestGap: number;
}

export interface Slot {
  slotIndex: number;
  flatIndex: number;
  midY: number;
}

export const ITEM_GAP = 2;

export function getBlockLength(nodes: FlatNode[], index: number): number {
  const source = nodes[index];
  if (source.kind !== "group" && source.kind !== "section") return 1;
  let end = index + 1;
  while (end < nodes.length && nodes[end].parentId === source.id) end++;
  return end - index;
}

export function computeSlots(
  container: HTMLElement,
  nodes: FlatNode[],
  dragId: string,
  isBlock: boolean,
  blockLength: number,
  scopeParentId: string | null,
): Slot[] {
  const items = container.querySelectorAll<HTMLElement>("[data-node-id]");
  const slots: Slot[] = [];
  const dragIndex = nodes.findIndex((n) => n.id === dragId);
  const intraGroup = scopeParentId !== null;

  let slotIdx = 0;
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];

    if (isBlock && i >= dragIndex && i < dragIndex + blockLength) {
      if (i === dragIndex) slotIdx++;
      continue;
    }
    if (!isBlock && node.id === dragId) {
      slotIdx++;
      continue;
    }

    const isSlotItem = intraGroup
      ? node.parentId === scopeParentId
      : node.depth === 0;

    if (isSlotItem) {
      const el = items[i];
      if (el) {
        const rect = el.getBoundingClientRect();
        slots.push({
          slotIndex: slotIdx,
          flatIndex: i,
          midY: rect.top + rect.height / 2,
        });
      }
      slotIdx++;
    }
  }
  return slots;
}

export function findDropGap(pointerY: number, slots: Slot[]): number {
  if (slots.length === 0) return 0;
  for (let i = 0; i < slots.length; i++) {
    if (pointerY < slots[i].midY) return slots[i].slotIndex;
  }
  return slots[slots.length - 1].slotIndex + 1;
}

export function resolveDepthAndParent(
  pointerX: number,
  containerLeft: number,
  dropGap: number,
  nodes: FlatNode[],
  isBlock: boolean,
  slots: Slot[],
  dragParentId: string | null,
  indentation: number,
): { depth: number; parentId: string | null } {
  if (isBlock) return { depth: 0, parentId: null };

  if (dragParentId) {
    const relX = pointerX - containerLeft;
    if (relX >= indentation * 0.5) {
      return { depth: 1, parentId: dragParentId };
    }
    return { depth: 0, parentId: null };
  }

  const relX = pointerX - containerLeft;
  const wantsNest = relX > indentation * 1.5;

  if (!wantsNest) return { depth: 0, parentId: null };

  let prevSlot: Slot | undefined;
  for (const slot of slots) {
    if (slot.slotIndex < dropGap) prevSlot = slot;
    else break;
  }

  if (!prevSlot) return { depth: 0, parentId: null };

  const prevNode = nodes[prevSlot.flatIndex];
  if (prevNode && (prevNode.kind === "group" || prevNode.kind === "section")) {
    return { depth: 1, parentId: prevNode.id };
  }

  return { depth: 0, parentId: null };
}

export function computeNestGap(
  pointerY: number,
  nodes: FlatNode[],
  targetParentId: string,
  dragId: string,
  allMidYs: number[],
): number {
  let gap = 0;
  for (let i = 0; i < nodes.length; i++) {
    if (nodes[i].parentId === targetParentId && nodes[i].id !== dragId) {
      if (pointerY >= allMidYs[i]) gap++;
    }
  }
  return gap;
}

export function gapToFlatIndex(
  gap: number,
  nodes: FlatNode[],
  dragId: string,
  isBlock: boolean,
  blockLength: number,
  scopeParentId: string | null,
): number {
  const dragIdx = nodes.findIndex((n) => n.id === dragId);
  const intraGroup = scopeParentId !== null;
  let count = 0;
  let lastRelevantIdx = -1;

  for (let i = 0; i < nodes.length; i++) {
    if (isBlock && i >= dragIdx && i < dragIdx + blockLength) {
      if (i === dragIdx) count++;
      continue;
    }
    if (!isBlock && nodes[i].id === dragId) {
      count++;
      continue;
    }

    const isRelevant = intraGroup
      ? nodes[i].parentId === scopeParentId
      : nodes[i].depth === 0;

    if (isRelevant) {
      if (count === gap) return i;
      lastRelevantIdx = i;
      count++;
    }
  }

  if (intraGroup && lastRelevantIdx !== -1) {
    return lastRelevantIdx + 1;
  }
  return nodes.length;
}

export function measureBlockHeight(
  container: HTMLElement,
  flatIdx: number,
  blockLength: number,
): number {
  const items = container.querySelectorAll<HTMLElement>("[data-node-id]");
  let height = 0;
  for (let i = flatIdx; i < flatIdx + blockLength && i < items.length; i++) {
    height += items[i].getBoundingClientRect().height;
  }
  height += (blockLength - 1) * ITEM_GAP;
  return height;
}

export function computeKbMove(
  nodes: FlatNode[],
  dragId: string,
  isBlock: boolean,
  blockLength: number,
  delta: number,
): FlatNode[] | null {
  const flatIdx = nodes.findIndex((n) => n.id === dragId);
  if (flatIdx === -1) return null;

  // --- Block movement (groups/sections skip past other top-level blocks) ---
  if (isBlock) {
    if (delta === 1) {
      const afterBlock = flatIdx + blockLength;
      let nextTL = -1;
      for (let i = afterBlock; i < nodes.length; i++) {
        if (nodes[i].depth === 0) {
          nextTL = i;
          break;
        }
      }
      if (nextTL === -1) return null;
      const nextBL = getBlockLength(nodes, nextTL);
      const target = nextTL + nextBL;
      const result = [...nodes];
      const block = result.splice(flatIdx, blockLength);
      const ins = Math.min(result.length, target - blockLength);
      result.splice(ins, 0, ...block);
      return result;
    }
    let prevTL = -1;
    for (let i = flatIdx - 1; i >= 0; i--) {
      if (nodes[i].depth === 0) {
        prevTL = i;
        break;
      }
    }
    if (prevTL === -1) return null;
    const result = [...nodes];
    const block = result.splice(flatIdx, blockLength);
    result.splice(prevTL, 0, ...block);
    return result;
  }

  // --- Single item: flat-list movement ---
  const target = flatIdx + delta;
  if (target < 0 || target >= nodes.length) return null;

  const targetNode = nodes[target];

  const result = [...nodes];
  const [item] = result.splice(flatIdx, 1);

  if (targetNode.kind === "group" || targetNode.kind === "section") {
    if (delta === 1) {
      result.splice(target, 0, {
        ...item,
        depth: 1,
        parentId: targetNode.id,
      });
    } else {
      result.splice(target, 0, { ...item, depth: 0, parentId: null });
    }
    return result;
  }

  result.splice(target, 0, {
    ...item,
    depth: targetNode.depth,
    parentId: targetNode.parentId,
  });
  return result;
}
