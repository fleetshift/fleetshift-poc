import { MenuGroup, MenuItem, MenuItemAction } from "@patternfly/react-core";
import { TimesIcon } from "@patternfly/react-icons";
import { useEffect, useRef } from "react";

import type { HistoryEntry } from "./types";

interface HistoryPanelProps {
  entries: HistoryEntry[];
  onSelect: (expression: string) => void;
  onToggleFavorite: (expression: string) => void;
  onRemove: (expression: string) => void;
  focusedIndex?: number;
  indexOffset?: number;
}

function formatTimestamp(ts: number): string {
  const date = new Date(ts);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);

  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins}m ago`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;

  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) return `${diffDays}d ago`;

  return date.toLocaleDateString();
}

function HistoryItem({
  entry,
  onSelect,
  onToggleFavorite,
  onRemove,
  isFocused,
  itemRef,
}: {
  entry: HistoryEntry;
  onSelect: (expr: string) => void;
  onToggleFavorite: (expr: string) => void;
  onRemove: (expr: string) => void;
  isFocused?: boolean;
  itemRef?: (el: HTMLElement | null) => void;
}) {
  return (
    <MenuItem
      ref={itemRef}
      isFocused={isFocused}
      onClick={() => onSelect(entry.expression)}
      description={formatTimestamp(entry.timestamp)}
      actions={
        <>
          <MenuItemAction
            icon="favorites"
            isFavorited={entry.favorite}
            onClick={() => onToggleFavorite(entry.expression)}
            aria-label={
              entry.favorite ? "Remove from favorites" : "Add to favorites"
            }
          />
          <MenuItemAction
            icon={<TimesIcon />}
            onClick={() => onRemove(entry.expression)}
            aria-label="Remove from history"
          />
        </>
      }
    >
      <code>{entry.expression}</code>
    </MenuItem>
  );
}

export default function HistoryPanel({
  entries,
  onSelect,
  onToggleFavorite,
  onRemove,
  focusedIndex,
  indexOffset = 0,
}: HistoryPanelProps) {
  const itemRefs = useRef<Map<number, HTMLElement>>(new Map());

  useEffect(() => {
    if (focusedIndex == null) return;
    const el = itemRefs.current.get(focusedIndex);
    el?.scrollIntoView({ block: "nearest" });
  }, [focusedIndex]);

  if (entries.length === 0) return null;

  const favorites = entries.filter((e) => e.favorite);
  const recent = entries.filter((e) => !e.favorite).slice(0, 10);
  const recentOffset = indexOffset + favorites.length;

  return (
    <>
      {favorites.length > 0 && (
        <MenuGroup label="Favorites">
          {favorites.map((entry, i) => {
            const globalIdx = indexOffset + i;
            return (
              <HistoryItem
                key={entry.expression}
                entry={entry}
                onSelect={onSelect}
                onToggleFavorite={onToggleFavorite}
                onRemove={onRemove}
                isFocused={focusedIndex === globalIdx}
                itemRef={(el: HTMLElement | null) => {
                  if (el) itemRefs.current.set(globalIdx, el);
                  else itemRefs.current.delete(globalIdx);
                }}
              />
            );
          })}
        </MenuGroup>
      )}
      {recent.length > 0 && (
        <MenuGroup label="Recent">
          {recent.map((entry, i) => {
            const globalIdx = recentOffset + i;
            return (
              <HistoryItem
                key={entry.expression}
                entry={entry}
                onSelect={onSelect}
                onToggleFavorite={onToggleFavorite}
                onRemove={onRemove}
                isFocused={focusedIndex === globalIdx}
                itemRef={(el: HTMLElement | null) => {
                  if (el) itemRefs.current.set(globalIdx, el);
                  else itemRefs.current.delete(globalIdx);
                }}
              />
            );
          })}
        </MenuGroup>
      )}
    </>
  );
}
