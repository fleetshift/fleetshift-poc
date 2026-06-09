import { useSortable } from "@dnd-kit/react/sortable";
import {
  GripVerticalIcon,
  TimesIcon,
  PencilAltIcon,
  TrashIcon,
} from "@patternfly/react-icons";
import { Button } from "@patternfly/react-core";
import type { FlatNode } from "./utilities";
import { INDENTATION } from "./utilities";
import "./TreeItem.scss";

interface TreeItemProps {
  node: FlatNode;
  index: number;
  label: string;
  pathSlug?: string;
  isDropTarget?: boolean;
  onRemove?: () => void;
  onEditSection?: () => void;
  onDeleteSection?: () => void;
}

export function TreeItem({
  node,
  index,
  label,
  pathSlug,
  isDropTarget,
  onRemove,
  onEditSection,
  onDeleteSection,
}: TreeItemProps) {
  const { ref, handleRef, isDragSource } = useSortable({
    id: node.id,
    index,
    data: { depth: node.depth, parentId: node.parentId, kind: node.kind },
    transition: { idle: true },
  });

  const isSection = node.kind === "section";
  const kindClass = isSection ? "section" : "page";

  return (
    <li
      ref={ref}
      className={`ome-tree-item ${isDragSource ? "ome-tree-item--dragging" : ""}`}
      // eslint-disable-next-line no-restricted-syntax -- dynamic: depth-based indentation computed at runtime
      style={{ marginLeft: node.depth * INDENTATION }}
    >
      <div
        className={`ome-tree-item__row ome-tree-item__row--${kindClass}${isDropTarget ? " ome-tree-item__row--drop-target" : ""}`}
      >
        <span ref={handleRef} className="ome-tree-item__handle">
          <GripVerticalIcon className="pf-v6-u-icon-color-subtle" />
        </span>

        <span
          className={`ome-tree-item__label ome-tree-item__label--${kindClass}`}
        >
          {label}
        </span>

        {!isSection && pathSlug && (
          <span className="ome-tree-item__path">/{pathSlug}</span>
        )}

        {isSection && (
          <>
            <Button
              variant="plain"
              size="sm"
              aria-label={`Edit ${label}`}
              onClick={onEditSection}
              icon={<PencilAltIcon />}
            />
            <Button
              variant="plain"
              size="sm"
              aria-label={`Delete ${label}`}
              onClick={onDeleteSection}
              icon={<TrashIcon />}
            />
          </>
        )}

        {!isSection && onRemove && (
          <Button
            variant="plain"
            size="sm"
            aria-label={`Remove ${label}`}
            onClick={onRemove}
            icon={<TimesIcon />}
          />
        )}
      </div>
    </li>
  );
}

export function TreeItemOverlay({
  label,
  isSection,
  descendantCount,
}: {
  label: string;
  isSection: boolean;
  descendantCount: number;
}) {
  const kindClass = isSection ? "section" : "page";

  return (
    <div className={`ome-tree-overlay ome-tree-overlay--${kindClass}`}>
      <GripVerticalIcon className="pf-v6-u-icon-color-subtle" />
      <span
        className={`ome-tree-overlay__label ome-tree-overlay__label--${kindClass}`}
      >
        {label}
      </span>
      {descendantCount > 0 && (
        <span className="ome-tree-overlay__badge">{descendantCount}</span>
      )}
    </div>
  );
}
