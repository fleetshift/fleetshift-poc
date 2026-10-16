import type { FlatNode } from "@ocm-resource-manager/common";
import {
  isCustomGroup,
  MORE_ENTRY_ID,
  NodeKind,
} from "@ocm-resource-manager/common";
import {
  Dropdown,
  DropdownItem,
  DropdownList,
  MenuToggle,
} from "@patternfly/react-core";
import {
  EllipsisVIcon,
  RhUiGripVerticalFillIcon,
} from "@patternfly/react-icons";
import clsx from "clsx";
import { motion, type MotionValue } from "motion/react";
import { useId, useState } from "react";

export interface TreeItemProps {
  node: FlatNode;
  label: string;
  isElevated: boolean;
  isGhost: boolean;
  isHidden: boolean;
  isDropTarget: boolean;
  isDragActive: boolean;
  isKbDrag: boolean;
  displacementY: number;
  dragX?: MotionValue<number>;
  dragY?: MotionValue<number>;
  onResetItem?: () => void;
  onEditGroup?: () => void;
  onDeleteGroup?: () => void;
  onSetIcon?: () => void;
  onHideItem?: () => void;
  onRestoreItem?: () => void;
}

export function TreeItem({
  node,
  label,
  isElevated,
  isGhost,
  isHidden,
  isDropTarget,
  isDragActive,
  isKbDrag,
  displacementY,
  dragX,
  dragY,
  onResetItem,
  onEditGroup,
  onDeleteGroup,
  onSetIcon,
  onHideItem,
  onRestoreItem,
}: TreeItemProps) {
  const isDivider = node.id === MORE_ENTRY_ID;
  const isContainer =
    node.kind === NodeKind.Group || node.kind === NodeKind.Section;
  const kindClass = isContainer ? "section" : "page";
  const isUserGroup =
    node.kind === NodeKind.Group &&
    node.groupMeta !== undefined &&
    isCustomGroup(node.groupMeta);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuId = useId();

  // Divider node — non-draggable "Hidden" label
  if (isDivider) {
    return (
      <li
        data-node-id={node.id}
        className="ocm-rm-settings-tree-item ocm-rm-settings-tree-item--divider"
      >
        <div className="ocm-rm-settings-nav-editor__hidden-divider">Hidden</div>
      </li>
    );
  }

  const hasActions =
    onSetIcon ||
    onHideItem ||
    onRestoreItem ||
    (isUserGroup && onEditGroup) ||
    (isUserGroup && onDeleteGroup) ||
    (!isContainer && onResetItem);

  return (
    <motion.li
      data-node-id={node.id}
      className={clsx(
        "ocm-rm-settings-tree-item",
        node.depth === 1 && "ocm-rm-settings-tree-item--nested",
        isElevated && "ocm-rm-settings-tree-item--elevated",
        isGhost && !isElevated && "ocm-rm-settings-tree-item--ghost",
        isHidden && !isElevated && "ocm-rm-settings-tree-item--hidden",
        isDropTarget && "ocm-rm-settings-tree-item--drop-target",
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
        className={`ocm-rm-settings-tree-item__row ocm-rm-settings-tree-item__row--${kindClass}`}
      >
        <button
          type="button"
          data-drag-handle
          className="ocm-rm-settings-tree-item__handle"
          aria-label={`Reorder ${label}`}
          aria-roledescription="sortable"
        >
          <RhUiGripVerticalFillIcon />
        </button>

        <span
          className={`ocm-rm-settings-tree-item__label ocm-rm-settings-tree-item__label--${kindClass}`}
        >
          {label}
        </span>

        {hasActions && (
          <Dropdown
            isOpen={menuOpen}
            onOpenChange={setMenuOpen}
            id={menuId}
            toggle={(toggleRef) => (
              <MenuToggle
                ref={toggleRef}
                variant="plain"
                onClick={() => setMenuOpen((prev) => !prev)}
                isExpanded={menuOpen}
                aria-label={`Actions for ${label}`}
              >
                <EllipsisVIcon />
              </MenuToggle>
            )}
            popperProps={{ position: "end" }}
          >
            <DropdownList>
              {onSetIcon && (
                <DropdownItem
                  key="icon"
                  onClick={() => {
                    setMenuOpen(false);
                    onSetIcon();
                  }}
                >
                  Set icon
                </DropdownItem>
              )}
              {isUserGroup && onEditGroup && (
                <DropdownItem
                  key="edit"
                  onClick={() => {
                    setMenuOpen(false);
                    onEditGroup();
                  }}
                >
                  Edit group
                </DropdownItem>
              )}
              {!isContainer && onResetItem && (
                <DropdownItem
                  key="reset"
                  onClick={() => {
                    setMenuOpen(false);
                    onResetItem();
                  }}
                >
                  Reset position
                </DropdownItem>
              )}
              {onHideItem && (
                <DropdownItem
                  key="hide"
                  onClick={() => {
                    setMenuOpen(false);
                    onHideItem();
                  }}
                >
                  Hide
                </DropdownItem>
              )}
              {onRestoreItem && (
                <DropdownItem
                  key="restore"
                  onClick={() => {
                    setMenuOpen(false);
                    onRestoreItem();
                  }}
                >
                  Restore
                </DropdownItem>
              )}
              {isUserGroup && onDeleteGroup && (
                <DropdownItem
                  key="delete"
                  isDanger
                  onClick={() => {
                    setMenuOpen(false);
                    onDeleteGroup();
                  }}
                >
                  Delete group
                </DropdownItem>
              )}
            </DropdownList>
          </Dropdown>
        )}
      </div>
    </motion.li>
  );
}
