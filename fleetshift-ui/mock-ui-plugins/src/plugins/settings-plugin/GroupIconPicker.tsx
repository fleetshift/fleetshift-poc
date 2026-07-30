import { getCachedPfIcon, loadPfIcon } from "@fleetshift/common";
import { Button, Icon, Skeleton } from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useCallback, useEffect, useState } from "react";

import IconGalleryModal from "./IconGalleryModal";

interface GroupIconPickerProps {
  /** PF icon name (e.g. "CogIcon") or null for no icon. */
  selected: string | null;
  onSelect: (iconName: string | null) => void;
}

function GroupIconPicker({ selected, onSelect }: GroupIconPickerProps) {
  const [galleryOpen, setGalleryOpen] = useState(false);

  // Load the selected icon for display
  const [SelectedIcon, setSelectedIcon] = useState<ComponentType | null>(null);
  useEffect(() => {
    if (!selected) {
      setSelectedIcon(null);
      return;
    }
    const cached = getCachedPfIcon(selected);
    if (cached) {
      setSelectedIcon(() => cached);
      return;
    }
    loadPfIcon(selected).then((comp) => {
      setSelectedIcon(() => comp);
    });
  }, [selected]);

  const handleSelect = useCallback(
    (iconName: string | null) => {
      onSelect(iconName);
    },
    [onSelect],
  );

  return (
    <>
      <Button
        variant="secondary"
        onClick={() => setGalleryOpen(true)}
        id="group-icon"
      >
        {selected ? (
          <span className="pf-v6-u-display-flex pf-v6-u-align-items-center pf-v6-u-gap-sm">
            {SelectedIcon ? (
              <Icon isInline>
                <SelectedIcon />
              </Icon>
            ) : (
              <Skeleton width="16px" height="16px" />
            )}
            {selected.replace(/Icon$/, "")}
          </span>
        ) : (
          "Choose icon..."
        )}
      </Button>

      <IconGalleryModal
        isOpen={galleryOpen}
        selected={selected}
        onSelect={handleSelect}
        onClose={() => setGalleryOpen(false)}
      />
    </>
  );
}

export default GroupIconPicker;
