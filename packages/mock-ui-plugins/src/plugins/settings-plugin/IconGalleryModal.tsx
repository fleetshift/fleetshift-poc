import "./IconGalleryModal.scss";

import { getCachedPfIcon, loadPfIcon } from "@fleetshift/common";
// The manifest is a flat string[] of PascalCase icon names, generated at
// build time by scripts/generate-icons.mjs.
import pfIcons from "@fleetshift/common/generated/pf-icons.json";
import { create, insert, search } from "@orama/orama";
import {
  Button,
  EmptyState,
  EmptyStateBody,
  Icon,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  SearchInput,
  Skeleton,
} from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IconGalleryModalProps {
  isOpen: boolean;
  selected: string | null;
  onSelect: (iconName: string | null) => void;
  onClose: () => void;
}

interface IconCellProps {
  name: string;
  isSelected: boolean;
  onClick: () => void;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Derive search keywords from a PascalCase icon name. */
function nameToWords(name: string): string {
  return name
    .replace(/Icon$/, "")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z])([A-Z][a-z])/g, "$1 $2")
    .toLowerCase();
}

// ---------------------------------------------------------------------------
// IconCell — lazy-loads icon on intersection
// ---------------------------------------------------------------------------

function IconCell({ name, isSelected, onClick }: IconCellProps) {
  const ref = useRef<HTMLButtonElement>(null);
  const [LoadedIcon, setLoadedIcon] = useState<ComponentType | null>(
    () => getCachedPfIcon(name) ?? null,
  );
  const [loading, setLoading] = useState(!LoadedIcon);

  useEffect(() => {
    if (LoadedIcon) return;

    const el = ref.current;
    if (!el) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          observer.disconnect();
          loadPfIcon(name).then((comp) => {
            setLoadedIcon(() => comp);
            setLoading(false);
          });
        }
      },
      { rootMargin: "100px" },
    );

    observer.observe(el);
    return () => observer.disconnect();
  }, [name, LoadedIcon]);

  return (
    <button
      ref={ref}
      type="button"
      className={`ome-settings-icon-cell${isSelected ? " ome-settings-icon-cell--selected" : ""}`}
      onClick={onClick}
      title={name}
      aria-label={name}
      aria-pressed={isSelected}
    >
      {loading ? (
        <Skeleton width="24px" height="24px" />
      ) : LoadedIcon ? (
        <Icon isInline>
          <LoadedIcon />
        </Icon>
      ) : null}
    </button>
  );
}

// ---------------------------------------------------------------------------
// IconGalleryModal
// ---------------------------------------------------------------------------

/** Maximum icons to show in the grid (avoids DOM overload). */
const MAX_VISIBLE = 200;

function IconGalleryModal({
  isOpen,
  selected,
  onSelect,
  onClose,
}: IconGalleryModalProps) {
  const [query, setQuery] = useState("");
  const [highlighted, setHighlighted] = useState<string | null>(selected);
  const oramaRef = useRef<Awaited<ReturnType<typeof create>> | null>(null);
  const [indexReady, setIndexReady] = useState(false);

  // Build Orama index on open, destroy on close
  useEffect(() => {
    if (!isOpen) {
      oramaRef.current = null;
      setIndexReady(false);
      setQuery("");
      return;
    }

    let cancelled = false;

    async function buildIndex() {
      const db = await create({
        schema: {
          name: "string" as const,
          words: "string" as const,
        },
      });

      for (const name of pfIcons as string[]) {
        if (cancelled) return;
        await insert(db, { name, words: nameToWords(name) });
      }

      if (!cancelled) {
        oramaRef.current = db;
        setIndexReady(true);
      }
    }

    buildIndex();
    return () => {
      cancelled = true;
    };
  }, [isOpen]);

  // Reset highlighted to current selection when modal opens
  useEffect(() => {
    if (isOpen) {
      setHighlighted(selected);
    }
  }, [isOpen, selected]);

  // Search results
  const filteredIcons = useMemo(() => {
    const allIcons = pfIcons as string[];

    if (!query.trim()) {
      return allIcons.slice(0, MAX_VISIBLE);
    }

    if (!indexReady || !oramaRef.current) {
      // Fallback: simple substring match while index builds
      const lower = query.toLowerCase();
      return allIcons
        .filter((n) => n.toLowerCase().includes(lower))
        .slice(0, MAX_VISIBLE);
    }

    const results = search(oramaRef.current, {
      term: query,
      properties: ["name", "words"],
      tolerance: 1,
      limit: MAX_VISIBLE,
    });

    return results.hits.map((h) => (h.document as { name: string }).name);
  }, [query, indexReady]);

  const handleConfirm = useCallback(() => {
    onSelect(highlighted);
    onClose();
  }, [highlighted, onSelect, onClose]);

  const handleClear = useCallback(() => {
    onSelect(null);
    onClose();
  }, [onSelect, onClose]);

  // Preview: load highlighted icon
  const [PreviewIcon, setPreviewIcon] = useState<ComponentType | null>(null);
  useEffect(() => {
    if (!highlighted) {
      setPreviewIcon(null);
      return;
    }
    const cached = getCachedPfIcon(highlighted);
    if (cached) {
      setPreviewIcon(() => cached);
      return;
    }
    loadPfIcon(highlighted).then((comp) => {
      setPreviewIcon(() => comp);
    });
  }, [highlighted]);

  return (
    <Modal variant="large" isOpen={isOpen} onClose={onClose}>
      <ModalHeader title="Select icon" />
      <ModalBody>
        {/* Preview row */}
        {highlighted && (
          <div className="ome-settings-icon-preview">
            <span className="ome-settings-icon-preview__icon">
              {PreviewIcon ? (
                <Icon isInline size="lg">
                  <PreviewIcon />
                </Icon>
              ) : (
                <Skeleton width="32px" height="32px" />
              )}
            </span>
            <span className="ome-settings-icon-preview__name">
              {highlighted}
            </span>
          </div>
        )}

        {/* Search */}
        <SearchInput
          className="pf-v6-u-mb-md"
          placeholder="Search icons..."
          value={query}
          onChange={(_e, val) => setQuery(val)}
          onClear={() => setQuery("")}
          aria-label="Search icons"
        />

        {/* Grid */}
        {filteredIcons.length === 0 ? (
          <EmptyState>
            <EmptyStateBody>
              No icons match <strong>&ldquo;{query}&rdquo;</strong>
            </EmptyStateBody>
          </EmptyState>
        ) : (
          <div
            className="ome-settings-icon-grid"
            role="grid"
            aria-label="Icon gallery"
          >
            {filteredIcons.map((name) => (
              <IconCell
                key={name}
                name={name}
                isSelected={highlighted === name}
                onClick={() => setHighlighted(name)}
              />
            ))}
          </div>
        )}
      </ModalBody>
      <ModalFooter>
        <Button
          variant="primary"
          onClick={handleConfirm}
          isDisabled={!highlighted}
        >
          Select
        </Button>
        <Button variant="secondary" onClick={handleClear}>
          No icon
        </Button>
        <Button variant="link" onClick={onClose}>
          Cancel
        </Button>
      </ModalFooter>
    </Modal>
  );
}

export default IconGalleryModal;
