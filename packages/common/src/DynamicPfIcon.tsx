import type { ComponentType } from "react";
import { useEffect, useState } from "react";

import { getCachedPfIcon, loadPfIcon } from "./pfIconLoader.js";

/** PF icons accept at least className; keep narrow to avoid `any`. */
type PfIconComponent = ComponentType<{ className?: string }>;

interface DynamicPfIconProps {
  /** PascalCase PF icon name, e.g. "CogIcon". */
  name: string;
  /** Optional className forwarded to the icon wrapper. */
  className?: string;
}

/**
 * Renders a PatternFly icon loaded on demand.
 *
 * Handles async dynamic-import loading internally — consumers just pass the
 * PascalCase icon name and get the rendered icon (or nothing while loading).
 *
 * @example
 * <DynamicPfIcon name="FolderOpenIcon" />
 */
export default function DynamicPfIcon({ name, className }: DynamicPfIconProps) {
  const [Icon, setIcon] = useState<PfIconComponent | null>(
    () => (getCachedPfIcon(name) as PfIconComponent | undefined) ?? null,
  );

  useEffect(() => {
    let active = true;
    const cached = getCachedPfIcon(name) as PfIconComponent | undefined;
    if (cached) {
      setIcon(() => cached);
      return;
    }
    loadPfIcon(name).then((comp) => {
      if (active && comp) setIcon(() => comp as PfIconComponent);
    });
    return () => {
      active = false;
    };
  }, [name]);

  if (!Icon) return null;
  return <Icon {...(className ? { className } : {})} />;
}
