import { useMotionValue } from "motion/react";
import { useMemo, useRef } from "react";

import type { SortableSectionProps } from "../SortableSection";
import { SortableSection } from "../SortableSection";

type SerializableProps = Omit<
  SortableSectionProps,
  "dragX" | "dragY" | "containerRef" | "pageMap"
> & {
  pages: Array<[string, { title: string; scope: string }]>;
};

export function TestSortableSection({ pages, ...rest }: SerializableProps) {
  const dragX = useMotionValue(0);
  const dragY = useMotionValue(0);
  const ref = useRef<HTMLUListElement>(null);
  const pageMap = useMemo(() => new Map(pages), [pages]);
  return (
    <SortableSection
      {...rest}
      pageMap={pageMap}
      dragX={dragX}
      dragY={dragY}
      containerRef={ref}
    />
  );
}
