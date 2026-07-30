import { type RefObject, useCallback, useEffect, useRef } from "react";

const BOTTOM_THRESHOLD = 30;

export interface UseAutoScrollReturn {
  containerRef: RefObject<HTMLDivElement | null>;
  scrollToBottom: () => void;
}

export function useAutoScroll(deps: unknown[]): UseAutoScrollReturn {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const atBottomRef = useRef(true);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const onScroll = () => {
      atBottomRef.current =
        el.scrollTop + el.clientHeight >= el.scrollHeight - BOTTOM_THRESHOLD;
    };

    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  const scrollToBottom = useCallback(() => {
    const el = containerRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, []);

  useEffect(() => {
    if (atBottomRef.current) {
      requestAnimationFrame(scrollToBottom);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return { containerRef, scrollToBottom };
}
