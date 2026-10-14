import { useEffect, useRef, useState } from "react";

const TYPING_MIN = 55;
const TYPING_JITTER = 45;
const DELETE_SPEED = 25;
const PAUSE_AFTER_TYPED = 2500;
const PAUSE_BEFORE_NEXT = 400;

export function useTypingPlaceholder(hints: string[], prefix: string): string {
  const [display, setDisplay] = useState(prefix);
  const idxRef = useRef(Math.floor(Math.random() * hints.length));
  const phaseRef = useRef<"typing" | "paused" | "deleting">("typing");
  const posRef = useRef(0);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;

    const tick = (): number => {
      const suffix = hints[idxRef.current];

      switch (phaseRef.current) {
        case "typing": {
          if (posRef.current < suffix.length) {
            posRef.current++;
            setDisplay(prefix + suffix.slice(0, posRef.current));
            return TYPING_MIN + Math.random() * TYPING_JITTER;
          }
          phaseRef.current = "paused";
          return PAUSE_AFTER_TYPED;
        }
        case "paused": {
          phaseRef.current = "deleting";
          return DELETE_SPEED;
        }
        case "deleting": {
          if (posRef.current > 0) {
            posRef.current--;
            setDisplay(prefix + suffix.slice(0, posRef.current));
            return DELETE_SPEED;
          }
          idxRef.current = (idxRef.current + 1) % hints.length;
          phaseRef.current = "typing";
          return PAUSE_BEFORE_NEXT;
        }
      }
    };

    const schedule = () => {
      const delay = tick();
      timer = setTimeout(schedule, delay);
    };

    timer = setTimeout(schedule, 600);
    return () => clearTimeout(timer);
  }, [hints, prefix]);

  return display;
}
