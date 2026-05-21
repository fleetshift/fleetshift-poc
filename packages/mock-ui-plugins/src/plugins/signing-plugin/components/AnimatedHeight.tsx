import {
  motion,
  useMotionValue,
  useSpring,
  type SpringOptions,
} from "motion/react";
import {
  useRef,
  useEffect,
  useState,
  type ReactNode,
  useCallback,
} from "react";

const springConfig: SpringOptions = {
  stiffness: 200,
  damping: 25,
  mass: 0.5,
};

type AnimatedHeightProps = {
  children: ReactNode;
  className?: string;
};

const AnimatedHeight = ({ children, className }: AnimatedHeightProps) => {
  const contentRef = useRef<HTMLDivElement>(null);
  const [initial, setInitial] = useState(true);
  const rawHeight = useMotionValue(0);
  const height = useSpring(rawHeight, springConfig);

  const measure = useCallback(() => {
    if (!contentRef.current) return;
    const h = contentRef.current.scrollHeight;
    if (initial) {
      rawHeight.jump(h);
      setInitial(false);
    } else {
      rawHeight.set(h);
    }
  }, [initial, rawHeight]);

  useEffect(() => {
    const el = contentRef.current;
    if (!el) return;
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [measure]);

  return (
    <motion.div style={{ height, overflow: "hidden" }} className={className}>
      <div ref={contentRef}>{children}</div>
    </motion.div>
  );
};

export default AnimatedHeight;
