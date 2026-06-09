import {
  motion,
  useMotionValue,
  useSpring,
  type SpringOptions,
} from "motion/react";
import { useRef, useEffect, type ReactNode } from "react";
import "./AnimatedHeight.scss";

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
  const rawHeight = useMotionValue(0);
  const height = useSpring(rawHeight, springConfig);
  const initialRef = useRef(true);

  useEffect(() => {
    const el = contentRef.current;
    if (!el) return;
    const observer = new ResizeObserver(() => {
      const h = el.scrollHeight;
      if (initialRef.current) {
        rawHeight.jump(h);
        initialRef.current = false;
      } else {
        rawHeight.set(h);
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, [rawHeight]);

  return (
    <motion.div style={{ height, overflow: "hidden" }} className={className}>
      <div ref={contentRef} className="ome-signing-animated-height__inner">
        {children}
      </div>
    </motion.div>
  );
};

export default AnimatedHeight;
