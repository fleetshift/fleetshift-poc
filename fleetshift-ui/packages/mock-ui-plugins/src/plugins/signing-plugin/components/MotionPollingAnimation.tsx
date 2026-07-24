import "../MotionPolling.scss";

import { motion } from "motion/react";
import type { ReactNode } from "react";

type MotionPollingAnimationProps = {
  children?: ReactNode;
};

const blobs = [
  { size: 22, duration: 12, yRange: 3, delay: 0 },
  { size: 16, duration: 12, yRange: 2, delay: -2.3 },
  { size: 20, duration: 12, yRange: 4, delay: -4.5 },
  { size: 18, duration: 8, yRange: 2, delay: -3 },
];

const ghosts = [
  { size: 28, duration: 7, yRange: 6, delay: 0, blur: 12 },
  { size: 20, duration: 9.5, yRange: 5, delay: -4, blur: 10 },
  { size: 24, duration: 5.5, yRange: 8, delay: -2.5, blur: 14 },
  { size: 16, duration: 6, yRange: 7, delay: -1.5, blur: 11 },
  { size: 22, duration: 8, yRange: 4, delay: -3.2, blur: 13 },
  { size: 18, duration: 4.8, yRange: 9, delay: -0.8, blur: 10 },
  { size: 26, duration: 11, yRange: 5, delay: -5, blur: 15 },
];

const blobTransition = (blob: (typeof blobs)[number]) => ({
  x: {
    duration: blob.duration,
    repeat: Infinity,
    ease: "linear" as const,
    delay: blob.delay,
  },
  y: {
    duration: blob.duration * 0.5,
    repeat: Infinity,
    repeatType: "mirror" as const,
    ease: "easeInOut" as const,
  },
});

const MotionPollingAnimation = ({ children }: MotionPollingAnimationProps) => (
  <div className="ome-signing-poll">
    <svg className="ome-signing-poll__svg-filters">
      <defs>
        <filter id="goo">
          <feGaussianBlur in="SourceGraphic" stdDeviation="10" result="blur" />
          <feColorMatrix
            in="blur"
            type="matrix"
            values="1 0 0 0 0  0 1 0 0 0  0 0 1 0 0  0 0 0 30 -11"
          />
        </filter>
      </defs>
    </svg>
    <div className="ome-signing-poll__bg" />
    {/* Blurry copies — SVG gooey filter creates liquid bridges */}
    <div className="ome-signing-poll__goo" aria-hidden="true">
      <div className="ome-signing-poll__pool ome-signing-poll__pool--left" />
      <div className="ome-signing-poll__pool ome-signing-poll__pool--right" />
      <div className="ome-signing-poll__pool ome-signing-poll__pool--top" />
      <div className="ome-signing-poll__pool ome-signing-poll__pool--bottom" />
      {blobs.map((blob, i) => (
        <motion.div
          key={i}
          className="ome-signing-poll__blob ome-signing-poll__blob--shadow"
          style={{ width: blob.size * 1.4, height: blob.size * 1.4 }}
          animate={{
            x: [-480, 480],
            y: [-blob.yRange, blob.yRange, -blob.yRange],
          }}
          transition={blobTransition(blob)}
        />
      ))}
      {ghosts.map((ghost, i) => (
        <motion.div
          key={`ghost-${i}`}
          className="ome-signing-poll__blob"
          style={{
            width: ghost.size,
            height: ghost.size,
            filter: `blur(${ghost.blur}px)`,
            opacity: 0.5,
          }}
          animate={{
            x: [-480, 480],
            y: [-ghost.yRange, ghost.yRange, -ghost.yRange],
          }}
          transition={blobTransition(ghost)}
        />
      ))}
    </div>
    {/* Sharp blobs — crisp circles on top */}
    <div className="ome-signing-poll__sharp" aria-hidden="true">
      {blobs.map((blob, i) => (
        <motion.div
          key={i}
          className="ome-signing-poll__blob"
          style={{ width: blob.size, height: blob.size }}
          animate={{
            x: [-480, 480],
            y: [-blob.yRange, blob.yRange, -blob.yRange],
          }}
          transition={blobTransition(blob)}
        />
      ))}
    </div>
    {children && <div className="ome-signing-poll__text">{children}</div>}
  </div>
);

export default MotionPollingAnimation;
