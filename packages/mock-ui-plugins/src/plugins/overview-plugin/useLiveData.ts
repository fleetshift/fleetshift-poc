import { useState, useEffect, useMemo } from "react";

function jitter(value: number, pct: number): number {
  const delta = value * pct * (Math.random() * 2 - 1);
  return Math.max(0, Math.round((value + delta) * 100) / 100);
}

export function useLiveSlos() {
  const [tick, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(
      () => setTick((t) => t + 1),
      20000 + Math.random() * 20000,
    );
    return () => clearInterval(id);
  }, []);

  return useMemo(() => {
    void tick;
    return [
      {
        service: "Global Fraud Analysis Pipeline",
        metric: "Transaction Approval Latency",
        target: "< 50ms p99",
        budgetRemaining: jitter(85, 0.02),
        budgetTotal: 100,
        trend: "stable" as const,
      },
      {
        service: "Edge Network (UPF)",
        metric: "Packet Processing Latency",
        target: "< 10ms p99",
        budgetRemaining: jitter(97, 0.01),
        budgetTotal: 100,
        trend: "stable" as const,
      },
    ];
  }, [tick]);
}
