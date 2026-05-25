import { Button } from "@patternfly/react-core";
import { useCallback, useState } from "react";

export enum ThemeToggleType {
  Dark = "dark",
  Glass = "glass",
}

function getThemeClass(theme: ThemeToggleType): string {
  return `pf-v6-theme-${theme}`;
}

function getThemeKey(theme: ThemeToggleType): string {
  return `fleetshift_${theme}_mode`;
}

export function initThemeMode(theme: ThemeToggleType): boolean {
  const THEME_KEY = getThemeKey(theme);
  const THEME_CLASS = getThemeClass(theme);
  const stored = localStorage.getItem(THEME_KEY);
  const isActive = stored === "true";
  if (isActive) {
    document.documentElement.classList.add(THEME_CLASS);
  }
  return isActive;
}

export type ThemeToggleProps = {
  theme: ThemeToggleType;
  OnIcon: React.ComponentType;
  OffIcon: React.ComponentType;
};

const ThemeToggle = ({ theme, OnIcon, OffIcon }: ThemeToggleProps) => {
  const [isActive, setIsActive] = useState(() => initThemeMode(theme));

  const toggle = useCallback(() => {
    setIsActive((prev) => {
      const next = !prev;
      const THEME_CLASS = getThemeClass(theme);
      document.documentElement.classList.toggle(THEME_CLASS, next);
      localStorage.setItem(getThemeKey(theme), String(next));
      return next;
    });
  }, [theme]);

  return (
    <Button
      variant="plain"
      aria-label={`Toggle ${theme} theme`}
      onClick={toggle}
    >
      {isActive ? <OnIcon /> : <OffIcon />}
    </Button>
  );
};

export default ThemeToggle;
