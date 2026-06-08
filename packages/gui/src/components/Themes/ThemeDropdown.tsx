import { useCallback, useEffect, useRef, useState } from "react";
import {
  MenuToggle,
  Panel,
  PanelMain,
  PanelMainBody,
  ToggleGroup,
  ToggleGroupItem,
  Title,
} from "@patternfly/react-core";
import { Popper } from "@patternfly/react-core/dist/esm/helpers/Popper/Popper";
import { PaletteIcon } from "@patternfly/react-icons";
import "./ThemeDropdown.scss";

type Theme = "default" | "felt";
type ColorScheme = "system" | "light" | "dark";
type ContrastMode = "system" | "default" | "high-contrast" | "glass";

const validThemes: string[] = [
  "default",
  "felt",
  "system",
  "dark",
  "light",
  "glass",
  "high-contrast",
];

const STORAGE_KEYS = {
  theme: "fleetshift_theme",
  colorScheme: "fleetshift_color_scheme",
  contrastMode: "fleetshift_contrast_mode",
};

function readStored<T extends string>(key: string, fallback: T): T {
  const r = localStorage.getItem(key);
  if (!r || typeof r !== "string" || !validThemes.includes(r as Theme)) {
    return fallback;
  }
  return r as T;
}

function systemPrefersDark(): boolean {
  return window.matchMedia("(prefers-color-scheme: dark)").matches;
}

function applyClasses(
  theme: Theme,
  colorScheme: ColorScheme,
  contrastMode: ContrastMode,
) {
  const html = document.documentElement;

  html.classList.toggle("pf-v6-theme-felt", theme === "felt");

  const dark =
    colorScheme === "dark" || (colorScheme === "system" && systemPrefersDark());
  html.classList.toggle("pf-v6-theme-dark", dark);

  html.classList.toggle("pf-v6-theme-glass", contrastMode === "glass");
  html.classList.toggle(
    "pf-v6-theme-high-contrast",
    contrastMode === "high-contrast",
  );
}

export function initThemeSettings() {
  const theme = readStored<Theme>(STORAGE_KEYS.theme, "default");
  const colorScheme = readStored<ColorScheme>(
    STORAGE_KEYS.colorScheme,
    "system",
  );
  const contrastMode = readStored<ContrastMode>(
    STORAGE_KEYS.contrastMode,
    "system",
  );
  applyClasses(theme, colorScheme, contrastMode);
}

const ThemeDropdown = () => {
  const [isOpen, setIsOpen] = useState(false);
  const toggleRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const [theme, setTheme] = useState<Theme>(() =>
    readStored(STORAGE_KEYS.theme, "default"),
  );
  const [colorScheme, setColorScheme] = useState<ColorScheme>(() =>
    readStored(STORAGE_KEYS.colorScheme, "system"),
  );
  const [contrastMode, setContrastMode] = useState<ContrastMode>(() =>
    readStored(STORAGE_KEYS.contrastMode, "system"),
  );

  const apply = useCallback((t: Theme, cs: ColorScheme, cm: ContrastMode) => {
    localStorage.setItem(STORAGE_KEYS.theme, t);
    localStorage.setItem(STORAGE_KEYS.colorScheme, cs);
    localStorage.setItem(STORAGE_KEYS.contrastMode, cm);
    applyClasses(t, cs, cm);
  }, []);

  useEffect(() => {
    if (colorScheme !== "system") return;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = () => applyClasses(theme, colorScheme, contrastMode);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, [theme, colorScheme, contrastMode]);

  useEffect(() => {
    if (!isOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (
        !panelRef.current?.contains(e.target as Node) &&
        !toggleRef.current?.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsOpen(false);
    };
    window.addEventListener("click", handleClick);
    window.addEventListener("keydown", handleKey);
    return () => {
      window.removeEventListener("click", handleClick);
      window.removeEventListener("keydown", handleKey);
    };
  }, [isOpen]);

  const handleTheme = useCallback(
    (_e: unknown, value: Theme) => {
      setTheme(value);
      apply(value, colorScheme, contrastMode);
    },
    [colorScheme, contrastMode, apply],
  );

  const handleColorScheme = useCallback(
    (_e: unknown, value: ColorScheme) => {
      setColorScheme(value);
      apply(theme, value, contrastMode);
    },
    [theme, contrastMode, apply],
  );

  const handleContrastMode = useCallback(
    (_e: unknown, value: ContrastMode) => {
      setContrastMode(value);
      apply(theme, colorScheme, value);
    },
    [theme, colorScheme, apply],
  );

  const toggle = (
    <MenuToggle
      ref={toggleRef}
      variant="plain"
      onClick={() => setIsOpen((prev) => !prev)}
      isExpanded={isOpen}
      aria-label="Theme settings"
    >
      <PaletteIcon />
    </MenuToggle>
  );

  const panel = (
    <Panel ref={panelRef} variant="raised" className="fs-theme-dropdown">
      <PanelMain>
        <PanelMainBody>
          <div className="fs-theme-dropdown__section">
            <Title headingLevel="h4" size="md">
              Theme
            </Title>
            <ToggleGroup isCompact aria-label="Theme">
              <ToggleGroupItem
                text="Default"
                isSelected={theme === "default"}
                onChange={(e) => handleTheme(e, "default")}
              />
              <ToggleGroupItem
                text="Project Felt"
                isSelected={theme === "felt"}
                onChange={(e) => handleTheme(e, "felt")}
              />
            </ToggleGroup>
          </div>
          <div className="fs-theme-dropdown__section">
            <Title headingLevel="h4" size="md">
              Color scheme
            </Title>
            <ToggleGroup isCompact aria-label="Color scheme">
              <ToggleGroupItem
                text="System"
                isSelected={colorScheme === "system"}
                onChange={(e) => handleColorScheme(e, "system")}
              />
              <ToggleGroupItem
                text="Light"
                isSelected={colorScheme === "light"}
                onChange={(e) => handleColorScheme(e, "light")}
              />
              <ToggleGroupItem
                text="Dark"
                isSelected={colorScheme === "dark"}
                onChange={(e) => handleColorScheme(e, "dark")}
              />
            </ToggleGroup>
          </div>
          <div className="fs-theme-dropdown__section">
            <Title headingLevel="h4" size="md">
              Contrast mode
            </Title>
            <ToggleGroup isCompact aria-label="Contrast mode">
              <ToggleGroupItem
                text="System"
                isSelected={contrastMode === "system"}
                onChange={(e) => handleContrastMode(e, "system")}
              />
              <ToggleGroupItem
                text="Default"
                isSelected={contrastMode === "default"}
                onChange={(e) => handleContrastMode(e, "default")}
              />
              <ToggleGroupItem
                text="High contrast"
                isSelected={contrastMode === "high-contrast"}
                onChange={(e) => handleContrastMode(e, "high-contrast")}
              />
              <ToggleGroupItem
                text="Glass"
                isSelected={contrastMode === "glass"}
                onChange={(e) => handleContrastMode(e, "glass")}
              />
            </ToggleGroup>
          </div>
        </PanelMainBody>
      </PanelMain>
    </Panel>
  );

  return (
    <Popper
      trigger={toggle}
      popper={panel}
      isVisible={isOpen}
      placement="bottom-end"
      flipBehavior={["left", "right"]}
      appendTo={() => document.body}
    />
  );
};

export default ThemeDropdown;
