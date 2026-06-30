import {
  MenuToggle,
  Select,
  SelectList,
  SelectOption,
} from "@patternfly/react-core";
import {
  CogIcon,
  CubesIcon,
  FolderOpenIcon,
  GlobeIcon,
  KeyIcon,
  LayerGroupIcon,
  ListIcon,
  LockIcon,
  ServerIcon,
  ShieldAltIcon,
  TagIcon,
  UsersIcon,
  WrenchIcon,
} from "@patternfly/react-icons";
import { useCallback, useMemo, useState } from "react";

export interface GroupIconOption {
  value: string;
  label: string;
  icon: React.ReactNode;
}

const ICON_OPTIONS: GroupIconOption[] = [
  { value: "folder-open", label: "Folder", icon: <FolderOpenIcon /> },
  { value: "cubes", label: "Cubes", icon: <CubesIcon /> },
  { value: "cog", label: "Settings", icon: <CogIcon /> },
  { value: "users", label: "Users", icon: <UsersIcon /> },
  { value: "shield-alt", label: "Security", icon: <ShieldAltIcon /> },
  { value: "key", label: "Key", icon: <KeyIcon /> },
  { value: "lock", label: "Lock", icon: <LockIcon /> },
  { value: "server", label: "Server", icon: <ServerIcon /> },
  { value: "globe", label: "Globe", icon: <GlobeIcon /> },
  { value: "wrench", label: "Tools", icon: <WrenchIcon /> },
  { value: "tag", label: "Tag", icon: <TagIcon /> },
  { value: "layer-group", label: "Layers", icon: <LayerGroupIcon /> },
  { value: "list", label: "List", icon: <ListIcon /> },
];

interface GroupIconPickerProps {
  selected: GroupIconOption | null;
  onSelect: (option: GroupIconOption | null) => void;
}

function GroupIconPicker({ selected, onSelect }: GroupIconPickerProps) {
  const [isOpen, setIsOpen] = useState(false);

  const optionMap = useMemo(() => {
    const map = new Map<string, GroupIconOption>();
    for (const opt of ICON_OPTIONS) {
      map.set(opt.value, opt);
    }
    return map;
  }, []);

  const handleSelect = useCallback(
    (_e: React.MouseEvent | undefined, value: string | number | undefined) => {
      if (typeof value !== "string") return;
      if (value === "__none__") {
        onSelect(null);
      } else {
        const opt = optionMap.get(value);
        if (opt) onSelect(opt);
      }
      setIsOpen(false);
    },
    [optionMap, onSelect],
  );

  const toggle = useCallback(
    (toggleRef: React.Ref<HTMLButtonElement>) => (
      <MenuToggle
        ref={toggleRef}
        onClick={() => setIsOpen((prev) => !prev)}
        isExpanded={isOpen}
        id="group-icon"
      >
        {selected ? (
          <span className="pf-v6-u-display-flex pf-v6-u-align-items-center pf-v6-u-gap-sm">
            {selected.icon} {selected.label}
          </span>
        ) : (
          "None"
        )}
      </MenuToggle>
    ),
    [isOpen, selected],
  );

  return (
    <Select
      isOpen={isOpen}
      selected={selected?.value}
      onSelect={handleSelect}
      onOpenChange={setIsOpen}
      toggle={toggle}
    >
      <SelectList>
        <SelectOption value="__none__">None</SelectOption>
        {ICON_OPTIONS.map((opt) => (
          <SelectOption key={opt.value} value={opt.value}>
            <span className="pf-v6-u-display-flex pf-v6-u-align-items-center pf-v6-u-gap-sm">
              {opt.icon} {opt.label}
            </span>
          </SelectOption>
        ))}
      </SelectList>
    </Select>
  );
}

export default GroupIconPicker;
