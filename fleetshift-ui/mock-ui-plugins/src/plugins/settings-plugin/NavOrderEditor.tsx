import "./NavOrderEditor.scss";

import type { FleetShiftApi } from "@fleetshift/common";
import {
  CORE_EXTENSION_META,
  orderByIds,
  useExtensionInstall,
  useNavOrder,
} from "@fleetshift/common";
import {
  Button,
  Content,
  DataList,
  DataListCell,
  DataListItemCells,
  Label,
  Title,
} from "@patternfly/react-core";
import type { DraggableObject } from "@patternfly/react-drag-drop";
import { DragDropSort } from "@patternfly/react-drag-drop";
import { useScalprum } from "@scalprum/react-core";
import { useCallback, useEffect, useMemo, useState } from "react";

interface NavEntry {
  id: string;
  scope: string;
  label: string;
}

function toDraggable(
  entries: NavEntry[],
  isInstalled: (scope: string) => boolean,
): DraggableObject[] {
  return entries.map((e) => ({
    id: e.id,
    content: (
      <DataListItemCells
        dataListCells={[
          <DataListCell key="label" isFilled>
            {e.label}
          </DataListCell>,
          ...(!isInstalled(e.scope)
            ? [
                <DataListCell key="status" alignRight isFilled={false}>
                  <Label status="warning" isCompact>
                    disabled
                  </Label>
                </DataListCell>,
              ]
            : []),
        ]}
      />
    ),
  }));
}

function useNavPages(): NavEntry[] {
  const { api } = useScalprum<{ api: FleetShiftApi }>();
  return useMemo(
    () =>
      api.fleetshift
        .getNavPages()
        .map((p) => ({ id: p.id, scope: p.scope, label: p.title })),
    [api],
  );
}

function splitBySection(entries: NavEntry[]) {
  const main: NavEntry[] = [];
  const bottom: NavEntry[] = [];
  for (const e of entries) {
    const meta = CORE_EXTENSION_META[e.scope];
    if (meta?.navSection === "bottom") {
      bottom.push(e);
    } else {
      main.push(e);
    }
  }
  return { main, bottom };
}

const NavOrderEditor = () => {
  const allEntries = useNavPages();
  const { order: savedOrder, setOrder } = useNavOrder();
  const { isInstalled } = useExtensionInstall();
  const { main: allMain, bottom: allBottom } = useMemo(
    () => splitBySection(allEntries),
    [allEntries],
  );

  const mainEntries = useMemo(
    () => orderByIds(allMain, savedOrder, "label"),
    [allMain, savedOrder],
  );
  const bottomEntries = useMemo(
    () => orderByIds(allBottom, savedOrder, "label"),
    [allBottom, savedOrder],
  );

  const [mainItems, setMainItems] = useState(() =>
    toDraggable(mainEntries, isInstalled),
  );

  useEffect(() => {
    setMainItems(toDraggable(mainEntries, isInstalled));
  }, [mainEntries, isInstalled]);

  const [bottomItems, setBottomItems] = useState(() =>
    toDraggable(bottomEntries, isInstalled),
  );

  useEffect(() => {
    setBottomItems(toDraggable(bottomEntries, isInstalled));
  }, [bottomEntries, isInstalled]);

  const persist = useCallback(
    (main: DraggableObject[], bottom: DraggableObject[]) => {
      setMainItems(main);
      setBottomItems(bottom);
      const merged = [
        ...main.map((d) => String(d.id)),
        ...bottom.map((d) => String(d.id)),
      ];
      setOrder(merged);
    },
    [setOrder],
  );

  const handleMainDrop = useCallback(
    (_event: unknown, items: DraggableObject[]) => {
      persist(items, bottomItems);
    },
    [persist, bottomItems],
  );

  const handleBottomDrop = useCallback(
    (_event: unknown, items: DraggableObject[]) => {
      persist(mainItems, items);
    },
    [persist, mainItems],
  );

  const handleReset = useCallback(() => {
    const { main, bottom } = splitBySection(allEntries);
    const allAlpha = [
      ...main.sort((a, b) => a.label.localeCompare(b.label)),
      ...bottom.sort((a, b) => a.label.localeCompare(b.label)),
    ].map((e) => e.id);
    setOrder(allAlpha);
  }, [allEntries, setOrder]);

  return (
    <div className="ome-settings-nav-order">
      <div className="ome-settings-nav-order__header">
        <Title headingLevel="h2">Navigation order</Title>
        <Button variant="link" onClick={handleReset}>
          Reset to default
        </Button>
      </div>
      <Content component="p">
        Drag items to reorder the navigation sidebar.
      </Content>

      <Title headingLevel="h3" className="pf-v6-u-mt-lg pf-v6-u-mb-sm">
        Main
      </Title>
      <DragDropSort
        items={mainItems}
        onDrop={handleMainDrop}
        variant="DataList"
        overlayProps={{ isCompact: true, style: { maxWidth: "500px" } }}
      >
        <DataList aria-label="Main navigation order" isCompact />
      </DragDropSort>

      <Title headingLevel="h3" className="pf-v6-u-mt-lg pf-v6-u-mb-sm">
        Bottom
      </Title>
      <DragDropSort
        items={bottomItems}
        onDrop={handleBottomDrop}
        variant="DataList"
        overlayProps={{ isCompact: true, style: { maxWidth: "500px" } }}
      >
        <DataList aria-label="Bottom navigation order" isCompact />
      </DragDropSort>
    </div>
  );
};

export default NavOrderEditor;
