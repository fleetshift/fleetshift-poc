import "leaflet/dist/leaflet.css";
import "./GlobalMap.scss";

import L from "leaflet";
import { useEffect, useRef, useState } from "react";

import { useFleetDataContext } from "../useFleetData";

const TILES = {
  light: "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
  dark: "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
};

const WORLD_COPIES = [-360, 0, 360];

const dotColor = (s: "healthy" | "degraded" | "critical") =>
  s === "healthy" ? "#3e8635" : s === "degraded" ? "#f0ab00" : "#c9190b";

function useIsDarkTheme() {
  const [dark, setDark] = useState(() =>
    document.documentElement.classList.contains("pf-v6-theme-dark"),
  );

  useEffect(() => {
    const observer = new MutationObserver(() => {
      setDark(document.documentElement.classList.contains("pf-v6-theme-dark"));
    });
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => observer.disconnect();
  }, []);

  return dark;
}

export default function GlobalMap(_props: { widgetId: string }) {
  const isDark = useIsDarkTheme();
  const { clusters } = useFleetDataContext();
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const tileRef = useRef<L.TileLayer | null>(null);
  const markersRef = useRef<L.LayerGroup>(L.layerGroup());

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const map = L.map(containerRef.current, {
      center: [30, 0],
      zoom: 2,
      minZoom: 2,
      maxZoom: 6,
      scrollWheelZoom: false,
      worldCopyJump: true,
      attributionControl: false,
      zoomControl: false,
    });
    tileRef.current = L.tileLayer(isDark ? TILES.dark : TILES.light, {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>',
    }).addTo(map);
    markersRef.current.addTo(map);
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!tileRef.current) return;
    tileRef.current.setUrl(isDark ? TILES.dark : TILES.light);
  }, [isDark]);

  useEffect(() => {
    const group = markersRef.current;
    group.clearLayers();
    for (const c of clusters) {
      for (const offset of WORLD_COPIES) {
        L.circleMarker([c.lat, c.lng + offset], {
          radius: 8,
          color: dotColor(c.status),
          fillColor: dotColor(c.status),
          fillOpacity: 0.7,
          weight: 2,
        })
          .bindTooltip(
            `<strong>${c.name}</strong><br/>${c.region} — ${c.status}`,
          )
          .addTo(group);
      }
    }
  }, [clusters]);

  return <div ref={containerRef} className="ome-overview-global-map" />;
}
