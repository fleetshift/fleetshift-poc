import { useEffect, useState } from "react";
import { MapContainer, TileLayer, CircleMarker, Tooltip } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { useFleetDataContext } from "../useFleetData";
import "./GlobalMap.scss";

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

  return (
    <MapContainer
      center={[30, 0]}
      zoom={2}
      minZoom={2}
      maxZoom={6}
      scrollWheelZoom={false}
      worldCopyJump
      attributionControl={false}
      zoomControl={false}
      className="ov-global-map"
    >
      <TileLayer
        key={isDark ? "dark" : "light"}
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
        url={isDark ? TILES.dark : TILES.light}
      />
      {clusters.flatMap((c) =>
        WORLD_COPIES.map((offset) => (
          <CircleMarker
            key={`${c.id}:${offset}`}
            center={[c.lat, c.lng + offset]}
            radius={8}
            pathOptions={{
              color: dotColor(c.status),
              fillColor: dotColor(c.status),
              fillOpacity: 0.7,
              weight: 2,
            }}
          >
            <Tooltip>
              <strong>{c.name}</strong>
              <br />
              {c.region} — {c.status}
            </Tooltip>
          </CircleMarker>
        )),
      )}
    </MapContainer>
  );
}
