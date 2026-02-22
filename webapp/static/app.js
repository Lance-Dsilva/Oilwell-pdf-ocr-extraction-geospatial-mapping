function fmtNum(value) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n.toLocaleString() : "0";
}

function row(label, value) {
  return `
    <div class="popup-label">${label}</div>
    <div class="popup-value">${value ?? "N/A"}</div>
  `;
}

function popupHtml(w) {
  const stim = w.stimulation_summary || {};
  const link = w.drillingedge_url && w.drillingedge_url !== "N/A"
    ? `<a class="popup-link" target="_blank" rel="noopener" href="${w.drillingedge_url}">DrillingEdge Page</a>`
    : "";

  return `
    <div class="popup-content">
      <h3>${w.well_name || "N/A"}</h3>
      <div class="popup-grid">
        ${row("API", w.api_number)}
        ${row("Operator", w.operator)}
        ${row("County/State", `${w.county || "N/A"}, ${w.state || "N/A"}`)}
        ${row("Well Status", w.well_status)}
        ${row("Well Type", w.well_type)}
        ${row("Closest City", w.closest_city)}
        ${row("Oil Produced", fmtNum(w.barrels_oil_produced))}
        ${row("Gas Produced", fmtNum(w.gas_produced))}
        ${row("Stim Count", String(stim.count ?? 0))}
        ${row("Latest Stim Date", stim.most_recent_date || "N/A")}
        ${row("PDF", w.pdf_filename || "N/A")}
        ${row("Lat/Lon", `${w.latitude ?? "N/A"}, ${w.longitude ?? "N/A"}`)}
      </div>
      ${link}
    </div>
  `;
}

function isNdCoordinate(lat, lon) {
  // Keep map points constrained to North Dakota oilfield region.
  return (
    typeof lat === "number" &&
    typeof lon === "number" &&
    Number.isFinite(lat) &&
    Number.isFinite(lon) &&
    lat >= 45 &&
    lat <= 50 &&
    lon >= -106 &&
    lon <= -96
  );
}

function offsetDuplicate(lat, lon, indexInGroup) {
  if (indexInGroup <= 0) return [lat, lon];
  const angle = indexInGroup * 0.85;
  const radius = 0.0025 * Math.sqrt(indexInGroup); 
  const dLat = radius * Math.sin(angle);
  const dLon = radius * Math.cos(angle);
  return [lat + dLat, lon + dLon];
}

async function init() {
  const map = L.map("map");
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
    maxZoom: 19,
  }).addTo(map);

  const res = await fetch("/api/wells");
  if (!res.ok) {
    document.getElementById("stats").textContent = "Failed to load /api/wells";
    map.setView([47.5, -100.5], 6);
    return;
  }

  const data = await res.json();
  const center = data.center || { lat: 47.5, lon: -100.5 };
  const wells = data.wells || [];
  map.setView([center.lat, center.lon], 7);

  let plotted = 0;
  const bounds = [];
  const coordCounts = new Map();
  for (const w of wells) {
    if (!isNdCoordinate(w.latitude, w.longitude)) {
      continue;
    }
    const key = `${w.latitude.toFixed(6)},${w.longitude.toFixed(6)}`;
    const seen = coordCounts.get(key) || 0;
    coordCounts.set(key, seen + 1);

    const [latAdj, lonAdj] = offsetDuplicate(w.latitude, w.longitude, seen);
    const marker = L.marker([latAdj, lonAdj]).addTo(map);
    marker.bindPopup(popupHtml(w), { maxWidth: 400 });
    bounds.push([latAdj, lonAdj]);
    plotted += 1;
  }

  if (bounds.length > 0) {
    map.fitBounds(bounds, { padding: [20, 20], maxZoom: 10 });
  }

  document.getElementById("stats").textContent =
    `Total wells: ${data.count} | Plotted: ${plotted}`;
}

init().catch((err) => {
  document.getElementById("stats").textContent = `Error: ${err}`;
});
