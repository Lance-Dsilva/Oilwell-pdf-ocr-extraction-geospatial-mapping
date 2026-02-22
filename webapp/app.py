from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Any
from flask import Flask, jsonify, render_template
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR.parent / 'oil_wells.db'
app = Flask(__name__, template_folder='templates', static_folder='static')

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

@app.get('/')
def index():
    return render_template('index.html')

@app.get('/api/wells')
def api_wells():
    query = '\n    SELECT\n        w.api_number,\n        w.well_name,\n        w.operator,\n        w.county,\n        w.state,\n        w.latitude,\n        w.longitude,\n        w.well_status,\n        w.well_type,\n        w.closest_city,\n        w.barrels_oil_produced,\n        w.gas_produced,\n        w.drillingedge_url,\n        w.pdf_filename,\n        COUNT(s.stimulation_id) AS stimulation_count,\n        MAX(s.date_stimulated) AS most_recent_stim_date\n    FROM wells w\n    LEFT JOIN stimulation s ON s.api_number = w.api_number\n    GROUP BY w.api_number\n    ORDER BY w.well_name\n    '
    wells: list[dict[str, Any]] = []
    lat_sum = 0.0
    lon_sum = 0.0
    valid_coords = 0
    with get_conn() as conn:
        rows = conn.execute(query).fetchall()
    for row in rows:
        lat = _to_float(row['latitude'])
        lon = _to_float(row['longitude'])
        if lat is not None and lon is not None:
            lat_sum += lat
            lon_sum += lon
            valid_coords += 1
        wells.append({'api_number': row['api_number'], 'well_name': row['well_name'] or 'N/A', 'operator': row['operator'] or 'N/A', 'county': row['county'] or 'N/A', 'state': row['state'] or 'N/A', 'latitude': lat, 'longitude': lon, 'well_status': row['well_status'] or 'N/A', 'well_type': row['well_type'] or 'N/A', 'closest_city': row['closest_city'] or 'N/A', 'barrels_oil_produced': _to_float(row['barrels_oil_produced']) or 0.0, 'gas_produced': _to_float(row['gas_produced']) or 0.0, 'drillingedge_url': row['drillingedge_url'] or 'N/A', 'pdf_filename': row['pdf_filename'] or 'N/A', 'stimulation_summary': {'count': int(row['stimulation_count'] or 0), 'most_recent_date': row['most_recent_stim_date'] or 'N/A'}})
    if valid_coords > 0:
        center = {'lat': lat_sum / valid_coords, 'lon': lon_sum / valid_coords}
    else:
        center = {'lat': 47.5, 'lon': -100.5}
    return jsonify({'count': len(wells), 'plottable_count': valid_coords, 'center': center, 'wells': wells})
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
