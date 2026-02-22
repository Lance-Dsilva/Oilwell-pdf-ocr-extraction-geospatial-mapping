import re
import json
import sqlite3
from pathlib import Path
EXTRACTED_DIR = 'extracted_data'
DB_PATH = 'oil_wells.db'
DDL = '\nCREATE TABLE IF NOT EXISTS wells (\n    api_number   TEXT PRIMARY KEY,\n    well_name    TEXT,\n    well_number  TEXT,\n    operator     TEXT,\n    county       TEXT,\n    state        TEXT,\n    shl_desc     TEXT,\n    latitude     REAL,\n    longitude    REAL,\n    datum        TEXT,\n    pdf_filename TEXT\n);\n\nCREATE TABLE IF NOT EXISTS stimulation (\n    stimulation_id INTEGER PRIMARY KEY AUTOINCREMENT,\n    api_number     TEXT REFERENCES wells(api_number),\n    date_stimulated TEXT,\n    formation      TEXT,\n    top_ft         REAL,\n    bottom_ft      REAL,\n    stages         INTEGER,\n    volume         REAL,\n    volume_units   TEXT,\n    treatment_type TEXT,\n    acid_percent   REAL,\n    lbs_proppant   REAL,\n    max_pressure   REAL,\n    max_rate       REAL,\n    details        TEXT\n);\n'

def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()

def collect_page_data(pages: list[dict]) -> tuple[dict, list[str]]:
    merged_fields: dict = {}
    all_texts: list[str] = []
    for page in pages:
        text = page.get('text', '')
        if text.strip():
            all_texts.append(text)
        merged_fields.update(page.get('fields', {}))
    return (merged_fields, all_texts)

def _search(patterns: list[re.Pattern], texts: list[str]) -> str | None:
    for text in texts:
        for pat in patterns:
            m = pat.search(text)
            if m:
                val = re.sub('\\s+', ' ', m.group(1)).strip()
                if val:
                    return val
    return None
_API_PATS = [re.compile('API\\s*[#Nn][Oo\\.]*\\s*[:\\s]+([0-9][\\d\\s\\-]{8,19})', re.I), re.compile('API\\s*:\\s*([0-9][\\d\\s\\-]{8,19})', re.I), re.compile('\\bAPI\\b\\s*([0-9][\\d\\-]{8,18})', re.I)]
_WNUM_PATS = [re.compile('NDIC\\s+File\\s+Number\\s*:\\s*(\\d+)', re.I), re.compile('ND\\s+Well\\s+File\\s*#\\s*[:\\s]+(\\d+)', re.I), re.compile('Well\\s+File\\s+No\\.?\\s*[:\\s]+(\\d+)', re.I), re.compile('Well\\s+or\\s+Facility\\s+No\\.?\\s*[:\\s]+(\\d+)', re.I)]
_OP_PATS = [re.compile('Well\\s+Operator\\s*:\\s*([^\\n]+)', re.I), re.compile('^Operator\\s*:\\s*([^\\n]+)', re.I | re.M)]
_CO_PATS = [re.compile('^County\\s*:\\s*([A-Za-z ]+?)$', re.I | re.M), re.compile('County,\\s*State\\s*:\\s*([A-Za-z ]+?)\\s+County', re.I)]
_SHL_PATS = [re.compile('Well\\s+Surface\\s+Hole\\s+Location\\s*\\(SHL\\)\\s*:\\s*([^\\n]+)', re.I), re.compile('Surface\\s+(?:Hole\\s+)?Location\\s*:\\s*([^\\n]+)', re.I), re.compile('\\bSHL\\s*:\\s*([^\\n]+)', re.I)]
_LAT_PATS = [re.compile("Lat(?:itude)?\\s*[:\\s]+(\\d{1,2}°\\s*\\d{1,2}'\\s*[\\d.]+\\s*[Nn])", re.I), re.compile('Lat(?:itude)?\\s*[:\\s]+(\\d{1,3}\\.\\d+)', re.I)]
_LON_PATS = [re.compile("Lon(?:gitude)?\\s*[:\\s]+(\\d{1,3}°\\s*\\d{1,2}'\\s*[\\d.]+\\s*[Ww])", re.I), re.compile('Lon(?:gitude)?\\s*[:\\s]+(\\d{1,3}\\.\\d+)', re.I)]
_DATUM_PATS = [re.compile('Datum\\s*:\\s*([^\\n:]{2,30})', re.I), re.compile('(NAD\\s*\\d+|WGS\\s*\\d+)', re.I)]

def _dms_to_decimal(dms_str: str) -> float | None:
    m = re.match("(\\d{1,3})\\s*°\\s*(\\d{1,2})'\\s*([\\d.]+)\\s*([NSEWnsew])", dms_str.strip())
    if not m:
        return None
    deg, mins, secs, direction = m.groups()
    dd = float(deg) + float(mins) / 60 + float(secs) / 3600
    if direction.upper() in ('S', 'W'):
        dd = -dd
    return round(dd, 7)

def _normalize_api(raw: str) -> str | None:
    candidate = re.sub('\\s+', '', raw).strip()
    if re.search('[A-Za-z]', candidate):
        return None
    digits = re.sub('[^\\d]', '', raw)
    if len(digits) < 8:
        return None
    if len(digits) == 14:
        return f'{digits[:2]}-{digits[2:5]}-{digits[5:10]}-{digits[10:12]}-{digits[12:]}'
    if len(digits) == 10:
        return f'{digits[:2]}-{digits[2:5]}-{digits[5:10]}'
    return candidate

def extract_well_row(data: dict) -> dict:
    pages = data.get('pages', [])
    merged_fields, all_texts = collect_page_data(pages)
    raw_api = merged_fields.get('API #') or merged_fields.get('API Number') or merged_fields.get('API') or _search(_API_PATS, all_texts)
    api = _normalize_api(raw_api) if raw_api else None

    def _valid_well_num(val: str | None) -> str | None:
        if not val:
            return None
        digits = re.sub('[^\\d]', '', val)
        if digits.isdigit() and 1000 <= int(digits) <= 199999:
            return digits
        return None
    well_number = _valid_well_num(merged_fields.get('NDIC File Number')) or _valid_well_num(merged_fields.get('ND Well File #')) or _valid_well_num(merged_fields.get('Well or Facility No')) or _valid_well_num(_search(_WNUM_PATS, all_texts))
    if not well_number:
        well_number = re.sub('[^\\d]', '', Path(data.get('pdf_filename', '')).stem)
    api_key = api if api else f'NDIC-{well_number}' if well_number else 'UNKNOWN'
    _OP_STOP = re.compile('\\s+(?:Kick-off|Rig|API|Telephone|Well\\s+Name|Job\\s+Type|Enseco)\\s*[#:]', re.I)
    _OP_JUNK_START = re.compile('^(?:Kick-off|Rig\\b|Job\\s+Type|Enseco|Well\\s+Name|Telephone)\\s*[#:\\d]', re.I)

    def _first_valid_operator() -> str | None:
        keys = ('Well Operator', 'Operator')
        for page in pages:
            flds = page.get('fields', {})
            for k in keys:
                val = flds.get(k, '')
                if not val:
                    continue
                if _OP_JUNK_START.match(val.strip()):
                    continue
                cleaned = _OP_STOP.split(val)[0]
                cleaned = re.split('\\s{3,}', cleaned)[0]
                cleaned = re.sub('\\s+', ' ', cleaned).strip()
                cleaned = re.sub('^[a-z]\\s+', '', cleaned)
                cleaned = re.sub('^([a-z])([A-Z])', lambda m: m.group(1).upper() + m.group(2), cleaned)
                if cleaned and (not cleaned.endswith(':')) and (len(cleaned) >= 5):
                    if re.match('^(?:Well|Lease|Field|County|State|None)$', cleaned, re.I):
                        continue
                    return cleaned
        val = _search(_OP_PATS, all_texts)
        if val and (not val.endswith(':')) and (not _OP_JUNK_START.match(val.strip())):
            val = re.sub('^[a-z]\\s+', '', val)
            val = re.sub('^([a-z])([A-Z])', lambda m: m.group(1).upper() + m.group(2), val)
            if len(val) >= 5 and (not re.match('^(?:Well|Lease|Field|County|State|None)$', val, re.I)):
                return val
        return None
    operator = _first_valid_operator()
    county = merged_fields.get('County') or _search(_CO_PATS, all_texts)
    if county:
        county = re.split('\\s*(?:State|Section|Township|Directional|:)', county, maxsplit=1)[0]
        county = re.sub('\\s+', ' ', county).strip().title()
        if not re.match('^[A-Za-z ]{2,30}$', county):
            county = None
    shl = merged_fields.get('Well Surface Hole Location (SHL)') or merged_fields.get('Surface Location') or merged_fields.get('SHL') or _search(_SHL_PATS, all_texts)
    lat_field_val = merged_fields.get('Latitude', '')
    lat_s = _search(_LAT_PATS, [lat_field_val] + all_texts)
    lon_s = _search(_LON_PATS, [lat_field_val] + all_texts)

    def _to_coord(s: str | None) -> float | None:
        if not s:
            return None
        if '°' in s:
            return _dms_to_decimal(s)
        try:
            return float(s)
        except ValueError:
            return None
    latitude = _to_coord(lat_s)
    longitude = _to_coord(lon_s)
    if latitude is not None and (not 45.0 <= latitude <= 50.0):
        latitude = None
    if longitude is not None:
        if 96.0 <= longitude <= 105.0:
            longitude = -longitude
        elif not -105.0 <= longitude <= -96.0:
            longitude = None
    datum_raw = merged_fields.get('Datum') or _search(_DATUM_PATS, [lat_field_val] + all_texts)

    def _clean_datum(d: str | None) -> str | None:
        if not d:
            return None
        d = re.sub('\\s+', ' ', d).strip()
        if re.search('North\\s+American\\s+Datum\\s+1983', d, re.I):
            return 'NAD83'
        if re.search('North\\s+American\\s+Datum\\s+1927', d, re.I):
            return 'NAD27'
        if re.search('\\d.*(?:ft|usft|RKB|WELL|@)', d, re.I):
            return None
        m = re.match('^((?:NAD|WGS|NAO)\\s*\\d{2,4})', d.strip(), re.I)
        if m:
            prefix = re.sub('NAO', 'NAD', m.group(1).strip(), flags=re.I)
            return prefix
        if re.search('NAD|WGS|North\\s+American|GRS|NAVD', d, re.I):
            if len(d) <= 25 and (not re.search('[()@\\\\]', d)):
                return d
        return None
    datum = _clean_datum(datum_raw)
    well_name = data.get('well_name', '')
    well_name = re.sub('[\\s\\u00a0]+', ' ', well_name).strip()
    well_name = re.sub('\\s+API\\s*:.*$', '', well_name, flags=re.I)
    well_name = re.sub('\\s+Well\\s+File\\s+No\\.?:?.*$', '', well_name, flags=re.I)
    well_name = re.sub('\\s+(?:Directional\\s+Drillers|Field|Pad\\s+OD|Company\\s+Man)(?:\\s*:|)\\s*\\S.*$', '', well_name, flags=re.I)
    well_name = well_name.strip()
    _JUNK_NAME = re.compile('(?:^|^.{0,5})(Location|Field\\s*/\\s*Prospect|Directional\\s+Drillers|Mud\\s+Record)\\s*:?$', re.I)
    if well_name.endswith(':') or _JUNK_NAME.match(well_name):
        well_name = ''
    return {'api_number': api_key, 'well_name': well_name, 'well_number': well_number, 'operator': operator, 'county': county, 'state': 'ND', 'shl_desc': shl, 'latitude': latitude, 'longitude': longitude, 'datum': datum, 'pdf_filename': data.get('pdf_filename', '')}
_STIM_HDR = re.compile('Date\\s+Stimulat', re.I)
_TREAT_HDR = re.compile('Type\\s+Treatment', re.I)
_DETAILS = re.compile('^Details\\s*$', re.I | re.M)
_STIM_ROW = re.compile('(\\d{1,2}/\\d{1,2}/\\d{4})\\s+([A-Za-z][A-Za-z ]{1,30}?)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+([\\d,]+)\\s+([A-Za-z]+)')
_TREAT_ROW = re.compile('^([A-Za-z][A-Za-z ]{1,30}?)\\s+([\\d,]+)\\s+([\\d,]+)(?:\\s+([\\d.]+))?(?:\\s+([\\d.]+))?', re.M)

def _to_float(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(s.replace(',', ''))
    except ValueError:
        return None

def parse_stimulation(page_text: str) -> list[dict]:
    records = []
    lines = page_text.splitlines()
    n = len(lines)
    i = 0
    while i < n:
        line = lines[i]
        if not _STIM_HDR.search(line):
            i += 1
            continue
        i += 1
        while i < n and (not lines[i].strip() or _STIM_HDR.search(lines[i])):
            i += 1
        if i >= n:
            break
        data_line = lines[i]
        sm = _STIM_ROW.search(data_line)
        if not sm:
            i += 1
            continue
        rec: dict = {'date_stimulated': sm.group(1), 'formation': sm.group(2).strip(), 'top_ft': _to_float(sm.group(3)), 'bottom_ft': _to_float(sm.group(4)), 'stages': int(sm.group(5)), 'volume': _to_float(sm.group(6)), 'volume_units': sm.group(7), 'treatment_type': None, 'acid_percent': None, 'lbs_proppant': None, 'max_pressure': None, 'max_rate': None, 'details': None}
        i += 1
        detail_parts: list[str] = []
        in_details = False
        while i < n:
            ln = lines[i]
            if _STIM_HDR.search(ln):
                break
            if _TREAT_HDR.search(ln):
                i += 1
                while i < n and (not lines[i].strip()):
                    i += 1
                if i < n:
                    tm = _TREAT_ROW.match(lines[i].strip())
                    if tm:
                        nums = [_to_float(tm.group(g)) for g in (2, 3, 4, 5)]
                        non_none = [x for x in nums if x is not None]
                        rec['treatment_type'] = tm.group(1).strip()
                        if len(non_none) == 4:
                            rec['acid_percent'] = non_none[0]
                            rec['lbs_proppant'] = non_none[1]
                            rec['max_pressure'] = non_none[2]
                            rec['max_rate'] = non_none[3]
                        elif len(non_none) == 3:
                            rec['lbs_proppant'] = non_none[0]
                            rec['max_pressure'] = non_none[1]
                            rec['max_rate'] = non_none[2]
                        elif len(non_none) >= 1:
                            rec['lbs_proppant'] = non_none[0]
                    i += 1
                continue
            if _DETAILS.search(ln):
                in_details = True
                i += 1
                continue
            if in_details and ln.strip():
                detail_parts.append(ln.strip())
            i += 1
        if detail_parts:
            rec['details'] = '\n'.join(detail_parts)
        records.append(rec)
    return records

def process_json(json_path: Path, conn: sqlite3.Connection) -> None:
    with open(json_path, encoding='utf-8') as fh:
        data = json.load(fh)
    well = extract_well_row(data)
    api_key = well['api_number']
    conn.execute("\n        INSERT INTO wells\n          (api_number, well_name, well_number, operator, county, state,\n           shl_desc, latitude, longitude, datum, pdf_filename)\n        VALUES (?,?,?,?,?,?,?,?,?,?,?)\n        ON CONFLICT(api_number) DO UPDATE SET\n          well_name  = CASE\n                         WHEN LENGTH(excluded.well_name) > LENGTH(COALESCE(wells.well_name,''))\n                         THEN excluded.well_name\n                         ELSE wells.well_name\n                       END,\n          well_number = COALESCE(wells.well_number, excluded.well_number),\n          operator    = COALESCE(wells.operator,    excluded.operator),\n          county      = COALESCE(wells.county,      excluded.county),\n          shl_desc    = COALESCE(wells.shl_desc,    excluded.shl_desc),\n          latitude    = COALESCE(wells.latitude,    excluded.latitude),\n          longitude   = COALESCE(wells.longitude,   excluded.longitude),\n          datum       = COALESCE(wells.datum,       excluded.datum)\n        ", (well['api_number'], well['well_name'], well['well_number'], well['operator'], well['county'], well['state'], well['shl_desc'], well['latitude'], well['longitude'], well['datum'], well['pdf_filename']))
    stim_count = 0
    for page in data.get('pages', []):
        text = page.get('text', '')
        if not text or 'Date Stimulat' not in text:
            continue
        for rec in parse_stimulation(text):
            conn.execute('\n                INSERT INTO stimulation\n                  (api_number, date_stimulated, formation, top_ft, bottom_ft,\n                   stages, volume, volume_units, treatment_type, acid_percent,\n                   lbs_proppant, max_pressure, max_rate, details)\n                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)\n                ', (api_key, rec['date_stimulated'], rec['formation'], rec['top_ft'], rec['bottom_ft'], rec['stages'], rec['volume'], rec['volume_units'], rec['treatment_type'], rec['acid_percent'], rec['lbs_proppant'], rec['max_pressure'], rec['max_rate'], rec['details']))
            stim_count += 1
    stim_note = f', {stim_count} stim row(s)' if stim_count else ''
    print(f'  {json_path.name:50s}  →  {api_key}{stim_note}')

def main() -> None:
    json_files = sorted(Path(EXTRACTED_DIR).glob('*.json'))
    if not json_files:
        print(f"No JSON files found in '{EXTRACTED_DIR}/'. Run extract_data.py first.")
        return
    print(f"Found {len(json_files)} JSON file(s) in '{EXTRACTED_DIR}/'\n")
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    for path in json_files:
        process_json(path, conn)
    conn.commit()
    conn.close()
    conn2 = sqlite3.connect(DB_PATH)
    w_count = conn2.execute('SELECT COUNT(*) FROM wells').fetchone()[0]
    s_count = conn2.execute('SELECT COUNT(*) FROM stimulation').fetchone()[0]
    conn2.close()
    print(f'\nDone.  Database: {DB_PATH}')
    print(f'  wells        : {w_count} row(s)')
    print(f'  stimulation  : {s_count} row(s)')
if __name__ == '__main__':
    main()
