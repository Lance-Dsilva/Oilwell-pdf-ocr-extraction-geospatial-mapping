from __future__ import annotations
import json
import os
import re
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import pypdf
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False
PDF_DIR = Path('DSCI560_Lab5')
OUTPUT_DIR = Path('extracted_data')
MIN_TEXT_CHARS_PER_PAGE = 60
OCR_DPI = 300

def sanitize_filename(name: str) -> str:
    safe = re.sub('[<>:"/\\\\|?*\\x00-\\x1f]', '_', name)
    safe = re.sub('[\\s_]+', ' ', safe).strip()
    return safe[:150] if safe else 'UNKNOWN'

def normalize_spaces(s: str) -> str:
    return re.sub('[\\s\\u00a0]+', ' ', s or '').strip()

def to_int(s: str | None) -> int | None:
    if not s:
        return None
    s = s.replace(',', '').strip()
    try:
        return int(s)
    except ValueError:
        return None

def to_float(s: str | None) -> float | None:
    if not s:
        return None
    s = s.replace(',', '').strip()
    try:
        return float(s)
    except ValueError:
        return None

def dms_to_decimal(deg: float, minutes: float, seconds: float, hemi: str) -> float:
    value = abs(deg) + minutes / 60.0 + seconds / 3600.0
    hemi = hemi.upper()
    if hemi in ('S', 'W'):
        value *= -1.0
    return value

def ocr_page(pdf_path: Path, page_number_1_indexed: int) -> str:
    if not OCR_AVAILABLE:
        return ''
    images = convert_from_path(str(pdf_path), dpi=OCR_DPI, first_page=page_number_1_indexed, last_page=page_number_1_indexed)
    if not images:
        return ''
    config = '--psm 6'
    text = pytesseract.image_to_string(images[0], config=config)
    return text or ''
WELL_NAME_PATTERNS = [re.compile('Well\\s+Name\\s+and\\s+Number\\s*\\n\\s*([^\\n]+)', re.I), re.compile('Well\\s+Name\\s*:\\s*([^\\n]+)', re.I), re.compile('Official\\s+Well\\s+Name\\s*:\\s*([^\\n]+)', re.I)]
API_PATTERNS = [re.compile('\\b(\\d{2}-\\d{3}-\\d{5})\\b'), re.compile('\\b(\\d{2}-\\d{3}-\\d{5,})\\b')]
OPERATOR_PATTERNS = [re.compile('\\bOperator\\s*\\n\\s*([^\\n]+)', re.I), re.compile('\\bOperator\\s*:\\s*([^\\n]+)', re.I)]
COUNTY_PATTERNS = [re.compile('\\bCounty\\s*\\n\\s*([A-Za-z]+)', re.I), re.compile('\\bCounty\\s*:\\s*([A-Za-z]+)', re.I), re.compile('\\bCounty\\s*([A-Za-z]+)\\b', re.I)]
STATE_PATTERNS = [re.compile('\\bState\\s*\\n\\s*([A-Z]{2})\\b', re.I)]
DMS_LAT = re.compile("(\\d{1,2})\\s*°\\s*(\\d{1,2})\\s*'\\s*([\\d.]+)\\s*([NS])", re.I)
DMS_LON = re.compile("(\\d{1,3})\\s*°\\s*(\\d{1,2})\\s*'\\s*([\\d.]+)\\s*([EW])", re.I)

def find_first(patterns: list[re.Pattern[str]], text: str) -> str | None:
    for pat in patterns:
        m = pat.search(text)
        if m:
            return normalize_spaces(m.group(1))
    return None

def extract_lat_lon(text: str) -> tuple[float | None, float | None]:
    lat_matches = list(DMS_LAT.finditer(text))
    lon_matches = list(DMS_LON.finditer(text))
    lat = lon = None
    if lat_matches:
        m = lat_matches[-1]
        lat = dms_to_decimal(float(m.group(1)), float(m.group(2)), float(m.group(3)), m.group(4))
    if lon_matches:
        m = lon_matches[-1]
        lon = dms_to_decimal(float(m.group(1)), float(m.group(2)), float(m.group(3)), m.group(4))
    return (lat, lon)
_STIM_HDR = re.compile('Well\\s+Specific\\s+Stimulat|Date\\s+Stimulat', re.I)
_TREAT_HDR = re.compile('Type\\s+Treatment', re.I)
_DETAILS = re.compile('^Details\\s*$', re.I)
_STIM_ROW = re.compile('(\\d{1,2}/\\d{1,2}/\\d{4})\\s+([A-Za-z][A-Za-z ]{1,40}?)\\s+(\\d{3,6})\\s+(\\d{3,6})\\s+(\\d{1,3})\\s+([\\d,]+)\\s+([A-Za-z]+)')
_TREAT_LINE = re.compile('^([A-Za-z][A-Za-z ]{1,30}?)\\s+([\\d,]+)(?:\\s+([\\d,]+))?(?:\\s+([\\d.]+))?(?:\\s+([\\d.]+))?$')
_PROPPANT_DETAIL = re.compile('^\\s*([A-Za-z0-9/ ]{2,40}?)\\s*[:\\-]\\s*([\\d,]+)\\s*$')

def parse_stimulation_records(page_text: str) -> list[dict[str, Any]]:
    if not page_text or not _STIM_HDR.search(page_text):
        return []
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    records: list[dict[str, Any]] = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        m = _STIM_ROW.search(ln)
        if not m:
            i += 1
            continue
        rec: dict[str, Any] = {'date_stimulated': m.group(1), 'stimulated_formation': m.group(2).strip(), 'top_ft': to_int(m.group(3)), 'bottom_ft': to_int(m.group(4)), 'stimulation_stages': to_int(m.group(5)), 'volume': to_int(m.group(6)), 'volume_units': m.group(7), 'treatment_type': None, 'acid_percent': None, 'lbs_proppant': None, 'max_treatment_pressure_psi': None, 'max_treatment_rate_bbls_min': None, 'proppant_details': [], 'raw_details': None}
        i += 1
        raw_details: list[str] = []
        while i < len(lines) and (not _STIM_ROW.search(lines[i])):
            cur = lines[i]
            tm = _TREAT_LINE.match(cur)
            if tm and (not rec['treatment_type']):
                rec['treatment_type'] = tm.group(1).strip()
                nums = [to_float(x) for x in tm.groups()[1:] if x is not None]
                if len(nums) == 4:
                    rec['acid_percent'] = nums[0]
                    rec['lbs_proppant'] = nums[1]
                    rec['max_treatment_pressure_psi'] = nums[2]
                    rec['max_treatment_rate_bbls_min'] = nums[3]
                elif len(nums) == 3:
                    rec['lbs_proppant'] = nums[0]
                    rec['max_treatment_pressure_psi'] = nums[1]
                    rec['max_treatment_rate_bbls_min'] = nums[2]
                elif len(nums) == 2:
                    rec['lbs_proppant'] = nums[0]
                    rec['max_treatment_pressure_psi'] = nums[1]
                i += 1
                continue
            dm = _PROPPANT_DETAIL.match(cur)
            if dm:
                rec['proppant_details'].append({'type': normalize_spaces(dm.group(1)), 'amount': to_int(dm.group(2))})
                i += 1
                continue
            raw_details.append(cur)
            i += 1
        if raw_details:
            rec['raw_details'] = '\n'.join(raw_details)
        records.append(rec)
    return records

@dataclass
class PageExtract:
    page_number: int
    method: str
    text: str

def extract_pages(pdf_path: Path) -> list[PageExtract]:
    warnings.filterwarnings('ignore', category=pypdf.errors.PdfReadWarning)
    pages: list[PageExtract] = []
    with open(pdf_path, 'rb') as f:
        reader = pypdf.PdfReader(f)
        for idx, page in enumerate(reader.pages, start=1):
            try:
                text = page.extract_text() or ''
            except Exception as exc:
                print(f'  warning: page {idx} text extraction failed ({type(exc).__name__}: {exc})')
                text = ''
            text = text.strip()
            needs_ocr = len(text) < MIN_TEXT_CHARS_PER_PAGE
            looks_like_stim = bool(_STIM_HDR.search(text))
            has_date = bool(re.search('\\d{1,2}/\\d{1,2}/\\d{4}', text))
            if looks_like_stim and (not has_date):
                needs_ocr = True
            if needs_ocr and OCR_AVAILABLE:
                ocr_text = ocr_page(pdf_path, idx).strip()
                if len(ocr_text) > len(text):
                    pages.append(PageExtract(idx, 'ocr', ocr_text))
                else:
                    pages.append(PageExtract(idx, 'pypdf', text))
            else:
                pages.append(PageExtract(idx, 'pypdf', text))
    return pages

def process_pdf(pdf_path: Path) -> dict[str, Any]:
    pages = extract_pages(pdf_path)
    full_text = '\n'.join((p.text for p in pages if p.text))
    well_name = find_first(WELL_NAME_PATTERNS, full_text) or pdf_path.stem
    well_name = normalize_spaces(re.sub('\\s+API\\s*:.*$', '', well_name, flags=re.I))
    api_number = find_first(API_PATTERNS, full_text)
    operator = find_first(OPERATOR_PATTERNS, full_text)
    county = find_first(COUNTY_PATTERNS, full_text)
    state = 'ND' if re.search('North\\s+Dakota', full_text, re.I) else find_first(STATE_PATTERNS, full_text) or 'N/A'
    latitude, longitude = extract_lat_lon(full_text)
    stim_records: list[dict[str, Any]] = []
    for p in pages:
        stim_records.extend(parse_stimulation_records(p.text))
    data: dict[str, Any] = {'pdf_filename': pdf_path.name, 'well_name': well_name, 'api_number': api_number, 'operator': operator, 'county': county, 'state': state, 'latitude': latitude, 'longitude': longitude, 'stimulation_records': stim_records, 'pages': [{'page_number': p.page_number, 'method': p.method, 'text_char_count': len(p.text or ''), 'text': p.text} for p in pages]}
    return data

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pdf_files = sorted(PDF_DIR.glob('*.pdf'))
    print(f'Found {len(pdf_files)} PDF(s) in: {PDF_DIR}')
    if not pdf_files:
        return
    if not OCR_AVAILABLE:
        print('WARNING: OCR dependencies not available.')
        print('Install: pip install pdf2image pytesseract pillow  &&  sudo apt-get install tesseract-ocr poppler-utils')
    for pdf_path in pdf_files:
        print(f'\nProcessing: {pdf_path.name}')
        data = process_pdf(pdf_path)
        out_name = sanitize_filename(data['well_name']) + '.json'
        out_path = OUTPUT_DIR / out_name
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f'  -> wrote: {out_path}')
        print(f'  well_name={data.get('well_name')}')
        print(f'  api_number={data.get('api_number')}')
        print(f'  stimulation_records={len(data.get('stimulation_records', []))}')
    print('\nDone.')
if __name__ == '__main__':
    main()
