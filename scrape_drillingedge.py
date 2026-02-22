from __future__ import annotations
import argparse
import sys
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
BASE_URL = 'https://www.drillingedge.com'
SEARCH_URL = f'{BASE_URL}/search'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
TEXT_COLUMNS = {'well_status': "TEXT DEFAULT 'N/A'", 'well_type': "TEXT DEFAULT 'N/A'", 'closest_city': "TEXT DEFAULT 'N/A'"}
NUM_COLUMNS = {'barrels_oil_produced': 'REAL DEFAULT 0', 'gas_produced': 'REAL DEFAULT 0'}
EXTRA_COLUMNS = {'drillingedge_url': "TEXT DEFAULT 'N/A'"}
FIELD_LABELS = {'well_status': ['well\\s*status'], 'well_type': ['well\\s*type', 'well\\s*purpose'], 'closest_city': ['closest\\s*city', 'nearest\\s*city'], 'barrels_oil_produced': ['barrels?\\s+of\\s+oil\\s+produced', 'oil\\s+produced', 'cumulative\\s+oil', 'oil\\s+production', 'total\\s+oil\\s+prod', 'oil\\s+prod'], 'gas_produced': ['gas\\s+produced', 'cumulative\\s+gas', 'gas\\s+production', 'total\\s+gas\\s+prod', 'gas\\s+prod']}

@dataclass
class ScrapedRecord:
    well_status: str = 'N/A'
    well_type: str = 'N/A'
    closest_city: str = 'N/A'
    barrels_oil_produced: float = 0.0
    gas_produced: float = 0.0
    drillingedge_url: str = 'N/A'

def normalize_text(s: str | None) -> str:
    if not s:
        return ''
    return re.sub('\\s+', ' ', s).strip()

def normalize_numeric(raw: str | None) -> float:
    if not raw:
        return 0.0
    s = normalize_text(raw)
    if not s or s.lower() in {'n/a', 'na', 'none', '-', '--'}:
        return 0.0
    m = re.search('(-?\\d+(?:,\\d{3})*(?:\\.\\d+)?)\\s*([kmb])?', s, re.I)
    if not m:
        return 0.0
    num = float(m.group(1).replace(',', ''))
    suffix = (m.group(2) or '').upper()
    if suffix == 'K':
        num *= 1000
    elif suffix == 'M':
        num *= 1000000
    elif suffix == 'B':
        num *= 1000000000
    return num

def production_numeric(raw: str | None) -> float:
    if not raw:
        return 0.0
    s = normalize_text(raw)
    if not s:
        return 0.0
    if 'members only' in s.lower():
        return 0.0
    if re.search('\\b\\d+\\s*N\\s+\\d+\\s*W\\b', s, re.I):
        return 0.0
    val = normalize_numeric(s)
    if val == 0:
        return 0.0
    has_prod_unit = bool(re.search('\\b(bbl|barrel|mcf|mmcf|bcf|mmbtu|gas|oil)\\b', s, re.I))
    if not has_prod_unit and 1900 <= val <= 2100:
        return 0.0
    return val

def sanitize_status(raw: str | None) -> str:
    s = normalize_text(raw)
    if not s:
        return 'N/A'
    if re.search('\\b\\d+\\s*N\\s+\\d+\\s*W\\b', s, re.I):
        return 'N/A'
    return s

def sanitize_city(raw: str | None) -> str:
    s = normalize_text(raw)
    if not s:
        return 'N/A'
    if re.search('\\b\\d+\\s*N\\s+\\d+\\s*W\\b', s, re.I):
        return 'N/A'
    return s

def api_digits(api_number: str | None) -> str:
    return re.sub('\\D', '', api_number or '')

def canonical_api(api_number: str | None) -> str:
    d = api_digits(api_number)
    if len(d) >= 10:
        if len(d) == 10:
            pass
        elif len(d) == 11:
            d = d[:5] + d[-5:]
        else:
            d = d[:10]
        return f'{d[:2]}-{d[2:5]}-{d[5:10]}'
    return normalize_text(api_number)

def name_tokens(well_name: str | None) -> list[str]:
    tokens = re.findall('[a-z0-9]+', (well_name or '').lower())
    return [t for t in tokens if len(t) > 2]

def slugify(value: str | None) -> str:
    s = normalize_text(value).lower()
    s = s.replace('&', ' and ')
    s = re.sub('[^a-z0-9]+', '-', s)
    return s.strip('-')

def state_slug(state: str | None) -> str:
    s = normalize_text(state).lower()
    if s in {'nd', 'north dakota'}:
        return 'north-dakota'
    return slugify(s)

def county_slug(county: str | None) -> str:
    c = normalize_text(county).lower()
    c = re.sub('\\bcounty\\b', '', c).strip()
    if not c:
        return ''
    return f'{slugify(c)}-county'

def ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute('PRAGMA table_info(wells)').fetchall()}
    for col, ddl in (TEXT_COLUMNS | NUM_COLUMNS | EXTRA_COLUMNS).items():
        if col not in existing:
            conn.execute(f'ALTER TABLE wells ADD COLUMN {col} {ddl}')
    conn.commit()

def load_wells(conn: sqlite3.Connection, limit: int | None=None, offset: int=0, only_missing: bool=True) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    where = ''
    if only_missing:
        where = "WHERE well_status IS NULL OR TRIM(well_status)='' OR well_status='N/A' OR well_type IS NULL OR TRIM(well_type)='' OR well_type='N/A' OR closest_city IS NULL OR TRIM(closest_city)='' OR closest_city='N/A' OR barrels_oil_produced IS NULL OR barrels_oil_produced=0 OR gas_produced IS NULL OR gas_produced=0"
    sql = f'SELECT api_number, well_name FROM wells {where} ORDER BY api_number LIMIT ? OFFSET ?'
    sql = sql.replace('SELECT api_number, well_name', 'SELECT api_number, well_name, county, state')
    lim = -1 if limit is None else limit
    return conn.execute(sql, (lim, offset)).fetchall()

def build_queries(api_number: str | None, well_name: str | None) -> list[str]:
    queries: list[str] = []
    digits = api_digits(api_number)
    if len(digits) >= 8:
        queries.append(api_number or digits)
        if len(digits) >= 10:
            queries.append(digits[:10])
        queries.append(digits)
    if normalize_text(well_name):
        queries.append(normalize_text(well_name))
    out: list[str] = []
    seen: set[str] = set()
    for q in queries:
        key = q.lower()
        if key not in seen:
            out.append(q)
            seen.add(key)
    return out

def extract_label_value_pairs(soup: BeautifulSoup) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for th in soup.find_all('th'):
        td = th.find_next_sibling('td')
        if td:
            label = normalize_text(th.get_text(' ', strip=True)).lower()
            value = normalize_text(td.get_text(' ', strip=True))
            if label and value:
                pairs[label] = value
    for dt in soup.select('dt'):
        dd = dt.find_next_sibling('dd')
        if dd:
            label = normalize_text(dt.get_text(' ', strip=True)).lower()
            value = normalize_text(dd.get_text(' ', strip=True))
            if label and value:
                pairs[label] = value
    for node in soup.find_all(string=re.compile(':')):
        text = normalize_text(str(node))
        if ':' not in text:
            continue
        left, right = text.split(':', 1)
        label = normalize_text(left).lower()
        value = normalize_text(right)
        if label and value and (len(label) < 80):
            pairs.setdefault(label, value)
    return pairs

def extract_field(soup: BeautifulSoup, pairs: dict[str, str], labels: Iterable[str]) -> str | None:
    for label_key, value in pairs.items():
        for pattern in labels:
            if re.search(f'\\b{pattern}\\b', label_key, re.I):
                return value
    text = soup.get_text('\n', strip=True)
    for pattern in labels:
        m = re.search(f'{pattern}\\s*:?\\s*([^\\n|]+)', text, re.I)
        if m:
            value = normalize_text(m.group(1))
            if value:
                return value
    return None

def parse_well_page(html: str, url: str) -> ScrapedRecord:
    soup = BeautifulSoup(html, 'html.parser')
    pairs = extract_label_value_pairs(soup)
    status = extract_field(soup, pairs, FIELD_LABELS['well_status']) or 'N/A'
    wtype = extract_field(soup, pairs, FIELD_LABELS['well_type']) or 'N/A'
    city = extract_field(soup, pairs, FIELD_LABELS['closest_city']) or 'N/A'
    oil = extract_field(soup, pairs, FIELD_LABELS['barrels_oil_produced'])
    gas = extract_field(soup, pairs, FIELD_LABELS['gas_produced'])
    return ScrapedRecord(well_status=sanitize_status(status), well_type=normalize_text(wtype) or 'N/A', closest_city=sanitize_city(city), barrels_oil_produced=production_numeric(oil), gas_produced=production_numeric(gas), drillingedge_url=url or 'N/A')

def direct_well_urls(api_number: str | None, well_name: str | None, county: str | None, state: str | None) -> list[str]:
    api = canonical_api(api_number)
    name_slug = slugify(well_name)
    urls: list[str] = []
    if not api or not name_slug:
        return urls
    st = state_slug(state) or 'north-dakota'
    co = county_slug(county)
    if co:
        urls.append(f'{BASE_URL}/{st}/{co}/wells/{name_slug}/{api}')
    urls.append(f'{BASE_URL}/{st}/wells/{name_slug}/{api}')
    urls.append(f'{BASE_URL}/wells/{name_slug}/{api}')
    return urls

def fetch_direct_well_page(session: requests.Session, api_number: str | None, well_name: str | None, county: str | None, state: str | None, timeout: int=25) -> ScrapedRecord | None:
    api = canonical_api(api_number)
    for url in direct_well_urls(api, well_name, county, state):
        try:
            res = session.get(url, headers=HEADERS, timeout=timeout)
        except requests.RequestException:
            continue
        if res.status_code != 200:
            continue
        text = res.text.lower()
        if 'well summary' not in text and 'well details' not in text and (api and api not in text):
            continue
        return parse_well_page(res.text, url)
    return None

def score_result(href: str, title: str, api_number: str | None, well_name: str | None, query: str) -> int:
    score = 0
    blob = f'{href} {title}'.lower()
    digits = api_digits(api_number)
    if digits and digits in re.sub('\\D', '', blob):
        score += 100
    q = normalize_text(query).lower()
    if q and q in blob:
        score += 15
    for tok in name_tokens(well_name):
        if tok in blob:
            score += 2
    return score

def best_search_result(html: str, api_number: str | None, well_name: str | None, query: str) -> str | None:
    soup = BeautifulSoup(html, 'html.parser')
    candidates: list[tuple[int, str]] = []
    for a in soup.select('a[href]'):
        href = a.get('href', '')
        if not href:
            continue
        if '/well' not in href and '/wells' not in href:
            continue
        full_url = urljoin(BASE_URL, href)
        title = normalize_text(a.get_text(' ', strip=True))
        score = score_result(full_url, title, api_number, well_name, query)
        candidates.append((score, full_url))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_url = candidates[0]
    if best_score <= 0:
        return None
    return best_url

def fetch_with_requests(session: requests.Session, api_number: str | None, well_name: str | None, county: str | None, state: str | None, timeout: int=25) -> ScrapedRecord | None:
    direct = fetch_direct_well_page(session, api_number, well_name, county, state, timeout=timeout)
    if direct:
        return direct
    queries = build_queries(api_number, well_name)
    for q in queries:
        for param_key in ('q', 'query'):
            try:
                res = session.get(SEARCH_URL, params={param_key: q}, headers=HEADERS, timeout=timeout)
                if res.status_code != 200:
                    continue
            except requests.RequestException:
                continue
            maybe_url = best_search_result(res.text, api_number, well_name, q)
            if not maybe_url:
                continue
            try:
                well_res = session.get(maybe_url, headers=HEADERS, timeout=timeout)
                if well_res.status_code != 200:
                    continue
            except requests.RequestException:
                continue
            return parse_well_page(well_res.text, maybe_url)
    return None

def fetch_with_selenium(api_number: str | None, well_name: str | None, county: str | None, state: str | None, headless: bool=True) -> ScrapedRecord | None:
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception:
        return None
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--window-size=1400,2000')
    options.add_argument(f'user-agent={HEADERS['User-Agent']}')
    driver = webdriver.Chrome(options=options)
    try:
        for url in direct_well_urls(api_number, well_name, county, state):
            driver.get(url)
            time.sleep(1.0)
            html = driver.page_source
            low = html.lower()
            if 'well summary' in low or 'well details' in low:
                return parse_well_page(html, url)
        for q in build_queries(api_number, well_name):
            driver.get(SEARCH_URL)
            wait = WebDriverWait(driver, 12)
            search_box = None
            selectors = [(By.CSS_SELECTOR, "input[name='q']"), (By.CSS_SELECTOR, "input[name='query']"), (By.CSS_SELECTOR, "input[type='search']"), (By.CSS_SELECTOR, "input[type='text']")]
            for by, sel in selectors:
                elems = driver.find_elements(by, sel)
                if elems:
                    search_box = elems[0]
                    break
            if search_box is None:
                continue
            search_box.clear()
            search_box.send_keys(q)
            search_box.send_keys(Keys.ENTER)
            time.sleep(1.5)
            html = driver.page_source
            best_url = best_search_result(html, api_number, well_name, q)
            if not best_url:
                continue
            driver.get(best_url)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            return parse_well_page(driver.page_source, best_url)
    finally:
        driver.quit()
    return None

def store_record(conn: sqlite3.Connection, api_number: str, rec: ScrapedRecord) -> None:
    conn.execute('\n        UPDATE wells\n           SET well_status = ?,\n               well_type = ?,\n               closest_city = ?,\n               barrels_oil_produced = ?,\n               gas_produced = ?,\n               drillingedge_url = ?\n         WHERE api_number = ?\n        ', (rec.well_status or 'N/A', rec.well_type or 'N/A', rec.closest_city or 'N/A', rec.barrels_oil_produced if rec.barrels_oil_produced is not None else 0, rec.gas_produced if rec.gas_produced is not None else 0, rec.drillingedge_url or 'N/A', api_number))

def check_connectivity(session: requests.Session, timeout: int=15) -> bool:
    try:
        res = session.get(BASE_URL, headers=HEADERS, timeout=timeout)
        return res.status_code < 500
    except requests.RequestException:
        return False

def main() -> None:
    parser = argparse.ArgumentParser(description='Scrape DrillingEdge data into wells table.')
    parser.add_argument('--db', default='oil_wells.db', help='Path to SQLite DB.')
    parser.add_argument('--limit', type=int, default=None, help='Max wells to process.')
    parser.add_argument('--offset', type=int, default=0, help='Start offset.')
    parser.add_argument('--sleep', type=float, default=0.7, help='Seconds between wells.')
    parser.add_argument('--no-only-missing', action='store_true', help='Process all wells, not just missing scraped fields.')
    parser.add_argument('--no-selenium-fallback', action='store_true', help='Disable Selenium fallback when requests parsing fails.')
    parser.add_argument('--selenium-headed', action='store_true', help='Run Selenium in headed mode (debugging).')
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    ensure_columns(conn)
    rows = load_wells(conn, limit=args.limit, offset=args.offset, only_missing=not args.no_only_missing)
    print(f'Loaded {len(rows)} wells to scrape from {args.db}')
    session = requests.Session()
    session.headers.update(HEADERS)
    if not check_connectivity(session):
        print('ERROR: Cannot reach https://www.drillingedge.com from this environment. Stopping to avoid writing default values for every well.')
        sys.exit(1)
    ok = 0
    miss = 0
    for idx, row in enumerate(rows, start=1):
        api = row['api_number']
        name = row['well_name']
        county = row['county']
        state = row['state']
        print(f'[{idx}/{len(rows)}] {api} | {name}')
        record = fetch_with_requests(session, api, name, county, state)
        if not record and (not args.no_selenium_fallback):
            record = fetch_with_selenium(api, name, county, state, headless=not args.selenium_headed)
        if not record:
            record = ScrapedRecord()
            miss += 1
            print('  -> no match found; storing defaults')
        else:
            ok += 1
            print(f'  -> status={record.well_status}, type={record.well_type}, city={record.closest_city}, oil={record.barrels_oil_produced}, gas={record.gas_produced}')
        store_record(conn, api, record)
        conn.commit()
        time.sleep(max(0.0, args.sleep))
    conn.close()
    print(f'Done. Scraped={ok}, defaulted={miss}, total={len(rows)}')
if __name__ == '__main__':
    main()
