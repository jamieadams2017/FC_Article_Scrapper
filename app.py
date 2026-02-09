#!/usr/bin/env python3
# FC_Article_Scraper.py — Streamlit GUI scraper with settings persistence

import os
import json
import time
import datetime
from typing import List, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as dateparser
from bs4 import BeautifulSoup, NavigableString, Tag

import gspread
from gspread.exceptions import SpreadsheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

import streamlit as st

TZINFOS = {
    # Bangladesh/India-style IST commonly appears in South Asian news sites as India Standard Time.
    "IST": datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
}

APP_TITLE = "FC Article Scrapper"

# =========================
# Config (edit defaults)
# =========================
DEFAULT_SHEET = "Rumor Scanner Scraped Data"
DEFAULT_WORKSHEET = "collected url"

SOURCES_WP = [
    ("Rumorscanner",        "https://rumorscanner.com/wp-json/wp/v2",          "fact-check"),
    ("Fact-watch",          "https://www.fact-watch.org/wp-json/wp/v2",        "ফ্যাক্টচেক"),
    ("Dismislab",           "https://dismislab.com/wp-json/wp/v2",             "factcheck"),
    ("Newschecker",      "https://bangladesh.newschecker.co/wp-json/wp/v2", "fact-checks-bn"),
]
FC_SITE_NAME = "Factcrescendo"
FC_API_BASE  = "https://bangladesh.factcrescendo.com/wp-json/wp/v2"
BOOM_SITE_NAME = "Boombd"
BOOM_BASE      = "https://www.boombd.com/fake-news"
AJK_SITE_NAME  = "Ajker Patrika"
AJK_BASE       = "https://www.ajkerpatrika.com/fact-check"

DISSENT_SITE_NAME = "The Dissent"
DISSENT_API_BASE = "https://thedissent.news/api/fact_checks"
DISSENT_ARTICLE_BASE = "https://thedissent.news/bn/fact-checks"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# =========================
# Settings persistence
# =========================
def _settings_dir() -> str:
    # Use %APPDATA%/Rumorscanner on Windows; ~/.rumor_scanner elsewhere
    appdata = os.getenv("APPDATA")
    if appdata:
        base = os.path.join(appdata, "Rumorscanner")
    else:
        base = os.path.join(os.path.expanduser("~"), ".rumor_scanner")
    os.makedirs(base, exist_ok=True)
    return base


SETTINGS_DIR = _settings_dir()
SETTINGS_PATH = os.path.join(SETTINGS_DIR, "settings.json")
CREDS_STORE_PATH = os.path.join(SETTINGS_DIR, "service_account.json")


def load_settings() -> dict:
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_settings(d: dict) -> None:
    try:
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, ensure_ascii=False)
    except Exception as e:
        # non-fatal; just log to console
        print(f"[warn] could not save settings: {e}")


# =========================
# Networking (retries)
# =========================
def make_session() -> requests.Session:
    s = requests.Session()
    # Use a realistic browser UA to avoid simple bot blocks
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )
    })
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


SESSION = make_session()


def http_get(url: str, **kw) -> requests.Response:
    kw.setdefault("timeout", 15)
    r = SESSION.get(url, **kw)
    r.raise_for_status()
    return r


# =========================
# Google Sheets helpers
# =========================
def _has_streamlit_secrets_creds() -> bool:
    try:
        return bool(st.secrets.get("google_service_account"))
    except Exception:
        return False


def open_sheet(creds_json_path: str, sheet_ref: str, worksheet_name: str):
    """
    Opens a Google Sheet + worksheet.
    Credentials source:
      - If creds_json_path is an existing file -> use it
      - Else -> use Streamlit secrets: st.secrets["google_service_account"]
    """
    if creds_json_path and os.path.isfile(creds_json_path):
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, SCOPES)
    else:
        if not _has_streamlit_secrets_creds():
            raise RuntimeError(
                "No credentials found. Upload a service account JSON, set GOOGLE_CREDS_JSON env var, "
                "or add [google_service_account] to Streamlit secrets."
            )
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            st.secrets["google_service_account"],
            SCOPES
        )

    gc = gspread.authorize(creds)

    try:
        if sheet_ref.startswith("http"):
            sh = gc.open_by_url(sheet_ref)
        elif len(sheet_ref) > 30:  # likely an ID
            sh = gc.open_by_key(sheet_ref)
        else:
            sh = gc.open(sheet_ref)
    except SpreadsheetNotFound:
        raise RuntimeError(f"Could not find spreadsheet: {sheet_ref}")

    # Worksheet/tab: open by name; create if missing
    worksheet_name = (worksheet_name or "").strip() or "Sheet1"
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=10)

    header = ws.get("A1:D1")
    if not header:
        ws.append_row(["Title", "URL", "Date", "Site"])

    return ws


def append_rows_batched(ws, rows: List[List[str]], batch_size: int = 200, max_retries: int = 6):
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]

        for attempt in range(1, max_retries + 1):
            try:
                ws.append_rows(chunk, value_input_option="RAW")
                break
            except Exception as e:
                wait = min(60, 2 ** (attempt - 1))
                print(f"[warn] append_rows failed (batch {i//batch_size + 1}, attempt {attempt}/{max_retries}): {e}")
                time.sleep(wait)

                if attempt == max_retries:
                    raise


# =========================
# Scraper helpers
# =========================
def day_bounds_utc(d: datetime.date) -> Tuple[str, str]:
    start = datetime.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=datetime.timezone.utc).isoformat()
    end   = datetime.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=datetime.timezone.utc).isoformat()
    return start, end


def dedupe(rows: List[Tuple[str, str, str, str]]) -> List[Tuple[str, str, str, str]]:
    seen = set()
    out: List[Tuple[str, str, str, str]] = []
    for t, u, d, s in rows:
        if u in seen:
            continue
        seen.add(u)
        out.append((t, u, d, s))
    return out


# =========================
# WordPress sources
# =========================
def get_category_id(api_base: str, slug: Optional[str]) -> Optional[int]:
    if not slug:
        return None
    r = http_get(f"{api_base}/categories", params={"slug": slug, "per_page": 1})
    try:
        data = r.json()
    except ValueError:
        return None
    return data[0]["id"] if data else None


def fetch_wp_posts(api_base: str, cat_id: Optional[int], target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    after, before = day_bounds_utc(target_date)
    page = 1
    while True:
        params = {
            "after": after,
            "before": before,
            "per_page": 100,
            "page": page,
            "_fields": "title,link,date_gmt",
        }
        if cat_id:
            params["categories"] = cat_id
        r = http_get(f"{api_base}/posts", params=params)

        try:
            posts = r.json()
        except ValueError:
            break

        if not posts:
            break
        for p in posts:
            title = BeautifulSoup(p["title"]["rendered"], "html.parser").get_text()
            rows.append((title, p["link"], p["date_gmt"][:10]))
        total_pages = int(r.headers.get("X-WP-TotalPages", "1"))
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.2)
    return rows


def fetch_fc_posts(api_base: str, target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    after, before = day_bounds_utc(target_date)
    page = 1
    while True:
        r = http_get(
            f"{api_base}/posts",
            params={"after": after, "before": before, "per_page": 100, "page": page, "_fields": "title,link,date_gmt"},
        )
        try:
            posts = r.json()
        except ValueError:
            break

        if not posts:
            break
        for p in posts:
            title = BeautifulSoup(p["title"]["rendered"], "html.parser").get_text()
            rows.append((title, p["link"], p["date_gmt"][:10]))
        total_pages = int(r.headers.get("X-WP-TotalPages", "1"))
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.2)
    return rows


# =========================
# BoomBD & Ajker Patrika
# =========================
def scrape_boombd(target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    seen = set()
    page = 1
    stop = False
    while not stop:
        r = http_get(BOOM_BASE + (f"/page/{page}/" if page > 1 else ""))
        soup = BeautifulSoup(r.text, "html.parser")
        h4s = soup.find_all("h4")
        if not h4s:
            break
        page_has_any = False
        for h4 in h4s:
            a = h4.find("a", href=True)
            if not a:
                continue
            link = a["href"].strip()
            if link.startswith("/"):
                link = "https://www.boombd.com" + link
            if link in seen:
                continue
            seen.add(link)
            title = a.get_text(strip=True)

            sib = h4.next_sibling
            date_str = None
            while sib:
                text = sib.strip() if isinstance(sib, NavigableString) else sib.get_text(strip=True)
                if "|" in text:
                    date_str = text.split("|", 1)[1].strip()
                    break
                sib = sib.next_sibling

            if not date_str:
                try:
                    pr = http_get(link)
                    ps = BeautifulSoup(pr.text, "html.parser")
                    t = ps.find("time", attrs={"datetime": True}) or ps.find("time")
                    if t:
                        date_str = t.get("datetime") or t.get_text(strip=True)
                except Exception:
                    date_str = None

            if not date_str:
                continue

            try:
                art_date = dateparser.parse(date_str, tzinfos=TZINFOS).date()
            except Exception:
                continue

            if art_date < target_date:
                stop = True
                break

            if art_date == target_date:
                rows.append((title, link, art_date.isoformat()))
                page_has_any = True

        if stop:
            break
        if not page_has_any and page > 3:
            break
        page += 1
        time.sleep(0.2)
    return rows


import re

AJK_PUBLISH_RE = re.compile(
    r"প্রকাশ\s*:\s*([০-৯]{1,2})\s+([^\s,]+)\s+([০-৯]{4})",
    re.UNICODE
)

AJK_BN_DIGITS = {"০":"0","১":"1","২":"2","৩":"3","৪":"4","৫":"5","৬":"6","৭":"7","৮":"8","৯":"9"}

AJK_BN_MONTHS = {
    "জানুয়ারি": 1, "জানুয়ারী": 1, "জানুয়ারী": 1,
    "ফেব্রুয়ারি": 2, "ফেব্রুয়ারী": 2, "ফেব্রুয়ারী": 2,
    "মার্চ": 3,
    "এপ্রিল": 4,
    "মে": 5,
    "জুন": 6,
    "জুলাই": 7,
    "আগস্ট": 8,
    "সেপ্টেম্বর": 9,
    "অক্টোবর": 10,
    "নভেম্বর": 11,
    "ডিসেম্বর": 12,
}

def _bn_to_int(s: str) -> int:
    return int("".join(AJK_BN_DIGITS.get(ch, ch) for ch in str(s)))

def _parse_ajk_publish_date(ps: BeautifulSoup) -> Optional[datetime.date]:
    txt = ps.get_text(" ", strip=True)
    m = AJK_PUBLISH_RE.search(txt)
    if not m:
        return None
    day_bn, month_bn, year_bn = m.group(1), m.group(2), m.group(3)
    month = AJK_BN_MONTHS.get(month_bn.strip())
    if not month:
        return None
    try:
        day = _bn_to_int(day_bn)
        year = _bn_to_int(year_bn)
        return datetime.date(year, month, day)
    except Exception:
        return None

def scrape_ajker(target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    seen = set()
    page = 1
    stop = False

    while not stop:
        r = http_get(AJK_BASE + (f"?page={page}" if page > 1 else ""))
        soup = BeautifulSoup(r.text, "html.parser")

        anchors = [a for a in soup.find_all("a", href=True) if "/fact-check/" in a["href"]]
        if not anchors:
            break

        found_any_for_date = False

        for a in anchors:
            url = a["href"].strip()
            url = url if url.startswith("http") else "https://www.ajkerpatrika.com" + url
            if url in seen:
                continue
            seen.add(url)

            try:
                pr = http_get(url)
            except Exception:
                continue

            ps = BeautifulSoup(pr.text, "html.parser")
            art_date = _parse_ajk_publish_date(ps)
            if not art_date:
                continue

            if art_date < target_date:
                stop = True
                break

            if art_date == target_date:
                h = ps.find(["h1", "h2"])
                title = h.get_text(strip=True) if h else a.get_text(strip=True) or "(no title)"
                rows.append((title, url, art_date.isoformat()))
                found_any_for_date = True

        if stop or (not found_any_for_date and page > 3):
            break

        page += 1
        time.sleep(0.2)

    return rows


# =========================
# The Dissent HTML helpers  (FIXED)
# =========================

# Bangla digits & months
BN_DIGITS = {
    "০": "0",
    "১": "1",
    "২": "2",
    "৩": "3",
    "৪": "4",
    "৫": "5",
    "৬": "6",
    "৭": "7",
    "৮": "8",
    "৯": "9",
}

BN_MONTHS = {
    "জানুয়ারি": 1, "জানুয়ারী": 1,
    "ফেব্রুয়ারি": 2, "ফেব্রুয়ারী": 2,
    "মার্চ": 3,
    "এপ্রিল": 4,
    "মে": 5,
    "জুন": 6,
    "জুলাই": 7,
    "আগস্ট": 8,
    "সেপ্টেম্বর": 9,
    "অক্টোবর": 10,
    "নভেম্বর": 11,
    "ডিসেম্বর": 12,
}

_BN_DATE_RE = re.compile(r"([০-৯]{1,2})\s+([^\s,]+)\s*,\s*([০-৯]{4})")


def parse_bangla_date(text: str) -> Optional[datetime.date]:
    if not text:
        return None

    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)

    m = _BN_DATE_RE.search(text)
    if not m:
        parts = text.replace(",", "").split()
        if len(parts) != 3:
            return None
        bd_day, bd_month, bd_year = parts
    else:
        bd_day, bd_month, bd_year = m.group(1), m.group(2), m.group(3)

    def convert_digits(s: str) -> int:
        return int("".join(BN_DIGITS.get(ch, ch) for ch in str(s)))

    try:
        day = convert_digits(bd_day)
        year = convert_digits(bd_year)
        month = BN_MONTHS.get(str(bd_month).strip())
        if month is None:
            return None
        return datetime.date(year, month, day)
    except Exception:
        return None


def dissent_api_get(page: int, per_page: int = 50) -> dict:
    params = {
        "include": "categories",
        "page": page,
        "per_page": per_page,
        "published": "true",
    }
    r = SESSION.get(
        DISSENT_API_BASE,
        params=params,
        headers={"Accept": "application/vnd.api+json", "User-Agent": SESSION.headers.get("User-Agent", "")},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def dissent_fetch_article_html(url: str) -> str:
    r = SESSION.get(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": SESSION.headers.get("User-Agent", ""),
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.text


def dissent_extract_bangla_title(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    if h1:
        txt = h1.get_text(strip=True)
        if txt:
            return txt

    for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
        txt = tag.get_text(strip=True)
        if txt and any("\u0980" <= ch <= "\u09FF" for ch in txt):
            return txt
    return None


def dissent_extract_date(soup: BeautifulSoup) -> Optional[datetime.date]:
    def find_date_in_text(text: str) -> Optional[datetime.date]:
        if not text:
            return None
        m = _BN_DATE_RE.search(text)
        if not m:
            return None
        raw = f"{m.group(1)} {m.group(2)}, {m.group(3)}"
        return parse_bangla_date(raw)

    h1 = soup.find("h1")
    if h1:
        container = h1.parent or soup
        header_text = container.get_text(" ", strip=True)
        d = find_date_in_text(header_text)
        if d:
            return d

        for elem in list(h1.next_elements)[:250]:
            if isinstance(elem, Tag):
                d = find_date_in_text(elem.get_text(" ", strip=True))
                if d:
                    return d

    return find_date_in_text(soup.get_text(" ", strip=True))


def dissent_fetch_for_date(target_date: datetime.date) -> List[Tuple[str, str, str]]:
    results: List[Tuple[str, str, str]] = []
    page = 1
    MAX_PAGES = 50

    while page <= MAX_PAGES:
        try:
            data = dissent_api_get(page)
        except Exception:
            break

        items = data.get("data", [])
        if not items:
            break

        for item in items:
            attrs = item.get("attributes", {})
            slug = attrs.get("slug")
            if not slug:
                continue

            url = f"{DISSENT_ARTICLE_BASE}/{slug}"

            try:
                html = dissent_fetch_article_html(url)
            except Exception:
                continue

            soup = BeautifulSoup(html, "html.parser")
            art_date = dissent_extract_date(soup)
            if not art_date:
                continue
            if art_date != target_date:
                continue

            title = dissent_extract_bangla_title(soup) or attrs.get("title") or "(no title)"
            results.append((title, url, art_date.isoformat()))

        page += 1

    return results


# =========================
# Core scraping (no UI)
# =========================
def run_scrape_core(
    start_date: datetime.date,
    end_date: datetime.date,
    sheet_ref: str,
    worksheet_name: str,
    creds_path: str,
    include_wp: List[bool],
    include_fc: bool,
    include_boom: bool,
    include_ajk: bool,
    include_dissent: bool,
    log_fn,
):
    if end_date < start_date:
        log_fn("ERROR: End date must be on or after start date.")
        return

    # Creds can come from file OR Streamlit secrets
    has_file_creds = bool(creds_path and os.path.isfile(creds_path))
    has_secret_creds = _has_streamlit_secrets_creds()
    if not has_file_creds and not has_secret_creds:
        log_fn("ERROR: No credentials found. Upload JSON or set Streamlit secrets [google_service_account].")
        return

    try:
        ws = open_sheet(creds_path if has_file_creds else "", sheet_ref, worksheet_name)
        log_fn("Connected to Google Sheet.")
    except Exception as e:
        log_fn(f"ERROR opening sheet: {e}")
        return

    all_rows: List[Tuple[str, str, str, str]] = []
    curr = start_date
    while curr <= end_date:
        log_fn(f"\n=== {curr.isoformat()} ===")

        for idx, (name, api, slug) in enumerate(SOURCES_WP):
            if not include_wp[idx]:
                continue
            try:
                if name == "Newschecker":
                    cid = None
                else:
                    cid = get_category_id(api, slug)

                posts = fetch_wp_posts(api, cid, curr)
                log_fn(f"  {name}: {len(posts)}")
                all_rows += [(t, u, d, name) for (t, u, d) in posts]
            except requests.HTTPError as e:
                resp = e.response
                if name == "Newschecker" and resp is not None and resp.status_code in (403, 503):
                    log_fn("  Newschecker: blocked by Cloudflare / forbidden. Skipping this site.")
                    continue
                else:
                    log_fn(f"  {name}: HTTP ERROR {e}")
            except Exception as e:
                log_fn(f"  {name}: ERROR {e}")

        if include_fc:
            try:
                posts = fetch_fc_posts(FC_API_BASE, curr)
                log_fn(f"  {FC_SITE_NAME}: {len(posts)}")
                all_rows += [(t, u, d, FC_SITE_NAME) for (t, u, d) in posts]
            except Exception as e:
                log_fn(f"  {FC_SITE_NAME}: ERROR {e}")

        if include_boom:
            try:
                posts = scrape_boombd(curr)
                log_fn(f"  {BOOM_SITE_NAME}: {len(posts)}")
                all_rows += [(t, u, d, BOOM_SITE_NAME) for (t, u, d) in posts]
            except Exception as e:
                log_fn(f"  {BOOM_SITE_NAME}: ERROR {e}")

        if include_ajk:
            try:
                posts = scrape_ajker(curr)
                log_fn(f"  {AJK_SITE_NAME}: {len(posts)}")
                all_rows += [(t, u, d, AJK_SITE_NAME) for (t, u, d) in posts]
            except Exception as e:
                log_fn(f"  {AJK_SITE_NAME}: ERROR {e}")

        if include_dissent:
            try:
                posts = dissent_fetch_for_date(curr)
                log_fn(f"  {DISSENT_SITE_NAME}: {len(posts)}")
                all_rows += [(t, u, d, DISSENT_SITE_NAME) for (t, u, d) in posts]
            except Exception as e:
                log_fn(f"  {DISSENT_SITE_NAME}: ERROR {e}")

        curr += datetime.timedelta(days=1)

    if all_rows:
        all_rows = dedupe(all_rows)
        try:
            append_rows_batched(ws, [[t, u, d, s] for (t, u, d, s) in all_rows])
            log_fn(f"\n✅ Appended {len(all_rows)} rows.")
        except Exception as e:
            log_fn(f"\nERROR appending rows: {e}")
    else:
        log_fn("\n⚠️ No posts found.")


# =========================
# Streamlit UI
# =========================
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.markdown(
        """
This app scrapes fact-checking articles from several Bangladeshi sites
for a given date range and saves the results into a Google Sheet.
"""
    )

    settings = load_settings()

    if "logs" not in st.session_state:
        st.session_state.logs = []

    st.sidebar.header("Configuration")

    today = datetime.date.today()
    default_start = settings.get("start_date")
    default_end = settings.get("end_date")

    if default_start:
        try:
            default_start = datetime.date.fromisoformat(default_start)
        except Exception:
            default_start = today
    else:
        default_start = today

    if default_end:
        try:
            default_end = datetime.date.fromisoformat(default_end)
        except Exception:
            default_end = today
    else:
        default_end = today

    start_date = st.sidebar.date_input("Start date", value=default_start)
    end_date = st.sidebar.date_input("End date", value=default_end)

    sheet_ref = st.sidebar.text_input(
        "Sheet (Name / ID / URL)",
        value=settings.get("sheet_ref", DEFAULT_SHEET),
    )

    worksheet_name = st.sidebar.text_input(
        "Worksheet / Tab name",
        value=settings.get("worksheet_name", DEFAULT_WORKSHEET),
    )

    st.sidebar.markdown("### Google Service Account JSON")
    st.sidebar.caption("You can either upload JSON (local) or use Streamlit secrets [google_service_account] (cloud).")
    uploaded_creds = st.sidebar.file_uploader("Upload credentials JSON", type=["json"])

    if uploaded_creds is not None:
        try:
            content = uploaded_creds.read()
            with open(CREDS_STORE_PATH, "wb") as f:
                f.write(content)
            st.sidebar.success("Credentials JSON saved.")
            settings["creds_path"] = CREDS_STORE_PATH
        except Exception as e:
            st.sidebar.error(f"Failed to save credentials: {e}")

    creds_path = settings.get("creds_path", "")
    if not creds_path:
        env_creds = os.getenv("GOOGLE_CREDS_JSON")
        if env_creds:
            creds_path = env_creds

    st.sidebar.markdown("### Sources")
    wp_defaults = settings.get("wp_sources", [True] * len(SOURCES_WP))
    wp_states = []
    for i, (name, _, _) in enumerate(SOURCES_WP):
        default_state = wp_defaults[i] if i < len(wp_defaults) else True
        wp_states.append(st.sidebar.checkbox(name, value=default_state))

    enable_fc = st.sidebar.checkbox("Factcrescendo", value=settings.get("enable_fc", True))
    enable_boom = st.sidebar.checkbox("Boombd", value=settings.get("enable_boom", True))
    enable_ajk = st.sidebar.checkbox("Ajker Patrika", value=settings.get("enable_ajk", True))
    enable_dissent = st.sidebar.checkbox(DISSENT_SITE_NAME, value=settings.get("enable_dissent", True))

    def collect_settings_dict():
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "sheet_ref": sheet_ref.strip(),
            "worksheet_name": worksheet_name.strip(),
            "creds_path": CREDS_STORE_PATH if os.path.isfile(CREDS_STORE_PATH) else creds_path,
            "wp_sources": wp_states,
            "enable_fc": enable_fc,
            "enable_boom": enable_boom,
            "enable_ajk": enable_ajk,
            "enable_dissent": enable_dissent,
        }

    st.markdown("### Run Scraper")
    run_button = st.button("Run scraping")

    log_container = st.empty()
    if st.session_state.logs:
        log_container.code("\n".join(st.session_state.logs))

    if run_button:
        if not sheet_ref.strip():
            st.error("Please provide a Sheet (name / ID / URL).")
            return

        if not worksheet_name.strip():
            st.error("Please provide a Worksheet / Tab name.")
            return

        effective_creds_path = CREDS_STORE_PATH if os.path.isfile(CREDS_STORE_PATH) else creds_path

        has_file_creds = bool(effective_creds_path and os.path.isfile(effective_creds_path))
        has_secret_creds = _has_streamlit_secrets_creds()
        if not has_file_creds and not has_secret_creds:
            st.error("No credentials found. Upload JSON or add [google_service_account] to Streamlit secrets.")
            return

        save_settings(collect_settings_dict())

        st.session_state.logs = []
        log_container.code("")

        def log_fn(msg: str):
            st.session_state.logs.append(msg)
            log_container.code("\n".join(st.session_state.logs))

        with st.spinner("Scraping in progress..."):
            run_scrape_core(
                start_date=start_date,
                end_date=end_date,
                sheet_ref=sheet_ref.strip(),
                worksheet_name=worksheet_name.strip(),
                creds_path=effective_creds_path if has_file_creds else "",
                include_wp=wp_states,
                include_fc=enable_fc,
                include_boom=enable_boom,
                include_ajk=enable_ajk,
                include_dissent=enable_dissent,
                log_fn=log_fn,
            )

        log_container.code("\n".join(st.session_state.logs))
        st.success("Finished. Check the log above for details.")


if __name__ == "__main__":
    main()
