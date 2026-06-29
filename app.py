#!/usr/bin/env python3
# FC_Article_Scraper.py — Streamlit GUI scraper with settings persistence

import os
import re
import json
import time
import datetime
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as dateparser
from bs4 import BeautifulSoup, NavigableString, Tag

import gspread
from gspread.exceptions import SpreadsheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

import streamlit as st

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
APP_TITLE = "FC Article Scrapper"
DEFAULT_SHEET = "Rumor Scanner Scraped Data"
DEFAULT_WORKSHEET = "collected url"
MAX_DATE_RANGE_DAYS = 31   # warn user if range is larger than this

TZINFOS = {
    "IST": datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# ─────────────────────────────────────────
# Unified Bangla date parser
# ─────────────────────────────────────────
_BN_DIGITS: Dict[str, str] = {
    "০": "0", "১": "1", "২": "2", "৩": "3", "৪": "4",
    "৫": "5", "৬": "6", "৭": "7", "৮": "8", "৯": "9",
}

_BN_MONTHS: Dict[str, int] = {
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

# Matches: "২৯ জুন, ২০২৬" or "২৯ জুন ২০২৬"
_BN_DATE_RE = re.compile(
    r"([০-৯]{1,2})\s+([^\s,،]+)\s*,?\s*([০-৯]{4})", re.UNICODE
)
# Matches Ajker Patrika's "প্রকাশ : ২৯ জুন ২০২৬" pattern
_AJK_PUBLISH_RE = re.compile(
    r"প্রকাশ\s*:\s*([০-৯]{1,2})\s+([^\s,]+)\s+([০-৯]{4})", re.UNICODE
)


def _bn_to_int(s: str) -> int:
    return int("".join(_BN_DIGITS.get(ch, ch) for ch in str(s)))


def parse_bangla_date(text: str) -> Optional[datetime.date]:
    """Unified Bangla date parser used by all scrapers.

    Handles:
      - ISO datetime strings: "2026-06-29" or "2026-06-29T10:30:00"
      - Bangla script dates: "২৯ জুন, ২০২৬" or "২৯ জুন ২০২৬"
      - Ajker Patrika publish pattern: "প্রকাশ : ২৯ জুন ২০২৬"
    """
    if not text:
        return None
    text = re.sub(r"\s+", " ", str(text).strip())

    # ISO format (from <time datetime="…"> attributes)
    if re.match(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return datetime.date.fromisoformat(text[:10])
        except ValueError:
            pass

    # Ajker Patrika publish pattern
    m = _AJK_PUBLISH_RE.search(text)
    if m:
        month = _BN_MONTHS.get(m.group(2).strip())
        if month:
            try:
                return datetime.date(_bn_to_int(m.group(3)), month, _bn_to_int(m.group(1)))
            except Exception:
                pass

    # Standard Bangla date
    m = _BN_DATE_RE.search(text)
    if m:
        month = _BN_MONTHS.get(m.group(2).strip())
        if month:
            try:
                return datetime.date(_bn_to_int(m.group(3)), month, _bn_to_int(m.group(1)))
            except Exception:
                pass

    return None


# ─────────────────────────────────────────
# Source config as data
# ─────────────────────────────────────────
@dataclass
class WPSource:
    name: str
    api_base: str
    category_slug: Optional[str] = None   # None = fetch all posts (no category filter)
    use_html_scraper: bool = False         # fall back to HTML when REST API is blocked
    html_base_url: str = ""


@dataclass
class HTMLSource:
    name: str
    base_url: str
    # CSS selectors / scraping strategy are handled per-source inside scrape_html_source()
    scraper_id: str   # "boombd" | "ajker" | "rumorscanner"


@dataclass
class DissentSource:
    name: str = "The Dissent"
    api_base: str = "https://thedissent.news/api/fact_checks"
    article_base: str = "https://thedissent.news/bn/fact-checks"


WP_SOURCES: List[WPSource] = [
    WPSource("Rumorscanner",  "https://rumorscanner.com/wp-json/wp/v2",          "fact-check",
             use_html_scraper=True, html_base_url="https://rumorscanner.com/category/fact-check"),
    WPSource("Fact-watch",    "https://www.fact-watch.org/wp-json/wp/v2",        "ফ্যাক্টচেক"),
    WPSource("Dismislab",     "https://dismislab.com/wp-json/wp/v2",             "factcheck"),
    WPSource("Newschecker",   "https://bangladesh.newschecker.co/wp-json/wp/v2", None),
    WPSource("Factcrescendo", "https://bangladesh.factcrescendo.com/wp-json/wp/v2"),
]

HTML_SOURCES: List[HTMLSource] = [
    HTMLSource("Boombd",        "https://www.boombd.com/fake-news",        "boombd"),
    HTMLSource("Ajker Patrika", "https://www.ajkerpatrika.com/fact-check", "ajker"),
]

DISSENT_SOURCE = DissentSource()


# ─────────────────────────────────────────
# Settings persistence
# ─────────────────────────────────────────
def _settings_dir() -> str:
    appdata = os.getenv("APPDATA")
    base = os.path.join(appdata, "Rumorscanner") if appdata \
        else os.path.join(os.path.expanduser("~"), ".rumor_scanner")
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
        print(f"[warn] could not save settings: {e}")


# ─────────────────────────────────────────
# Networking
# ─────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
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


# ─────────────────────────────────────────
# Google Sheets helpers
# ─────────────────────────────────────────
def _has_streamlit_secrets_creds() -> bool:
    try:
        return bool(st.secrets.get("google_service_account"))
    except Exception:
        return False


def open_sheet(creds_json_path: str, sheet_ref: str, worksheet_name: str):
    if creds_json_path and os.path.isfile(creds_json_path):
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, SCOPES)
    else:
        if not _has_streamlit_secrets_creds():
            raise RuntimeError(
                "No credentials found. Upload a service account JSON or "
                "add [google_service_account] to Streamlit secrets."
            )
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            st.secrets["google_service_account"], SCOPES
        )

    gc = gspread.authorize(creds)

    try:
        if sheet_ref.startswith("http"):
            sh = gc.open_by_url(sheet_ref)
        elif len(sheet_ref) > 30:
            sh = gc.open_by_key(sheet_ref)
        else:
            sh = gc.open(sheet_ref)
    except SpreadsheetNotFound:
        raise RuntimeError(f"Could not find spreadsheet: {sheet_ref}")

    worksheet_name = (worksheet_name or "").strip() or "Sheet1"
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=10)

    if not ws.get("A1:D1"):
        ws.append_row(["Title", "URL", "Date", "Site"])

    return ws


def get_existing_urls(ws) -> set:
    """Fetch all URLs already in column B to avoid duplicates across runs."""
    try:
        values = ws.col_values(2)  # column B = URL
        return set(v.strip() for v in values if v.strip() and v.strip() != "URL")
    except Exception:
        return set()


def append_rows_batched(ws, rows: List[List[str]], batch_size: int = 200, max_retries: int = 6):
    for i in range(0, len(rows), batch_size):
        chunk = rows[i: i + batch_size]
        for attempt in range(1, max_retries + 1):
            try:
                ws.append_rows(chunk, value_input_option="RAW")
                break
            except Exception as e:
                # Sheets quota resets every 100s; use a longer wait on repeated failures
                wait = min(120, 10 * (2 ** (attempt - 1)))
                print(f"[warn] append_rows failed (batch {i // batch_size + 1}, "
                      f"attempt {attempt}/{max_retries}): {e}  – waiting {wait}s")
                time.sleep(wait)
                if attempt == max_retries:
                    raise


# ─────────────────────────────────────────
# Scraper helpers
# ─────────────────────────────────────────
def day_bounds_utc(d: datetime.date) -> Tuple[str, str]:
    start = datetime.datetime(d.year, d.month, d.day, 0, 0, 0,
                              tzinfo=datetime.timezone.utc).isoformat()
    end = datetime.datetime(d.year, d.month, d.day, 23, 59, 59,
                            tzinfo=datetime.timezone.utc).isoformat()
    return start, end


def dedupe(
    rows: List[Tuple[str, str, str, str]],
    existing_urls: Optional[set] = None,
) -> List[Tuple[str, str, str, str]]:
    """Remove duplicates within the current batch and against the sheet."""
    seen = set(existing_urls or [])
    out: List[Tuple[str, str, str, str]] = []
    for t, u, d, s in rows:
        if u in seen:
            continue
        seen.add(u)
        out.append((t, u, d, s))
    return out


# ─────────────────────────────────────────
# WordPress scraper
# ─────────────────────────────────────────
def get_category_id(api_base: str, slug: Optional[str]) -> Optional[int]:
    if not slug:
        return None
    r = http_get(f"{api_base}/categories", params={"slug": slug, "per_page": 1})
    try:
        data = r.json()
    except ValueError:
        return None
    return data[0]["id"] if data else None


def fetch_wp_posts(api_base: str, cat_id: Optional[int],
                   target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    after, before = day_bounds_utc(target_date)
    page = 1
    while True:
        params = {
            "after": after, "before": before,
            "per_page": 100, "page": page,
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


# ─────────────────────────────────────────
# HTML scrapers (unified entry point)
# ─────────────────────────────────────────
def _parse_date_from_card(art: Tag) -> Optional[datetime.date]:
    """Try to extract a date from an article card using multiple strategies."""
    # 1. <time datetime="…">
    time_tag = art.find("time")
    if time_tag:
        d = parse_bangla_date(time_tag.get("datetime", "") or time_tag.get_text(strip=True))
        if d:
            return d
    # 2. Bangla date anywhere in the card text
    return parse_bangla_date(art.get_text(" ", strip=True))


def _fetch_article_date(url: str) -> Optional[datetime.date]:
    """Fetch an article page and try to extract its date (last resort)."""
    try:
        r = http_get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        t = soup.find("time", attrs={"datetime": True}) or soup.find("time")
        if t:
            d = parse_bangla_date(t.get("datetime", "") or t.get_text(strip=True))
            if d:
                return d
        return parse_bangla_date(soup.get_text(" ", strip=True))
    except Exception:
        return None


def scrape_html_source(source: HTMLSource,
                       target_date: datetime.date) -> List[Tuple[str, str, str]]:
    """Generic HTML category-page scraper dispatching to per-site logic."""
    if source.scraper_id == "boombd":
        return _scrape_boombd(source.base_url, target_date)
    if source.scraper_id == "ajker":
        return _scrape_ajker(source.base_url, target_date)
    if source.scraper_id == "rumorscanner":
        return _scrape_rumorscanner(source.base_url, target_date)
    raise ValueError(f"Unknown scraper_id: {source.scraper_id}")


def _scrape_rumorscanner(base_url: str,
                         target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    seen: set = set()
    page = 1
    stop = False

    while not stop:
        url = base_url if page == 1 else f"{base_url}/page/{page}/"
        try:
            r = http_get(url)
        except Exception:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        articles = soup.find_all("article") or soup.find_all(["h2", "h3"])
        if not articles:
            break

        found_any = False
        for art in articles:
            a_tag = art.find("a", href=True)
            if not a_tag:
                continue
            link = a_tag["href"].strip()
            if not link.startswith("http"):
                link = "https://rumorscanner.com" + link
            if any(x in link for x in ("/category/", "/tag/", "/page/")):
                continue
            if link in seen:
                continue
            seen.add(link)

            heading = art.find(["h2", "h3", "h4"])
            title = (heading.get_text(strip=True) if heading else None) \
                or a_tag.get("title") or a_tag.get_text(strip=True)

            art_date = _parse_date_from_card(art) or _fetch_article_date(link)
            if not art_date:
                continue
            if art_date < target_date:
                stop = True
                break
            if art_date == target_date and title and link:
                rows.append((title, link, art_date.isoformat()))
                found_any = True

        if stop or (not found_any and page > 3):
            break
        page += 1
        time.sleep(0.3)

    return rows


def _scrape_boombd(base_url: str,
                   target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    seen: set = set()
    page = 1
    stop = False

    while not stop:
        r = http_get(base_url + (f"/page/{page}/" if page > 1 else ""))
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

            date_str = None
            sib = h4.next_sibling
            while sib:
                text = sib.strip() if isinstance(sib, NavigableString) \
                    else sib.get_text(strip=True)
                if "|" in text:
                    date_str = text.split("|", 1)[1].strip()
                    break
                sib = sib.next_sibling

            art_date: Optional[datetime.date] = None
            if date_str:
                try:
                    art_date = dateparser.parse(date_str, tzinfos=TZINFOS).date()
                except Exception:
                    pass
            if not art_date:
                art_date = _fetch_article_date(link)
            if not art_date:
                continue

            if art_date < target_date:
                stop = True
                break
            if art_date == target_date:
                rows.append((title, link, art_date.isoformat()))
                page_has_any = True

        if stop or (not page_has_any and page > 3):
            break
        page += 1
        time.sleep(0.2)

    return rows


def _scrape_ajker(base_url: str,
                  target_date: datetime.date) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    seen: set = set()
    page = 1
    stop = False

    while not stop:
        r = http_get(base_url + (f"?page={page}" if page > 1 else ""))
        soup = BeautifulSoup(r.text, "html.parser")
        anchors = [a for a in soup.find_all("a", href=True)
                   if "/fact-check/" in a["href"]]
        if not anchors:
            break

        found_any = False
        for a in anchors:
            url = a["href"].strip()
            if not url.startswith("http"):
                url = "https://www.ajkerpatrika.com" + url
            if url in seen:
                continue
            seen.add(url)

            try:
                pr = http_get(url)
            except Exception:
                continue

            ps = BeautifulSoup(pr.text, "html.parser")
            art_date = parse_bangla_date(ps.get_text(" ", strip=True))
            if not art_date:
                continue
            if art_date < target_date:
                stop = True
                break
            if art_date == target_date:
                h = ps.find(["h1", "h2"])
                title = h.get_text(strip=True) if h else a.get_text(strip=True) or "(no title)"
                rows.append((title, url, art_date.isoformat()))
                found_any = True

        if stop or (not found_any and page > 3):
            break
        page += 1
        time.sleep(0.2)

    return rows


# ─────────────────────────────────────────
# The Dissent scraper
# ─────────────────────────────────────────
def _dissent_extract_title(soup: BeautifulSoup) -> Optional[str]:
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


def _dissent_extract_date(soup: BeautifulSoup) -> Optional[datetime.date]:
    h1 = soup.find("h1")
    if h1:
        container = h1.parent or soup
        d = parse_bangla_date(container.get_text(" ", strip=True))
        if d:
            return d
        for elem in list(h1.next_elements)[:250]:
            if isinstance(elem, Tag):
                d = parse_bangla_date(elem.get_text(" ", strip=True))
                if d:
                    return d
    return parse_bangla_date(soup.get_text(" ", strip=True))


def _dissent_date_from_attrs(attrs: dict) -> Optional[datetime.date]:
    """Extract date directly from API attributes without fetching the article page."""
    for field in ("published-at", "publishedAt", "created-at", "createdAt",
                  "updated-at", "updatedAt", "date"):
        val = attrs.get(field)
        if val:
            d = parse_bangla_date(str(val))
            if d:
                return d
            try:
                return datetime.date.fromisoformat(str(val)[:10])
            except Exception:
                pass
    return None


def _dissent_fetch_article(url: str) -> Tuple[Optional[datetime.date], Optional[str]]:
    """Fetch one Dissent article page and return (date, title)."""
    try:
        html = SESSION.get(
            url,
            headers={"Accept": "text/html,*/*",
                     "User-Agent": SESSION.headers.get("User-Agent", "")},
            timeout=15,
        ).text
        soup = BeautifulSoup(html, "html.parser")
        return _dissent_extract_date(soup), _dissent_extract_title(soup)
    except Exception:
        return None, None


def scrape_dissent(source: DissentSource,
                   target_date: datetime.date) -> List[Tuple[str, str, str]]:
    """
    Scrape The Dissent for articles on target_date.

    Strategy:
      1. Page through the API (newest-first).
      2. Try to get the date from API attributes directly (no article fetch needed).
      3. Items with no API date are queued for parallel article page fetches.
      4. Stop paging as soon as an article older than target_date is seen.
    """
    results: List[Tuple[str, str, str]] = []
    needs_fetch: List[Tuple[str, str]] = []
    stop = False
    page = 1

    while page <= 50 and not stop:
        try:
            r = SESSION.get(
                source.api_base,
                params={"include": "categories", "page": page,
                        "per_page": 50, "published": "true"},
                headers={"Accept": "application/vnd.api+json",
                         "User-Agent": SESSION.headers.get("User-Agent", "")},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
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

            url = f"{source.article_base}/{slug}"
            api_title = attrs.get("title") or "(no title)"

            # Fast path: date from API attributes (no HTTP request needed)
            art_date = _dissent_date_from_attrs(attrs)

            if art_date is not None:
                if art_date < target_date:
                    stop = True  # API is newest-first; nothing older will match
                    break
                if art_date == target_date:
                    results.append((api_title, url, art_date.isoformat()))
            else:
                # Date unknown from API — queue for parallel article fetch
                needs_fetch.append((url, api_title))

        page += 1

    # Parallel fetch for items whose date wasn't in the API response
    if needs_fetch:
        with ThreadPoolExecutor(max_workers=8) as ex:
            future_map = {ex.submit(_dissent_fetch_article, url): (url, api_title)
                          for url, api_title in needs_fetch}
            for future in as_completed(future_map):
                url, api_title = future_map[future]
                art_date, page_title = future.result()
                if art_date and art_date == target_date:
                    results.append((page_title or api_title, url, art_date.isoformat()))

    return results



# ─────────────────────────────────────────
# Parallel scraping for a single date
# ─────────────────────────────────────────
def scrape_one_date(
    target_date: datetime.date,
    wp_enabled: List[bool],
    html_enabled: List[bool],
    dissent_enabled: bool,
    log_fn: Callable[[str], None],
) -> List[Tuple[str, str, str, str]]:
    """Run all enabled sources in parallel for target_date and return rows."""

    tasks: Dict[str, Callable] = {}

    # WP sources
    for enabled, src in zip(wp_enabled, WP_SOURCES):
        if not enabled:
            continue
        if src.use_html_scraper:
            html_src = HTMLSource(src.name, src.html_base_url, "rumorscanner")
            tasks[src.name] = lambda s=html_src, d=target_date: scrape_html_source(s, d)
        else:
            def _wp_task(src=src, d=target_date):
                cid = get_category_id(src.api_base, src.category_slug)
                return fetch_wp_posts(src.api_base, cid, d)
            tasks[src.name] = _wp_task

    # HTML sources
    for enabled, src in zip(html_enabled, HTML_SOURCES):
        if not enabled:
            continue
        tasks[src.name] = lambda s=src, d=target_date: scrape_html_source(s, d)

    # The Dissent
    if dissent_enabled:
        tasks[DISSENT_SOURCE.name] = lambda d=target_date: scrape_dissent(DISSENT_SOURCE, d)

    rows: List[Tuple[str, str, str, str]] = []
    failed: List[str] = []

    with ThreadPoolExecutor(max_workers=min(8, len(tasks))) as executor:
        future_to_name = {executor.submit(fn): name for name, fn in tasks.items()}
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                posts = future.result()
                log_fn(f"  {name}: {len(posts)}")
                rows += [(t, u, d, name) for (t, u, d) in posts]
            except requests.HTTPError as e:
                resp = e.response
                if name == "Newschecker" and resp is not None \
                        and resp.status_code in (403, 503):
                    log_fn(f"  {name}: blocked by Cloudflare – skipped")
                else:
                    log_fn(f"  {name}: HTTP ERROR {e}")
                failed.append(name)
            except Exception as e:
                log_fn(f"  {name}: ERROR {e}")
                failed.append(name)

    return rows, failed


# ─────────────────────────────────────────
# Core scraping (no UI)
# ─────────────────────────────────────────
def run_scrape_core(
    start_date: datetime.date,
    end_date: datetime.date,
    sheet_ref: str,
    worksheet_name: str,
    creds_path: str,
    wp_enabled: List[bool],
    html_enabled: List[bool],
    dissent_enabled: bool,
    log_fn: Callable[[str], None],
    progress_fn: Optional[Callable[[float], None]] = None,
):
    if end_date < start_date:
        log_fn("ERROR: End date must be on or after start date.")
        return

    has_file_creds = bool(creds_path and os.path.isfile(creds_path))
    if not has_file_creds and not _has_streamlit_secrets_creds():
        log_fn("ERROR: No credentials found.")
        return

    try:
        ws = open_sheet(creds_path if has_file_creds else "", sheet_ref, worksheet_name)
        log_fn("Connected to Google Sheet.")
    except Exception as e:
        log_fn(f"ERROR opening sheet: {e}")
        return

    # Fetch existing URLs once to deduplicate against the sheet
    log_fn("Fetching existing URLs from sheet for deduplication…")
    existing_urls = get_existing_urls(ws)
    log_fn(f"  Found {len(existing_urls)} existing URLs.")

    total_days = (end_date - start_date).days + 1
    all_rows: List[Tuple[str, str, str, str]] = []
    all_failed: List[str] = []
    curr = start_date
    day_num = 0

    while curr <= end_date:
        log_fn(f"\n=== {curr.isoformat()} ===")
        rows, failed = scrape_one_date(
            curr, wp_enabled, html_enabled, dissent_enabled, log_fn
        )
        all_rows.extend(rows)
        all_failed.extend(failed)

        day_num += 1
        if progress_fn:
            progress_fn(day_num / total_days)

        curr += datetime.timedelta(days=1)

    if all_rows:
        all_rows = dedupe(all_rows, existing_urls)
        if all_rows:
            try:
                append_rows_batched(ws, [[t, u, d, s] for (t, u, d, s) in all_rows])
                log_fn(f"\n✅ Appended {len(all_rows)} new rows.")
            except Exception as e:
                log_fn(f"\nERROR appending rows: {e}")
        else:
            log_fn("\n⚠️ All found posts already exist in the sheet – nothing new to add.")
    else:
        log_fn("\n⚠️ No posts found.")

    if all_failed:
        unique_failed = sorted(set(all_failed))
        log_fn(f"\n⚠️ Sources with errors: {', '.join(unique_failed)}")


# ─────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.markdown(
        "Scrapes fact-checking articles from Bangladeshi sites "
        "for a given date range and saves results into a Google Sheet."
    )

    settings = load_settings()

    if "logs" not in st.session_state:
        st.session_state.logs = []

    # ── Sidebar ──────────────────────────
    st.sidebar.header("Configuration")

    today = datetime.date.today()

    def _parse_saved_date(key: str) -> datetime.date:
        try:
            return datetime.date.fromisoformat(settings.get(key, ""))
        except Exception:
            return today

    start_date = st.sidebar.date_input("Start date", value=_parse_saved_date("start_date"))
    end_date   = st.sidebar.date_input("End date",   value=_parse_saved_date("end_date"))

    # Date range warning
    if end_date >= start_date:
        days = (end_date - start_date).days + 1
        if days > MAX_DATE_RANGE_DAYS:
            st.sidebar.warning(
                f"⚠️ {days}-day range selected. This will make many HTTP requests "
                f"and may take several minutes."
            )

    sheet_ref = st.sidebar.text_input(
        "Sheet (Name / ID / URL)",
        value=settings.get("sheet_ref", DEFAULT_SHEET),
    )
    worksheet_name = st.sidebar.text_input(
        "Worksheet / Tab name",
        value=settings.get("worksheet_name", DEFAULT_WORKSHEET),
    )

    st.sidebar.markdown("### Google Service Account JSON")
    st.sidebar.caption("Upload JSON (local) or use Streamlit secrets [google_service_account] (cloud).")
    uploaded_creds = st.sidebar.file_uploader("Upload credentials JSON", type=["json"])

    if uploaded_creds is not None:
        try:
            with open(CREDS_STORE_PATH, "wb") as f:
                f.write(uploaded_creds.read())
            st.sidebar.success("Credentials JSON saved.")
            settings["creds_path"] = CREDS_STORE_PATH
        except Exception as e:
            st.sidebar.error(f"Failed to save credentials: {e}")

    creds_path = settings.get("creds_path", "") or os.getenv("GOOGLE_CREDS_JSON", "")

    # ── Sources ──────────────────────────
    st.sidebar.markdown("### Sources")

    wp_defaults = settings.get("wp_enabled", [True] * len(WP_SOURCES))
    wp_enabled = [
        st.sidebar.checkbox(src.name, value=wp_defaults[i] if i < len(wp_defaults) else True)
        for i, src in enumerate(WP_SOURCES)
    ]

    html_defaults = settings.get("html_enabled", [True] * len(HTML_SOURCES))
    html_enabled = [
        st.sidebar.checkbox(src.name, value=html_defaults[i] if i < len(html_defaults) else True)
        for i, src in enumerate(HTML_SOURCES)
    ]

    dissent_enabled = st.sidebar.checkbox(
        DISSENT_SOURCE.name, value=settings.get("dissent_enabled", True)
    )

    # ── Run ──────────────────────────────
    st.markdown("### Run Scraper")
    run_button = st.button("▶ Run scraping")

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
        if end_date < start_date:
            st.error("End date must be on or after start date.")
            return

        effective_creds = CREDS_STORE_PATH if os.path.isfile(CREDS_STORE_PATH) else creds_path
        if not (os.path.isfile(effective_creds) or _has_streamlit_secrets_creds()):
            st.error("No credentials found. Upload JSON or add [google_service_account] to Streamlit secrets.")
            return

        save_settings({
            "start_date":     start_date.isoformat(),
            "end_date":       end_date.isoformat(),
            "sheet_ref":      sheet_ref.strip(),
            "worksheet_name": worksheet_name.strip(),
            "creds_path":     CREDS_STORE_PATH if os.path.isfile(CREDS_STORE_PATH) else creds_path,
            "wp_enabled":     wp_enabled,
            "html_enabled":   html_enabled,
            "dissent_enabled": dissent_enabled,
        })

        st.session_state.logs = []
        log_container.code("")

        progress_bar = st.progress(0.0, text="Starting…")

        def log_fn(msg: str):
            st.session_state.logs.append(msg)
            log_container.code("\n".join(st.session_state.logs))

        def progress_fn(pct: float):
            progress_bar.progress(min(pct, 1.0), text=f"{int(pct * 100)}% complete")

        with st.spinner("Scraping in progress…"):
            run_scrape_core(
                start_date=start_date,
                end_date=end_date,
                sheet_ref=sheet_ref.strip(),
                worksheet_name=worksheet_name.strip(),
                creds_path=effective_creds if os.path.isfile(effective_creds) else "",
                wp_enabled=wp_enabled,
                html_enabled=html_enabled,
                dissent_enabled=dissent_enabled,
                log_fn=log_fn,
                progress_fn=progress_fn,
            )

        progress_bar.progress(1.0, text="Done!")
        log_container.code("\n".join(st.session_state.logs))
        st.success("Finished. Check the log above for details.")


if __name__ == "__main__":
    main()
