#!/usr/bin/env python3
"""
sebi_multi_section_scraper.py

- Extends your original script to iterate multiple SEBI sections (acts, rules, regs, circulars, ...).
- Produces:
    - sebi_master.csv   (single master for all categories; adds `category` and `error` columns)
    - new_entries.json  (array of newly discovered rows this run; includes `category` and `error`)
- Behavior highlights (matches your preferences):
    - Single master CSV for all categories (dedupe requires both title AND link to match)
    - Up to NUM_ENTRIES per section (use what's available if fewer)
    - Filenames: "<category>_<safe_title>.pdf" (no date)
    - Writes GITHUB_SHA (if present) into source_commit
    - If detail page fails, the row is included with pdf_link="" and error filled (no hard abort)
    - Prints a short run summary (counts per category) to stdout
"""

from playwright.sync_api import sync_playwright
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from pathlib import Path
import csv, hashlib, re, datetime, os, sys, json, time

# ----------------- EDITABLE CONFIG -----------------
NUM_ENTRIES = 10
MASTER_CSV = "sebi_master.csv"
NEW_JSON = "new_entries.json"
CSV_DELIM = "|"

# Delay and retry settings (politeness + reliability)
DETAIL_PAGE_DELAY = 0.8      # seconds between detail page fetches
DETAIL_PAGE_RETRIES = 2      # number of retries on failures (in addition to first attempt)
RETRY_BACKOFF_BASE = 1.0     # seconds (exponential backoff multiplier)

# Section mapping: URL -> canonical category name
SECTIONS = {
    # Acts
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=1&smid=0": "act",
    # Rules
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0": "rule",
    # Regulations
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=3&smid=0": "regulation",
    # General Orders
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=4&smid=0": "general_order",
    # Guidelines
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=5&smid=0": "guideline",
    # Master Circular
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=6&smid=0": "master_circular",
    # Advisory/Guidance
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=96&smid=0": "advisory",
    # Circulars
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0": "circular",
    # Gazette
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=82&smid=0": "gazette",
    # Guidance Notes
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=85&smid=0": "guidance_note",
}
# ---------------------------------------------------

# Helpers
def normalize_date(date_str):
    """
    If date_str is only a year (e.g., '2021'), convert to ISO '2021-01-01'.
    Otherwise return as-is.
    """
    if not date_str:
        return date_str

    s = date_str.strip()

    # match exactly a 4-digit year
    if re.fullmatch(r"\d{4}", s):
        return f"{s}-01-01"

    return date_str


def safe_filename(s: str, fallback: str = "document"):
    if not s:
        s = fallback
    s = s.strip().replace("\r", " ").replace("\n", " ")
    s = re.sub(r'[\/\\\:\*\?"<>\|]+', '_', s)
    s = re.sub(r'\s+', ' ', s)
    max_base = 150
    base = s[:max_base].strip()
    if not base.lower().endswith('.pdf'):
        base = base + '.pdf'
    # remove problematic leading/trailing spaces/underscores
    base = base.strip().strip('_')
    return base

def make_id(date, title, link):
    base = f"{date}|{title}|{link}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def normalize_text(s):
    return (s or "").strip()

def normalize_link(link):
    if not link:
        return ""
    # strip fragment, trailing slash for consistent comparison
    parsed = urlparse(link)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path.rstrip('/')
    query = parsed.query
    base = f"{scheme}://{netloc}{path}"
    if query:
        base = base + "?" + query
    return base

# ---------------- Extraction functions (adapted from your script) ----------------
def find_pdf_url_on_page(page):
    """
    Try multiple heuristics to find a direct PDF URL on a detail page.
    Returns absolute URL or None.
    """
    # common selectors that may point to PDF
    selectors = [
        "iframe[src$='.pdf']",
        "iframe[src*='.pdf']",
        "embed[src$='.pdf']",
        "embed[src*='.pdf']",
        "object[data$='.pdf']",
        "object[data*='.pdf']",
        "a[href$='.pdf']",
        "a[href*='.pdf']"
    ]
    for sel in selectors:
        el = page.query_selector(sel)
        if el:
            for attr in ("src","href","data"):
                try:
                    v = el.get_attribute(attr)
                except Exception:
                    v = None
                if v and ".pdf" in v.lower():
                    return _clean_pdf_candidate(page.url, v)

    # generic attribute scan
    all_elements = page.query_selector_all("*")
    for el in all_elements:
        for attr in ("src","href","data","data-src"):
            try:
                v = el.get_attribute(attr)
            except Exception:
                v = None
            if v and ".pdf" in v.lower():
                return _clean_pdf_candidate(page.url, v)

    # look for iframe with file= param (SEBI often uses /web/?file=... )
    for ifr in page.query_selector_all("iframe"):
        try:
            src = ifr.get_attribute("src") or ""
        except Exception:
            src = ""
        if "file=" in src and ".pdf" in src:
            m = re.search(r"[?&]file=([^&]+)", src)
            if m:
                candidate = unquote(m.group(1))
                if ".pdf" in candidate:
                    return _clean_pdf_candidate(page.url, candidate)

    # last resort: search plain text links (hrefs) for .pdf
    anchors = page.query_selector_all("a")
    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            href = ""
        if ".pdf" in href.lower():
            return _clean_pdf_candidate(page.url, href)

    return None

def _clean_pdf_candidate(page_url, candidate):
    """
    Normalize and return an absolute direct PDF URL from various candidate forms.
    Handles SEBI's '/web/?file=' wrapper and relative links.
    """
    candidate = candidate.strip()
    # if candidate contains /web/?file=... extract that param
    if "file=" in candidate:
        # extract file param robustly
        try:
            parsed = urlparse(candidate)
            qs = parse_qs(parsed.query)
            file_vals = qs.get("file") or []
            if file_vals:
                candidate = unquote(file_vals[0])
            else:
                # fallback: regex
                m = re.search(r"[?&]file=([^&]+)", candidate)
                if m:
                    candidate = unquote(m.group(1))
        except Exception:
            pass

    # If candidate is relative, join with page URL
    if not urlparse(candidate).netloc:
        abs_url = urljoin(page_url, candidate)
    else:
        abs_url = candidate

    # Some SEBI links embed the real PDF as another param; try to handle nested 'file=' again
    if "file=" in abs_url and ".pdf" in abs_url:
        try:
            parsed = urlparse(abs_url)
            qs = parse_qs(parsed.query)
            file_vals = qs.get("file") or []
            if file_vals:
                nested = unquote(file_vals[0])
                if nested:
                    abs_url = urljoin(abs_url, nested)
        except Exception:
            pass

    # Final cleanup: ensure it looks like a pdf url
    if ".pdf" not in abs_url.lower():
        return None

    return abs_url

def extract_entries_from_listing(page, base_url):
    # Try table first
    table = page.query_selector("table")
    rows = []
    if table:
        trs = table.query_selector_all("tbody tr") or table.query_selector_all("tr")
        for r in trs:
            tds = r.query_selector_all("td")
            if not tds:
                continue
            date = normalize_text(tds[0].inner_text()) if len(tds)>=1 else ""
            title = ""
            link = ""
            a = r.query_selector("a")
            if a:
                title = normalize_text(a.inner_text())
                href = a.get_attribute("href") or ""
                link = urljoin(base_url, href)
            else:
                if len(tds) >= 2:
                    title = normalize_text(tds[1].inner_text())
            if title:
                rows.append({"date": date, "title": title, "link": link})
    else:
        # fallback anchors in common listing containers
        anchors = page.query_selector_all("div#content a, div.listing a, ul li a, div.content a, div#main a")
        seen = set()
        for a in anchors:
            title = normalize_text(a.inner_text())
            href = a.get_attribute("href") or ""
            link = urljoin(base_url, href)
            key = (title, link)
            if title and key not in seen:
                # try to heuristically find date nearby
                date = ""
                try:
                    prev = a.evaluate("node => node.previousSibling ? node.previousSibling.textContent : ''")
                    if prev:
                        date = normalize_text(prev)
                    if not date:
                        date = normalize_text(a.evaluate("node => node.parentElement && node.parentElement.previousElementSibling ? node.parentElement.previousElementSibling.textContent : ''"))
                except Exception:
                    date = ""
                rows.append({"date": date, "title": title, "link": link})
                seen.add(key)
    return rows

# ----------------- I/O helpers -----------------
def load_master_csv(path):
    results = []
    if not os.path.exists(path):
        return results
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter=CSV_DELIM)
        for r in rdr:
            results.append(r)
    return results

def atomic_write_text(path: str, text: str, mode="w"):
    tmp = path + ".tmp"
    with open(tmp, mode, encoding="utf-8", newline='') as f:
        f.write(text)
    os.replace(tmp, path)

def write_csv(path: str, rows: list):
    headers = ["id","date","title","link","pdf_link","pdf_filename","pdf_downloaded","created_at","source_commit","category","error"]
    tmp_path = str(Path(path))
    # write to tmp and move
    with open(tmp_path + ".tmp", "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=headers,
            delimiter=CSV_DELIM,
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\'
        )
        writer.writeheader()
        for r in rows:
            safe_row = {k: ("" if r.get(k) is None else str(r.get(k))) for k in headers}
            # ensure pdf_filename sanitized & includes category prefix if present
            cat = safe_row.get("category","").strip()
            base_filename = safe_filename(safe_row.get('pdf_filename', ''), fallback='document.pdf')
            if cat:
                # prefix category only if not already present
                prefix = f"{cat}_"
                if not base_filename.lower().startswith(prefix.lower()):
                    base_filename = prefix + base_filename
            safe_row['pdf_filename'] = base_filename
            writer.writerow(safe_row)
    # replace atomically
    os.replace(tmp_path + ".tmp", tmp_path)

def write_json(path: str, rows: list):
    headers = ["id","date","title","link","pdf_link","pdf_filename","pdf_downloaded","created_at","source_commit","category","error"]
    safe_rows = []
    for r in rows:
        safe_row = {k: ("" if r.get(k) is None else r.get(k)) for k in headers}
        for k in headers:
            if safe_row[k] is None:
                safe_row[k] = ""
            else:
                safe_row[k] = str(safe_row[k])
        # ensure pdf_filename sanitized & includes category prefix
        cat = safe_row.get("category","").strip()
        base_filename = safe_filename(safe_row.get('pdf_filename', ''), fallback='document.pdf')
        if cat:
            prefix = f"{cat}_"
            if not base_filename.lower().startswith(prefix.lower()):
                base_filename = prefix + base_filename
        safe_row['pdf_filename'] = base_filename
        safe_rows.append(safe_row)
    atomic_write_text(path, json.dumps(safe_rows, ensure_ascii=False, indent=2))

# ----------------- Main -----------------
def main():
    github_sha = os.environ.get("GITHUB_SHA") or os.environ.get("GITHUB_COMMIT") or ""
    master_rows = load_master_csv(MASTER_CSV)
    # Build set of existing (title, link) pairs normalized
    existing_pairs = set()
    for r in master_rows:
        t = (r.get("title") or "").strip().lower()
        l = normalize_link(r.get("link") or "")
        if t and l:
            existing_pairs.add((t, l))

    new_master = master_rows.copy()
    new_entries = []

    # Counters per category
    summary = {}
    for cat in set(SECTIONS.values()):
        summary[cat] = {"scanned": 0, "new": 0, "skipped": 0, "errors": 0}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        for listing_url, category in SECTIONS.items():
            print(f"\n=== Scanning category: {category}  -> {listing_url}")
            page = context.new_page()
            try:
                page.goto(listing_url, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(600)
            except Exception as ex:
                print(f"  ERROR loading listing {listing_url}: {ex}")
                summary[category]["errors"] += 1
                try:
                    page.close()
                except:
                    pass
                continue

            entries = extract_entries_from_listing(page, listing_url)
            if not entries:
                print("  No entries found on listing page.")
                try:
                    page.close()
                except:
                    pass
                continue

            entries = entries[:NUM_ENTRIES]
            page.close()

            for e in entries:
                summary[category]["scanned"] += 1
                date = normalize_date(e.get("date") or "")
                title = e.get("title") or ""
                link = e.get("link") or ""
                title_key = title.strip().lower()
                link_norm = normalize_link(link)
                if (title_key, link_norm) in existing_pairs:
                    summary[category]["skipped"] += 1
                    # print skip lightly
                    # print("  SKIP (exists):", title)
                    continue

                # It's new (by title+link). Open detail page with retries to find pdf
                print("  NEW:", title)
                detail_page = context.new_page()
                pdf_url = None
                error_msg = ""
                attempt = 0
                success = False
                while attempt <= DETAIL_PAGE_RETRIES and not success:
                    try:
                        attempt += 1
                        detail_page.goto(link, wait_until="networkidle", timeout=45000)
                        detail_page.wait_for_timeout(600)
                        pdf_url = find_pdf_url_on_page(detail_page)
                        if pdf_url:
                            success = True
                        else:
                            # no pdf found but that may be legitimate -- mark success but with no pdf
                            success = True
                        break
                    except Exception as ex:
                        err = str(ex)
                        error_msg = err[:200]
                        if attempt <= DETAIL_PAGE_RETRIES:
                            backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                            print(f"    detail page error (attempt {attempt}), retrying after {backoff}s: {err}")
                            time.sleep(backoff)
                        else:
                            print(f"    detail page failed after {attempt} attempts: {err}")
                    finally:
                        # small politeness delay between detail pages
                        time.sleep(DETAIL_PAGE_DELAY)

                try:
                    detail_page.close()
                except:
                    pass

                # Clean pdf_url so it doesn't contain /web/?file= wrapper
                clean_url = ""
                if pdf_url:
                    try:
                        # _clean_pdf_candidate already returns cleaned absolute url; still normalize
                        clean_url = pdf_url
                    except Exception:
                        clean_url = pdf_url

                pdf_filename = safe_filename(title)
                # prefix category
                if category:
                    if not pdf_filename.lower().startswith(f"{category}_"):
                        pdf_filename = f"{category}_{pdf_filename}"

                entry_id = make_id(date, title, link)
                created_at = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

                row = {
                    "id": entry_id,
                    "date": date,
                    "title": title,
                    "link": link,
                    "pdf_link": clean_url or "",
                    "pdf_filename": pdf_filename,
                    "pdf_downloaded": "no",
                    "created_at": created_at,
                    "source_commit": github_sha,
                    "category": category,
                    "error": (error_msg or "")
                }

                # append to master and new_entries
                new_master.append(row)
                new_entries.append(row)
                existing_pairs.add((title_key, link_norm))
                summary[category]["new"] += 1

        browser.close()

    # Write outputs
    print(f"\nWriting master CSV ({MASTER_CSV}) with {len(new_master)} rows.")
    write_csv(MASTER_CSV, new_master)

    print(f"Writing new entries JSON ({NEW_JSON}) with {len(new_entries)} rows.")
    write_json(NEW_JSON, new_entries)

    # Print run summary
    print("\n=== Run summary per category ===")
    total_scanned = total_new = total_skipped = total_errors = 0
    for cat, cnts in summary.items():
        print(f" {cat}: scanned={cnts['scanned']}, new={cnts['new']}, skipped={cnts['skipped']}, errors={cnts['errors']}")
        total_scanned += cnts['scanned']
        total_new += cnts['new']
        total_skipped += cnts['skipped']
        total_errors += cnts['errors']
    print(f"\nTotals: scanned={total_scanned}, new={total_new}, skipped={total_skipped}, errors={total_errors}")
    print("\nDone. Commit both files (sebi_master.csv and new_entries.json) in the same commit from your GitHub Action if desired.")

if __name__ == "__main__":
    main()

