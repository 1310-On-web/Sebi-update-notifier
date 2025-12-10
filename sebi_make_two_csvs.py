#!/usr/bin/env python3
"""
sebi_make_two_csvs.py (modified)
- Use: run in GitHub Actions (Playwright browsers installed)
- Produces:
    - sebi_master.csv  (full canonical list)  <-- unchanged behavior
    - new_entries.json  (only newly discovered rows this run)  <-- replaced CSV with JSON
- NOTE: script does NOT download PDFs. Power Automate will download them.
"""

from playwright.sync_api import sync_playwright
from urllib.parse import urljoin, urlparse, unquote
from pathlib import Path
import csv, hashlib, re, datetime, os, sys, json

# EDITABLE
BASE_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0"
NUM_ENTRIES = 10
MASTER_CSV = "sebi_master.csv"
NEW_JSON = "new_entries.json"   # replaced new_entries.csv with JSON output
CSV_DELIM = "|"

# Helpers
import re
def safe_filename(s: str, fallback: str = "document"):
    """
    Return a sanitized filename that ends with .pdf and has no invalid SharePoint/Windows characters.
    Truncates to a safe length.
    """
    if not s:
        s = fallback
    # Normalize whitespace
    s = s.strip().replace("\r", " ").replace("\n", " ")
    # Remove characters not allowed in file names
    s = re.sub(r'[\/\\\:\*\?"<>\|]+', '_', s)
    # Collapse multiple spaces
    s = re.sub(r'\s+', ' ', s)
    # Truncate to a safe length (leave room for ".pdf")
    max_base = 150
    base = s[:max_base].strip()
    # Ensure .pdf suffix
    if not base.lower().endswith('.pdf'):
        base = base + '.pdf'
    return base


def make_id(date, title, link):
    base = f"{date}|{title}|{link}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def normalize_text(s):
    return (s or "").strip()

# Extract functions (similar to earlier scripts but without downloads)
def find_pdf_url_on_page(page):
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
                v = el.get_attribute(attr)
                if v and ".pdf" in v.lower():
                    return urljoin(page.url, v)
    # search all elements
    all_elements = page.query_selector_all("*")
    for el in all_elements:
        for attr in ("src","href","data","data-src"):
            try:
                v = el.get_attribute(attr)
            except Exception:
                v = None
            if v and ".pdf" in v.lower():
                return urljoin(page.url, v)
    # iframe params
    for ifr in page.query_selector_all("iframe"):
        src = ifr.get_attribute("src") or ""
        if "file=" in src and ".pdf" in src:
            m = re.search(r"[?&]file=([^&]+)", src)
            if m:
                candidate = unquote(m.group(1))
                if ".pdf" in candidate:
                    return urljoin(page.url, candidate)
    return None

def extract_entries_from_listing(page):
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
                link = urljoin(BASE_URL, href)
            else:
                if len(tds) >= 2:
                    title = normalize_text(tds[1].inner_text())
            if title:
                rows.append({"date": date, "title": title, "link": link})
    else:
        # fallback anchors
        anchors = page.query_selector_all("div#content a, div.listing a, ul li a, div.content a, div#main a")
        seen = set()
        for a in anchors:
            title = normalize_text(a.inner_text())
            href = a.get_attribute("href") or ""
            link = urljoin(BASE_URL, href)
            key = (title, link)
            if title and key not in seen:
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

def load_master_csv(path):
    results = []
    if not os.path.exists(path):
        return results
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter=CSV_DELIM)
        for r in rdr:
            results.append(r)
    return results

def write_csv(path: str, rows: list):
    """
    Write rows (list of dicts) to CSV using '|' delimiter, no BOM, LF line endings,
    and minimal quoting. Ensures file is plain text (no extra surrounding quotes).
    """
    headers = ["id","date","title","link","pdf_link","pdf_filename","pdf_downloaded","created_at","source_commit"]
    # Open with newline='' to let csv module manage line endings (it will write '\r\n' by default on Windows,
    # but on Linux runners it will write '\n'. To force '\n', we can post-process if required.)
    with open(path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=headers,
            delimiter='|',
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\'
        )
        writer.writeheader()
        for r in rows:
            # Convert any None to empty string to avoid 'None' text
            safe_row = {k: ("" if r.get(k) is None else str(r.get(k))) for k in headers}
            # Ensure pdf_filename has no newlines or extraneous quotes
            safe_row['pdf_filename'] = safe_filename(safe_row.get('pdf_filename', ''), fallback='document.pdf')
            writer.writerow(safe_row)

    # Force LF-only newlines (useful if running on Windows runner and you want consistent '\n' on GitHub)
    try:
        with open(path, "rb") as f:
            data = f.read()
        data = data.replace(b'\r\n', b'\n')
        with open(path, "wb") as f:
            f.write(data)
    except Exception:
        # if anything goes wrong here, ignore; file already written
        pass

def write_json(path: str, rows: list):
    """
    Write rows to a JSON file (UTF-8) as an array of objects.
    Make sure to normalize values and avoid None.
    """
    headers = ["id","date","title","link","pdf_link","pdf_filename","pdf_downloaded","created_at","source_commit"]
    safe_rows = []
    for r in rows:
        safe_row = {k: ("" if r.get(k) is None else r.get(k)) for k in headers}
        # ensure strings for consistency
        for k in headers:
            if safe_row[k] is None:
                safe_row[k] = ""
            else:
                safe_row[k] = str(safe_row[k])
        safe_row['pdf_filename'] = safe_filename(safe_row.get('pdf_filename', ''), fallback='document.pdf')
        safe_rows.append(safe_row)
    # Write pretty JSON (compact is fine too; using indent=2 for readability in repo)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(safe_rows, f, ensure_ascii=False, indent=2)

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        print("Opening listing:", BASE_URL)
        page.goto(BASE_URL, wait_until="networkidle")
        page.wait_for_timeout(800)

        entries = extract_entries_from_listing(page)
        if not entries:
            print("No entries found; exiting.")
            browser.close()
            sys.exit(0)
        entries = entries[:NUM_ENTRIES]

        # Load master and build a set of existing titles (normalized)
        master_rows = load_master_csv(MASTER_CSV)
        existing_titles = set()
        for r in master_rows:
            t = (r.get("title") or "").strip().lower()
            if t:
                existing_titles.add(t)

        # Prepare list for new master (start from existing master rows)
        new_master = master_rows.copy()
        new_entries = []

        for e in entries:
            date = e.get("date") or ""
            title = e.get("title") or ""
            link = e.get("link") or ""
            title_key = title.strip().lower()
            if title_key in existing_titles:
                print("SKIP (exists):", title)
                continue
            # Open detail page to find pdf link
            print("NEW:", title)
            detail_page = context.new_page()
            try:
                detail_page.goto(link, wait_until="networkidle", timeout=45000)
                detail_page.wait_for_timeout(600)
                pdf_url = find_pdf_url_on_page(detail_page)
            except Exception as ex:
                print("  Error opening detail page:", ex)
                pdf_url = None
            finally:
                try:
                    detail_page.close()
                except:
                    pass

            pdf_filename = safe_filename(title)
            entry_id = make_id(date, title, link)
            created_at = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            row = {
                "id": entry_id,
                "date": date,
                "title": title,
                "link": link,
                "pdf_link": pdf_url or "",
                "pdf_filename": pdf_filename,
                "pdf_downloaded": "no",
                "created_at": created_at,
                "source_commit": ""  # GitHub Action will fill commit sha optionally
            }
            new_master.append(row)
            new_entries.append(row)
            existing_titles.add(title_key)

        # Write master CSV (unchanged)
        print(f"Writing master CSV ({MASTER_CSV}) with {len(new_master)} rows.")
        write_csv(MASTER_CSV, new_master)

        # Write new entries as JSON (replaces new_entries.csv)
        print(f"Writing new entries JSON ({NEW_JSON}) with {len(new_entries)} rows.")
        write_json(NEW_JSON, new_entries)

        browser.close()
        print("Done. Please commit both files (sebi_master.csv and new_entries.json) in the same commit from your GitHub Action.")

if __name__ == "__main__":
    main()
