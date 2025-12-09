#sebi_update_and_download_missing.py

"""

Behavior:
- Scrape top NUM_ENTRIES from SEBI listing page (table or fallback).
- For each entry, check if its title exists in the CSV (case-insensitive).
- If title NOT in CSV:
    - open the entry page, find embedded PDF URL,
    - download the PDF (always re-download and overwrite if file exists),
    - append a row to the CSV with pdf info and downloaded=yes.
- If title exists in CSV: skip (verbose log).
- CSV used: sebi_circulars_last10_with_pdfs.csv (same as before).
- Prints verbose logs to console.

Requirements:
    pip install playwright
    python -m playwright install
Run:
    python sebi_update_and_download_missing.py
"""

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
import csv, os, re
from urllib.parse import urljoin, urlparse, unquote
from pathlib import Path

BASE_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0"
NUM_ENTRIES = 10
OUTPUT_CSV = "sebi_circulars_last10_with_pdfs.csv"
PDF_DIR = Path("pdfs")
PDF_DIR.mkdir(exist_ok=True)

# --- Helpers -----------------------------------------------------------------

def log(msg):
    print(msg)

def safe_filename(s: str, fallback: str = "file"):
    s = s.strip().replace("\n", " ").replace("\r", " ")
    s = re.sub(r"[\/\\\:\*\?\"<>\|]+", "_", s)
    s = re.sub(r"\s+", " ", s)
    if not s:
        return fallback
    return s[:180]

def choose_name_for_pdf(pdf_url: str, entry_title: str):
    parsed = urlparse(pdf_url)
    name = Path(unquote(parsed.path)).name
    if name and "." in name:
        return safe_filename(name, fallback=safe_filename(entry_title, "document")) 
    return safe_filename(entry_title, fallback="document") + ".pdf"

def normalize_text(s):
    return s.strip() if s else ""

def read_existing_titles(csv_path):
    titles = set()
    if not os.path.exists(csv_path):
        log(f"[CSV] {csv_path} not found; will create one when appending.")
        return titles
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                t = (row.get("title") or "").strip().lower()
                if t:
                    titles.add(t)
        log(f"[CSV] Loaded {len(titles)} existing titles from {csv_path}.")
    except Exception as e:
        log(f"[CSV] Error reading {csv_path}: {e}")
    return titles

# --- Extraction of listing entries ------------------------------------------

def extract_entries_from_table(page):
    table = page.query_selector("table")
    if not table:
        return []
    rows = table.query_selector_all("tbody tr") or table.query_selector_all("tr")
    results = []
    for r in rows:
        tds = r.query_selector_all("td")
        if not tds:
            continue
        date = normalize_text(tds[0].inner_text()) if len(tds) >= 1 else ""
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
        if title or date:
            results.append({"date": date, "title": title, "link": link})
    return results

def fallback_extract_entries(page):
    results = []
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
            results.append({"date": date, "title": title, "link": link})
            seen.add(key)
    return results

# --- Find PDF on entry page -------------------------------------------------

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
            for attr in ("src", "href", "data"):
                v = el.get_attribute(attr)
                if v and ".pdf" in v.lower():
                    return urljoin(page.url, v)

    # search all elements for attributes containing .pdf
    all_elements = page.query_selector_all("*")
    for el in all_elements:
        for attr in ("src", "href", "data", "data-src"):
            try:
                v = el.get_attribute(attr)
            except Exception:
                v = None
            if v and ".pdf" in v.lower():
                return urljoin(page.url, v)

    # look for iframe with file=... or viewer params
    iframe_candidates = page.query_selector_all("iframe")
    for ifr in iframe_candidates:
        src = ifr.get_attribute("src") or ""
        if "file=" in src and ".pdf" in src:
            m = re.search(r"[?&]file=([^&]+)", src)
            if m:
                candidate = unquote(m.group(1))
                if ".pdf" in candidate:
                    return urljoin(page.url, candidate)
        if "pdfjs" in src or "viewer" in src:
            m = re.search(r"src=([^&]+)", src)
            if m:
                candidate = unquote(m.group(1))
                if ".pdf" in candidate:
                    return urljoin(page.url, candidate)
    return None

def download_pdf_via_playwright_request(context, pdf_url, dest_path):
    try:
        headers = {"User-Agent": "Playwright Script", "Referer": BASE_URL}
        resp = context.request.get(pdf_url, headers=headers, timeout=45000)
        if resp.status != 200:
            log(f"  [DOWNLOAD] HTTP {resp.status} for {pdf_url}")
            return False
        content = resp.body()
        with open(dest_path, "wb") as fh:
            fh.write(content)
        return True
    except Exception as e:
        log(f"  [DOWNLOAD] Exception for {pdf_url}: {e}")
        return False

# --- Main -------------------------------------------------------------------

def main():
    existing_titles = read_existing_titles(OUTPUT_CSV)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        log(f"[START] Opening listing page: {BASE_URL}")
        page.goto(BASE_URL, wait_until="networkidle")
        page.wait_for_timeout(800)

        entries = extract_entries_from_table(page)
        if not entries:
            entries = fallback_extract_entries(page)

        if not entries:
            log("[ERROR] No entries found on the listing page. Exiting.")
            browser.close()
            return

        entries = entries[:NUM_ENTRIES]
        log(f"[FOUND] {len(entries)} entries on page; will check top {len(entries)} against CSV.")

        # Prepare CSV: ensure header exists; open for append
        csv_needs_header = not os.path.exists(OUTPUT_CSV)
        try:
            csvfile = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
        except Exception as e:
            log(f"[ERROR] Cannot open CSV file {OUTPUT_CSV} for append: {e}")
            browser.close()
            return

        fieldnames = ["date", "title", "link", "pdf_link", "pdf_filename", "pdf_downloaded"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csv_needs_header:
            writer.writeheader()
            log(f"[CSV] Created CSV and wrote header: {OUTPUT_CSV}")

        # Process each entry
        added = 0
        for idx, e in enumerate(entries, start=1):
            date = e.get("date", "")
            title_raw = e.get("title", "") or f"entry_{idx}"
            title_key = title_raw.strip().lower()
            link = e.get("link", "")
            log(f"\n[CHECK {idx}] Title: {title_raw}")

            if title_key in existing_titles:
                log("  [SKIP] Title already present in CSV. Skipping.")
                continue

            # Not present -> process: open page, find pdf, download (overwrite), append CSV
            log(f"  [PROCESS] Title not present. Opening entry page: {link}")
            pdf_url = None
            pdf_file = ""
            downloaded = False
            try:
                entry_page = context.new_page()
                entry_page.goto(link, wait_until="networkidle", timeout=60000)
                entry_page.wait_for_timeout(700)

                pdf_url = find_pdf_url_on_page(entry_page)
                if not pdf_url:
                    # attempt to find anchor that likely leads to pdf
                    anchors = entry_page.query_selector_all("a")
                    for a in anchors:
                        href = a.get_attribute("href") or ""
                        txt = (a.inner_text() or "").lower()
                        if href and (href.lower().endswith(".pdf") or (".pdf" in href.lower() and ("view" in txt or "pdf" in txt))):
                            pdf_url = urljoin(entry_page.url, href)
                            break

                # check frames
                if not pdf_url:
                    for fr in entry_page.frames:
                        try:
                            fr_url = fr.url or ""
                            if fr_url and fr_url.lower().endswith(".pdf"):
                                pdf_url = fr_url
                                break
                            # search inside frame
                            for sel in ("iframe[src$='.pdf']", "embed[src$='.pdf']", "object[data$='.pdf']", "a[href$='.pdf']"):
                                el = fr.query_selector(sel)
                                if el:
                                    for attr in ("src","href","data"):
                                        v = el.get_attribute(attr)
                                        if v and ".pdf" in v.lower():
                                            pdf_url = urljoin(fr.url, v)
                                            break
                                if pdf_url:
                                    break
                            if pdf_url:
                                break
                        except Exception:
                            continue

                if pdf_url:
                    pdf_url = urljoin(entry_page.url, pdf_url)
                    log("  [FOUND] PDF URL:", pdf_url)
                    suggested_name = choose_name_for_pdf(pdf_url, title_raw)
                    dest_path = PDF_DIR / suggested_name
                    if not str(dest_path).lower().endswith(".pdf"):
                        dest_path = Path(str(dest_path) + ".pdf")
                    # Always re-download and overwrite as per your choice
                    log(f"  [DOWNLOAD] Downloading and overwriting (if exists) to: {dest_path}")
                    ok = download_pdf_via_playwright_request(context, pdf_url, dest_path)
                    if ok:
                        log(f"  [OK] Downloaded to: {dest_path}")
                        pdf_file = str(dest_path)
                        downloaded = True
                    else:
                        log("  [ERROR] Failed to download PDF from URL.")
                else:
                    log("  [WARN] No PDF found on the entry page.")
                entry_page.close()
            except PWTimeoutError as e:
                log(f"  [ERROR] Timeout loading entry page: {e}")
            except Exception as ex:
                log(f"  [ERROR] Exception processing entry: {ex}")

            # Append row to CSV regardless of whether pdf was found (to record attempted entry)
            try:
                writer.writerow({
                    "date": date,
                    "title": title_raw,
                    "link": link,
                    "pdf_link": pdf_url or "",
                    "pdf_filename": pdf_file,
                    "pdf_downloaded": "yes" if downloaded else "no"
                })
                csvfile.flush()
                # update in-memory existing_titles so subsequent runs in same run don't duplicate
                existing_titles.add(title_key)
                added += 1
                log(f"  [CSV] Appended row for title. pdf_downloaded={'yes' if downloaded else 'no'}.")
            except Exception as e:
                log(f"  [ERROR] Failed to append to CSV: {e}")

        csvfile.close()
        browser.close()
        log(f"\n[FINISHED] Added {added} new entries (appended) to {OUTPUT_CSV}. PDFs (if any) are in {PDF_DIR}/")

if __name__ == "__main__":
    main()
