"""
ILO Corpus Pipeline — Step 2: Download PDFs
============================================
Downloads PDFs listed in the metadata CSV produced by Step 1.
Tries the Alternative URL first (direct download), then falls back to the
Main URL using Playwright browser automation.

Only keeps files that pass PyPDF2 validation (i.e. are real, readable PDFs).
Progress is saved after every 5,000 rows so the script can be restarted safely.
On a re-run, rows whose PDF (PDF_OUTPUT_FOLDER/<Record ID>.pdf) already exists and
passes validation are skipped; an existing file that fails validation is deleted and
downloaded again.

The browser fallback opens visible Chromium windows (headless=False), so this step
needs a display. If the browser cannot be started, a message is printed and only the
rows that needed the browser are recorded as missed.

Output:
    PDF_OUTPUT_FOLDER/                       — downloaded PDF files
    download_reports/                        — per-batch success/failure CSVs
    FINAL_<timestamp>_downloaded.csv         — master successful downloads log
    FINAL_<timestamp>_missed.csv             — master failures log
    (<timestamp> is the run time, e.g. FINAL_20260401_093000_downloaded.csv)

Dependencies:
    pip install -r requirements.txt
    python -m playwright install chromium

Usage:
    python 02_download_pdfs.py
    Run it from the folder that holds the metadata CSV (the current working
    directory); outputs are written there. Set METADATA_CSV and PDF_OUTPUT_FOLDER
    below to override the defaults.
"""

import asyncio
import glob
import os
import sys
import random
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright
import requests
from PyPDF2 import PdfReader


# The released metadata file. It is never picked automatically, so a new run cannot
# use it by accident (Step 4 rewrites the IN_CORPUS column of the file it finds).
SHIPPED_RELEASE_CSV = "ILO_labordoc_metadata_MAR2026.csv"


def _find_labordoc_csv():
    """Auto-detect the labordoc metadata CSV produced by Step 1 (ILO_labordoc_metadata_DATE.csv).

    The released file ILO_labordoc_metadata_MAR2026.csv is skipped. To use it
    deliberately, set METADATA_CSV below by hand.
    """
    matches = sorted(glob.glob("ILO_labordoc_metadata_*.csv"), key=os.path.getmtime, reverse=True)
    candidates = [m for m in matches if os.path.basename(m) != SHIPPED_RELEASE_CSV]
    if not candidates:
        if matches:
            raise FileNotFoundError(
                f"Only the released file {SHIPPED_RELEASE_CSV} was found in the current directory, "
                "and it is not used automatically. Run Step 1 first, or set "
                "METADATA_CSV below by hand to use the released file."
            )
        raise FileNotFoundError(
            "No ILO_labordoc_metadata_DATE.csv found in the current directory. "
            "Run Step 1 first, or set METADATA_CSV manually below."
        )
    return candidates[0]


# ── CONFIGURATION ─────────────────────────────────────────────────────────────
METADATA_CSV      = _find_labordoc_csv()   # Auto-detected from Step 1 output (ILO_labordoc_metadata_DATE.csv)
PDF_OUTPUT_FOLDER = "pdf_downloads"        # Folder where PDFs are saved
REPORTS_FOLDER    = "download_reports"     # Folder for per-batch success/failure logs
BATCH_SIZE        = 5000                   # Save progress every N rows

_browser_warning_shown = False

# ── PDF VALIDATION ─────────────────────────────────────────────────────────────
def validate_pdf_file(pdf_path):
    """Returns True if file is a readable PDF; deletes it and returns False otherwise."""
    try:
        reader = PdfReader(pdf_path)
        if len(reader.pages) > 0:
            reader.pages[0].extract_text()
            return True
        os.remove(pdf_path)
        return False
    except Exception:
        try:
            os.remove(pdf_path)
        except Exception:
            pass
        return False


# ── DIRECT DOWNLOAD (Alternative URL) ─────────────────────────────────────────
def direct_pdf_download(url, destination_dir, record_id):
    try:
        os.makedirs(destination_dir, exist_ok=True)
        filename = f"{record_id}.pdf"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        if "pdf" not in content_type and len(response.content) < 1000:
            return None

        destination_path = os.path.join(destination_dir, filename)
        with open(destination_path, "wb") as f:
            f.write(response.content)

        if validate_pdf_file(destination_path):
            return destination_path
        return None

    except Exception:
        return None


# ── PLAYWRIGHT DOWNLOAD (Main URL / fallback) ──────────────────────────────────
async def playwright_download(url, destination_dir, record_id):
    """Opens URL in a browser and triggers the ILO download button."""
    global _browser_warning_shown
    filename = f"{record_id}.pdf"
    os.makedirs(destination_dir, exist_ok=True)

    try:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=False)
            except Exception as e:
                if not _browser_warning_shown:
                    _browser_warning_shown = True
                    print("\n  WARNING: the Playwright browser could not be started "
                          f"({str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__}).")
                    print("  Rows that need the browser will be recorded as missed. "
                          "Install it with:  python -m playwright install chromium")
                    print("  (the browser opens visible windows, so a display is required)\n")
                return None

            try:
                context = await browser.new_context(accept_downloads=True)
                page = await context.new_page()

                await page.goto(url, timeout=60000)
                async with page.expect_download() as download_info:
                    await page.evaluate("""() => {
                        const icon = document.querySelector('md-icon[md-svg-icon="file:ic_download_24px"]');
                        if (!icon) throw new Error('Download icon not found');
                        const clickable = icon.closest('button, a, div[role="button"]');
                        if (clickable) clickable.click();
                        else throw new Error('Clickable parent not found');
                    }""")
                download = await download_info.value
                destination_path = os.path.join(destination_dir, filename)
                await download.save_as(destination_path)

                if validate_pdf_file(destination_path):
                    return destination_path
                return None

            except Exception:
                return None
            finally:
                await browser.close()
    except Exception:
        return None


# ── BATCH PROCESSING ───────────────────────────────────────────────────────────
async def process_batch(batch_df, batch_num, start_idx, total_rows):
    successful = []
    failed = []

    for idx, (_, row) in enumerate(batch_df.iterrows()):
        current_row = start_idx + idx + 1
        print(f"Row {current_row}/{total_rows}")

        record_id = str(row.get("Record ID", "")).strip().replace(".0", "")
        if not record_id or record_id.lower() == "nan":
            record_id = f"unknown_{current_row}"

        alt_url  = str(row.get("Alternative URL", "")).strip()
        main_url = str(row.get("Main URL", "")).strip()
        alt_url  = "" if alt_url.lower() == "nan" else alt_url
        main_url = "" if main_url.lower() == "nan" else main_url

        pdf_path   = None
        source_used = None

        # Skip rows whose PDF was already downloaded and still passes validation
        existing_path = os.path.join(PDF_OUTPUT_FOLDER, f"{record_id}.pdf")
        if os.path.exists(existing_path) and validate_pdf_file(existing_path):
            pdf_path = existing_path
            source_used = "Already downloaded (skipped)"
        else:
            await asyncio.sleep(random.uniform(0.5, 2.0))

            if alt_url:
                pdf_path = direct_pdf_download(alt_url, PDF_OUTPUT_FOLDER, record_id)
                if pdf_path:
                    source_used = "Alternative URL (Direct)"
                else:
                    pdf_path = await playwright_download(alt_url, PDF_OUTPUT_FOLDER, record_id)
                    if pdf_path:
                        source_used = "Alternative URL (Playwright)"

            if not pdf_path and main_url:
                pdf_path = await playwright_download(main_url, PDF_OUTPUT_FOLDER, record_id)
                if pdf_path:
                    source_used = "Main URL"

        row_data = row.to_dict()
        row_data["Batch_Number"] = batch_num
        row_data["Row_Number"]   = current_row

        if pdf_path:
            row_data["Downloaded_File"] = pdf_path
            row_data["Source_Used"]     = source_used
            successful.append(row_data)
            print(f"  OK: {os.path.basename(pdf_path)}")
        else:
            failed.append(row_data)
            print(f"  FAILED: row {current_row}")

    return successful, failed


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    sys.stdout.reconfigure(encoding="utf-8")

    if not os.path.exists(METADATA_CSV):
        print(f"ERROR: Metadata file not found: {METADATA_CSV}")
        return

    print(f"Using metadata file: {METADATA_CSV}")
    os.makedirs(REPORTS_FOLDER, exist_ok=True)

    df = pd.read_csv(METADATA_CSV, encoding="utf-8-sig")
    if "Main URL" not in df.columns or "Alternative URL" not in df.columns:
        print("ERROR: CSV must contain 'Main URL' and 'Alternative URL' columns.")
        return

    total_rows  = len(df)
    batch_base  = os.path.splitext(os.path.basename(METADATA_CSV))[0]
    print(f"Loaded {total_rows:,} rows from {METADATA_CSV}")
    print(f"PDFs will be saved to: {PDF_OUTPUT_FOLDER}")

    all_successful = []
    all_failed     = []

    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch_end    = min(batch_start + BATCH_SIZE, total_rows)
        batch_num    = (batch_start // BATCH_SIZE) + 1
        batch_df     = df.iloc[batch_start:batch_end]

        print(f"\n--- Batch {batch_num}: rows {batch_start+1}–{batch_end} ---")

        try:
            succ, fail = asyncio.run(
                process_batch(batch_df, batch_num, batch_start, total_rows)
            )
        except Exception as e:
            print(f"Batch {batch_num} error: {e}")
            continue

        all_successful.extend(succ)
        all_failed.extend(fail)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if succ:
            path = os.path.join(REPORTS_FOLDER, f"{batch_base}_b{batch_num:02d}_{ts}_downloaded.csv")
            pd.DataFrame(succ).to_csv(path, index=False)
        if fail:
            path = os.path.join(REPORTS_FOLDER, f"{batch_base}_b{batch_num:02d}_{ts}_missed.csv")
            pd.DataFrame(fail).to_csv(path, index=False)

        print(f"  Batch {batch_num}: {len(succ)} downloaded, {len(fail)} failed")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if all_successful:
        pd.DataFrame(all_successful).to_csv(f"FINAL_{ts}_downloaded.csv", index=False)
    if all_failed:
        pd.DataFrame(all_failed).to_csv(f"FINAL_{ts}_missed.csv", index=False)

    print(f"\nDone. {len(all_successful):,} downloaded, {len(all_failed):,} failed.")
    print(f"PDFs saved to: {PDF_OUTPUT_FOLDER}")


if __name__ == "__main__":
    main()
