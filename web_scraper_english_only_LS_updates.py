import asyncio
import os
import json
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from PyPDF2 import PdfReader
from collections import Counter
from playwright.async_api import async_playwright
import requests
from langdetect import detect_langs 


from xml.sax.saxutils import escape

# ----------- English Detection Function -----------
def is_mostly_english(text, threshold=0.8): #You can change the english text percentage here - current set to 80%.
    """
    Returns True if at least `threshold` proportion of the text is English.
    """
    try:
        langs = detect_langs(text)
        for lang_prob in langs:
            if (lang_prob.lang == "en" and lang_prob.prob >= threshold):
                return True
    except Exception:
        return False
    return False

def flatten(val):
    if isinstance(val, list):
        return "; ".join(str(v) for v in val)
    return str(val)

def export_to_xml(json_path, xml_path):
    if not os.path.exists(json_path):
        print("❌ No JSON found to export to XML.")
        return

    with open(json_path, encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"❌ Error reading JSON for XML export: {e}")
            return

    with open(xml_path, "w", encoding="utf-8") as out:
        out.write('<corpus>\n')
        for doc_id, doc in data.items():
            meta = doc.get("metadata", {})
            
            # Always present attributes
            attrs = [
                f'id="{escape(doc_id)}"',
                f'timestamp="{escape(doc.get("timestamp", ""))}"'
            ]

            # Export all metadata fields dynamically
            for key, val in meta.items():
                safe_key = key.lower().replace(" ", "_")
                safe_val = escape(flatten(val))
                attrs.append(f'{safe_key}="{safe_val}"')

            out.write(f'  <doc {" ".join(attrs)}>\n')
            # Use CDATA to safely include full text
            out.write(f'    <![CDATA[\n{doc.get("text", "")}\n    ]]>\n')
            out.write('  </doc>\n')
        out.write('</corpus>\n')

    print(f"✅ XML exported to {xml_path}")




ACTIVE_BATCH_FOLDER = "Active Batch"
PDF_DOWNLOAD_FOLDER = "downloads"  # Change if needed


# ----------- Direct Download Function (Alternative URL) -----------
def direct_pdf_download(url, destination_dir):
    try:
        os.makedirs(destination_dir, exist_ok=True)
        url_id = url.rstrip("/").split("/")[-1]

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()

        _, file_ext = os.path.splitext(url_id)
        if not file_ext:
            file_ext = ".pdf"
        destination_path = os.path.join(destination_dir, url_id)

        with open(destination_path, "wb") as f:
            f.write(response.content)

        print(f"✅ Direct PDF downloaded: {destination_path}")
        return destination_path
    except Exception as e:
        print(f"❌ Error direct-downloading {url}: {e}")
        return None



# ----------- Playwright Download Function (Main URL) -----------
async def click_download_button(url, destination_dir):
    url_id = url.rstrip("/").split("/")[-1]
    os.makedirs(destination_dir, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            await page.goto(url, timeout=60000)

            # Click download
            download_future = page.wait_for_event("download")

            await page.evaluate('''() => {
                const icon = document.querySelector('md-icon[md-svg-icon="file:ic_download_24px"]');
                if (!icon) throw new Error('Download icon not found');
                const clickable = icon.closest('button, a, div[role="button"]');
                if (clickable) clickable.click();
                else throw new Error('Clickable parent not found');
            }''')

            print("✅ Download button clicked")

            download = await download_future
            original_filename = download.suggested_filename
            _, file_ext = os.path.splitext(original_filename)
            new_filename = f"{url_id}{file_ext}"
            destination_path = os.path.join(destination_dir, new_filename)

            await download.save_as(destination_path)
            print(f"✅ Download saved as: {destination_path}")
            return destination_path

        except Exception as e:
            print(f"❌ Error during download from {url}: {e}")
            return None
        finally:
            await browser.close()


# ----------- Text Extraction Function -----------
def extract_cleaned_text_from_pdf(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        pages_text = [page.extract_text() or "" for page in reader.pages]

        # Detect the most common header and footer lines
        first_lines = [pg.splitlines()[0] for pg in pages_text if pg.splitlines()]
        last_lines = [pg.splitlines()[-1] for pg in pages_text if pg.splitlines()]

        header, _ = Counter(first_lines).most_common(1)[0] if first_lines else ("", 0)
        footer, _ = Counter(last_lines).most_common(1)[0] if last_lines else ("", 0)

        removed = {
            "headers": [header] if header else [],
            "footers": [footer] if footer else []
        }

        cleaned_pages = ["\n".join(page.splitlines()) for page in pages_text]
        return "\n\n".join(cleaned_pages).strip(), removed

    except Exception as e:
        print(f"❌ Error extracting text from {pdf_path}: {e}")
        return "", {"headers": [], "footers": []}


# ----------- JSON Writing Function (Now Returns Status) -----------
def append_to_batch_json(json_path, pdf_name, cleaned_text, removed_parts, metadata):
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = {}
    else:
        data = {}

    # Add English Text flag
    english_flag = "Yes" if is_mostly_english(cleaned_text) else "No"
    metadata["English Text"] = english_flag

    if english_flag != "Yes":
        print(f"⚠️ Skipped {pdf_name} (not English)")
        return "Non-English"

    # Change timestamp format to 4SEP25 (Windows compatible)
    dt = datetime.now()
    formatted_date = f"{dt.day}{dt.strftime('%b%y').upper()}"  # e.g., 4SEP25
    data[pdf_name] = {
        "text": cleaned_text,
        "removed": removed_parts,
        "metadata": metadata,
        "timestamp": formatted_date
    }

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"✅ JSON updated: {pdf_name} added (English only)")
    return "English"




# ----------- Main Scraper -----------
def get_batch_file():
    for fname in os.listdir(ACTIVE_BATCH_FOLDER):
        if fname.endswith(".csv") and "_scraped" not in fname and "_missed" not in fname:
            return os.path.join(ACTIVE_BATCH_FOLDER, fname)
    return None


def run_scraper_on_batch():
    batch_file = get_batch_file()
    if not batch_file:
        print("❌ No valid batch file found in Active Batch.")
        return

    # Use custom date format for output names
    dt = datetime.now()
    date_str = f"{dt.day}{dt.strftime('%b%y').upper()}"  # e.g., 4SEP25
    base_name = f"web_scraper_{date_str}"

    # JSON and XML outputs use new naming
    json_path = os.path.join(ACTIVE_BATCH_FOLDER, f"{base_name}.json")

    df = pd.read_csv(batch_file)
    if 'Main URL' not in df.columns or 'Alternative URL' not in df.columns:
        print(f"❌ Required columns missing in {batch_file}")
        return

    scraped_records = []
    missed_records = []
    non_english_records = []

    async def scrape_all():
        for index, row in df.iterrows():
            alt_url = str(row['Alternative URL']).strip() if pd.notna(row['Alternative URL']) else ""
            main_url = str(row['Main URL']).strip() if pd.notna(row['Main URL']) else ""

            pdf_path = None
            source_used = None

            # ✅ Try Alternative URL only if it looks like a PDF
            if alt_url and alt_url.lower().endswith(".pdf"):
                print(f"🔍 Trying Alternative URL: {alt_url}")
                pdf_path = direct_pdf_download(alt_url, PDF_DOWNLOAD_FOLDER)
                if pdf_path:
                    source_used = "Alternative URL"

            # ✅ Fall back to Main URL if no PDF from Alternative
            if not pdf_path and main_url:
                print(f"🔍 Falling back to Main URL: {main_url}")
                pdf_path = await click_download_button(main_url, PDF_DOWNLOAD_FOLDER)
                if pdf_path:
                    source_used = "Main URL"

            if pdf_path:
                cleaned_text, removed_parts = extract_cleaned_text_from_pdf(pdf_path)
                if cleaned_text:
                    pdf_name = os.path.basename(pdf_path)
                    metadata = row.to_dict()
                    metadata["Source Used"] = source_used
                    status = append_to_batch_json(json_path, pdf_name, cleaned_text, removed_parts, metadata)

                    if status == "English":
                        scraped_records.append(row)
                    elif status == "Non-English":
                        non_english_records.append(row)
                    else:
                        missed_records.append(row)
                else:
                    missed_records.append(row)
            else:
                missed_records.append(row)

    asyncio.run(scrape_all())

    # Remove 'tale2' and add date to metadata CSVs
    csv_base = os.path.splitext(os.path.basename(batch_file))[0].replace("tale2", date_str)
    scraped_path = os.path.join(ACTIVE_BATCH_FOLDER, f"{csv_base}_scraped.csv")
    missed_path = os.path.join(ACTIVE_BATCH_FOLDER, f"{csv_base}_missed.csv")
    non_english_path = os.path.join(ACTIVE_BATCH_FOLDER, f"{csv_base}_non_english.csv")

    if scraped_records:
        pd.DataFrame(scraped_records).to_csv(scraped_path, index=False)
    if missed_records:
        pd.DataFrame(missed_records).to_csv(missed_path, index=False)
    if non_english_records:
        pd.DataFrame(non_english_records).to_csv(non_english_path, index=False)

    print("✅ Scraping complete. JSON, scraped, missed, and non-English files saved to Active Batch.")

    # Export XML corpus with new naming
    xml_path = os.path.join(ACTIVE_BATCH_FOLDER, f"{base_name}.xml")
    export_to_xml(json_path, xml_path)




# ----------- Run it! -----------
if __name__ == "__main__":
    run_scraper_on_batch()
