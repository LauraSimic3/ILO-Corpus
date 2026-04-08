"""
ILO Corpus Pipeline — Step 3: Extract Text from PDFs and Save as JSON
======================================================================
For each downloaded PDF:
  1. Extracts text using PyMuPDF
  2. Checks the text is at least 80% English (langdetect)
  3. Matches metadata from the ilo_metadata CSV using the Record ID in the filename (set by Step 2)
  4. Saves a JSON file per document containing text + metadata

Non-English PDFs and PDFs with insufficient text are skipped and logged.

Output:
    JSON_OUTPUT_FOLDER/      — one .json file per accepted PDF
    extract_log_<ts>.log     — full processing log
    extract_failures_<ts>.log — skipped files with reason

Dependencies:
    pip install pymupdf langdetect pandas tqdm

Usage:
    python 03_extract_text_to_json.py
    Set PDF_FOLDER, JSON_OUTPUT_FOLDER, and METADATA_CSV below before running.
"""

import glob
import os
import sys
import json
import re
import logging
from datetime import datetime
from pathlib import Path

import fitz          # PyMuPDF
import pandas as pd
from langdetect import detect_langs, LangDetectException
from tqdm import tqdm


def _find_labordoc_csv():
    """Auto-detect the labordoc metadata CSV produced by Step 1 (ilo_labordoc_metadata_DATE.csv)."""
    matches = sorted(glob.glob("ilo_labordoc_metadata_*.csv"), key=os.path.getmtime, reverse=True)
    if not matches:
        raise FileNotFoundError(
            "No ilo_labordoc_metadata_DATE.csv found in the current directory. "
            "Run Step 1 first, or set METADATA_CSV manually below."
        )
    return matches[0]


# ── CONFIGURATION ─────────────────────────────────────────────────────────────
PDF_FOLDER         = "pdf_downloads"     # Folder containing downloaded PDFs (Step 2 output)
JSON_OUTPUT_FOLDER = "json_output"       # Where JSON files are saved
METADATA_CSV       = _find_labordoc_csv()  # Auto-detected from Step 1 output (ilo_labordoc_metadata_DATE.csv)
ENGLISH_THRESHOLD  = 0.80                # Minimum English confidence (0–1)

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE      = f"extract_log_{_ts}.log"
FAILURES_LOG  = f"extract_failures_{_ts}.log"

# ── LOGGING ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ── METADATA LOADING ───────────────────────────────────────────────────────────
def load_metadata(csv_path):
    """
    Load ilo_metadata CSV and build lookup dictionaries keyed by:
      - Record ID (int)
      - Ilo Name (lowercase string)
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False)
    logger.info(f"Loaded {len(df):,} metadata rows from {csv_path}")

    by_record_id = {}
    by_ilo_name  = {}

    def clean(value):
        if pd.isna(value):
            return None
        s = str(value).strip()
        return s if s and s.lower() != "nan" else None

    for _, row in df.iterrows():
        row_dict = row.to_dict()

        rid = clean(row.get("Record ID"))
        if rid:
            try:
                by_record_id[int(float(rid))] = row_dict
            except ValueError:
                pass

        ilo = clean(row.get("Ilo Name"))
        if ilo:
            by_ilo_name[ilo.lower()] = row_dict

    logger.info(f"  Lookup by Record ID: {len(by_record_id):,} entries")
    logger.info(f"  Lookup by Ilo Name:  {len(by_ilo_name):,} entries")
    return by_record_id, by_ilo_name


# ── TEXT EXTRACTION ────────────────────────────────────────────────────────────
def extract_text(pdf_path):
    """Extract text from PDF using PyMuPDF. Limited to 1,000 pages."""
    doc = fitz.open(pdf_path)
    parts = []
    for page_num in range(len(doc)):
        try:
            text = doc[page_num].get_text()
            if text.strip():
                parts.append(text)
        except Exception:
            continue
    doc.close()
    return "\n".join(parts).strip()


# ── ENGLISH DETECTION ──────────────────────────────────────────────────────────
def is_english(text, threshold=ENGLISH_THRESHOLD):
    """
    Returns (is_english: bool, confidence: float, primary_lang: str).
    Uses the first 10,000 characters for speed.
    """
    if not text or len(text.strip()) < 100:
        return False, 0.0, "insufficient_text"
    try:
        sample = re.sub(r"\s+", " ", text[:10000])
        lang_probs = detect_langs(sample)
        primary = str(lang_probs[0]).split(":")[0] if lang_probs else "unknown"
        en_conf = 0.0
        for lp in lang_probs:
            code, conf = str(lp).split(":")
            if code == "en":
                en_conf = float(conf)
                break
        return en_conf >= threshold, en_conf, primary
    except LangDetectException:
        return False, 0.0, "detection_failed"
    except Exception:
        return False, 0.0, "error"


# ── FILENAME PARSING ───────────────────────────────────────────────────────────
def record_id_from_filename(filename):
    """Extract 15-digit ILO Record ID from PDF filename if present."""
    name = filename.replace(".pdf", "").replace(".json", "")
    # Handle duplicated IDs: 995671157002676_995671157002676
    if "_" in name:
        parts = name.split("_")
        if len(parts) == 2 and parts[0] == parts[1]:
            name = parts[0]
        elif parts[0].isdigit() and len(parts[0]) == 15:
            name = parts[0]
    if name.replace(".0", "").isdigit():
        name = name.split(".0")[0]
    if name.isdigit() and len(name) == 15:
        try:
            return int(name)
        except ValueError:
            pass
    return None


def ilo_name_from_filename(filename):
    """Extract ILO call-number style name (e.g. 09466(2008-2)) from filename."""
    for pattern in [
        r"(\d{5}\(\d{4}(?:-\d+)+\))",
        r"(\d{5}\(\d{4}\))",
    ]:
        m = re.search(pattern, filename)
        if m:
            return m.group(1)
    return None


# ── METADATA MATCHING ──────────────────────────────────────────────────────────
def clean_value(value):
    if pd.isna(value) or str(value).strip().lower() in ("", "nan"):
        return None
    return str(value).strip()


def extract_year(value):
    if not value:
        return None
    s = str(value)
    if s.isdigit() and len(s) == 4 and 1900 <= int(s) <= 2030:
        return s
    m = re.search(r"\b(19\d{2}|20\d{2})\b", s)
    if m and 1900 <= int(m.group(1)) <= 2030:
        return m.group(1)
    return None


FIELD_MAP = {
    "Year":                  "year",
    "Main Title":            "title",
    "Record ID":             "id",
    "Corporate Author":      "corporate_author",
    "Responsibility":        "responsibility",
    "Publisher":             "publisher",
    "Publication Place":     "publication_place",
    "Publication Date":      "publication_date",
    "Subtitle":              "subtitle",
    "Personal Author":       "personal_author",
    "Topical Subject":       "subject",
    "Physical Description":  "physical_description",
    "ISSN/ISBN":             "isbn",
    "Bibliography Note":     "bibliography_note",
    "Subject Source":        "subject_source",
    "System Control Number": "system_control_number",
    "Leader (Format)":       "leader_format",
    "Main URL":              "main_url",
    "Resource URL":          "resource_url",
    "Alternative URL":       "alternative_url",
    "Ilo Name":              "ilo_name",
    "Abstract/Summary":      "abstract",
    "Material Type":         "material_type",
    "Variant Title":         "variant_title",
}


def build_metadata(source_row, match_method):
    meta = {}
    for src_field, tgt_field in FIELD_MAP.items():
        val = clean_value(source_row.get(src_field))
        if val:
            meta[tgt_field] = val

    year = extract_year(
        clean_value(source_row.get("Year")) or clean_value(source_row.get("Publication Date"))
    )
    if year:
        meta["publication_date"] = year
        try:
            meta["year"] = int(year)
        except ValueError:
            pass

    return meta, match_method


def find_metadata(filename, by_record_id, by_ilo_name):
    rid = record_id_from_filename(filename)
    if rid and rid in by_record_id:
        return build_metadata(by_record_id[rid], "Record ID")

    ilo = ilo_name_from_filename(filename)
    if ilo and ilo.lower() in by_ilo_name:
        return build_metadata(by_ilo_name[ilo.lower()], "Ilo Name")

    return {"ilo_name": filename.replace(".pdf", "")}, "No match — filename only"


# ── SINGLE FILE PROCESSING ─────────────────────────────────────────────────────
def process_pdf(pdf_path, output_folder, by_record_id, by_ilo_name):
    filename = os.path.basename(pdf_path)
    json_path = os.path.join(output_folder, filename.replace(".pdf", ".pdf.json"))

    if os.path.exists(json_path):
        return {"status": "already_done", "filename": filename}

    try:
        text = extract_text(pdf_path)
    except Exception as e:
        return {"status": "error", "filename": filename, "error": str(e)}

    if not text or len(text.strip()) < 100:
        return {"status": "insufficient_text", "filename": filename, "text_length": len(text) if text else 0}

    english, confidence, primary = is_english(text)
    if not english:
        return {"status": "not_english", "filename": filename, "en_confidence": confidence, "primary_lang": primary}

    metadata, match_method = find_metadata(filename, by_record_id, by_ilo_name)

    json_data = {
        "text": text,
        "metadata": metadata,
        "doc_name": filename.replace(".pdf", ""),
        "processing_info": {
            "timestamp":               datetime.now().isoformat(),
            "source_pdf":              filename,
            "text_length":             len(text),
            "english_confidence":      confidence,
            "primary_language":        primary,
            "english_threshold_used":  ENGLISH_THRESHOLD,
            "metadata_match_method":   match_method,
        },
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)

    return {
        "status":        "success",
        "filename":      filename,
        "text_length":   len(text),
        "en_confidence": confidence,
        "match_method":  match_method,
    }


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    sys.stdout.reconfigure(encoding="utf-8")

    if not os.path.exists(PDF_FOLDER):
        print(f"ERROR: PDF folder not found: {PDF_FOLDER}")
        return
    if not os.path.exists(METADATA_CSV):
        print(f"ERROR: Metadata CSV not found: {METADATA_CSV}")
        return

    os.makedirs(JSON_OUTPUT_FOLDER, exist_ok=True)

    by_record_id, by_ilo_name = load_metadata(METADATA_CSV)

    pdf_files = sorted(Path(PDF_FOLDER).glob("*.pdf"), key=lambda p: p.stat().st_mtime)
    total = len(pdf_files)

    if total == 0:
        print(f"No PDF files found in: {PDF_FOLDER}")
        return

    logger.info(f"Processing {total:,} PDFs — English threshold: {ENGLISH_THRESHOLD:.0%}")

    stats = {k: 0 for k in ("success", "already_done", "insufficient_text", "not_english", "error", "with_metadata", "without_metadata")}
    stats["total"] = total
    start = datetime.now()

    with tqdm(total=total, desc="Processing") as pbar:
        for pdf_path in pdf_files:
            result = process_pdf(str(pdf_path), JSON_OUTPUT_FOLDER, by_record_id, by_ilo_name)
            status = result["status"]
            stats[status] = stats.get(status, 0) + 1

            if status == "success":
                match = result.get("match_method", "")
                if "No match" in match:
                    stats["without_metadata"] += 1
                else:
                    stats["with_metadata"] += 1
                logger.info(f"OK  {result['filename']}  len={result['text_length']:,}  en={result['en_confidence']:.1%}  [{match}]")

            elif status in ("not_english", "insufficient_text"):
                logger.info(f"SKIP({status})  {result['filename']}")
                with open(FAILURES_LOG, "a", encoding="utf-8") as f:
                    f.write(f"{result['filename']}|{status}|en={result.get('en_confidence', '')}\n")

            elif status == "error":
                logger.error(f"ERROR  {result['filename']}  {result.get('error')}")
                with open(FAILURES_LOG, "a", encoding="utf-8") as f:
                    f.write(f"{result['filename']}|error|{result.get('error', '')}\n")

            pbar.update(1)

    elapsed = (datetime.now() - start).total_seconds()
    print("\n" + "=" * 60)
    print(f"Total PDFs:          {stats['total']:,}")
    print(f"Saved to JSON:       {stats['success']:,}  (with metadata: {stats['with_metadata']:,})")
    print(f"Already done:        {stats['already_done']:,}")
    print(f"Insufficient text:   {stats['insufficient_text']:,}")
    print(f"Not English:         {stats['not_english']:,}")
    print(f"Errors:              {stats['error']:,}")
    print(f"Time:                {elapsed/60:.1f} min")
    print(f"Output:              {JSON_OUTPUT_FOLDER}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
