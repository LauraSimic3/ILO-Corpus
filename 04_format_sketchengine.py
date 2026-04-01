"""
ILO Corpus Pipeline — Step 4 (OPTIONAL): Format JSON Files for SketchEngine
=============================================================================
Converts the JSON files produced by Step 3 into SketchEngine vertical corpus
XML format: each document becomes a <doc> element with metadata as attributes,
containing <p> paragraph elements and <s> sentence elements.

Output files are split to stay under 500 MB each (SketchEngine upload limit).
Output filenames follow the pattern: ILO_Corpus_Batch_01.xml, _02.xml, etc.

This step is optional — it is only needed if you intend to upload the corpus
to SketchEngine. The JSON files from Step 3 are usable independently.

Dependencies:
    pip install tqdm

Usage:
    python 04_format_sketchengine.py
    Set JSON_INPUT_FOLDER and XML_OUTPUT_FOLDER below before running.
"""

import os
import sys
import json
import html
import re
import logging
from datetime import datetime
from tqdm import tqdm

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
JSON_INPUT_FOLDER  = "json_output"          # JSON files from Step 3
XML_OUTPUT_FOLDER  = "sketchengine_xml"     # Where batch XML files are saved
MAX_BATCH_SIZE_MB  = 450                    # Split files above this size (SketchEngine limit: 500 MB)

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"se_format_{_ts}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ── TEXT CLEANING ──────────────────────────────────────────────────────────────
def clean_text(text):
    if not text or not isinstance(text, str):
        return ""
    text = " ".join(text.split())
    return html.escape(text)


def clean_attr(value):
    if not value or str(value).strip().lower() in ("", "nan"):
        return ""
    value = str(value).strip()
    value = value.replace('"', "&quot;").replace("'", "&apos;")
    value = value.replace("\n", " ").replace("\r", " ")
    return " ".join(value.split())


def split_sentences(text):
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z])", text)
    return [s.strip() for s in sentences if s.strip() and len(s.strip()) > 3]


# ── JSON → XML CONVERSION ──────────────────────────────────────────────────────
ATTR_FIELDS = [
    ("id",                   "id"),
    ("title",                "title"),
    ("publication_date",     "publication_date"),
    ("year",                 "year"),
    ("publisher",            "publisher"),
    ("publication_place",    "publication_place"),
    ("personal_author",      "author"),
    ("corporate_author",     "corporate_author"),
    ("responsibility",       "responsibility"),
    ("subject",              "subject"),
    ("subtitle",             "subtitle"),
    ("abstract",             "abstract"),
    ("variant_title",        "variant_title"),
    ("physical_description", "physical_description"),
    ("isbn",                 "isbn"),
    ("bibliography_note",    "bibliography_note"),
    ("subject_source",       "subject_source"),
    ("system_control_number","system_control_number"),
    ("leader_format",        "leader_format"),
    ("material_type",        "material_type"),
    ("main_url",             "main_url"),
    ("resource_url",         "resource_url"),
    ("alternative_url",      "alternative_url"),
    ("ilo_name",             "ilo_name"),
]


def json_to_xml_lines(json_path):
    """Convert a single JSON file to a list of XML lines for one <doc> element."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Could not read {os.path.basename(json_path)}: {e}")
        return []

    text_content = data.get("text", "")
    metadata     = data.get("metadata", {})

    if not text_content:
        return []

    attrs = []
    for meta_key, xml_attr in ATTR_FIELDS:
        val = metadata.get(meta_key)
        if val and str(val).strip().lower() not in ("", "nan"):
            attrs.append(f'{xml_attr}="{clean_attr(val)}"')

    doc_tag = "<doc " + " ".join(attrs) + ">" if attrs else "<doc>"
    lines   = [doc_tag]

    for para in text_content.split("\n\n"):
        para = para.strip()
        if not para or len(para) < 10:
            continue
        lines.append("<p>")
        for sent in split_sentences(para):
            lines.append(f"<s>{clean_text(sent)}</s>")
        lines.append("</p>")

    lines.append("</doc>")
    return lines


# ── BATCHING AND OUTPUT ────────────────────────────────────────────────────────
def get_file_size_mb(path):
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except Exception:
        return 0


def save_batch(xml_lines, batch_number, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    filename = f"ILO_Corpus_Batch_{batch_number:02d}.xml"
    filepath = os.path.join(output_dir, filename)
    content  = '<?xml version="1.0" encoding="UTF-8"?>\n' + "\n".join(xml_lines)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    size_mb = get_file_size_mb(filepath)
    logger.info(f"  Saved {filename}  ({size_mb:.1f} MB)")
    return filepath, size_mb


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    sys.stdout.reconfigure(encoding="utf-8")

    json_files = sorted(
        f for f in os.listdir(JSON_INPUT_FOLDER) if f.endswith(".json")
    )
    if not json_files:
        print(f"No JSON files found in: {JSON_INPUT_FOLDER}")
        return

    logger.info(f"Found {len(json_files):,} JSON files in {JSON_INPUT_FOLDER}")

    # Sort largest first so batches fill more evenly
    sized = []
    for fn in json_files:
        fp = os.path.join(JSON_INPUT_FOLDER, fn)
        sized.append((fn, get_file_size_mb(fp)))
    sized.sort(key=lambda x: x[1], reverse=True)

    # Build batches by estimated XML size (JSON * 1.5 overhead estimate)
    batches = []
    current_batch = []
    current_size  = 0.0
    for fn, size_mb in sized:
        est = size_mb * 1.5
        if (current_size + est > MAX_BATCH_SIZE_MB and current_batch) or len(current_batch) >= 800:
            batches.append(current_batch)
            current_batch = [(fn, size_mb)]
            current_size  = est
        else:
            current_batch.append((fn, size_mb))
            current_size += est
    if current_batch:
        batches.append(current_batch)

    logger.info(f"Splitting into {len(batches)} batch(es)  (max {MAX_BATCH_SIZE_MB} MB each)")

    total_docs = 0
    output_info = []

    for batch_idx, batch in enumerate(batches):
        batch_num = batch_idx + 1
        logger.info(f"\n--- Batch {batch_num}/{len(batches)} ({len(batch)} files) ---")

        xml_lines  = []
        docs_in_batch = 0

        for fn, _ in tqdm(batch, desc=f"Batch {batch_num}"):
            fp    = os.path.join(JSON_INPUT_FOLDER, fn)
            lines = json_to_xml_lines(fp)
            if lines:
                xml_lines.extend(lines)
                docs_in_batch += 1

        filepath, size_mb = save_batch(xml_lines, batch_num, XML_OUTPUT_FOLDER)
        total_docs += docs_in_batch
        output_info.append((batch_num, filepath, size_mb, docs_in_batch))

        if size_mb > 500:
            logger.warning(f"  Batch {batch_num} is {size_mb:.1f} MB — EXCEEDS SketchEngine 500 MB limit!")

    print("\n" + "=" * 60)
    print(f"Total documents converted: {total_docs:,}")
    print(f"Output XML files:")
    for bn, fp, sz, dc in output_info:
        status = "OK" if sz <= 500 else "OVERSIZED"
        print(f"  Batch {bn:02d}: {sz:.1f} MB  {dc:,} docs  [{status}]")
    print(f"Output folder: {XML_OUTPUT_FOLDER}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
