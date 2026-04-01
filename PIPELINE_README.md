# ILO Corpus Pipeline

A reproducible pipeline for building a corpus of English-language International Labour Organisation (ILO) documents from the available ILO Labordoc catalogue API.

This repository accompanies the data paper:
> *[Full citation to be added on publication]*

---

## What this pipeline produces

| Artefact | Description |
|---|---|
| `ilo_labordoc_metadata_MAR2026.csv` | Full bibliographic metadata for ~128,000 ILO catalogue records (1900–2024) |
| `ilo_corpus_metadata_MAR2026.csv` | Curated subset — the documents actually downloaded and included in the corpus |
| PDF files | Downloaded from ILO Labordoc (not shared — see copyright note below) |
| JSON files | Extracted plain text + metadata per document |
| SketchEngine XML *(optional)* | Corpus in SketchEngine vertical format |

> **Copyright note:** PDF files and XML corpus files are not shared in this repository because the ILO retains copyright over its publications. The metadata files (`ilo_labordoc_metadata_MAR2026.csv` and `ilo_corpus_metadata_MAR2026.csv`) are shared so that others can assess the scope of the corpus and replicate or adapt the collection for their own purposes.

---

## Requirements

```
Python 3.9+
pip install requests pandas pymupdf langdetect tqdm PyPDF2 openpyxl
python -m playwright install chromium   # for PDF downloading via browser automation
```

---

## Pipeline overview

```
Step 1  01_collect_metadata.py   Query ILO API → ilo_labordoc_metadata_MAR2026.csv
Step 2  02_download_pdfs.py      Download PDFs → pdf_downloads/
Step 3  03_extract_text_to_json.py  Extract text, detect English → json_output/
Step 4  04_format_sketchengine.py   (optional) JSON → XML → sketchengine_xml/
Step 5  05_build_and_verify.py   Build corpus metadata CSV + verify alignment across all three sources
```

---

## Step-by-step instructions

### Step 1 — Collect metadata from the ILO API

**Script:** `01_collect_metadata.py`

Queries the ILO Alma SRU API year by year and saves bibliographic metadata for all English-language ILO publications.

**Configure** (top of script):
```python
START_YEAR = 1900   # first year to collect
END_YEAR   = 2024   # last year to collect
OUTPUT_CSV = "ilo_labordoc_metadata_MAR2026.csv"
```

**Run:**
```
python 01_collect_metadata.py
```

**Output:** `ilo_labordoc_metadata_MAR2026.csv` — one row per catalogue record, 24 columns including Record ID, title, URLs, publication date, author, subject, and Ilo Name (the ILO's internal call number, used for PDF filename matching in later steps).

> **API pagination:** The ILO Alma SRU API limits responses to 50 records per request. To ensure comprehensive coverage the script works around this by querying one year at a time and paginating through all 50-record batches within each year until all records for that year are retrieved. This is why the script is structured by year — without this approach, records beyond the first 50 per query would be silently missed. The API returns records in MARCXML format; the script parses this and maps the relevant MARC fields to a flat CSV structure.

> The full 1900–2024 collection produces approximately 128,000 rows and takes several hours to run. You can restrict `START_YEAR`/`END_YEAR` to collect a subset by date range.

---

### Step 2 — Download PDFs

**Script:** `02_download_pdfs.py`

Attempts to download a PDF for every row in the metadata CSV. The script uses two methods in sequence:

**Method 1 — Direct download (Alternative URL)**
The `Alternative URL` column in the metadata contains direct links to PDF files hosted on the ILO public library server (`ilo.org/public/libdoc/...`). The script attempts a straightforward HTTP GET request for these first. This is faster but not all records have a working direct URL.

**Method 2 — Browser automation (Main URL, Playwright)**
If the direct download fails or no Alternative URL is present, the script falls back to the `Main URL` — the ILO Labordoc catalogue page for that record. Playwright launches a Chromium browser, navigates to the page, and clicks the download button. This method is reliable but slow, as each download opens and closes a fresh browser instance. This was the primary method used to build the original corpus.

Downloaded files are validated with PyPDF2; any file that fails validation is deleted automatically.

> **Scope note:** This script will attempt to download PDFs for **all records** in the metadata CSV regardless of language. The metadata `Language` field is not always reliably populated, so the script does not pre-filter by language. English filtering happens in Step 3. If you want to limit the download scope (e.g. by year range, subject, or language), filter your metadata CSV before running this script.

**Configure** (top of script):
```python
METADATA_CSV      = "ilo_labordoc_metadata_MAR2026.csv"   # input: output of Step 1
PDF_OUTPUT_FOLDER = "pdf_downloads"       # where PDFs are saved
```

**Run:**
```
python 02_download_pdfs.py
```

**Output:**
- `pdf_downloads/` — one PDF file per successfully downloaded document
- `download_reports/` — per-batch success and failure CSVs
- `FINAL_<timestamp>_downloaded.csv` / `FINAL_<timestamp>_missed.csv` — master download logs

> Not all records will have a downloadable PDF — some catalogue records do not have a publicly accessible full-text document. The `FINAL_missed.csv` file records every failed attempt with the URLs tried.

> **Resumable:** The script processes rows in batches of 5,000 and saves progress after each batch. If interrupted, you can re-run it; already-downloaded PDFs are not re-downloaded (the JSON skip logic in Step 3 handles duplicates).

---

### Step 3 — Extract text and save as JSON

**Script:** `03_extract_text_to_json.py`

For each PDF in `pdf_downloads/`:
1. Extracts text using PyMuPDF
2. Detects the language using `langdetect` — skips documents where English confidence falls below the threshold
3. Matches metadata from `ilo_labordoc_metadata_MAR2026.csv` using the filename (Record ID or Ilo Name)
4. Saves a `.json` file containing the extracted text and matched metadata

**Configure** (top of script):
```python
PDF_FOLDER         = "pdf_downloads"     # input: output of Step 2
JSON_OUTPUT_FOLDER = "json_output"       # where JSON files are saved
METADATA_CSV       = "ilo_labordoc_metadata_MAR2026.csv"  # metadata for matching
ENGLISH_THRESHOLD  = 0.80                # minimum English confidence (0–1)
```

**Run:**
```
python 03_extract_text_to_json.py
```

**Output:**
- `json_output/` — one `.pdf.json` file per accepted English document
- `extract_log_<timestamp>.log` — full processing log
- `extract_failures_<timestamp>.log` — skipped files with reason (not English / no text / error)

> **The English threshold** of 0.80 means at least 80% of the detected text must be classified as English. Documents below this threshold are skipped and logged. Adjust this value if your corpus has different language requirements.

> **Metadata matching:** Because PDFs are named by Record ID in Step 2, each file is matched directly to its metadata row. Match failures should not occur under normal circumstances.

---

### Step 4 — Format for SketchEngine (optional)

**Script:** `04_format_sketchengine.py`

Converts the JSON files from Step 3 into SketchEngine vertical corpus XML format. Each document becomes a `<doc>` element with metadata stored as XML attributes, containing `<p>` (paragraph) and `<s>` (sentence) child elements.

Output is split into multiple files to stay under the SketchEngine 500 MB per-file upload limit.

**Configure** (top of script):
```python
JSON_INPUT_FOLDER = "json_output"       # input: output of Step 3
XML_OUTPUT_FOLDER = "sketchengine_xml"  # where XML batch files are saved
MAX_BATCH_SIZE_MB = 450                 # split threshold (keep under 500 MB)
```

**Run:**
```
python 04_format_sketchengine.py
```

**Output:**
- `sketchengine_xml/ILO_Corpus_Batch_01.xml`, `_02.xml`, … — SketchEngine-ready XML batch files

> This step is only needed if you intend to use the corpus in SketchEngine. The JSON files produced by Step 3 are usable independently with any other NLP toolchain.

---

### Step 5 — Build corpus metadata and verify alignment

**Script:** `05_build_and_verify.py`

This script does three things in sequence:

**1. Scan** — reads your JSON files (Step 3) or SketchEngine XML files (Step 4) and extracts the Record ID and metadata for every document that made it into your corpus. JSON is preferred as the source if both are present.

**2. Build** — writes `ilo_corpus_metadata_NEW.csv`: one row per corpus document with all available metadata fields. Also stamps `IN_CORPUS=YES` in `ilo_labordoc_metadata_MAR2026.csv` for every record present in your corpus, and `IN_CORPUS=NO` for all others.

**3. Verify** — runs cross-checks across all three sources to confirm they are fully aligned:
- Source file count (JSON/XML) == `ilo_corpus_metadata_NEW.csv` rows == `ilo_labordoc_metadata_MAR2026.csv` IN_CORPUS=YES
- All Record IDs consistent across all three sources
- No malformed publication dates in corpus metadata
- If both JSON and XML exist, their document counts match

**Configure** (top of script):
```python
JSON_FOLDER      = "json_output"                        # Step 3 output (preferred source)
XML_FOLDER       = "sketchengine_xml"                   # Step 4 output (used if no JSON)
ILO_LABORDOC_CSV = "ilo_labordoc_metadata_MAR2026.csv"  # Full metadata from Step 1
CORPUS_OUT_CSV   = "ilo_corpus_metadata_NEW.csv"        # Created by this script
```

**Run:**
```
python 05_build_and_verify.py
```

**Output:**
- `ilo_corpus_metadata_NEW.csv` — corpus metadata, one row per document
- `ilo_labordoc_metadata_MAR2026.csv` — updated in place with `IN_CORPUS` flag
- PASS/FAIL verification report printed to console

---

## Metadata files

Two metadata files are shared alongside this pipeline:

| File | Rows | Description |
|---|---|---|
| `ilo_labordoc_metadata_MAR2026.csv` | ~128,000 | Raw API output: all English ILO catalogue records 1900–2024. **Do not overwrite** — this is the authoritative source. |
| `ilo_corpus_metadata_MAR2026.csv` | 53,830 | Curated subset: documents that were successfully downloaded, passed English detection, and were included in the final corpus. Publication dates have been cleaned (brackets removed, Arabic digits converted, Year field used as fallback). |

> **Publication date note:** The `Publication Date` field in `ilo_labordoc_metadata_MAR2026.csv` is the raw API value and may contain non-standard formats (brackets, question marks, Arabic digits, date ranges). The `publication_date` field in `ilo_corpus_metadata_MAR2026.csv` is a cleaned 4-digit year. These differences are by design — see Step 3 script comments for the cleaning logic.

---

## ILO Corpus scope

| Property | Value |
|---|---|
| Total documents | 53,830 |
| Publication years | 1919–2024 |
| Language | English |
| Source | ILO Labordoc catalogue API |
| Document types | Reports, working papers, studies, guidelines, conference proceedings |

---

## Citation

If you use this pipeline or the metadata files, please cite:
> *[Full citation to be added on publication]*

---

## Licence

The pipeline scripts are released under the MIT Licence.

The metadata files (`ilo_labordoc_metadata_MAR2026.csv`, `ilo_corpus_metadata_MAR2026.csv`) are derived from the ILO Labordoc catalogue API and are shared here for research and reproducibility purposes. Users should satisfy themselves with the ILO's terms of use before any further redistribution.

PDF files and XML corpus files contain extracted text from ILO publications and are subject to ILO copyright. They are not shared in this repository.
