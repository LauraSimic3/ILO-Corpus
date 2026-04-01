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
playwright install chromium   # for PDF downloading via browser automation
```

---

## Pipeline overview

```
Step 1  01_collect_metadata.py   Query ILO API → ilo_labordoc_metadata_MAR2026.csv
Step 2  02_download_pdfs.py      Download PDFs → pdf_downloads/
Step 3  03_extract_text_to_json.py  Extract text, detect English → json_output/
Step 4  04_format_sketchengine.py   (optional) JSON → XML → sketchengine_xml/
Step 5  05_verify_corpus.py      Verify alignment across all artefacts
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

> The full 1900–2024 collection produces approximately 128,000 rows and takes several hours to run due to API pagination. You can restrict `START_YEAR`/`END_YEAR` to collect a subset by date range.

---

### Step 2 — Download PDFs

**Script:** `02_download_pdfs.py`

Downloads a PDF for each row in the metadata CSV. Tries the `Alternative URL` column first (direct HTTP download), then falls back to the `Main URL` column using Playwright browser automation to click the ILO Labordoc download button.

Downloaded files are validated with PyPDF2; any file that fails validation is deleted automatically.

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

> Not all records in `ilo_labordoc_metadata_MAR2026.csv` will have a downloadable PDF. Expect a success rate of approximately 40–50% depending on the year range. The `FINAL_missed.csv` file records every failed attempt with the URLs tried.

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

> **Metadata matching:** The script tries to match each PDF filename to a row in `ilo_labordoc_metadata_MAR2026.csv` using the ILO Record ID (15-digit number) or the Ilo Name (ILO call number, e.g. `09466(2008-2)`). Files that cannot be matched still produce a JSON file but with minimal metadata (filename only).

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

### Step 5 — Verify corpus integrity

**Script:** `05_verify_corpus.py`

Runs a verification suite confirming that:
- XML files contain no missing IDs, duplicate IDs, or malformed publication dates
- `ilo_corpus_metadata_MAR2026.csv` has no duplicate IDs or invalid dates
- `ilo_labordoc_metadata_MAR2026.csv` IN_CORPUS=YES count matches `ilo_corpus_metadata_MAR2026.csv` row count
- ID sets are fully aligned across all three sources

**Configure** (top of script):
```python
XML_FOLDER  = "sketchengine_xml"    # Step 4 output (or skip if not using SketchEngine)
CORPUS_CSV  = "ilo_corpus_metadata_MAR2026.csv" # your curated metadata subset
ILO_CSV     = "ilo_labordoc_metadata_MAR2026.csv"    # full ILO metadata from Step 1
```

**Run:**
```
python 05_verify_corpus.py
```

The script prints a PASS/FAIL result for each check and a final summary. If all checks pass, the corpus is clean and fully aligned.

> **Note on `ilo_corpus_metadata_MAR2026.csv`:** This file is not produced automatically by the pipeline. It is the curated, cleaned metadata subset you create from `ilo_labordoc_metadata_MAR2026.csv` corresponding to the documents you actually downloaded and accepted. The `IN_CORPUS` column in `ilo_labordoc_metadata_MAR2026.csv` is the flag that marks which records are included. See the data paper for details on the curation decisions made for this corpus.

---

## Metadata files

Two metadata files are shared alongside this pipeline:

| File | Rows | Description |
|---|---|---|
| `ilo_labordoc_metadata_MAR2026.csv` | ~128,000 | Raw API output: all English ILO catalogue records 1900–2024. **Do not overwrite** — this is the authoritative source. |
| `ilo_corpus_metadata_MAR2026.csv` | 53,830 | Curated subset: documents that were successfully downloaded, passed English detection, and were included in the final corpus. Publication dates have been cleaned (brackets removed, Arabic digits converted, Year field used as fallback). |

> **Publication date note:** The `Publication Date` field in `ilo_labordoc_metadata_MAR2026.csv` is the raw API value and may contain non-standard formats (brackets, question marks, Arabic digits, date ranges). The `publication_date` field in `ilo_corpus_metadata_MAR2026.csv` is a cleaned 4-digit year. These differences are by design — see Step 3 script comments for the cleaning logic.

---

## Corpus scope

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
The metadata files (`ilo_labordoc_metadata_MAR2026.csv`, `ilo_corpus_metadata_MAR2026.csv`) are released under CC BY 4.0.
PDF files and XML corpus files are subject to ILO copyright and are not shared.
