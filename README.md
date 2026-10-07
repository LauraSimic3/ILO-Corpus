# ILO Corpus

**Version MAR2026.** Catalogue snapshot harvested November 2025. This release corresponds to the corpus version analysed in the accompanying paper.

A century-scale corpus of English-language International Labour Organization (ILO) publications, built from the ILO Labordoc catalogue API. The corpus comprises **53,830 documents** spanning **1919–2024**, totalling over 916 million words, and is designed to support large-scale computational analysis of ILO institutional discourse.

This repository provides the pipeline used to construct the corpus, shared metadata files, and documentation to enable replication and adaptation.

**This release contains metadata, scripts and documentation only. The text of the documents is not distributed.**

> This repository accompanies the data paper:
> *[Full citation forthcoming]*

---

## What is in this repository

| Item | Description |
|---|---|
| `01_collect_metadata.py` | Query the ILO Labordoc API and collect bibliographic metadata |
| `02_download_pdfs.py` | Download PDFs for all catalogue records |
| `03_extract_text_to_json.py` | Extract text, detect English, match metadata |
| `04_build_and_verify.py` | Build corpus metadata CSV and verify alignment |
| `05_format_sketchengine.py` | *(Optional)* Convert JSON to SketchEngine XML format |
| `ILO_labordoc_metadata_MAR2026.csv` | Labordoc catalogue metadata, all languages (128,584 rows; Year 1919–2026) — via Git LFS |
| `ILO_Corpus_metadata_MAR2026.csv` | Corpus subset metadata: the 53,830 English records dated 1919–2024 — via Git LFS |
| `PIPELINE_README.md` | Full step-by-step pipeline documentation |
| `CHANGELOG.md` | Future changes will be logged here |

---

## Corpus scope

| Property | Value |
|---|---|
| Total documents | 53,830 |
| Publication years | 1919–2024 |
| Total words | ~916 million |
| Language | English |
| Source | ILO Labordoc catalogue API |
| Document types | Reports, working papers, studies, guidelines, conference proceedings |

---

## Getting started

See [PIPELINE_README.md](PIPELINE_README.md) for full instructions.

**Requirements:**

A virtual environment is recommended to avoid dependency conflicts:
```
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
python -m playwright install chromium
```

**Quick start:**
```
python 01_collect_metadata.py    # collect metadata from ILO API
python 02_download_pdfs.py       # download PDFs
python 03_extract_text_to_json.py  # extract text
python 04_build_and_verify.py    # build and verify corpus metadata
```

Output filenames are automatically dated (e.g. `ILO_labordoc_metadata_08APR2026.csv`). Steps 2–4 auto-detect the output from the previous step — no manual filename configuration needed. File names are case-sensitive on some systems; the scripts read and write the same names as the released files (`ILO_labordoc_metadata_*.csv`, `ILO_Corpus_metadata_*.csv`).

### Starting from the released metadata, or refreshing from the live catalogue

There are two routes. Choose one.

**Route A: rebuild the MAR2026 document set (start at Script 2).** Use the released `ILO_labordoc_metadata_MAR2026.csv` and keep only the 53,830 rows flagged `IN_CORPUS = YES`. Script 2 needs the `Record ID`, `Main URL` and `Alternative URL` columns, which this file has. The corpus file `ILO_Corpus_metadata_MAR2026.csv` cannot be used as the Script 2 input because its columns are named differently (`id`, `main_url`, `alternative_url`). Script 2 tries every row of its input, so filter first:

```
mkdir mar2026_rebuild && cd mar2026_rebuild
python -c "import pandas as pd; d = pd.read_csv('../ILO_labordoc_metadata_MAR2026.csv', dtype=str, encoding='utf-8-sig'); d[d['IN_CORPUS'] == 'YES'].to_csv('ILO_labordoc_metadata_MAR2026_corpus.csv', index=False, encoding='utf-8-sig')"
python ../02_download_pdfs.py
python ../03_extract_text_to_json.py
python ../04_build_and_verify.py
```

Run the scripts from a folder that holds only this filtered file, because Steps 2–4 pick up the most recently modified `ILO_labordoc_metadata_*.csv` in the current folder. Keep the full released file safe: Step 4 rewrites the `IN_CORPUS` column of the file it finds. This route fetches the same records, but it downloads from the live ILO site today. The PDFs, and so the extracted text, can differ slightly from those used for the paper (see Known limitations).

**Route B: refresh from the live catalogue (start at Script 1).** Run `01_collect_metadata.py` and continue from Step 2. Re-running Script 1 queries the live ILO catalogue, so results may differ from the released metadata file (harvested November 2025), for example if records have been added or back-catalogued. The released files correspond to the MAR2026 corpus.

Some changes were also made to the scripts after the corpus was built, to improve the flow between steps, so a fresh run may not match the released files exactly.

### Collecting other languages

`01_collect_metadata.py` collects records in **all languages** by default. To collect one language only, set `LANGUAGE_FILTER` near the top of the script to a three-letter MARC language code, for example `LANGUAGE_FILTER = "eng"` (English), `"fre"` (French) or `"spa"` (Spanish). Records that list several languages (e.g. `eng | fre | spa`) match every language they list. The language check in Step 3 is configured for English; see `ENGLISH_THRESHOLD` and `is_english` in `03_extract_text_to_json.py` to change it.

---

## Using the output

The pipeline writes **one JSON file per document** (extracted text plus metadata) and, optionally, **XML batch files** (one `<doc>` element per document). JSON can be loaded directly with Python (`json`, `pandas`) or R (`jsonlite`). Tools that do not read JSON (for example Sketch Engine, #LancsBox X and AntConc) can take the XML or the extracted text instead.

### Language check

Step 3 applies `langdetect` to the **first 10,000 characters** of the extracted text of each document and keeps the document if the English probability is **at least 0.80**. There is one decision per document; passages in other languages elsewhere in a retained document are not removed.

### XML structure (Script 5)

- Each document is a `<doc>` element with all metadata fields as XML attributes (`&`, `<`, `>` and quotation marks in attribute values are escaped).
- **`<p>` elements are page-level segments, not paragraphs.** Script 3 leaves a blank line between pages, and Script 5 creates a `<p>` at each blank line, so a `<p>` corresponds approximately to a PDF page. Do not treat `<p>` as a paragraph boundary when querying the corpus (for example in Sketch Engine).
- `<s>` elements are created by a simple punctuation rule: a split after `.`, `!` or `?` followed by whitespace and a capital letter. Sentence boundaries are therefore approximate. Sketch Engine does its own processing after upload, so counts shown there can differ from the XML.

### The `IN_CORPUS` flag

In the Labordoc metadata file, `IN_CORPUS` records `YES` or `NO` only: whether the record is among the 53,830 corpus documents. It does not record why a record was excluded.

---

## Known limitations

- Headers, footers and page numbers remain in the extracted text.
- End-of-line hyphenation is not rejoined. Line breaks are kept in the JSON and replaced by spaces in the XML.
- Passages in other languages inside retained documents remain (the language check is one decision per document).
- `<p>` and `<s>` are approximate (see above).
- Extracted text can differ slightly from the released corpus (for example soft hyphens, or spaces inside words), depending on the PDF file and the PyMuPDF version.
- Four catalogue records appear twice in `ILO_labordoc_metadata_MAR2026.csv` (128,584 rows, 128,580 unique Record IDs). The duplicates are `995694371002676`, `991729143402676`, `991939433402676` and `991900663402676`; each appears under two `Year` values. Script 1 now removes duplicate Record IDs, but the released file is unchanged.

---

## Metadata files

The two metadata files are stored in this repository via [Git LFS](https://git-lfs.com). To download them, either:
- Clone the repository with Git LFS installed: `git lfs install` then `git clone https://github.com/LauraSimic3/ILO-Corpus.git`
- Or download each file individually from the GitHub interface.

| File | Content |
|---|---|
| `ILO_labordoc_metadata_MAR2026.csv` | Labordoc catalogue records in **all languages**: 128,584 rows, `Year` values 1919–2026. |
| `ILO_Corpus_metadata_MAR2026.csv` | The **53,830 English records dated 1919–2024** that make up the corpus. |

### Provenance of released files

Both CSV files were produced during the original build and keep some columns that the released scripts do not create. The released files were not regenerated for this release.

**`ILO_labordoc_metadata_MAR2026.csv` (29 columns).**
- Columns a fresh run of Step 1 produces (24): `Main URL`, `Record ID`, `Main Title`, `Publication Place`, `Publisher`, `Publication Date`, `Physical Description`, `Topical Subject`, `Resource URL`, `Alternative URL`, `Subtitle`, `Responsibility`, `Personal Author`, `Corporate Author`, `Abstract/Summary`, `Bibliography Note`, `Variant Title`, `Subject Source`, `System Control Number`, `Material Type`, `Language`, `ISSN/ISBN`, `Ilo Name`, `Leader (Format)`, `Year` (plus the `IN_CORPUS` column that Step 4 adds).
- Columns in the released file that the scripts do not create: `DOWNLOAD_ATTEMPTED`, `DOWNLOAD_RESULT`, `LIKELY_ENGLISH`.
- In the released file `Year` is the harvest year. A fresh run of Step 1 takes `Year` from each record's own publication date.

**`ILO_Corpus_metadata_MAR2026.csv` (33 columns).**
- Columns a fresh run of Step 4 produces (24): `id`, `title`, `publication_date`, `year`, `publisher`, `publication_place`, `personal_author`, `corporate_author`, `responsibility`, `subject`, `subtitle`, `abstract`, `variant_title`, `physical_description`, `isbn`, `bibliography_note`, `subject_source`, `system_control_number`, `leader_format`, `material_type`, `main_url`, `resource_url`, `alternative_url`, `ilo_name`.
- The released file names the author column `author` (the XML attribute name) where a fresh run writes `personal_author`.
- Columns in the released file that the scripts do not create: `batch`, `downloaded_file`, `english_text`, `lang`, `processing_date`, `processing_method`, `row_number`, `source_used`, `filename`.
- Some text values contain XML character entities such as `&apos;` and `&quot;`.

---

## Licence and terms

- The scripts and documentation are licensed under the MIT Licence (see `LICENSE`).
- The metadata files derive from the ILO Labordoc catalogue, and the ILO's terms apply: <https://www.ilo.org/rights-and-permissions>.
- ILO documents remain under ILO copyright and are not distributed.

If you use these scripts or the metadata files, please cite the accompanying paper (citation forthcoming; see `CITATION.cff`).
