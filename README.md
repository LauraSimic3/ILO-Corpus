# ILO Corpus

A century-scale corpus of English-language International Labour Organisation (ILO) publications, built from the ILO Labordoc catalogue API. The corpus comprises **53,830 documents** spanning **1919–2024**, totalling over 916 million words, and is designed to support large-scale computational analysis of ILO institutional discourse.

This repository provides the pipeline used to construct the corpus, shared metadata files, and documentation to enable replication and adaptation.

> This repository accompanies the data paper:
> *[Full citation to be added on publication]*

---

## What is in this repository

| Item | Description |
|---|---|
| `01_collect_metadata.py` | Query the ILO Labordoc API and collect bibliographic metadata |
| `02_download_pdfs.py` | Download PDFs for all catalogue records |
| `03_extract_text_to_json.py` | Extract text, detect English, match metadata |
| `04_build_and_verify.py` | Build corpus metadata CSV and verify alignment |
| `05_format_sketchengine.py` | *(Optional)* Convert JSON to SketchEngine XML format |
| `ILO_labordoc_metadata_MAR2026.csv` | Full ILO catalogue metadata (~128,000 records) — via Git LFS |
| `ILO_Corpus_metadata_MAR2026.csv` | Corpus subset metadata (53,830 documents) — via Git LFS |
| `PIPELINE_README.md` | Full step-by-step pipeline documentation |

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
```
Python 3.9+
pip install requests pandas pymupdf langdetect tqdm PyPDF2 openpyxl
python -m playwright install chromium
```

**Quick start:**
```
python 01_collect_metadata.py    # collect metadata from ILO API
python 02_download_pdfs.py       # download PDFs
python 03_extract_text_to_json.py  # extract text
python 04_build_and_verify.py    # build and verify corpus metadata
```

Output filenames are automatically dated (e.g. `ilo_labordoc_metadata_08APR2026.csv`). Steps 2–4 auto-detect the output from the previous step — no manual filename configuration needed.

---

## Metadata files

The two metadata files are stored in this repository via [Git LFS](https://git-lfs.com). To download them, either:
- Clone the repository with Git LFS installed: `git lfs install` then `git clone https://github.com/LauraSimic3/ILO-Corpus.git`
- Or download each file individually from the GitHub interface.

---

## Licence

Pipeline scripts are released under the MIT Licence.

Metadata files are derived from the ILO Labordoc catalogue API and are shared for research and reproducibility purposes. Users should satisfy themselves with the ILO's terms of use before any further redistribution. PDF files and extracted text are subject to ILO copyright and are not shared in this repository.
