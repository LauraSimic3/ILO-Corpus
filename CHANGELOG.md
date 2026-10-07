# Changelog

## MAR2026

Release corresponding to the corpus version analysed in the accompanying paper (catalogue snapshot harvested November 2025). The released CSV files and the text output of Scripts 3 and 5 are unchanged, except for the attribute escaping in Script 5 noted below.

### Scripts

- **Script 1 (`01_collect_metadata.py`)**
  - Queries one publication year per request (`main_pub_date` >= year and <= year). Previously each query covered the year and the following year, so years overlapped.
  - New `LANGUAGE_FILTER` setting at the top of the script. Default `None` collects all languages (matching the released all-language file). Set it to a three-letter code such as `"eng"` for one language. Previously the query was hard-wired to English.
  - Removes duplicate Record IDs before saving.
  - `Year` is taken from each record's own publication date, with the query year used only when the date contains no year. Previously `Year` was the query year.
- **Filenames (Scripts 1–4)**: names read and written now match the released files exactly, including case: `ILO_labordoc_metadata_DATE.csv` and `ILO_Corpus_metadata_DATE.csv`.
- **Script 5 (`05_format_sketchengine.py`)**: the docstring and a code comment now say `<p>` elements are page-level segments, not paragraphs. Quotation marks and apostrophes in attributes were already escaped. No other code changed.
- **Script 5 escaping:** Script 5 now escapes `&`, `<` and `>` in metadata attributes so that the `<doc>` tags are valid XML. Output for records without those characters is unchanged (unchanged for all other records).
- **Script 3 (`03_extract_text_to_json.py`)**: Record IDs of 12 to 15 digits in PDF filenames are now matched to the metadata (previously only 15-digit IDs matched, so records with shorter IDs, 693 in the corpus, were saved without their metadata). The extracted text is unchanged.
- **`requirements.txt`**: `pymupdf` pinned to 1.27.2.3, the version named in the paper. Other pins checked against the paper (requests 2.32.3, pandas 2.2.3, PyPDF2 3.0.1, langdetect 1.0.9, playwright 1.55.0) and unchanged.

### Documentation

- README: version and snapshot statement; corrected description of both CSV files (Labordoc file holds all languages, 128,584 rows, `Year` 1919–2026; corpus file holds the 53,830 English records dated 1919–2024); statement that document text is not distributed; "Using the output"; language-check description (first 10,000 characters, probability at least 0.80, one decision per document); `<p>` described as page-level segments and `<s>` as a punctuation rule; `IN_CORPUS` is yes/no only; how to collect other languages; "Known limitations"; "Provenance of released files".
- PIPELINE_README: same corrections; `<p>` is a page-level segment, approximately a PDF page, not a paragraph.
- Added `LICENSE` (MIT, for code and documentation) and `CITATION.cff` (citation forthcoming).
- README: "Licence and terms" section (MIT for scripts and documentation; the metadata files derive from the ILO Labordoc catalogue and the ILO's terms apply; ILO documents remain under ILO copyright and are not distributed). No licence is assigned to the metadata files.
- README: two routes (A: rebuild the MAR2026 document set from Script 2 with the released metadata; B: refresh from the live catalogue with Script 1) and a note that re-running Script 1 queries the live catalogue, so results may differ from the released file.
- README "Known limitations": small differences in extracted text from the released corpus.
