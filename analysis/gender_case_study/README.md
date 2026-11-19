# Gender terms case study: derived outputs (Section 5)

This folder contains the derived Sketch Engine outputs behind the gender-terms case study in Section 5 of the accompanying paper: the term frequencies (Table 7), the frequencies by year (Figures 3–6), and the collocate lists (Table 8).

**No document text is included.** ILO documents are under copyright (see the main README). These files hold search queries, counts and association scores only. To check the results against the texts, rebuild the corpus with the pipeline in this repository, index it in Sketch Engine and run the queries below. A rebuilt corpus downloads the PDFs from the live ILO site, so counts may differ slightly from those reported.

## Corpus and settings

- Corpus: ILO Corpus, version MAR2026, indexed in Sketch Engine (English tokeniser and TreeTagger pipeline v2, word sketch grammar v3.3, term grammar v3.1).
- Corpus size used for normalisation: 916,887,792 words (Table 4).
- Collocations: Sketch Engine collocation function, window of ±5 words, ranked by logDice.

## Queries

Each term was searched with this CQL query, which matches the lower-cased word form or the lower-cased lemma:

```
[lc="TERM" | lemma_lc="TERM"]
```

with TERM = `woman`, `man`, `gender`, `female`, `male`, `girl`, `boy`. The exact query for each term is listed in `gender_terms_frequency.csv`.

## Files

| File | Content | Paper |
|---|---|---|
| `gender_terms_frequency.csv` | Query, total frequency and per-million-words (pmw) frequency for each term | Table 7 |
| `gender_terms_by_year.csv` | Frequency and pmw frequency for each term by publication year, 1919–2024, with the corpus word count for each year (Sketch Engine) | Figures 3–6, Section 5.3 |
| `collocations_top100_raw/` | One file per term: the top 100 collocates from Sketch Engine (Freq, Coll. freq., T-score, MI, logDice) | Table 8 (source) |
| `collocations_top100_analysed.xlsx` | The same top 100 collocates, one tab per term, each marked Kept or Excluded (with the reason for exclusion). The 15 collocates reported in Table 8 are highlighted and numbered in the `Table 8 rank` column | Table 8 |

## How Table 8 was produced

From the top 100 collocates for each term (ranked by logDice), we excluded function words, punctuation, OCR artefacts, and variant forms of the search term itself. Upper- and lower-case forms of the same word were merged, keeping the higher-ranked form. Table 8 reports the first 15 remaining collocates, which are marked in the `Table 8 rank` column.

## Licence

The derived query, frequency and collocate files in this folder are licensed under the [Creative Commons Attribution 4.0 International licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/). See `LICENSE` in this folder.
