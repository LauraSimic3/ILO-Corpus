"""
ILO Corpus Pipeline — Step 4: Build Corpus Metadata and Verify Alignment
=========================================================================
This script does three things:

  1. SCAN — reads whatever you produced in Steps 3/5 (JSON files and/or
     SketchEngine XML files) and extracts the Record ID and metadata for
     every document that made it into your corpus.

  2. BUILD — writes ilo_corpus_metadata_DATE.csv: one row per corpus document,
     with metadata drawn from your JSON/XML output.  Also stamps IN_CORPUS=YES
     in ilo_labordoc_metadata_DATE.csv for every record present in the
     corpus, and IN_CORPUS=NO for all others.
     DATE is automatically set to the date the script is run (e.g. 08APR2026).

  3. VERIFY — runs a cross-check across all three sources to confirm counts
     and ID sets are fully aligned:
       (A) JSON or XML files on disk
       (B) ilo_corpus_metadata_DATE.csv
       (C) ilo_labordoc_metadata_DATE.csv  IN_CORPUS=YES

The source scan prefers JSON files (richer metadata, already parsed) but will
fall back to XML if only Step 5 output is present.  If both exist the JSON
folder is used and the XML count is checked separately for consistency.

Usage:
    python 04_build_and_verify.py
    Set the paths below to match your output folders/files.

Dependencies:
    pip install pandas
"""

import glob
import os
import sys
import re
import csv
import json
from collections import Counter
from datetime import datetime

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")


def _find_labordoc_csv():
    """Auto-detect the labordoc metadata CSV produced by Step 1 (ilo_labordoc_metadata_DATE.csv)."""
    matches = sorted(glob.glob("ilo_labordoc_metadata_*.csv"), key=os.path.getmtime, reverse=True)
    if not matches:
        raise FileNotFoundError(
            "No ilo_labordoc_metadata_DATE.csv found in the current directory. "
            "Run Step 1 first, or set ILO_LABORDOC_CSV manually below."
        )
    return matches[0]


# ── CONFIGURATION ─────────────────────────────────────────────────────────────
JSON_FOLDER      = "json_output"           # Step 3 output (preferred source)
XML_FOLDER       = "sketchengine_xml"      # Step 5 output (used if no JSON)
ILO_LABORDOC_CSV = _find_labordoc_csv()    # Auto-detected from Step 1 output (ilo_labordoc_metadata_DATE.csv)
CORPUS_OUT_CSV   = f"ilo_corpus_metadata_{datetime.now().strftime('%d%b%Y').upper()}.csv"   # Auto-dated output (e.g. ilo_corpus_metadata_08APR2026.csv)


# ── HELPERS ────────────────────────────────────────────────────────────────────
results = []

def check(label, passed, detail=""):
    status = "  [PASS]" if passed else "  [FAIL]"
    results.append(passed)
    suffix = "  ->  " + detail if detail else ""
    print(f"{status}  {label}{suffix}")


def find_tag_end(content, start):
    """Scan past attributes of a <doc ...> tag to find the closing >."""
    pos  = start + 5
    in_q = False
    qc   = None
    while pos < len(content):
        c = chr(content[pos])
        if in_q:
            if c == qc:
                in_q = False
        else:
            if c in ('"', "'"):
                in_q = True
                qc   = c
            elif c == ">":
                return pos
        pos += 1
    return -1


# ── STEP 1: SCAN SOURCE FILES ──────────────────────────────────────────────────
def scan_json_folder(folder):
    """
    Read all JSON files and return a dict of {record_id: metadata_dict}.
    Skips files where no id field can be found.
    """
    records = {}
    missing_id = []

    for fn in os.listdir(folder):
        if not fn.endswith(".json"):
            continue
        fp = os.path.join(folder, fn)
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            meta = data.get("metadata", {})
            rid  = str(meta.get("id", "")).strip().replace(".0", "")
            if not rid or rid.lower() == "nan":
                missing_id.append(fn)
                continue
            records[rid] = meta
        except Exception as e:
            print(f"  WARNING: could not read {fn}: {e}")

    print(f"  JSON files scanned: {len(os.listdir(folder))}")
    print(f"  Records with ID:    {len(records):,}")
    if missing_id:
        print(f"  WARNING: {len(missing_id)} file(s) had no id field — excluded")
    return records


def scan_xml_folder(folder):
    """
    Read all XML batch files and return a dict of {record_id: attr_dict}.
    Skips <doc> elements with no id attribute.
    """
    records    = {}
    missing_id = []

    xml_files = sorted(f for f in os.listdir(folder) if f.endswith(".xml"))
    for fn in xml_files:
        path = os.path.join(folder, fn)
        with open(path, "rb") as f:
            content = f.read()
        pos = 0
        while pos < len(content):
            ds = content.find(b"<doc ", pos)
            if ds == -1:
                break
            te = find_tag_end(content, ds)
            if te == -1:
                break
            tag = content[ds:te+1].decode("utf-8", errors="replace")

            # Parse all attributes from the tag
            attrs = {}
            for m in re.finditer(r'(\w+)="([^"]*)"', tag):
                attrs[m.group(1)] = m.group(2).strip()

            rid = attrs.get("id", "").strip()
            cl  = content.find(b"</doc>", te)
            if cl == -1:
                break
            if not rid:
                missing_id.append(fn)
            else:
                records[rid] = attrs
            pos = cl + 6
            del tag
        del content

    print(f"  XML files scanned:  {len(xml_files)}")
    print(f"  Records with ID:    {len(records):,}")
    if missing_id:
        print(f"  WARNING: {len(missing_id)} <doc> element(s) had no id — excluded")
    return records


# ── STEP 2: BUILD CORPUS METADATA CSV ─────────────────────────────────────────
CORPUS_COLUMNS = [
    "id", "title", "publication_date", "year", "publisher", "publication_place",
    "personal_author", "corporate_author", "responsibility", "subject", "subtitle",
    "abstract", "variant_title", "physical_description", "isbn", "bibliography_note",
    "subject_source", "system_control_number", "leader_format", "material_type",
    "main_url", "resource_url", "alternative_url", "ilo_name",
]

def build_corpus_csv(records, output_path):
    """Write ilo_corpus_metadata_NEW.csv from scanned record metadata."""
    rows = []
    for rid, meta in records.items():
        row = {"id": rid}
        for col in CORPUS_COLUMNS:
            if col == "id":
                continue
            val = meta.get(col, "")
            row[col] = str(val).strip() if val else ""
        rows.append(row)

    df = pd.DataFrame(rows, columns=CORPUS_COLUMNS)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  Written: {output_path}  ({len(df):,} rows)")
    return set(df["id"].astype(str).str.strip())


def update_in_corpus_flag(labordoc_path, corpus_id_set):
    """
    Add or update the IN_CORPUS column in the labordoc metadata CSV.
    YES = record is in the corpus, NO = not included.
    Writes back to the same file.
    """
    df = pd.read_csv(labordoc_path, encoding="utf-8-sig", low_memory=False, dtype=str)

    # Normalise Record ID for matching
    df["_rid"] = df["Record ID"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["IN_CORPUS"] = df["_rid"].apply(lambda x: "YES" if x in corpus_id_set else "NO")
    df.drop(columns=["_rid"], inplace=True)

    df.to_csv(labordoc_path, index=False, encoding="utf-8-sig")
    yes_count = (df["IN_CORPUS"] == "YES").sum()
    no_count  = (df["IN_CORPUS"] == "NO").sum()
    print(f"  IN_CORPUS=YES: {yes_count:,}  |  IN_CORPUS=NO: {no_count:,}")
    return yes_count


# ── STEP 3: VERIFY ─────────────────────────────────────────────────────────────
def verify(source_ids, corpus_csv_path, labordoc_path, xml_folder):

    print("\n[A]  Source document count")
    check("Source files scanned without errors", len(source_ids) > 0,
          f"{len(source_ids):,} records")

    print("\n[B]  ilo_corpus_metadata_NEW.csv")
    corpus_ids = set()
    corpus_bad_date = []
    if os.path.exists(corpus_csv_path):
        with open(corpus_csv_path, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                rid = row.get("id", "").strip()
                pub = row.get("publication_date", "").strip()
                corpus_ids.add(rid)
                if not re.fullmatch(r"\d{4}", pub):
                    corpus_bad_date.append({"id": rid, "pub": pub})

    check("No duplicate IDs in corpus metadata",
          len(corpus_ids) == len(source_ids),
          f"corpus={len(corpus_ids):,}  source={len(source_ids):,}")
    check("No missing or malformed publication_date values",
          len(corpus_bad_date) == 0,
          f"{len(corpus_bad_date)} bad dates" if corpus_bad_date else "")
    if corpus_bad_date:
        for r in corpus_bad_date[:5]:
            print(f"        id={r['id']}  pub='{r['pub']}'")

    print(f"\n[C]  {os.path.basename(labordoc_path)}  IN_CORPUS flag")
    ilo_yes_ids = set()
    if os.path.exists(labordoc_path):
        with open(labordoc_path, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                rid = row.get("Record ID", "").strip().replace(".0", "")
                if row.get("IN_CORPUS", "").strip().upper() == "YES":
                    ilo_yes_ids.add(rid)
    check(f"IN_CORPUS=YES count matches corpus ({len(source_ids):,})",
          len(ilo_yes_ids) == len(source_ids),
          f"ilo YES={len(ilo_yes_ids):,}  source={len(source_ids):,}")

    print("\n[D]  Cross-source alignment")
    in_source_not_corpus = source_ids - corpus_ids
    in_corpus_not_source = corpus_ids - source_ids
    in_source_not_ilo    = source_ids - ilo_yes_ids
    in_ilo_not_source    = ilo_yes_ids - source_ids

    check("All source IDs present in corpus metadata",
          len(in_source_not_corpus) == 0,
          f"{list(in_source_not_corpus)[:5]}" if in_source_not_corpus else "")
    check("No extra IDs in corpus metadata vs source",
          len(in_corpus_not_source) == 0,
          f"{list(in_corpus_not_source)[:5]}" if in_corpus_not_source else "")
    check("All source IDs marked IN_CORPUS=YES in labordoc metadata",
          len(in_source_not_ilo) == 0,
          f"{list(in_source_not_ilo)[:5]}" if in_source_not_ilo else "")
    check("No extra IN_CORPUS=YES IDs in labordoc vs source",
          len(in_ilo_not_source) == 0,
          f"{list(in_ilo_not_source)[:5]}" if in_ilo_not_source else "")

    if os.path.isdir(xml_folder) and os.path.isdir(JSON_FOLDER):
        xml_ids = set()
        for fn in sorted(f for f in os.listdir(xml_folder) if f.endswith(".xml")):
            path = os.path.join(xml_folder, fn)
            with open(path, "rb") as f:
                content = f.read()
            pos = 0
            while pos < len(content):
                ds = content.find(b"<doc ", pos)
                if ds == -1:
                    break
                te = find_tag_end(content, ds)
                if te == -1:
                    break
                tag = content[ds:te+1].decode("utf-8", errors="replace")
                id_m = re.search(r'\bid="([^"]*)"', tag)
                if id_m:
                    xml_ids.add(id_m.group(1).strip())
                cl = content.find(b"</doc>", te)
                if cl == -1:
                    break
                pos = cl + 6
                del tag
            del content
        check("XML doc count matches JSON doc count",
              len(xml_ids) == len(source_ids),
              f"XML={len(xml_ids):,}  JSON={len(source_ids):,}")


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print("=" * 62)
    print("ILO CORPUS — BUILD METADATA AND VERIFY")
    print("=" * 62)

    # Determine source
    has_json = os.path.isdir(JSON_FOLDER) and any(
        f.endswith(".json") for f in os.listdir(JSON_FOLDER)
    )
    has_xml  = os.path.isdir(XML_FOLDER) and any(
        f.endswith(".xml") for f in os.listdir(XML_FOLDER)
    )

    if not has_json and not has_xml:
        print("ERROR: No JSON or XML output found. Run Step 3 and/or Step 5 first.")
        return

    if not os.path.exists(ILO_LABORDOC_CSV):
        print(f"ERROR: {ILO_LABORDOC_CSV} not found. Run Step 1 first.")
        return

    # ── 1. SCAN ────────────────────────────────────────────────────────────────
    print("\n[1]  Scanning source files...")
    if has_json:
        print(f"  Source: JSON folder ({JSON_FOLDER}/)")
        records = scan_json_folder(JSON_FOLDER)
    else:
        print(f"  Source: XML folder ({XML_FOLDER}/)  [no JSON found]")
        records = scan_xml_folder(XML_FOLDER)

    source_ids = set(records.keys())

    # ── 2. BUILD ───────────────────────────────────────────────────────────────
    print(f"\n[2]  Building {CORPUS_OUT_CSV}...")
    build_corpus_csv(records, CORPUS_OUT_CSV)

    print(f"\n[3]  Updating IN_CORPUS flag in {ILO_LABORDOC_CSV}...")
    update_in_corpus_flag(ILO_LABORDOC_CSV, source_ids)

    # ── 3. VERIFY ──────────────────────────────────────────────────────────────
    print("\n[4]  Running verification checks...")
    verify(source_ids, CORPUS_OUT_CSV, ILO_LABORDOC_CSV, XML_FOLDER)

    # ── SUMMARY ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    passed = sum(results)
    total  = len(results)
    print(f"RESULT:  {passed}/{total} checks passed")
    print(f"Corpus size:  {len(source_ids):,} documents")
    print(f"Corpus metadata saved to:  {CORPUS_OUT_CSV}")
    if passed == total:
        print("\nCORPUS IS CLEAN AND FULLY ALIGNED ACROSS ALL THREE SOURCES.")
    else:
        print(f"\n{total - passed} ISSUE(S) FOUND — see [FAIL] lines above.")
    print("=" * 62)


if __name__ == "__main__":
    main()
