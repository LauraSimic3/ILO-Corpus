"""
ILO Corpus Pipeline — Step 5: Verify Corpus Integrity
======================================================
Runs a suite of checks across the three corpus artefacts produced by this
pipeline to confirm they are fully aligned before archiving or publishing:

  (A) SketchEngine XML batch files    — from Step 4 (optional)
  (B) corpus_metadata CSV             — your curated metadata subset
  (C) ilo_metadata CSV                — full API metadata (source of truth)
  (D) Cross-source alignment          — counts and ID sets must match

Adjust the three paths below to point to your actual files.
Run this script last, after all other pipeline steps are complete.

Usage:
    python 05_verify_corpus.py
    Set XML_FOLDER, CORPUS_CSV, and ILO_CSV below before running.

Note on ilo_metadata Publication Date:
    The Publication Date field in ilo_metadata comes directly from the ILO
    API and is NOT required to be a clean 4-digit year.  Only corpus_metadata
    publication_date is validated here.  Differences between the two are
    expected and by design (see corpus pipeline documentation).
"""

import os
import sys
import re
import csv
from collections import Counter

sys.stdout.reconfigure(encoding="utf-8")

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
XML_FOLDER  = "sketchengine_xml"    # Folder containing XML batch files (Step 4)
CORPUS_CSV  = "ilo_corpus_metadata_MAR2026.csv" # Your curated corpus metadata CSV
ILO_CSV     = "ilo_labordoc_metadata_MAR2026.csv"    # Full ILO metadata CSV from Step 1

# ── HELPERS ────────────────────────────────────────────────────────────────────
results = []

def check(label, passed, detail=""):
    status = "  [PASS]" if passed else "  [FAIL]"
    results.append(passed)
    suffix = "  ->  " + detail if detail else ""
    print(f"{status}  {label}{suffix}")


def find_tag_end(content, start):
    """Scan past attributes of a <doc ...> tag to find the closing >."""
    pos   = start + 5
    in_q  = False
    qc    = None
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


BAD_DATE = re.compile(
    r"^$|^\[|^\d{3}-|^\d{4}\?|^\d{4}-\d{4}$|^[٠-٩]|\[s\.d"
)


# ── A. XML FILES ───────────────────────────────────────────────────────────────
print("=" * 62)
print("CORPUS VERIFICATION REPORT")
print("=" * 62)

if not os.path.isdir(XML_FOLDER):
    print(f"\n[A]  XML FILES — folder not found: {XML_FOLDER}")
    print("     Skipping XML checks (Step 4 / SketchEngine output not present).")
    xml_id_set   = set()
    xml_pub_years = []
    yr_min = yr_max = 0
    skip_xml = True
else:
    skip_xml = False
    print("\n[A]  XML FILES")

    xml_files     = sorted(f for f in os.listdir(XML_FOLDER) if f.endswith(".xml"))
    tmp_files     = [f for f in os.listdir(XML_FOLDER) if f.endswith(".tmp")]
    xml_ids       = {}
    xml_bad_date  = []
    xml_no_id     = []
    xml_pub_years = []
    xml_doc_total = 0

    check("No stale .tmp files in XML folder",
          len(tmp_files) == 0,
          f"{len(tmp_files)} found" if tmp_files else "")

    for fn in xml_files:
        path = os.path.join(XML_FOLDER, fn)
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
            id_m  = re.search(r'\bid="([^"]*)"', tag)
            pd_m  = re.search(r'\bpublication_date="([^"]*)"', tag)
            doc_id = id_m.group(1).strip() if id_m else None
            pub    = pd_m.group(1).strip() if pd_m else None
            cl     = content.find(b"</doc>", te)
            if cl == -1:
                break
            xml_doc_total += 1
            if not doc_id:
                xml_no_id.append(fn)
            else:
                xml_ids[doc_id] = xml_ids.get(doc_id, fn)
            if pub is None or BAD_DATE.match(pub) or not re.fullmatch(r"\d{4}", pub or ""):
                xml_bad_date.append({"id": doc_id, "pub": pub, "file": fn})
            else:
                xml_pub_years.append(int(pub))
            pos = cl + 6
            del tag
        del content

    xml_dup_ids = {k: v for k, v in xml_ids.items() if "&" in str(v)}
    xml_id_set  = set(xml_ids.keys())

    check("No docs with missing id attribute",
          len(xml_no_id) == 0,
          f"{len(xml_no_id)} found" if xml_no_id else "")
    check("No duplicate IDs across XML files",
          len(xml_dup_ids) == 0,
          f"{len(xml_dup_ids)} dupes" if xml_dup_ids else "")
    check("No bad or missing publication_date values",
          len(xml_bad_date) == 0,
          f"{len(xml_bad_date)} bad dates" if xml_bad_date else "")
    if xml_bad_date:
        for r in xml_bad_date[:10]:
            print(f"        id={r['id']}  pub='{r['pub']}'  [{r['file']}]")

    yr_min = min(xml_pub_years) if xml_pub_years else 0
    yr_max = max(xml_pub_years) if xml_pub_years else 0
    yr_future = sum(1 for y in xml_pub_years if y > 2024)
    check("publication_date range 1919–2024 (no future dates)",
          yr_min >= 1919 and yr_max <= 2024,
          f"range: {yr_min}–{yr_max}  future: {yr_future}")

    decade_ctr = Counter((y // 10) * 10 for y in xml_pub_years)
    print("      Year distribution by decade:")
    for decade in sorted(decade_ctr):
        bar = "#" * (decade_ctr[decade] // 200)
        print(f"        {decade}s: {decade_ctr[decade]:>6,}  {bar}")


# ── B. CORPUS METADATA CSV ─────────────────────────────────────────────────────
print(f"\n[B]  corpus_metadata  ({CORPUS_CSV})")

corpus_ids     = {}
corpus_dups    = []
corpus_bad_pub = []
corpus_pub_years = []

if not os.path.exists(CORPUS_CSV):
    print(f"     File not found: {CORPUS_CSV}")
else:
    with open(CORPUS_CSV, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = row.get("id", "").strip()
            pub = row.get("publication_date", "").strip()
            if rid in corpus_ids:
                corpus_dups.append(rid)
            corpus_ids[rid] = pub
            if not re.fullmatch(r"\d{4}", pub):
                corpus_bad_pub.append({"id": rid, "pub": pub})
            else:
                corpus_pub_years.append(int(pub))

    check("No duplicate IDs in corpus_metadata",
          len(corpus_dups) == 0,
          f"{len(corpus_dups)} dupes" if corpus_dups else "")
    check("No bad publication_date values in corpus_metadata",
          len(corpus_bad_pub) == 0,
          f"{len(corpus_bad_pub)} bad" if corpus_bad_pub else "")
    if corpus_bad_pub:
        for r in corpus_bad_pub[:5]:
            print(f"        id={r['id']}  pub='{r['pub']}'")
    cm_min = min(corpus_pub_years) if corpus_pub_years else 0
    cm_max = max(corpus_pub_years) if corpus_pub_years else 0
    check("corpus_metadata date range 1919–2024",
          cm_min >= 1919 and cm_max <= 2024,
          f"range: {cm_min}–{cm_max}")


# ── C. ILO METADATA CSV ────────────────────────────────────────────────────────
print(f"\n[C]  ilo_metadata  ({ILO_CSV})")

ilo_yes_ids = {}

if not os.path.exists(ILO_CSV):
    print(f"     File not found: {ILO_CSV}")
else:
    with open(ILO_CSV, "r", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            rid = row.get("Record ID", "").strip()
            if row.get("IN_CORPUS", "").strip().upper() == "YES":
                ilo_yes_ids[rid] = True

    print(f"      IN_CORPUS=YES records: {len(ilo_yes_ids):,}")
    print("      Note: ilo_metadata Publication Date is raw API data and is not")
    print("      validated here. Differences from corpus_metadata are expected.")


# ── D. CROSS-SOURCE ALIGNMENT ──────────────────────────────────────────────────
print("\n[D]  CROSS-SOURCE ALIGNMENT")

corpus_id_set = set(corpus_ids.keys())
ilo_yes_set   = set(ilo_yes_ids.keys())

if not skip_xml:
    check("XML doc count == corpus_metadata rows == ilo IN_CORPUS=YES",
          len(xml_id_set) == len(corpus_id_set) == len(ilo_yes_set),
          f"XML={len(xml_id_set):,}  corpus={len(corpus_id_set):,}  ilo={len(ilo_yes_set):,}")

    in_xml_not_corpus = xml_id_set - corpus_id_set
    in_corpus_not_xml = corpus_id_set - xml_id_set
    check("No XML IDs missing from corpus_metadata",
          len(in_xml_not_corpus) == 0,
          f"{list(in_xml_not_corpus)[:5]}" if in_xml_not_corpus else "")
    check("No corpus IDs missing from XML",
          len(in_corpus_not_xml) == 0,
          f"{list(in_corpus_not_xml)[:5]}" if in_corpus_not_xml else "")
else:
    check("corpus_metadata rows == ilo IN_CORPUS=YES",
          len(corpus_id_set) == len(ilo_yes_set),
          f"corpus={len(corpus_id_set):,}  ilo={len(ilo_yes_set):,}")

in_corpus_not_ilo = corpus_id_set - ilo_yes_set
in_ilo_not_corpus = ilo_yes_set   - corpus_id_set
check("No corpus IDs missing from ilo IN_CORPUS=YES",
      len(in_corpus_not_ilo) == 0,
      f"{list(in_corpus_not_ilo)[:5]}" if in_corpus_not_ilo else "")
check("No ilo IN_CORPUS=YES IDs absent from corpus_metadata",
      len(in_ilo_not_corpus) == 0,
      f"{list(in_ilo_not_corpus)[:5]}" if in_ilo_not_corpus else "")


# ── SUMMARY ────────────────────────────────────────────────────────────────────
print("\n" + "=" * 62)
passed = sum(results)
total  = len(results)
print(f"RESULT:  {passed}/{total} checks passed")
if corpus_id_set:
    print(f"Corpus size:            {len(corpus_id_set):,} documents")
if xml_pub_years:
    print(f"Publication date range: {yr_min} – {yr_max}")
if passed == total:
    print("\nCORPUS IS CLEAN AND FULLY ALIGNED ACROSS ALL SOURCES.")
else:
    print(f"\n{total - passed} ISSUE(S) FOUND — see [FAIL] lines above.")
print("=" * 62)
