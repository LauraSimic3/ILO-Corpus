import requests
import xml.etree.ElementTree as ET
import pandas as pd

BASE_URL = (
    "https://ilo.alma.exlibrisgroup.com/view/sru/41ILO_INST"
    "?version=1.2&operation=searchRetrieve&recordSchema=marcxml"
    "&maximumRecords=50&startRecord={start}"  # <-- changed from 50 to 5
    "&query=alma.subjects=%22ILO%20pub%22%20AND%20alma.language=%22eng%22"
    "%20AND%20alma.main_pub_date%3E%3D%22{year}%22"
    "%20AND%20alma.main_pub_date%3C%3D%22{next_year}%22"
    "&sortBy=alma.main_pub_date/sort.ascending"
)

def fetch_records(year, start=1):
    url = BASE_URL.format(start=start, year=year, next_year=year + 1)
    response = requests.get(url)
    response.raise_for_status()
    return ET.fromstring(response.content)

def get_subfield(record, tag, code, ns):
    field = record.find(f".//marc:datafield[@tag='{tag}']/marc:subfield[@code='{code}']", ns)
    return field.text if field is not None else ""

def parse_marcxml(record_elem, record_identifier):
    ns = {"marc": "http://www.loc.gov/MARC21/slim"}

    def get_first_nonempty(options):
        for tag, code in options:
            val = get_subfield(record_elem, tag, code, ns)
            if val:
                return val
        return ""

    leader = record_elem.find("marc:leader", ns)
    control_001 = record_elem.find(".//marc:controlfield[@tag='001']", ns)
    system_id = get_subfield(record_elem, "035", "a", ns)

    return {
        "Main URL": f"https://labordoc.ilo.org/discovery/delivery/41ILO_INST:41ILO_V2/{record_identifier}",
        "Record ID": control_001.text if control_001 is not None else "",
        "Main Title": get_subfield(record_elem, "245", "a", ns),
        "Publication Place": get_first_nonempty([("260", "a"), ("264", "a")]),
        "Publisher": get_first_nonempty([("260", "b"), ("264", "b")]),
        "Publication Date": get_first_nonempty([("260", "c"), ("264", "c")]),
        "Physical Description": get_subfield(record_elem, "300", "a", ns),
        "Topical Subject": get_subfield(record_elem, "650", "a", ns),
        "Resource URL": get_subfield(record_elem, "856", "u", ns),
        "Alternative URL": get_subfield(record_elem, "856", "u", ns),
        "Subtitle": get_subfield(record_elem, "245", "b", ns),
        "Responsibility": get_subfield(record_elem, "245", "c", ns),
        "Personal Author": get_subfield(record_elem, "100", "a", ns),
        "Corporate Author": get_first_nonempty([("110", "a"), ("710", "a")]),
        "Abstract/Summary": get_subfield(record_elem, "520", "a", ns),
        "Bibliography Note": get_subfield(record_elem, "504", "a", ns),
        "Variant Title": get_subfield(record_elem, "246", "a", ns),
        "Subject Source": get_subfield(record_elem, "650", "2", ns),
        "System Control Number": system_id,
        "Material Type": get_subfield(record_elem, "245", "h", ns),
        "Language": get_subfield(record_elem, "041", "a", ns),
        "ISSN/ISBN": get_first_nonempty([("022", "a"), ("020", "a")]),
        "Ilo Name": get_subfield(record_elem, "AVA", "d", ns),  # New field added
        "Leader (Format)": leader.text if leader is not None else ""
    }

def collect_year_metadata(year):
    all_records = []
    start = 1

    # Only fetch the first batch of 5 records for each year
    root = fetch_records(year, start)

    num_records_elem = root.find(".//{http://www.loc.gov/zing/srw/}numberOfRecords")
    if num_records_elem is None:
        return all_records
    total_records = int(num_records_elem.text)

    records = root.findall(".//{http://www.loc.gov/zing/srw/}record")
    if not records:
        return all_records

    for rec in records:
        record_identifier_elem = rec.find(".//{http://www.loc.gov/zing/srw/}recordIdentifier")
        record_identifier = record_identifier_elem.text if record_identifier_elem is not None else ""
        marc_record = rec.find(".//{http://www.loc.gov/MARC21/slim}record")
        if marc_record is not None:
            data = parse_marcxml(marc_record, record_identifier)
            data["Year"] = year
            all_records.append(data)

    print(f"Fetched {len(all_records)} / {total_records} records for {year}")

    # Do not fetch more batches; only the first 5
    return all_records

if __name__ == "__main__":
    START_YEAR = 1900
    END_YEAR = 2025

    final_records = []
    for y in range(START_YEAR, END_YEAR + 1):
        final_records.extend(collect_year_metadata(y))

    if final_records:
        df = pd.DataFrame(final_records)
        df.to_excel("ilo_all_years_metadata.xlsx", index=False)
        print(f"Saved {len(final_records)} records to ilo_all_years_metadata.xlsx")
    else:
        print("No records found for any year.")
