"""
filter_xml.py

Streams through Wikipedia stub-meta-history XML dump files and extracts
revision metadata for pages whose titles match a politician/party list.

Supports both uncompressed .xml and compressed .xml.gz inputs.
Outputs one Parquet file per input XML file.

Usage:
    python filter_xml.py <xml_file_or_glob> [--titles politician_titles.csv] [--outdir output]

Examples:
    python filter_xml.py enwiki-latest-stub-meta-history1.xml/enwiki-latest-stub-meta-history1.xml
    python filter_xml.py *.xml.gz
    python filter_xml.py data/*.xml --titles my_titles.csv --outdir results
"""

import argparse
import csv
import gzip
import glob
import os
import sys
import time
import xml.etree.ElementTree as ET
import builtins

# Force all print() calls to flush immediately (in order to monitor progress)
_original_print = builtins.print
def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _original_print(*args, **kwargs)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required. Install with:  pip install pyarrow")
    sys.exit(1)

# Wikipedia dump XML namespace
DUMP_NS = "http://www.mediawiki.org/xml/export-0.11/"


def strip_ns(tag: str) -> str:
    """Remove the XML namespace URI prefix from a tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def ns_tag(local: str) -> str:
    """Return a fully-qualified tag name with the dump namespace."""
    return f"{{{DUMP_NS}}}{local}"


def load_title_set(csv_path: str) -> set[str]:
    """Load the politician title set from the CSV produced by Stage 1."""
    titles = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            titles.add(row["wikipedia_title"])
    return titles


def open_xml(path: str):
    """Open an XML file, handling .gz compression."""
    if path.endswith(".gz"):
        return gzip.open(path, "rb")
    return open(path, "rb")


# Parquet schema
SCHEMA = pa.schema([
    ("page_id", pa.int64()),
    ("page_title", pa.string()),
    ("is_redirect", pa.bool_()),
    ("redirect_target", pa.string()),
    ("revision_id", pa.int64()),
    ("parent_id", pa.int64()),
    ("timestamp", pa.string()),
    ("contributor_username", pa.string()),
    ("contributor_id", pa.int64()),
    ("contributor_ip", pa.string()),
    ("comment", pa.string()),
    ("is_minor", pa.bool_()),
    ("text_bytes", pa.int64()),
    ("text_sha1", pa.string()),
])

BATCH_SIZE = 50_000  # rows per Arrow batch before flushing


def extract_revisions(page_elem, page_id, page_title, is_redirect, redirect_target):
    """Yield flat dicts for every <revision> inside a <page> element."""
    for child in page_elem:
        if strip_ns(child.tag) != "revision":
            continue

        row = {
            "page_id": page_id,
            "page_title": page_title,
            "is_redirect": is_redirect,
            "redirect_target": redirect_target,
            "revision_id": None,
            "parent_id": None,
            "timestamp": None,
            "contributor_username": None,
            "contributor_id": None,
            "contributor_ip": None,
            "comment": None,
            "is_minor": False,
            "text_bytes": None,
            "text_sha1": None,
        }

        for rev_child in child:
            tag = strip_ns(rev_child.tag)
            text = (rev_child.text or "").strip()

            if tag == "id" and row["revision_id"] is None:
                row["revision_id"] = int(text) if text else None
            elif tag == "parentid":
                row["parent_id"] = int(text) if text else None
            elif tag == "timestamp":
                row["timestamp"] = text or None
            elif tag == "contributor":
                for cc in rev_child:
                    ctag = strip_ns(cc.tag)
                    ctext = (cc.text or "").strip()
                    if ctag == "username":
                        row["contributor_username"] = ctext or None
                    elif ctag == "id":
                        row["contributor_id"] = int(ctext) if ctext else None
                    elif ctag == "ip":
                        row["contributor_ip"] = ctext or None
            elif tag == "comment":
                row["comment"] = text or None
            elif tag == "minor":
                row["is_minor"] = True
            elif tag == "text":
                b = rev_child.attrib.get("bytes", "")
                row["text_bytes"] = int(b) if b else None
                row["text_sha1"] = rev_child.attrib.get("sha1")
            elif tag == "sha1" and not row["text_sha1"]:
                row["text_sha1"] = text or None

        yield row


def rows_to_batch(rows: list[dict]) -> pa.RecordBatch:
    """Convert a list of row dicts into a PyArrow RecordBatch."""
    arrays = {field.name: [] for field in SCHEMA}
    for r in rows:
        for field in SCHEMA:
            arrays[field.name].append(r.get(field.name))
    return pa.RecordBatch.from_pydict(arrays, schema=SCHEMA)


def process_file(xml_path: str, title_set: set[str], out_dir: str):
    """Stream through one XML dump file and write matched pages to Parquet."""
    basename = os.path.basename(xml_path).replace(".xml.gz", "").replace(".xml", "")
    # Simplify names like "enwiki-latest-stub-meta-history1" -> "1"
    num = ""
    for ch in reversed(basename):
        if ch.isdigit():
            num = ch + num
        else:
            break
    out_name = f"politician_revisions_{num or basename}.parquet"
    out_path = os.path.join(out_dir, out_name)

    file_size = os.path.getsize(xml_path)
    file_size_gb = file_size / (1024 ** 3)
    print(f"\nProcessing: {xml_path} ({file_size_gb:.2f} GB)")
    print(f"Output:     {out_path}")

    pages_scanned = 0
    pages_matched = 0
    revisions_written = 0
    batch_buffer: list[dict] = []
    writer = None

    start_time = time.time()
    last_report = start_time

    try:
        source = open_xml(xml_path)
        context = ET.iterparse(source, events=("end",))

        for event, elem in context:
            tag = strip_ns(elem.tag)

            if tag != "page":
                continue

            pages_scanned += 1

            # Extract page-level fields
            title_elem = elem.find(ns_tag("title"))
            title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

            if title not in title_set:
                elem.clear()
                # Progress report every 30s
                now = time.time()
                if now - last_report > 30:
                    elapsed = now - start_time
                    print(
                        f"  [{elapsed:>6.0f}s] Scanned {pages_scanned:,} pages, "
                        f"matched {pages_matched:,}, "
                        f"revisions {revisions_written:,}"
                    )
                    last_report = now
                continue

            # Matched -- extract data
            pages_matched += 1
            id_elem = elem.find(ns_tag("id"))
            page_id = int(id_elem.text.strip()) if id_elem is not None and id_elem.text else 0
            ns_elem = elem.find(ns_tag("ns"))

            redirect_elem = elem.find(ns_tag("redirect"))
            is_redirect = redirect_elem is not None
            redirect_target = redirect_elem.attrib.get("title") if is_redirect else None

            for row in extract_revisions(elem, page_id, title, is_redirect, redirect_target):
                batch_buffer.append(row)
                revisions_written += 1

            # Flush batch if large enough
            if len(batch_buffer) >= BATCH_SIZE:
                batch = rows_to_batch(batch_buffer)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, SCHEMA, compression="snappy")
                writer.write_batch(batch)
                batch_buffer.clear()

            elem.clear()

            # Print each match
            rev_count = revisions_written  # approximate
            print(
                f"  MATCH: {title!r} (page_id={page_id}, "
                f"total revisions so far: {revisions_written:,})"
            )

    except KeyboardInterrupt:
        print("\nInterrupted! Flushing partial results ...")

    # Flush remaining rows
    if batch_buffer:
        batch = rows_to_batch(batch_buffer)
        if writer is None:
            writer = pq.ParquetWriter(out_path, SCHEMA, compression="snappy")
        writer.write_batch(batch)

    if writer is not None:
        writer.close()

    elapsed = time.time() - start_time
    print(f"\n  Done in {elapsed:.1f}s")
    print(f"  Pages scanned: {pages_scanned:,}")
    print(f"  Pages matched: {pages_matched:,}")
    print(f"  Revisions written: {revisions_written:,}")
    if revisions_written > 0:
        print(f"  Output: {out_path}")
    else:
        print(f"  No matches found in this file.")
        if os.path.exists(out_path):
            os.remove(out_path)

    return {
        "file": xml_path,
        "pages_scanned": pages_scanned,
        "pages_matched": pages_matched,
        "revisions_written": revisions_written,
        "elapsed_s": elapsed,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Filter Wikipedia stub-meta-history XML dumps to politician pages."
    )
    parser.add_argument(
        "xml_files",
        nargs="+",
        help="Path(s) to XML dump files (supports globs, .xml and .xml.gz)",
    )
    parser.add_argument(
        "--titles",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "politician_titles.csv"
        ),
        help="Path to politician_titles.csv (default: same directory as this script)",
    )
    parser.add_argument(
        "--outdir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "output"),
        help="Output directory for Parquet files (default: ./output)",
    )
    args = parser.parse_args()

    # Expand globs
    xml_files = []
    for pattern in args.xml_files:
        expanded = glob.glob(pattern)
        if expanded:
            xml_files.extend(expanded)
        else:
            xml_files.append(pattern)

    # Validate inputs
    if not os.path.exists(args.titles):
        print(f"ERROR: Title list not found: {args.titles}")
        print("Run fetch_politician_titles.py first to generate it.")
        sys.exit(1)

    for f in xml_files:
        if not os.path.exists(f):
            print(f"ERROR: XML file not found: {f}")
            sys.exit(1)

    os.makedirs(args.outdir, exist_ok=True)

    # Load title set
    print(f"Loading politician titles from: {args.titles}")
    title_set = load_title_set(args.titles)
    print(f"Loaded {len(title_set):,} unique titles to match against.\n")

    # Process each file
    summaries = []
    for xml_path in xml_files:
        summary = process_file(xml_path, title_set, args.outdir)
        summaries.append(summary)

    # Final summary
    print(f"\n{'=' * 60}")
    print("  OVERALL SUMMARY")
    print(f"{'=' * 60}")
    total_pages = sum(s["pages_scanned"] for s in summaries)
    total_matched = sum(s["pages_matched"] for s in summaries)
    total_revisions = sum(s["revisions_written"] for s in summaries)
    total_time = sum(s["elapsed_s"] for s in summaries)
    print(f"  Files processed:    {len(summaries)}")
    print(f"  Total pages scanned: {total_pages:,}")
    print(f"  Total pages matched: {total_matched:,}")
    print(f"  Total revisions:     {total_revisions:,}")
    print(f"  Total time:          {total_time:.1f}s ({total_time/60:.1f}m)")

    # Write matched titles for coverage analysis
    matched_titles_path = os.path.join(args.outdir, "matched_titles.txt")
    # Collect from parquet files
    matched_titles = set()
    for fname in os.listdir(args.outdir):
        if fname.endswith(".parquet"):
            try:
                table = pq.read_table(
                    os.path.join(args.outdir, fname), columns=["page_title"]
                )
                matched_titles.update(table.column("page_title").to_pylist())
            except Exception:
                pass
    with open(matched_titles_path, "w", encoding="utf-8") as f:
        for t in sorted(matched_titles):
            f.write(t + "\n")
    print(f"\n  Matched titles saved to: {matched_titles_path}")
    print(f"  Unique politician pages found: {len(matched_titles)}")


if __name__ == "__main__":
    main()
