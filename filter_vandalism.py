"""
filter_vandalism.py

Detects vandalism events in politician Wikipedia revision data using:
  1. SHA1 revert detection (primary): when a revision's content hash
     matches a previous non-adjacent revision, intermediate edits are vandalism.
  2. Keyword detection (secondary): edit summaries containing revert/vandalism
     keywords confirm the detection method.

Input:  output/all_politician_revisions.parquet, politician_parties.csv
Output: output/vandalism_events.parquet
"""

import csv
import os
import sys
import time
from datetime import datetime

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required.  pip install pyarrow")
    sys.exit(1)

import builtins
_original_print = builtins.print
def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _original_print(*args, **kwargs)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PARQUET = os.path.join(SCRIPT_DIR, "output", "all_politician_revisions.parquet")
PARTIES_CSV = os.path.join(SCRIPT_DIR, "politician_parties.csv")
OUTPUT_PARQUET = os.path.join(SCRIPT_DIR, "output", "vandalism_events.parquet")

REVERT_KEYWORDS = {"revert", "rvv", "rv/v", "undid", "rollback", "vandal"}

OUTPUT_SCHEMA = pa.schema([
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
    ("event_type", pa.string()),
    ("vandalism_group_id", pa.int64()),
    ("restored_to_revision_id", pa.int64()),
    ("restoration_time_seconds", pa.float64()),
    ("detection_method", pa.string()),
    ("party_group", pa.string()),
])

BATCH_SIZE = 100_000


def has_revert_keyword(comment: str | None) -> bool:
    if not comment:
        return False
    cl = comment.lower()
    return any(kw in cl for kw in REVERT_KEYWORDS)


def load_party_map(csv_path: str) -> dict[str, str]:
    party_map = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            party_map[row["wikipedia_title"]] = row["party_group"]
    return party_map


def parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def detect_vandalism_for_page(revisions: list[dict], party_group: str, group_id_start: int):
    """
    Walk through a page's revisions in chronological order.
    Detect SHA1 reverts and tag intermediate edits as vandalism.
    Returns a list of output row dicts and the next group_id to use.
    """
    if len(revisions) < 3:
        return [], group_id_start

    # Sort by timestamp to ensure chronological order
    revisions.sort(key=lambda r: r.get("timestamp") or "")

    output_rows = []
    group_id = group_id_start

    # Map sha1 -> index of most recent occurrence in the revision list
    sha1_index: dict[str, int] = {}

    for i, rev in enumerate(revisions):
        sha1 = rev.get("text_sha1")
        if not sha1:
            continue

        if sha1 in sha1_index:
            prev_idx = sha1_index[sha1]

            # Skip if it's the immediately preceding revision (not a revert)
            if prev_idx == i - 1:
                sha1_index[sha1] = i
                continue

            # Skip if all intermediate revisions have the same sha1 (metadata edits)
            intermediate = revisions[prev_idx + 1 : i]
            if all(r.get("text_sha1") == sha1 for r in intermediate):
                sha1_index[sha1] = i
                continue

            # This is a revert. Tag intermediate edits as vandalism.
            restoration_ts = parse_ts(rev.get("timestamp"))
            keyword_in_restorer = has_revert_keyword(rev.get("comment"))

            for vandal_rev in intermediate:
                vandal_ts = parse_ts(vandal_rev.get("timestamp"))
                rest_time = None
                if restoration_ts and vandal_ts:
                    rest_time = (restoration_ts - vandal_ts).total_seconds()

                vandal_has_keyword = has_revert_keyword(vandal_rev.get("comment"))
                if keyword_in_restorer:
                    method = "both"
                else:
                    method = "sha1_revert"

                row = dict(vandal_rev)
                row["event_type"] = "vandalism"
                row["vandalism_group_id"] = group_id
                row["restored_to_revision_id"] = None
                row["restoration_time_seconds"] = rest_time
                row["detection_method"] = method
                row["party_group"] = party_group
                output_rows.append(row)

            # Tag the restoring revision
            restore_row = dict(rev)
            restore_row["event_type"] = "restoration"
            restore_row["vandalism_group_id"] = group_id
            restore_row["restored_to_revision_id"] = revisions[prev_idx].get("revision_id")
            restore_row["restoration_time_seconds"] = None
            restore_row["detection_method"] = "both" if keyword_in_restorer else "sha1_revert"
            restore_row["party_group"] = party_group
            output_rows.append(restore_row)

            group_id += 1

        sha1_index[sha1] = i

    return output_rows, group_id


def rows_to_batch(rows: list[dict]) -> pa.RecordBatch:
    arrays = {field.name: [] for field in OUTPUT_SCHEMA}
    for r in rows:
        for field in OUTPUT_SCHEMA:
            arrays[field.name].append(r.get(field.name))
    return pa.RecordBatch.from_pydict(arrays, schema=OUTPUT_SCHEMA)


def main():
    if not os.path.exists(INPUT_PARQUET):
        print(f"ERROR: Input not found: {INPUT_PARQUET}")
        sys.exit(1)
    if not os.path.exists(PARTIES_CSV):
        print(f"ERROR: Party data not found: {PARTIES_CSV}")
        print("Run fetch_parties.py first.")
        sys.exit(1)

    print("Loading party affiliations ...")
    party_map = load_party_map(PARTIES_CSV)
    print(f"  {len(party_map):,} title -> party mappings loaded.")

    print("Loading revision data ...")
    table = pq.read_table(INPUT_PARQUET)
    print(f"  {len(table):,} total revisions across {len(table.column('page_title').unique()):,} pages.")

    # Group revisions by page_title
    print("Grouping revisions by page ...")
    page_groups: dict[str, list[dict]] = {}
    columns = table.column_names
    for i in range(len(table)):
        row = {col: table.column(col)[i].as_py() for col in columns}
        title = row["page_title"]
        if title not in page_groups:
            page_groups[title] = []
        page_groups[title].append(row)

    print(f"  {len(page_groups):,} unique pages.")

    # Process each page
    print("\nDetecting vandalism ...")
    start_time = time.time()
    group_id = 0
    all_vandalism_rows = []
    pages_processed = 0
    pages_with_vandalism = 0
    last_report = start_time

    writer = None

    for title, revisions in page_groups.items():
        party = party_map.get(title, "Other")
        events, group_id = detect_vandalism_for_page(revisions, party, group_id)

        if events:
            pages_with_vandalism += 1
            all_vandalism_rows.extend(events)

        pages_processed += 1

        # Flush periodically
        if len(all_vandalism_rows) >= BATCH_SIZE:
            batch = rows_to_batch(all_vandalism_rows)
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT_PARQUET, OUTPUT_SCHEMA, compression="snappy")
            writer.write_batch(batch)
            all_vandalism_rows.clear()

        now = time.time()
        if now - last_report > 15:
            elapsed = now - start_time
            print(
                f"  [{elapsed:>5.0f}s] {pages_processed:,}/{len(page_groups):,} pages, "
                f"{pages_with_vandalism:,} with vandalism, "
                f"{group_id:,} vandalism groups"
            )
            last_report = now

    # Flush remaining
    if all_vandalism_rows:
        batch = rows_to_batch(all_vandalism_rows)
        if writer is None:
            writer = pq.ParquetWriter(OUTPUT_PARQUET, OUTPUT_SCHEMA, compression="snappy")
        writer.write_batch(batch)

    if writer is not None:
        writer.close()

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Pages processed:       {pages_processed:,}")
    print(f"  Pages with vandalism:  {pages_with_vandalism:,}")
    print(f"  Vandalism groups:      {group_id:,}")

    if os.path.exists(OUTPUT_PARQUET):
        result = pq.read_table(OUTPUT_PARQUET)
        vandalism_count = sum(1 for i in range(len(result)) if result.column("event_type")[i].as_py() == "vandalism")
        restoration_count = sum(1 for i in range(len(result)) if result.column("event_type")[i].as_py() == "restoration")
        size_mb = os.path.getsize(OUTPUT_PARQUET) / (1024 ** 2)
        print(f"  Total vandalism edits: {vandalism_count:,}")
        print(f"  Total restorations:    {restoration_count:,}")
        print(f"  Output size:           {size_mb:.1f} MB")
        print(f"  Output: {OUTPUT_PARQUET}")
    else:
        print("  No vandalism detected.")


if __name__ == "__main__":
    main()
