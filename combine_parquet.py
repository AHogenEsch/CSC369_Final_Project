"""
combine_parquet.py

Reads all individual politician_revisions_*.parquet files from the output
directory and writes a single combined Parquet file.
"""

import os
import sys
import time

try:
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required.  pip install pyarrow")
    sys.exit(1)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
COMBINED_FILE = os.path.join(OUTPUT_DIR, "all_politician_revisions.parquet")


def main():
    files = sorted([
        os.path.join(OUTPUT_DIR, f)
        for f in os.listdir(OUTPUT_DIR)
        if f.startswith("politician_revisions_") and f.endswith(".parquet")
    ])

    if not files:
        print("No parquet files found in output/")
        sys.exit(1)

    print(f"Found {len(files)} parquet files to combine.")

    start = time.time()
    tables = []
    total_rows = 0
    for f in files:
        t = pq.read_table(f)
        total_rows += len(t)
        tables.append(t)
        print(f"  Read {os.path.basename(f)}: {len(t):,} rows", flush=True)

    import pyarrow as pa
    combined = pa.concat_tables(tables)
    print(f"\nCombined: {len(combined):,} total rows")

    pq.write_table(combined, COMBINED_FILE, compression="snappy")
    size_mb = os.path.getsize(COMBINED_FILE) / (1024 ** 2)
    elapsed = time.time() - start
    print(f"Written to: {COMBINED_FILE}")
    print(f"File size: {size_mb:.1f} MB")
    print(f"Done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
