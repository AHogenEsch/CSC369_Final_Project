"""
coverage_report.py

After running filter_xml.py on one or more dump files, this script
compares the matched politician pages against the full title list to
report coverage: how many expected pages were found, which are missing,
and basic stats about the extracted revisions.

Usage:
    python coverage_report.py [--titles politician_titles.csv] [--outdir output]
"""

import argparse
import csv
import os
import sys

try:
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required.  pip install pyarrow")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Coverage report for politician filter.")
    parser.add_argument(
        "--titles",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "politician_titles.csv"
        ),
    )
    parser.add_argument(
        "--outdir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "output"),
    )
    args = parser.parse_args()

    # Load expected titles
    expected = {}
    with open(args.titles, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            expected[row["wikipedia_title"]] = row

    print(f"Expected titles (from Wikidata): {len(expected):,}")

    # Load matched titles from all Parquet files
    parquet_files = [
        os.path.join(args.outdir, f)
        for f in os.listdir(args.outdir)
        if f.endswith(".parquet")
    ]

    if not parquet_files:
        print(f"\nNo Parquet files found in {args.outdir}")
        print("Run filter_xml.py first.")
        return

    print(f"Parquet files found: {len(parquet_files)}")

    matched_titles = set()
    total_revisions = 0
    total_pages = 0
    revisions_per_page = {}

    for pf in sorted(parquet_files):
        table = pq.read_table(pf, columns=["page_title", "page_id", "revision_id"])
        titles_in_file = set(table.column("page_title").to_pylist())
        n_revisions = len(table)
        n_pages = len(titles_in_file)
        matched_titles.update(titles_in_file)
        total_revisions += n_revisions
        total_pages += n_pages

        for title in titles_in_file:
            if title not in revisions_per_page:
                revisions_per_page[title] = 0
        # Count revisions per title
        for title in table.column("page_title").to_pylist():
            revisions_per_page[title] = revisions_per_page.get(title, 0) + 1

        print(f"  {os.path.basename(pf)}: {n_pages:,} pages, {n_revisions:,} revisions")

    # Coverage stats
    found = matched_titles & set(expected.keys())
    missing = set(expected.keys()) - matched_titles
    unexpected = matched_titles - set(expected.keys())

    coverage_pct = len(found) / len(expected) * 100 if expected else 0

    print(f"\n{'=' * 60}")
    print(f"  COVERAGE REPORT")
    print(f"{'=' * 60}")
    print(f"  Expected titles:    {len(expected):,}")
    print(f"  Found in dumps:     {len(found):,} ({coverage_pct:.1f}%)")
    print(f"  Missing:            {len(missing):,}")
    print(f"  Total revisions:    {total_revisions:,}")

    if unexpected:
        print(f"  Unexpected matches: {len(unexpected):,} (in Parquet but not in title list)")

    # Top pages by revision count
    print(f"\n  Top 20 pages by revision count:")
    sorted_pages = sorted(revisions_per_page.items(), key=lambda x: -x[1])
    for i, (title, count) in enumerate(sorted_pages[:20], 1):
        entry_type = expected.get(title, {}).get("type", "?")
        print(f"    {i:>3}. {title} ({count:,} revisions) [{entry_type}]")

    # Bottom 20 by revision count (might flag stubs or redirects)
    print(f"\n  Bottom 20 pages by revision count:")
    for i, (title, count) in enumerate(sorted_pages[-20:], 1):
        entry_type = expected.get(title, {}).get("type", "?")
        print(f"    {i:>3}. {title} ({count:,} revisions) [{entry_type}]")

    # Write missing titles to file for reference
    missing_path = os.path.join(args.outdir, "missing_titles.txt")
    with open(missing_path, "w", encoding="utf-8") as f:
        for title in sorted(missing):
            entry = expected[title]
            f.write(f"{title}\t{entry.get('type', '')}\t{entry.get('wikidata_id', '')}\n")
    print(f"\n  Missing titles saved to: {missing_path}")

    # Sample of missing titles (well-known politicians that are missing)
    well_known = [
        "Barack Obama", "Donald Trump", "Joe Biden", "Kamala Harris",
        "George Washington", "Thomas Jefferson", "Theodore Roosevelt",
        "Franklin D. Roosevelt", "John F. Kennedy", "Ronald Reagan",
        "Hillary Clinton", "Bernie Sanders", "Nancy Pelosi",
        "Mitch McConnell", "Alexandria Ocasio-Cortez",
        "Democratic Party (United States)", "Republican Party (United States)",
    ]
    print(f"\n  Well-known title check:")
    for name in well_known:
        if name in found:
            count = revisions_per_page.get(name, 0)
            print(f"    FOUND    {name} ({count:,} revisions)")
        elif name in missing:
            print(f"    MISSING  {name}")
        elif name not in expected:
            print(f"    NOT IN LIST  {name}")

    # Estimate total file coverage
    print(f"\n  NOTE: Coverage depends on which dump files have been processed.")
    print(f"  Politician pages are spread across all 27 dump files.")
    print(f"  Process more files to improve coverage.")


if __name__ == "__main__":
    main()
