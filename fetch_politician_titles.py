"""
fetch_politician_titles.py

Queries the Wikidata SPARQL endpoint to build a comprehensive list of
US politician and US political party English Wikipedia article titles.

Output: politician_titles.csv
Columns: wikidata_id, wikipedia_title, type, description
"""

import csv
import os
import sys
import time
import urllib.parse
import urllib.request
import json

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
OUTPUT_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "politician_titles.csv"
)

USER_AGENT = "CSC369-FinalProject/1.0 (Wikipedia vandalism research; Python)"

# ---------------------------------------------------------------------------
# SPARQL queries
# ---------------------------------------------------------------------------

# Query 1: US politicians by occupation
# Humans with citizenship=US AND occupation=politician or political candidate
QUERY_POLITICIANS_BY_OCCUPATION = """
SELECT DISTINCT ?item ?articleTitle ?description WHERE {
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P27 wd:Q30 .
  { ?item wdt:P106 wd:Q82955 . }
  UNION
  { ?item wdt:P106 wd:Q13231463 . }
  ?article schema:about ?item ;
           schema:isPartOf <https://en.wikipedia.org/> ;
           schema:name ?articleTitle .
  OPTIONAL {
    ?item schema:description ?description .
    FILTER(LANG(?description) = "en")
  }
}
"""

# Query 2: US politicians by position held (P39)
# Catches people who held federal office but might not have occupation=politician
QUERY_POLITICIANS_BY_POSITION = """
SELECT DISTINCT ?item ?articleTitle ?description WHERE {
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P39 ?position .
  ?position wdt:P17 wd:Q30 .
  VALUES ?posType {
    wd:Q11696   # President of the US
    wd:Q11699   # Vice President of the US
    wd:Q4416090 # US Senator
    wd:Q13217683 # US Representative
    wd:Q889821  # state governor of a US state
  }
  ?position wdt:P279* ?posType .
  ?article schema:about ?item ;
           schema:isPartOf <https://en.wikipedia.org/> ;
           schema:name ?articleTitle .
  OPTIONAL {
    ?item schema:description ?description .
    FILTER(LANG(?description) = "en")
  }
}
"""

# Query 3: People who directly held one of these specific positions
# (simpler, avoids subclass traversal timeout issues)
QUERY_POLITICIANS_DIRECT_POSITIONS = """
SELECT DISTINCT ?item ?articleTitle ?description WHERE {
  ?item wdt:P31 wd:Q5 .
  VALUES ?position {
    wd:Q11696    # President of the US
    wd:Q11699    # Vice President of the US
    wd:Q4416090  # US Senator
    wd:Q13217683 # US Representative
    wd:Q889821   # state governor
    wd:Q1115127  # lieutenant governor
    wd:Q2985460  # Secretary of State (US federal)
    wd:Q842606   # Mayor in the US
  }
  ?item wdt:P39 ?position .
  ?article schema:about ?item ;
           schema:isPartOf <https://en.wikipedia.org/> ;
           schema:name ?articleTitle .
  OPTIONAL {
    ?item schema:description ?description .
    FILTER(LANG(?description) = "en")
  }
}
"""

# Query 4: US political parties
QUERY_PARTIES = """
SELECT DISTINCT ?item ?articleTitle ?description WHERE {
  ?item wdt:P31/wdt:P279* wd:Q7278 .
  ?item wdt:P17 wd:Q30 .
  ?article schema:about ?item ;
           schema:isPartOf <https://en.wikipedia.org/> ;
           schema:name ?articleTitle .
  OPTIONAL {
    ?item schema:description ?description .
    FILTER(LANG(?description) = "en")
  }
}
"""


def run_sparql_query(query: str, label: str) -> list[dict]:
    """Execute a SPARQL query against the Wikidata endpoint and return rows."""
    params = urllib.parse.urlencode({"query": query, "format": "json"})
    url = f"{SPARQL_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    print(f"  Running query: {label} ...")
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ERROR on query '{label}': {e}")
        return []

    elapsed = time.time() - start
    bindings = data.get("results", {}).get("bindings", [])
    print(f"  -> {len(bindings)} results in {elapsed:.1f}s")
    return bindings


def extract_rows(bindings: list[dict], entry_type: str) -> list[dict]:
    """Convert SPARQL JSON bindings to flat dicts."""
    rows = []
    for b in bindings:
        wikidata_id = b["item"]["value"].rsplit("/", 1)[-1]
        title = b["articleTitle"]["value"]
        desc = b.get("description", {}).get("value", "")
        rows.append({
            "wikidata_id": wikidata_id,
            "wikipedia_title": title,
            "type": entry_type,
            "description": desc,
        })
    return rows


def main():
    print("Fetching US politician and party titles from Wikidata ...\n")

    all_rows: dict[str, dict] = {}

    queries = [
        (QUERY_POLITICIANS_BY_OCCUPATION, "US politicians by occupation", "politician"),
        (QUERY_POLITICIANS_DIRECT_POSITIONS, "US politicians by position held", "politician"),
        (QUERY_PARTIES, "US political parties", "party"),
    ]

    # Try the subclass-based position query; fall back if it times out
    try_subclass = True

    for query, label, entry_type in queries:
        bindings = run_sparql_query(query, label)
        rows = extract_rows(bindings, entry_type)
        for r in rows:
            key = r["wikipedia_title"]
            if key not in all_rows:
                all_rows[key] = r

    # Also try the broader position-by-subclass query
    if try_subclass:
        bindings = run_sparql_query(
            QUERY_POLITICIANS_BY_POSITION, "US politicians by position (broad)"
        )
        rows = extract_rows(bindings, "politician")
        for r in rows:
            key = r["wikipedia_title"]
            if key not in all_rows:
                all_rows[key] = r

    # Deduplicate and sort
    unique_rows = sorted(all_rows.values(), key=lambda r: r["wikipedia_title"])

    # Count by type
    politician_count = sum(1 for r in unique_rows if r["type"] == "politician")
    party_count = sum(1 for r in unique_rows if r["type"] == "party")

    print(f"\n{'=' * 50}")
    print(f"  Total unique titles: {len(unique_rows)}")
    print(f"    Politicians: {politician_count}")
    print(f"    Parties:     {party_count}")
    print(f"{'=' * 50}")

    # Spot-check: print a sample
    print("\nSample titles:")
    for r in unique_rows[:10]:
        print(f"  [{r['type']:>10}] {r['wikipedia_title']}")
    if len(unique_rows) > 10:
        print(f"  ... and {len(unique_rows) - 10} more")

    # Write CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["wikidata_id", "wikipedia_title", "type", "description"]
        )
        writer.writeheader()
        writer.writerows(unique_rows)

    print(f"\nSaved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
