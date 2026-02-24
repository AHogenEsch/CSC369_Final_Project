"""
fetch_parties.py

Queries Wikidata SPARQL for party affiliations (P102) of all politicians
in politician_titles.csv. Produces politician_parties.csv with a
normalized party_group column (Democrat, Republican, Libertarian, Other).
"""

import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "CSC369-FinalProject/1.0 (Wikipedia vandalism research; Python)"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(SCRIPT_DIR, "politician_titles.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "politician_parties.csv")

PARTY_NORMALIZATION = {
    "Democratic Party": "Democrat",
    "Democratic Party of the United States": "Democrat",
    "Democrat": "Democrat",
    "Republican Party": "Republican",
    "Republican Party of the United States": "Republican",
    "Republican": "Republican",
    "Libertarian Party": "Libertarian",
    "Libertarian Party of the United States": "Libertarian",
    "Libertarian": "Libertarian",
}

BATCH_SIZE = 500


def run_sparql(query: str, label: str) -> list[dict]:
    # Use POST to avoid URL length limits with large VALUES blocks
    post_data = urllib.parse.urlencode({"query": query, "format": "json"}).encode("utf-8")
    req = urllib.request.Request(
        SPARQL_ENDPOINT,
        data=post_data,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ERROR ({label}): {e}", flush=True)
        return []
    elapsed = time.time() - start
    bindings = data.get("results", {}).get("bindings", [])
    print(f"  {label}: {len(bindings)} results in {elapsed:.1f}s", flush=True)
    return bindings


def fetch_parties_for_batch(qids: list[str], batch_num: int) -> dict[str, str]:
    """Fetch party labels for a batch of Wikidata QIDs."""
    values = " ".join(f"wd:{qid}" for qid in qids)
    query = f"""
    SELECT ?item ?partyLabel WHERE {{
      VALUES ?item {{ {values} }}
      ?item wdt:P102 ?party .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    """
    bindings = run_sparql(query, f"batch {batch_num} ({len(qids)} QIDs)")
    result = {}
    for b in bindings:
        qid = b["item"]["value"].rsplit("/", 1)[-1]
        party = b["partyLabel"]["value"]
        if qid not in result:
            result[qid] = party
        else:
            existing = result[qid]
            existing_group = normalize_party(existing)
            new_group = normalize_party(party)
            if existing_group == "Other" and new_group != "Other":
                result[qid] = party
    return result


def normalize_party(raw: str) -> str:
    if not raw:
        return "Other"
    for key, group in PARTY_NORMALIZATION.items():
        if key.lower() in raw.lower():
            return group
    return "Other"


def main():
    print("Loading politician_titles.csv ...", flush=True)
    entries = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            entries.append(row)
    print(f"  {len(entries):,} entries loaded.", flush=True)

    politicians = [e for e in entries if e["type"] == "politician"]
    parties = [e for e in entries if e["type"] == "party"]
    print(f"  Politicians: {len(politicians):,}, Parties: {len(parties):,}", flush=True)

    qid_to_party_raw: dict[str, str] = {}

    # Batch query politicians
    qids = [e["wikidata_id"] for e in politicians]
    total_batches = (len(qids) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nFetching party affiliations in {total_batches} batches ...", flush=True)

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        result = fetch_parties_for_batch(batch, batch_num)
        qid_to_party_raw.update(result)
        # Rate-limit to avoid Wikidata throttling
        if batch_num < total_batches:
            time.sleep(2)

    print(f"\nParty data retrieved for {len(qid_to_party_raw):,} / {len(politicians):,} politicians.", flush=True)

    # Build output rows
    output_rows = []
    for e in entries:
        qid = e["wikidata_id"]
        title = e["wikipedia_title"]

        if e["type"] == "party":
            # Party pages map to themselves
            if "democrat" in title.lower():
                party_group = "Democrat"
            elif "republican" in title.lower():
                party_group = "Republican"
            elif "libertarian" in title.lower():
                party_group = "Libertarian"
            else:
                party_group = "Other"
            output_rows.append({
                "wikipedia_title": title,
                "wikidata_id": qid,
                "party_raw": title,
                "party_group": party_group,
            })
        else:
            raw = qid_to_party_raw.get(qid, "")
            group = normalize_party(raw)
            output_rows.append({
                "wikipedia_title": title,
                "wikidata_id": qid,
                "party_raw": raw,
                "party_group": group,
            })

    # Stats
    from collections import Counter
    groups = Counter(r["party_group"] for r in output_rows)
    print(f"\nParty group breakdown:", flush=True)
    for g, c in sorted(groups.items(), key=lambda x: -x[1]):
        print(f"  {g:>12}: {c:,}", flush=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["wikipedia_title", "wikidata_id", "party_raw", "party_group"])
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\nSaved to: {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
