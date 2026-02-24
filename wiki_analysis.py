"""
wiki_analysis.py

Analyzes vandalism_events.parquet to answer all project questions:
  - Party-level vandalism counts and restoration times
  - Temporal patterns (top weeks, months, election proximity)
  - Per-politician rankings
  - Time-series plots colored by party
  - Post-2008 filtered analysis (excluding early Wikipedia boom)
  - Normalized per-article vandalism rates
  - Calendar month seasonality

Input:  output/vandalism_events.parquet, politician_parties.csv
Output: output/analysis_results.txt
        output/vandalism_monthly.png
        output/vandalism_quarterly.png
        output/vandalism_monthly_2008plus.png
"""

import csv
import os
import sys
import warnings
from datetime import datetime, timedelta
from collections import Counter

warnings.filterwarnings("ignore", message="Converting to PeriodArray")

try:
    import pandas as pd
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError as e:
    print(f"ERROR: Missing dependency: {e}")
    print("Install with:  pip install pandas matplotlib")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PARQUET = os.path.join(SCRIPT_DIR, "output", "vandalism_events.parquet")
PARTIES_CSV = os.path.join(SCRIPT_DIR, "politician_parties.csv")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
RESULTS_FILE = os.path.join(OUTPUT_DIR, "analysis_results.txt")

PARTY_COLORS = {
    "Democrat": "#2166ac",
    "Republican": "#d6604d",
    "Libertarian": "#4dac26",
    "Other": "#b8860b",
}

PARTY_ORDER = ["Democrat", "Republican", "Libertarian", "Other"]

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


def election_dates():
    """US general elections: first Tuesday in November of even years."""
    dates = []
    for year in range(2002, 2026, 2):
        for day in range(2, 9):
            candidate = datetime(year, 11, day)
            if candidate.weekday() == 1:  # 0=Mon, 1=Tue
                dates.append(candidate)
                break
    return dates


def section(title: str) -> str:
    return f"\n{'=' * 70}\n  {title}\n{'=' * 70}\n"


def make_time_series_plot(data, period_col, freq_label, filename):
    """Line graph of vandalism events over time, one line per party."""
    pivot = data.groupby([period_col, "party_group"]).size().unstack(fill_value=0)
    for party in PARTY_ORDER:
        if party not in pivot.columns:
            pivot[party] = 0
    pivot = pivot[PARTY_ORDER]
    pivot.index = pivot.index.to_timestamp()

    fig, ax = plt.subplots(figsize=(18, 6))
    for party in PARTY_ORDER:
        ax.plot(
            pivot.index, pivot[party],
            color=PARTY_COLORS[party],
            label=party,
            linewidth=1.0,
            alpha=0.85,
        )

    for edate in election_dates():
        if pivot.index.min() <= pd.Timestamp(edate) <= pivot.index.max():
            ax.axvline(pd.Timestamp(edate), color="gray",
                       linestyle="--", alpha=0.3, linewidth=0.7)

    ax.set_xlabel("Date")
    ax.set_ylabel(f"Vandalism Events per {freq_label}")
    ax.set_title(f"Wikipedia Politician Vandalism Over Time ({freq_label} bins)")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.autofmt_xdate()
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {out_path}", flush=True)


def print_party_vandalism_counts(out, data, label):
    """Print vandalism totals and percentages grouped by party."""
    counts = data.groupby("party_group").size().reindex(PARTY_ORDER, fill_value=0)
    total = len(data)
    out(f"  Vandalism count by party ({label}):")
    for party in PARTY_ORDER:
        c = counts.get(party, 0)
        pct = c / total * 100 if total else 0
        out(f"    {party:>12}: {c:>8,} ({pct:>5.1f}%)")
    out(f"\n  Most vandalized party ({label}):  {counts.idxmax()} ({counts.max():,})")
    out(f"  Least vandalized party ({label}): {counts.idxmin()} ({counts.min():,})")
    return counts


def print_restoration_times(out, data, label):
    """Print mean and median restoration times grouped by party."""
    out(f"\n  Restoration time by party ({label}):")
    mean = data.groupby("party_group")["restoration_time_seconds"].mean().reindex(PARTY_ORDER)
    median = data.groupby("party_group")["restoration_time_seconds"].median().reindex(PARTY_ORDER)
    out(f"    {'Party':>12}  {'Mean':>14}  {'Median':>14}")
    for party in PARTY_ORDER:
        m, md = mean.get(party), median.get(party)
        if pd.notna(m) and pd.notna(md):
            out(f"    {party:>12}  {m/3600:>10.1f} hrs  {md/3600:>10.1f} hrs")
        else:
            out(f"    {party:>12}  {'N/A':>14}  {'N/A':>14}")


def print_top_vandalized(out, data, label, n=10):
    """Print the top N most vandalized politicians."""
    counts = data.groupby("page_title").size().sort_values(ascending=False)
    out(f"\n  Top {n} most vandalized ({label}):")
    for i, (title, count) in enumerate(counts.head(n).items(), 1):
        party = data[data["page_title"] == title]["party_group"].iloc[0]
        out(f"    {i:>2}. {title} ({party}): {count:,}")
    return counts


def print_election_proximity(out, data, elections, label):
    """Compare vandalism in the 90 days before vs. after each election."""
    out(f"\n  Election proximity ({label}):")
    out("  (Comparing vandalism in 3 months before vs. 3 months after)")
    for edate in elections:
        edate_utc = pd.Timestamp(edate, tz="UTC")
        pre_start = pd.Timestamp(edate - timedelta(days=90), tz="UTC")
        post_end = pd.Timestamp(edate + timedelta(days=90), tz="UTC")

        pre = data[(data["timestamp_dt"] >= pre_start) & (data["timestamp_dt"] < edate_utc)]
        post = data[(data["timestamp_dt"] >= edate_utc) & (data["timestamp_dt"] < post_end)]
        if len(pre) > 0 or len(post) > 0:
            change = ((len(post) - len(pre)) / len(pre) * 100) if len(pre) > 0 else float("inf")
            out(f"    {edate.year} election ({edate.strftime('%b %d')}): "
                f"before={len(pre):,}, after={len(post):,}, change={change:+.1f}%")


def print_normalized_table(out, party_counts, party_article_counts, total_articles, total_v, label):
    """Print per-article normalized vandalism rates."""
    out(f"\n  Vandalism per party -- raw vs. normalized ({label}):")
    out(f"    {'Party':>12}  {'Articles':>8}  {'Vandalism':>10}  "
        f"{'Per Article':>12}  {'% Articles':>10}  {'% Vandalism':>12}")
    for party in PARTY_ORDER:
        articles = party_article_counts[party]
        v_count = party_counts.get(party, 0)
        per_article = v_count / articles if articles else 0
        pct_articles = articles / total_articles * 100 if total_articles else 0
        pct_vandalism = v_count / total_v * 100 if total_v else 0
        out(f"    {party:>12}  {articles:>8,}  {v_count:>10,}  "
            f"{per_article:>12.1f}  {pct_articles:>9.1f}%  {pct_vandalism:>11.1f}%")


def main():
    if not os.path.exists(INPUT_PARQUET):
        print(f"ERROR: {INPUT_PARQUET} not found. Run filter_vandalism.py first.")
        sys.exit(1)

    # ── Load data ────────────────────────────────────────────────────
    print("Loading vandalism events ...", flush=True)
    df = pd.read_parquet(INPUT_PARQUET)
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True)
    print(f"  {len(df):,} total records loaded.", flush=True)

    vandalism = df[df["event_type"] == "vandalism"].copy()
    print(f"  Vandalism edits:  {len(vandalism):,}", flush=True)
    print(f"  Restorations:     {(df['event_type'] == 'restoration').sum():,}", flush=True)

    # Discard negative restoration times (caused by out-of-order timestamps)
    bad_mask = vandalism["restoration_time_seconds"].notna() & (vandalism["restoration_time_seconds"] < 0)
    vandalism.loc[bad_mask, "restoration_time_seconds"] = None
    if bad_mask.sum() > 0:
        print(f"  Filtered {bad_mask.sum():,} records with negative restoration times.", flush=True)

    lines = []
    def out(text=""):
        lines.append(text)
        print(text, flush=True)

    elections = election_dates()

    # ── All-time party analysis ──────────────────────────────────────
    out(section("PARTY ANALYSIS"))
    party_counts = print_party_vandalism_counts(out, vandalism, "all time")
    print_restoration_times(out, vandalism, "all time")

    # ── Temporal analysis ────────────────────────────────────────────
    out(section("TEMPORAL ANALYSIS"))

    vandalism["year_month"] = vandalism["timestamp_dt"].dt.to_period("M")
    vandalism["year_week"] = vandalism["timestamp_dt"].dt.to_period("W")

    month_counts = vandalism.groupby("year_month").size().sort_values(ascending=False)
    out("  Top 5 months with most vandalism:")
    for i, (period, count) in enumerate(month_counts.head(5).items(), 1):
        out(f"    {i}. {period}: {count:,} vandalism edits")

    week_counts = vandalism.groupby("year_week").size().sort_values(ascending=False)
    out("\n  Top 5 weeks with most vandalism:")
    for i, (period, count) in enumerate(week_counts.head(5).items(), 1):
        out(f"    {i}. {period}: {count:,} vandalism edits")

    out("\n  Longest gap between consecutive vandalism events:")
    sorted_ts = vandalism["timestamp_dt"].sort_values()
    if len(sorted_ts) > 1:
        diffs = sorted_ts.diff().dropna()
        max_gap_idx = diffs.idxmax()
        gap_end = sorted_ts.loc[max_gap_idx]
        gap_start = sorted_ts.loc[:max_gap_idx].iloc[-2]
        out(f"    {diffs.max().days} days ({diffs.max()})")
        out(f"    From: {gap_start}")
        out(f"    To:   {gap_end}")

    print_election_proximity(out, vandalism, elections, "all time")

    # ── Per-politician analysis ──────────────────────────────────────
    out(section("PER-POLITICIAN ANALYSIS"))
    politician_counts = print_top_vandalized(out, vandalism, "all time")

    out("\n  Top 10 least vandalized (with at least 1 event):")
    least = politician_counts.tail(10).sort_values(ascending=True)
    for i, (title, count) in enumerate(least.items(), 1):
        party = vandalism[vandalism["page_title"] == title]["party_group"].iloc[0]
        out(f"    {i:>2}. {title} ({party}): {count:,} vandalism edits")

    # Median restoration times (requires >= 5 events for meaningful stats)
    med_rest = vandalism.groupby("page_title")["restoration_time_seconds"].median().dropna()
    pages_with_enough = politician_counts[politician_counts >= 5].index
    med_rest_filtered = med_rest[med_rest.index.isin(pages_with_enough)]

    out("\n  Top 5 FASTEST median restoration times (>= 5 events):")
    for i, (title, med) in enumerate(med_rest_filtered.sort_values().head(5).items(), 1):
        out(f"    {i}. {title}: {med/3600:.1f} hours median ({politician_counts[title]:,} events)")

    out("\n  Top 5 SLOWEST median restoration times (>= 5 events):")
    for i, (title, med) in enumerate(med_rest_filtered.sort_values(ascending=False).head(5).items(), 1):
        out(f"    {i}. {title}: {med/3600:.1f} hours median ({politician_counts[title]:,} events)")

    # ── Detection method breakdown ───────────────────────────────────
    out(section("DETECTION METHOD BREAKDOWN"))
    for method, count in vandalism["detection_method"].value_counts().items():
        out(f"    {method}: {count:,} ({count / len(vandalism) * 100:.1f}%)")

    anon = vandalism["contributor_ip"].notna().sum()
    registered = vandalism["contributor_username"].notna().sum()
    out(f"\n  Anonymous vandalism:  {anon:,} ({anon / len(vandalism) * 100:.1f}%)")
    out(f"  Registered vandalism: {registered:,} ({registered / len(vandalism) * 100:.1f}%)")

    # ── Plots (all-time) ────────────────────────────────────────────
    print("\nGenerating plots ...", flush=True)
    vandalism["month"] = vandalism["timestamp_dt"].dt.to_period("M")
    vandalism["quarter"] = vandalism["timestamp_dt"].dt.to_period("Q")

    make_time_series_plot(vandalism, "month", "Month", "vandalism_monthly.png")
    make_time_series_plot(vandalism, "quarter", "Quarter", "vandalism_quarterly.png")

    # ── Post-2008 analysis ───────────────────────────────────────────
    out(section("POST-2008 ANALYSIS (2008-01-01 onward)"))

    cutoff = pd.Timestamp("2008-01-01", tz="UTC")
    v2008 = vandalism[vandalism["timestamp_dt"] >= cutoff].copy()
    out(f"  Vandalism events from 2008 onward: {len(v2008):,} "
        f"(excluded {len(vandalism) - len(v2008):,} pre-2008 events)")

    party_counts_2008 = print_party_vandalism_counts(out, v2008, "2008+")
    print_restoration_times(out, v2008, "2008+")
    print_top_vandalized(out, v2008, "2008+")
    print_election_proximity(out, v2008,
                             [e for e in elections if e.year >= 2008], "2008+")

    v2008["month"] = v2008["timestamp_dt"].dt.to_period("M")
    make_time_series_plot(v2008, "month", "Month", "vandalism_monthly_2008plus.png")

    # ── Normalized party analysis ────────────────────────────────────
    out(section("NORMALIZED PARTY ANALYSIS"))

    party_article_counts = Counter()
    with open(PARTIES_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            party_article_counts[row["party_group"]] += 1

    out("  Number of politicians (Wikipedia articles) per party:")
    for party in PARTY_ORDER:
        out(f"    {party:>12}: {party_article_counts[party]:>6,}")

    total_articles = sum(party_article_counts[p] for p in PARTY_ORDER)
    print_normalized_table(out, party_counts, party_article_counts,
                           total_articles, len(vandalism), "all time")
    print_normalized_table(out, party_counts_2008, party_article_counts,
                           total_articles, len(v2008), "2008+ only")

    # ── Calendar month seasonality ───────────────────────────────────
    out(section("CALENDAR MONTH SEASONALITY"))

    vandalism["cal_month"] = vandalism["timestamp_dt"].dt.month
    v2008["cal_month"] = v2008["timestamp_dt"].dt.month

    min_year = vandalism["timestamp_dt"].dt.year.min()
    max_year = vandalism["timestamp_dt"].dt.year.max()
    num_years_all = max_year - min_year + 1

    min_year_2008 = v2008["timestamp_dt"].dt.year.min()
    max_year_2008 = v2008["timestamp_dt"].dt.year.max()
    num_years_2008 = max_year_2008 - min_year_2008 + 1

    cal_totals = vandalism.groupby("cal_month").size()
    cal_totals_2008 = v2008.groupby("cal_month").size()

    out(f"  All-time calendar month totals and averages ({min_year}-{max_year}, {num_years_all} years):")
    out(f"    {'Month':<12}  {'Total':>8}  {'Avg/Year':>10}")
    for m in range(1, 13):
        total = cal_totals.get(m, 0)
        out(f"    {MONTH_NAMES[m]:<12}  {total:>8,}  {total / num_years_all:>10,.0f}")

    out(f"\n  Post-2008 calendar month totals and averages ({min_year_2008}-{max_year_2008}, {num_years_2008} years):")
    out(f"    {'Month':<12}  {'Total':>8}  {'Avg/Year':>10}")
    for m in range(1, 13):
        total = cal_totals_2008.get(m, 0)
        out(f"    {MONTH_NAMES[m]:<12}  {total:>8,}  {total / num_years_2008:>10,.0f}")

    peak_all, low_all = cal_totals.idxmax(), cal_totals.idxmin()
    peak_2008, low_2008 = cal_totals_2008.idxmax(), cal_totals_2008.idxmin()
    out(f"\n  All-time: highest = {MONTH_NAMES[peak_all]} ({cal_totals[peak_all]:,}), "
        f"lowest = {MONTH_NAMES[low_all]} ({cal_totals[low_all]:,})")
    out(f"  Post-2008: highest = {MONTH_NAMES[peak_2008]} ({cal_totals_2008[peak_2008]:,}), "
        f"lowest = {MONTH_NAMES[low_2008]} ({cal_totals_2008[low_2008]:,})")

    # ── Save results ─────────────────────────────────────────────────
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\nResults saved to: {RESULTS_FILE}", flush=True)
    print("\nAll analysis complete.", flush=True)


if __name__ == "__main__":
    main()
