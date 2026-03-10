"""
vandal_analysis.py

Profiles the users behind politician Wikipedia vandalism:
  - Vandal census (anonymous vs. registered)
  - Edits-per-vandal distribution
  - Party targeting patterns and focus index (HHI)
  - Activity span statistics
  - Page diversity and serial vandals
  - Hour-of-day patterns
  - Anon vs. registered comparison

All analyses are run on both all-time and post-2008 data.

Input:  output/vandalism_events.parquet
Output: output/vandal_analysis_results.txt
        output/vandal_edit_distribution.png
        output/vandal_hourly_pattern.png
"""

import os
import sys
import warnings
import numpy as np

warnings.filterwarnings("ignore", message="Converting to PeriodArray")

try:
    import pandas as pd
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as e:
    print(f"ERROR: Missing dependency: {e}")
    print("Install with:  pip install pandas matplotlib numpy")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PARQUET = os.path.join(SCRIPT_DIR, "output", "vandalism_events.parquet")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
RESULTS_FILE = os.path.join(OUTPUT_DIR, "vandal_analysis_results.txt")

PARTY_COLORS = {
    "Democrat": "#2166ac",
    "Republican": "#d6604d",
    "Libertarian": "#4dac26",
    "Other": "#b8860b",
}
PARTY_ORDER = ["Democrat", "Republican", "Libertarian", "Other"]


def section(title: str) -> str:
    return f"\n{'=' * 70}\n  {title}\n{'=' * 70}\n"


def fmt_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    if pd.isna(seconds) or seconds < 0:
        return "N/A"
    hours = seconds / 3600
    if hours < 24:
        return f"{hours:.1f} hours"
    days = hours / 24
    if days < 365:
        return f"{days:.1f} days"
    years = days / 365.25
    return f"{years:.2f} years"


def is_bot_username(vandal_id: str) -> bool:
    """Heuristic: Wikipedia bots conventionally include 'bot' in their name."""
    if vandal_id.startswith("ip:"):
        return False
    name = vandal_id[2:].lower()
    return "bot" in name


def build_vandal_profiles(data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-vandal statistics from the raw vandalism rows."""
    grouped = data.groupby("vandal_id")

    profiles = pd.DataFrame({
        "edit_count": grouped.size(),
        "is_anon": grouped["is_anon"].first(),
        "first_edit": grouped["timestamp_dt"].min(),
        "last_edit": grouped["timestamp_dt"].max(),
        "unique_pages": grouped["page_title"].nunique(),
        "unique_parties": grouped["party_group"].nunique(),
    })

    profiles["active_span_seconds"] = (
        profiles["last_edit"] - profiles["first_edit"]
    ).dt.total_seconds()

    profiles["is_bot"] = pd.Series(
        {vid: is_bot_username(vid) for vid in profiles.index}
    )

    return profiles


def compute_party_focus(data: pd.DataFrame) -> pd.DataFrame:
    """For each vandal, compute per-party proportions and the HHI focus index."""
    party_counts = data.groupby(["vandal_id", "party_group"]).size().unstack(fill_value=0)
    for p in PARTY_ORDER:
        if p not in party_counts.columns:
            party_counts[p] = 0
    party_counts = party_counts[PARTY_ORDER]

    totals = party_counts.sum(axis=1)
    proportions = party_counts.div(totals, axis=0)

    hhi = (proportions ** 2).sum(axis=1)
    proportions["hhi"] = hhi
    proportions["edit_count"] = totals

    return proportions


def print_census(out, profiles, label):
    """Section 1: Vandal Census."""
    total = len(profiles)
    anon = profiles["is_anon"].sum()
    registered = total - anon

    anon_edits = profiles.loc[profiles["is_anon"], "edit_count"].sum()
    reg_edits = profiles.loc[~profiles["is_anon"], "edit_count"].sum()
    total_edits = anon_edits + reg_edits

    bots = profiles["is_bot"].sum()
    bot_edits = profiles.loc[profiles["is_bot"], "edit_count"].sum()
    humans_reg = registered - bots
    human_reg_edits = reg_edits - bot_edits

    out(f"  Total unique vandals ({label}): {total:,}")
    out(f"    Anonymous (unique IPs):    {anon:,} ({anon / total * 100:.1f}%)")
    out(f"    Registered (unique users): {registered:,} ({registered / total * 100:.1f}%)")
    out(f"      Likely bots:            {bots:,} ({bots / total * 100:.2f}%)")
    out(f"      Human editors:          {humans_reg:,} ({humans_reg / total * 100:.1f}%)")
    out(f"  Total vandalism edits: {total_edits:,}")
    out(f"    From anonymous:  {anon_edits:,} ({anon_edits / total_edits * 100:.1f}%)")
    out(f"    From registered: {reg_edits:,} ({reg_edits / total_edits * 100:.1f}%)")
    out(f"      From bots:    {bot_edits:,} ({bot_edits / total_edits * 100:.1f}%)")
    out(f"      From humans:  {human_reg_edits:,} ({human_reg_edits / total_edits * 100:.1f}%)")


def print_edits_per_vandal(out, profiles, label):
    """Section 2: Edits Per Vandal statistics."""
    ec = profiles["edit_count"]
    out(f"  Edits per vandal ({label}):")
    out(f"    Mean:   {ec.mean():.2f}")
    out(f"    Median: {ec.median():.1f}")
    out(f"    Q1:     {ec.quantile(0.25):.1f}")
    out(f"    Q3:     {ec.quantile(0.75):.1f}")
    out(f"    Max:    {ec.max():,}")

    one_and_done = (ec == 1).sum()
    out(f"\n  One-and-done vandals (exactly 1 edit): {one_and_done:,} "
        f"({one_and_done / len(profiles) * 100:.1f}%)")

    for threshold in [2, 5, 10, 50, 100]:
        count = (ec >= threshold).sum()
        out(f"    >= {threshold:>3} edits: {count:>8,} ({count / len(profiles) * 100:.1f}%)")


def make_edit_distribution_plot(profiles, filename, label):
    """Histogram of edits-per-vandal with log-scaled x-axis."""
    ec = profiles["edit_count"].values

    bins = np.logspace(0, np.log10(ec.max() + 1), 50)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.hist(ec, bins=bins, color="#4a7fb5", edgecolor="white", linewidth=0.3)
    ax.set_xscale("log")
    ax.set_xlabel("Number of Vandalism Edits (log scale)")
    ax.set_ylabel("Number of Vandals")
    ax.set_title(f"Distribution of Vandalism Edits per Vandal ({label})")
    ax.grid(True, alpha=0.3, which="both")
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {out_path}", flush=True)


def print_party_targeting(out, focus_df, profiles, label):
    """Section 3: Party targeting patterns."""
    total = len(profiles)

    single_party = (profiles["unique_parties"] == 1).sum()
    cross_party = total - single_party
    out(f"  Party targeting ({label}):")
    out(f"    Single-party vandals:  {single_party:,} ({single_party / total * 100:.1f}%)")
    out(f"    Cross-party vandals:   {cross_party:,} ({cross_party / total * 100:.1f}%)")

    single_party_vandals = profiles[profiles["unique_parties"] == 1].index
    single_focus = focus_df.loc[focus_df.index.isin(single_party_vandals)]
    out("\n  Single-party vandals target breakdown:")
    for party in PARTY_ORDER:
        is_target = (single_focus[party] == 1.0).sum()
        out(f"    {party:>12}: {is_target:>8,} ({is_target / len(single_focus) * 100:.1f}%)")

    out(f"\n  Party Focus Index / HHI ({label}):")
    out("    (1.0 = all edits on one party, 0.25 = even split across 4)")
    hhi = focus_df["hhi"]
    out(f"    Overall mean:   {hhi.mean():.4f}")
    out(f"    Overall median: {hhi.median():.4f}")

    anon_ids = profiles[profiles["is_anon"]].index
    reg_ids = profiles[~profiles["is_anon"]].index
    hhi_anon = hhi[hhi.index.isin(anon_ids)]
    hhi_reg = hhi[hhi.index.isin(reg_ids)]
    out(f"    Anonymous  mean: {hhi_anon.mean():.4f}  median: {hhi_anon.median():.4f}")
    out(f"    Registered mean: {hhi_reg.mean():.4f}  median: {hhi_reg.median():.4f}")

    frequent = focus_df[focus_df["edit_count"] >= 5].copy()
    if len(frequent) > 0:
        out(f"\n  Average party proportions for vandals with 5+ edits "
            f"({len(frequent):,} vandals, {label}):")
        out(f"    {'Party':>12}  {'Mean Proportion':>16}  {'Median Proportion':>18}")
        for party in PARTY_ORDER:
            m = frequent[party].mean()
            md = frequent[party].median()
            out(f"    {party:>12}  {m:>16.4f}  {md:>18.4f}")


def print_activity_span(out, profiles, label):
    """Section 4: Activity span for vandals with 2+ edits."""
    multi = profiles[profiles["edit_count"] >= 2].copy()
    total_vandals = len(profiles)

    out(f"  Activity span analysis ({label}):")
    out(f"    Vandals with 2+ edits: {len(multi):,} "
        f"({len(multi) / total_vandals * 100:.1f}% of all vandals)")

    if len(multi) == 0:
        out("    No multi-edit vandals found.")
        return

    span = multi["active_span_seconds"]
    out(f"\n    Active window (first edit to last edit):")
    out(f"      Mean:   {fmt_duration(span.mean())}")
    out(f"      Median: {fmt_duration(span.median())}")
    out(f"      Q1:     {fmt_duration(span.quantile(0.25))}")
    out(f"      Q3:     {fmt_duration(span.quantile(0.75))}")
    out(f"      Max:    {fmt_duration(span.max())}")

    one_day = (span <= 86400).sum()
    one_year = (span > 365.25 * 86400).sum()
    out(f"\n    One-day vandals (active <= 24 hrs): {one_day:,} "
        f"({one_day / len(multi) * 100:.1f}%)")
    out(f"    Long-term vandals (active > 1 year): {one_year:,} "
        f"({one_year / len(multi) * 100:.1f}%)")

    anon_span = multi.loc[multi["is_anon"], "active_span_seconds"]
    reg_span = multi.loc[~multi["is_anon"], "active_span_seconds"]
    out(f"\n    By type (vandals with 2+ edits):")
    out(f"      Anonymous  -- mean: {fmt_duration(anon_span.mean())}, "
        f"median: {fmt_duration(anon_span.median())}")
    out(f"      Registered -- mean: {fmt_duration(reg_span.mean())}, "
        f"median: {fmt_duration(reg_span.median())}")


def print_page_diversity(out, profiles, data, label):
    """Section 5: Page diversity and serial vandals."""
    up = profiles["unique_pages"]
    out(f"  Page diversity ({label}):")
    out(f"    Mean unique pages per vandal:   {up.mean():.2f}")
    out(f"    Median unique pages per vandal: {up.median():.1f}")

    one_page = (up == 1).sum()
    two_to_five = ((up >= 2) & (up <= 5)).sum()
    five_plus = (up > 5).sum()
    total = len(profiles)
    out(f"    1 page:    {one_page:,} ({one_page / total * 100:.1f}%)")
    out(f"    2-5 pages: {two_to_five:,} ({two_to_five / total * 100:.1f}%)")
    out(f"    5+ pages:  {five_plus:,} ({five_plus / total * 100:.1f}%)")


def print_detailed_top10(out, profiles, focus_df, data, label):
    """Detailed profiles for the top 10 most prolific registered vandals."""
    reg = profiles[~profiles["is_anon"]].sort_values("edit_count", ascending=False)
    top10 = reg.head(10)

    out(f"  Top 10 most prolific registered vandals ({label}):")
    out(f"  (Accounts with 'bot' in the name are flagged as likely bots)")
    out("")

    for i, (vid, row) in enumerate(top10.iterrows(), 1):
        username = vid[2:]
        bot_tag = " [BOT]" if row["is_bot"] else ""
        out(f"  --- #{i}: {username}{bot_tag} ---")
        out(f"    Total detected vandalism edits: {row['edit_count']:,}")
        out(f"    Unique pages targeted:          {row['unique_pages']:,}")
        out(f"    Unique parties targeted:         {row['unique_parties']}")
        out(f"    First edit: {row['first_edit'].strftime('%Y-%m-%d %H:%M UTC')}")
        out(f"    Last edit:  {row['last_edit'].strftime('%Y-%m-%d %H:%M UTC')}")
        out(f"    Active span: {fmt_duration(row['active_span_seconds'])}")

        if vid in focus_df.index:
            row_focus = focus_df.loc[vid]
            out(f"    Party Focus Index (HHI): {row_focus['hhi']:.4f}")
            out(f"    Party breakdown:")
            for party in PARTY_ORDER:
                proportion = row_focus[party]
                count = int(round(proportion * row["edit_count"]))
                out(f"      {party:>12}: {count:>6,} edits ({proportion * 100:>5.1f}%)")

        top_pages = (
            data[data["vandal_id"] == vid]
            .groupby("page_title").size()
            .sort_values(ascending=False)
            .head(5)
        )
        out(f"    Top 5 targeted pages:")
        for title, cnt in top_pages.items():
            out(f"      {title}: {cnt:,}")
        out("")


def make_hourly_plot(data, filename):
    """Hour-of-day plot comparing anonymous vs. registered vandals."""
    data = data.copy()
    data["hour"] = data["timestamp_dt"].dt.hour

    anon_hours = data[data["is_anon"]].groupby("hour").size()
    reg_hours = data[~data["is_anon"]].groupby("hour").size()

    anon_hours = anon_hours.reindex(range(24), fill_value=0)
    reg_hours = reg_hours.reindex(range(24), fill_value=0)

    anon_pct = anon_hours / anon_hours.sum() * 100
    reg_pct = reg_hours / reg_hours.sum() * 100

    fig, ax = plt.subplots(figsize=(12, 5))
    x = np.arange(24)
    width = 0.35
    ax.bar(x - width / 2, anon_pct, width, label="Anonymous", color="#e08214", alpha=0.85)
    ax.bar(x + width / 2, reg_pct, width, label="Registered", color="#4a7fb5", alpha=0.85)
    ax.set_xlabel("Hour of Day (UTC)")
    ax.set_ylabel("% of Vandalism Edits")
    ax.set_title("Vandalism by Hour of Day (UTC) -- Anonymous vs. Registered")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{h:02d}" for h in range(24)])
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {out_path}", flush=True)


def print_anon_vs_registered_summary(out, profiles, focus_df, label):
    """Section 6: Side-by-side comparison table."""
    anon = profiles[profiles["is_anon"]]
    reg = profiles[~profiles["is_anon"]]

    anon_hhi = focus_df.loc[focus_df.index.isin(anon.index), "hhi"]
    reg_hhi = focus_df.loc[focus_df.index.isin(reg.index), "hhi"]

    multi_anon = anon[anon["edit_count"] >= 2]
    multi_reg = reg[reg["edit_count"] >= 2]

    out(f"\n  Anon vs. Registered summary ({label}):")
    out(f"    {'Metric':<30}  {'Anonymous':>14}  {'Registered':>14}")
    out(f"    {'-' * 30}  {'-' * 14}  {'-' * 14}")
    out(f"    {'Unique vandals':<30}  {len(anon):>14,}  {len(reg):>14,}")
    out(f"    {'Total edits':<30}  {anon['edit_count'].sum():>14,}  "
        f"{reg['edit_count'].sum():>14,}")
    out(f"    {'Mean edits/vandal':<30}  {anon['edit_count'].mean():>14.2f}  "
        f"{reg['edit_count'].mean():>14.2f}")
    out(f"    {'Median edits/vandal':<30}  {anon['edit_count'].median():>14.1f}  "
        f"{reg['edit_count'].median():>14.1f}")
    out(f"    {'Mean pages/vandal':<30}  {anon['unique_pages'].mean():>14.2f}  "
        f"{reg['unique_pages'].mean():>14.2f}")
    out(f"    {'Mean HHI (party focus)':<30}  {anon_hhi.mean():>14.4f}  "
        f"{reg_hhi.mean():>14.4f}")
    out(f"    {'One-and-done rate':<30}  "
        f"{(anon['edit_count'] == 1).mean() * 100:>13.1f}%  "
        f"{(reg['edit_count'] == 1).mean() * 100:>13.1f}%")

    anon_span_str = fmt_duration(multi_anon["active_span_seconds"].mean()) if len(multi_anon) > 0 else "N/A"
    reg_span_str = fmt_duration(multi_reg["active_span_seconds"].mean()) if len(multi_reg) > 0 else "N/A"
    out(f"    {'Mean active span (2+ edits)':<30}  {anon_span_str:>14}  {reg_span_str:>14}")


def run_analysis(out, data, label, plot_suffix=""):
    """Run all analysis sections on a given dataset."""
    out(section(f"VANDAL CENSUS ({label})"))
    profiles = build_vandal_profiles(data)
    print_census(out, profiles, label)

    out(section(f"EDITS PER VANDAL ({label})"))
    print_edits_per_vandal(out, profiles, label)

    plot_file = f"vandal_edit_distribution{plot_suffix}.png"
    make_edit_distribution_plot(profiles, plot_file, label)

    out(section(f"PARTY TARGETING PATTERNS ({label})"))
    focus_df = compute_party_focus(data)
    print_party_targeting(out, focus_df, profiles, label)

    out(section(f"ACTIVITY SPAN ({label})"))
    print_activity_span(out, profiles, label)

    out(section(f"PAGE DIVERSITY ({label})"))
    print_page_diversity(out, profiles, data, label)

    out(section(f"TOP 10 REGISTERED VANDALS -- DETAILED PROFILES ({label})"))
    print_detailed_top10(out, profiles, focus_df, data, label)

    out(section(f"ANON VS. REGISTERED COMPARISON ({label})"))
    print_anon_vs_registered_summary(out, profiles, focus_df, label)

    return profiles, focus_df


def main():
    if not os.path.exists(INPUT_PARQUET):
        print(f"ERROR: {INPUT_PARQUET} not found. Run filter_vandalism.py first.")
        sys.exit(1)

    # -- Load data --------------------------------------------------------
    print("Loading vandalism events ...", flush=True)
    df = pd.read_parquet(INPUT_PARQUET)
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True)
    print(f"  {len(df):,} total records loaded.", flush=True)

    vandalism = df[df["event_type"] == "vandalism"].copy()
    print(f"  Vandalism edits: {len(vandalism):,}", flush=True)

    # -- Build vandal_id and is_anon --------------------------------------
    vandalism["is_anon"] = vandalism["contributor_ip"].notna()
    vandalism["vandal_id"] = vandalism.apply(
        lambda r: f"ip:{r['contributor_ip']}" if r["is_anon"]
        else f"u:{r['contributor_username']}",
        axis=1,
    )
    print(f"  Unique vandals: {vandalism['vandal_id'].nunique():,}", flush=True)

    lines = []
    def out(text=""):
        lines.append(text)
        print(text, flush=True)

    # -- All-time analysis ------------------------------------------------
    out(section("ALL-TIME VANDAL ANALYSIS"))
    profiles_all, focus_all = run_analysis(out, vandalism, "All Time")

    # -- Hour-of-day plot (all-time) --------------------------------------
    out(section("HOUR-OF-DAY PATTERNS (All Time)"))
    vandalism_copy = vandalism.copy()
    vandalism_copy["hour"] = vandalism_copy["timestamp_dt"].dt.hour
    anon_hours = vandalism_copy[vandalism_copy["is_anon"]].groupby("hour").size()
    reg_hours = vandalism_copy[~vandalism_copy["is_anon"]].groupby("hour").size()
    anon_hours = anon_hours.reindex(range(24), fill_value=0)
    reg_hours = reg_hours.reindex(range(24), fill_value=0)

    peak_anon = anon_hours.idxmax()
    peak_reg = reg_hours.idxmax()
    out(f"  Peak hour for anonymous vandalism:  {peak_anon:02d}:00 UTC "
        f"({anon_hours[peak_anon]:,} edits)")
    out(f"  Peak hour for registered vandalism: {peak_reg:02d}:00 UTC "
        f"({reg_hours[peak_reg]:,} edits)")

    low_anon = anon_hours.idxmin()
    low_reg = reg_hours.idxmin()
    out(f"  Lowest hour for anonymous:  {low_anon:02d}:00 UTC "
        f"({anon_hours[low_anon]:,} edits)")
    out(f"  Lowest hour for registered: {low_reg:02d}:00 UTC "
        f"({reg_hours[low_reg]:,} edits)")

    make_hourly_plot(vandalism, "vandal_hourly_pattern.png")

    # -- Post-2008 analysis -----------------------------------------------
    cutoff = pd.Timestamp("2008-01-01", tz="UTC")
    v2008 = vandalism[vandalism["timestamp_dt"] >= cutoff].copy()
    out(section("POST-2008 VANDAL ANALYSIS"))
    out(f"  Events from 2008 onward: {len(v2008):,} "
        f"(excluded {len(vandalism) - len(v2008):,} pre-2008 events)")

    profiles_2008, focus_2008 = run_analysis(out, v2008, "Post-2008", plot_suffix="_2008plus")

    # -- Save results -----------------------------------------------------
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\nResults saved to: {RESULTS_FILE}", flush=True)
    print("\nAll vandal analysis complete.", flush=True)


if __name__ == "__main__":
    main()
