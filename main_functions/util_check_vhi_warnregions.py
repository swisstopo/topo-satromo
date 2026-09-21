# -*- coding: utf-8 -*-
r"""
Check / QA the published VHI warnregion statistics for a date range.

For each date in the range the warnregion PARQUET assets (vegetation + forest)
are streamed directly from the FSDI data host (cloud-native, no download to
disk) and analysed:

- asset update timestamps (STAC 'updated'), incl. their distribution
  -> verify that a reprocessing actually refreshed the expected dates
- validity checks per region and date:
    * 100 < vhi_mean < 110  (mean averaged over a MIX of real and sentinel
      pixels - the actual corruption pattern; an exact 110 is NOT flagged,
      see VHI_MISSING_DATA below - it always means "no real data", however
      the (separately, sometimes wrongly reported) availability reads)
    * availability outside 0..100             (old fallback wrote 110)
    * availability == 100.0 for ALL regions   (parameter-mix bug signature)
- staleness check: for every date/suffix, the STAC 'updated' timestamp of
  each of the three warnregion files (.csv, .geojson, .parquet) is compared
  against --cutoff-date. A stale timestamp is NOT proof the file is wrong -
  the STAC backend only bumps 'updated' on a genuine content change, so an
  already-correct file reprocessed to identical output keeps an old
  timestamp forever. Every stale entry is therefore actually resolved:
  matching a known bug pattern in the parquet-sourced data, or (if not) by
  fetching that exact csv/geojson and comparing its values against the
  current data. Only entries that fail this - a real bug, or content that
  genuinely differs - are reported as needing reprocessing, with a
  ready-to-run command per date (*_stale_dates.csv); the rest are reported
  as a one-line harmless summary, not spelled out row by row.
- per-region summary (min/max/mean VHI and availability) written to CSV
- a graphical report: update-date panel + VHI/availability heatmaps
  (region x date) and a small-multiples time series per region

Checked environment:
- default: INT   (sys-data.int.bgdi.ch)
- --PROD:  PROD  (data.geo.admin.ch)

Usage (paths are resolved to the repo root, run from anywhere):
    python main_functions/util_check_vhi_warnregions.py 2026-08-01 2026-08-15
    python main_functions/util_check_vhi_warnregions.py 2026-08-01 2026-08-15 --PROD
    python main_functions/util_check_vhi_warnregions.py 2026-08-01 2026-08-15 --outdir report
    python main_functions/util_check_vhi_warnregions.py 2026-08-01 2026-08-15 --cutoff-date 2026-09-09

Options:
    --PROD         check PROD (data.geo.admin.ch) instead of INT
    --suffixes     which warnregion sets to check (default: vegetation forest)
    --threads      number of dates fetched in parallel (default: 3)
    --outdir       output directory for PNG/CSV report (default: current dir)
    --cutoff-date  staleness check threshold, YYYY-MM-DD (default: 2026-09-09,
                   the date of the PROD parameter-mix-bug reprocessing run)

Exit code is 1 if any validity or staleness check fails, 0 otherwise (CI-friendly).
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# make the repo root importable so the sibling util can be reused
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from main_functions.util_reprocess_vhi_warnregions import (  # noqa: E402
    daterange, COLLECTION, WARNREGION_FORMATS)

HOSTS = {"prod": "data.geo.admin.ch", "int": "sys-data.int.bgdi.ch"}

# VHI missing-data sentinel (configuration/*.py, PRODUCT_VHI['missing_data']).
# Real VHI is clamped to 0..100 (step1_processor_vhi.py), so an average can
# equal this sentinel EXACTLY only if every contributing pixel was itself the
# sentinel - i.e. the region genuinely had zero real data that day. That
# holds regardless of what the (separate, on old un-reprocessed PROD data
# sometimes wrong) availability_percentage column reports for the same row.
VHI_MISSING_DATA = 110

# chart colors: validated reference palette (dataviz), light mode
COL_VEG = "#2a78d6"      # categorical slot 1 (blue)  - vegetation
COL_FOREST = "#eb6834"   # categorical slot 2 (orange) - forest
COL_SURFACE = "#fcfcfb"
COL_TEXT = "#0b0b0b"
COL_TEXT_2 = "#52514e"
COL_NODATA = "#e3e2de"   # masked cells (region without data that day)
# reserved status colors (never reused for suffix/format identity)
COL_CRITICAL = "#d03b3b"  # known bug pattern
COL_SERIOUS = "#ec835a"   # content differs from current data


def parse_args():
    parser = argparse.ArgumentParser(
        description="QA check of the published VHI warnregion parquet files "
                    "on INT (default) or PROD (--PROD).")
    parser.add_argument("start_date", help="start date YYYY-MM-DD (inclusive)")
    parser.add_argument("end_date", help="end date YYYY-MM-DD (inclusive)")
    parser.add_argument("--PROD", action="store_true", dest="prod",
                        help="check PROD (data.geo.admin.ch) instead of INT")
    parser.add_argument("--suffixes", nargs="+", default=["vegetation", "forest"],
                        choices=["vegetation", "forest"],
                        help="which warnregion sets to check (default: vegetation forest)")
    parser.add_argument("--threads", type=int, default=3,
                        help="number of dates fetched in parallel (default: 3)")
    parser.add_argument("--outdir", default=".",
                        help="output directory for the PNG/CSV report (default: current dir)")
    parser.add_argument("--cutoff-date", default="2026-09-09",
                        help="staleness check: flag csv/geojson/parquet assets whose STAC "
                             "'updated' timestamp is missing or before this date, i.e. not "
                             "(yet) touched by a reprocessing run (default: 2026-09-09, the "
                             "PROD parameter-mix-bug reprocessing run)")
    args = parser.parse_args()

    for name, label in [("start_date", "start_date"), ("end_date", "end_date"),
                        ("cutoff_date", "--cutoff-date")]:
        try:
            datetime.strptime(getattr(args, name), "%Y-%m-%d")
        except ValueError:
            parser.error(f"{label} must be in YYYY-MM-DD format")
    if args.start_date > args.end_date:
        parser.error("start_date must be <= end_date")
    if args.threads < 1:
        parser.error("--threads must be >= 1")
    return args


def fetch_date(date_str, host, suffixes):
    """
    Fetch the warnregion assets of one date from the STAC data host: the
    parquet content (for the stats/plots) plus the STAC 'updated' timestamp
    of every warnregion format (csv/geojson/parquet), for the staleness check.

    Returns (date_str, result) where result is a dict:
      {'status': 'ok'|'no_item',
       '<suffix>': {'df': DataFrame, 'updated': str},   # parquet only, if present
       'asset_updates': [{'suffix', 'format', 'updated'|None}, ...]}
    """
    item_id = date_str + "t235959"
    r = requests.get(f"https://{host}/api/stac/v0.9/collections/{COLLECTION}/items/{item_id}")
    if r.status_code != 200:
        return date_str, {"status": "no_item"}
    assets = r.json()["assets"]

    result = {"status": "ok", "asset_updates": []}
    for suffix in suffixes:
        for ext in WARNREGION_FORMATS:
            key = f"{COLLECTION}_{item_id}_{suffix}-warnregions{ext}"
            asset = assets.get(key)
            result["asset_updates"].append({
                "suffix": suffix,
                "format": ext.lstrip("."),
                # 'updated' is only set once an asset is overwritten; a
                # never-reprocessed file only carries 'created'
                "updated": (asset.get("updated", asset.get("created"))
                           if asset is not None else None),
            })

        key = f"{COLLECTION}_{item_id}_{suffix}-warnregions.parquet"
        if key not in assets:
            continue
        resp = requests.get(assets[key]["href"])
        if resp.status_code != 200:
            continue
        df = pd.read_parquet(BytesIO(resp.content))
        df = df.drop(columns=[c for c in ("geometry",) if c in df.columns])
        result[suffix] = {"df": df,
                          "updated": assets[key].get("updated",
                                                     assets[key].get("created", ""))}
    return date_str, result


def load_region_names():
    """REGION_NR -> Name from the warnregion shapefile (best effort)."""
    try:
        import geopandas as gpd
        gdf = gpd.read_file(REPO_ROOT / "assets" / "warnregionen_vhi_2056.shp")

        def clean(name):
            # the shapefile is UTF-8 but gets decoded as latin-1/cp1252
            # ("ö" -> "Ã¶"); repair if possible, and replace the cp1252 dash
            try:
                name = name.encode("latin-1").decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass
            return name.replace("\x96", "-")

        names = gdf.Name.astype(str).map(clean)
        return dict(zip(gdf.REGION_NR.astype(int), names))
    except Exception:
        return {}


def collect(args, host):
    """Fetch all dates and assemble one tidy DataFrame + update-date records."""
    dates = list(daterange(args.start_date, args.end_date))
    rows, updates, asset_updates, no_item, missing_asset = [], [], [], [], []

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = [pool.submit(fetch_date, d, host, args.suffixes) for d in dates]
        for future in as_completed(futures):
            date_str, result = future.result()
            if result["status"] == "no_item":
                no_item.append(date_str)
                continue
            for au in result.get("asset_updates", []):
                asset_updates.append({"date": date_str, **au})
            for suffix in args.suffixes:
                if suffix not in result:
                    missing_asset.append(f"{date_str}/{suffix}")
                    continue
                df = result[suffix]["df"].copy()
                df["date"] = date_str
                df["suffix"] = suffix
                rows.append(df[["date", "suffix", "REGION_NR",
                                "vhi_mean", "availability_percentage"]])
                updates.append({"date": date_str, "suffix": suffix,
                                "updated": result[suffix]["updated"]})

    data = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["date", "suffix", "REGION_NR", "vhi_mean", "availability_percentage"])
    return (data, pd.DataFrame(updates), pd.DataFrame(asset_updates),
            sorted(no_item), sorted(missing_asset))


def no_data_mask(data):
    """
    Legitimate 'no data': vhi_mean carries the exact missing-data sentinel,
    see VHI_MISSING_DATA. This is CORRECT (not a violation) irrespective of
    what availability_percentage says on the same row - two known, harmless
    ways the pair can look "inconsistent" without being wrong:
      - on old, un-reprocessed PROD rows the availability count itself was
        buggy (the parameter-mix bug) even though the mean is unaffected,
        so availability can read > 0 alongside an exact-110 mean
      - a tiny sliver of real, valid pixels in an otherwise fully masked
        region rounds availability down to 0.0 at the published 1-decimal
        precision, so availability can read 0 alongside a plausible mean
    Neither case is flagged; see run_checks() for the pattern that IS.
    """
    return data.vhi_mean == VHI_MISSING_DATA


def run_checks(data):
    """Return (violations DataFrame, signature dates list, no_data_count)."""
    v = []
    # sentinel leaked into the mean: a value strictly between the real VHI
    # range (0..100) and the sentinel (110) means the mean was averaged over
    # a MIX of real and sentinel pixels - the actual corruption pattern. An
    # exact 110 is never flagged, see no_data_mask().
    bad_mean = data[(data.vhi_mean > 100) & (data.vhi_mean < VHI_MISSING_DATA)]
    for _, r in bad_mean.iterrows():
        v.append({**r[["date", "suffix", "REGION_NR"]].to_dict(),
                  "check": f"vhi_mean between 100 and the {VHI_MISSING_DATA} sentinel "
                          "(mix of real and missing-data pixels)",
                  "value": r.vhi_mean})
    # availability outside the valid range (old fallback exported 110)
    bad_avail = data[(data.availability_percentage < 0) | (data.availability_percentage > 100)]
    for _, r in bad_avail.iterrows():
        v.append({**r[["date", "suffix", "REGION_NR"]].to_dict(),
                  "check": "availability outside 0..100",
                  "value": r.availability_percentage})
    violations = pd.DataFrame(v)

    # parameter-mix bug signature: availability exactly 100.0 for >90% of regions
    signature = []
    for (date, suffix), grp in data.groupby(["date", "suffix"]):
        if len(grp) and (grp.availability_percentage == 100.0).mean() > 0.9:
            signature.append(f"{date}/{suffix}")
    return violations, sorted(signature), int(no_data_mask(data).sum())


def check_staleness(asset_updates, cutoff_date):
    """
    Of the three warnregion files (csv/geojson/parquet) per date and suffix,
    list those whose STAC 'updated' timestamp is missing (never reprocessed,
    only ever 'created') or before `cutoff_date` (YYYY-MM-DD) - i.e. not
    (yet) touched by the reprocessing run that finished on that date.
    """
    if not len(asset_updates):
        return asset_updates.assign(updated_date=[])
    au = asset_updates.copy()
    au["updated_date"] = au.updated.str[:10]
    stale = au[au.updated_date.isna() | (au.updated_date < cutoff_date)]
    return stale.sort_values(["date", "suffix", "format"])


def mark_known_bug(stale, violations, signature):
    """
    Cross-reference each stale (date, suffix, format) row against the
    validity checks: whether the PARQUET-sourced data for that (date,
    suffix) matches a KNOWN bug pattern (a validity violation, or the
    parameter-mix-bug signature). This alone is not enough to call the rest
    "harmless" - it only rules out the bug patterns we know to look for.
    See verify_stale_content() for the actual proof. Adds a 'known_bug'
    column.
    """
    if not len(stale):
        return stale.assign(known_bug=[])
    broken_pairs = set(zip(violations.date, violations.suffix)) if len(violations) else set()
    broken_pairs |= {tuple(s.split("/", 1)) for s in signature}
    stale = stale.copy()
    stale["known_bug"] = [(d, s) in broken_pairs for d, s in zip(stale.date, stale.suffix)]
    return stale


def fetch_format_content(host, date_str, suffix, fmt):
    """
    Fetch REGION_NR -> (vhi_mean, availability_percentage) directly from
    the published csv or geojson asset (small, no geometry parsing needed
    for csv; geojson feature properties for geojson). Returns None if the
    asset is missing or unreadable.
    """
    item_id = date_str + "t235959"
    base = f"https://{host}/{COLLECTION}/{item_id}/{COLLECTION}_{item_id}_{suffix}-warnregions"
    try:
        if fmt == "csv":
            r = requests.get(base + ".csv")
            if r.status_code != 200:
                return None
            df = pd.read_csv(StringIO(r.text))
            return {int(row.REGION_NR): (row.vhi_mean, row.availability_percentage)
                    for row in df.itertuples()}
        if fmt == "geojson":
            r = requests.get(base + ".geojson")
            if r.status_code != 200:
                return None
            feats = r.json().get("features", [])
            return {int(f["properties"]["REGION_NR"]):
                    (f["properties"]["vhi_mean"], f["properties"]["availability_percentage"])
                    for f in feats}
    except Exception:
        return None
    return None  # parquet: already in `data`, nothing to fetch


def verify_stale_content(stale, data, host):
    """
    The only real proof that a stale (old-'updated') csv/geojson asset is
    actually fine: fetch that EXACT file and compare its per-region values
    against the parquet-sourced `data` this whole report is built on - not
    just "does the parquet look free of known bug patterns" (mark_known_bug
    above), which says nothing about whether THIS OTHER FILE agrees with
    it. Adds a 'content_verified' column (parquet rows are trivially True,
    since they ARE the source of `data`); one HTTP request per unique
    (date, suffix, format), cached across duplicate rows.
    """
    if not len(stale):
        return stale.assign(content_verified=[])

    def normalize(mean, avail):
        return (int(round(float(mean))), round(float(avail), 1))

    ref_cache = {}
    content_cache = {}
    verified = []
    for row in stale.itertuples():
        if row.format == "parquet":
            verified.append(True)
            continue
        key = (row.date, row.suffix, row.format)
        if key not in content_cache:
            content_cache[key] = fetch_format_content(host, row.date, row.suffix, row.format)
        content = content_cache[key]
        if content is None:
            verified.append(False)
            continue
        ref_key = (row.date, row.suffix)
        if ref_key not in ref_cache:
            ref = data[(data.date == row.date) & (data.suffix == row.suffix)]
            ref_cache[ref_key] = {int(r.REGION_NR): normalize(r.vhi_mean, r.availability_percentage)
                                  for r in ref.itertuples()}
        ref_map = ref_cache[ref_key]
        content_norm = {rn: normalize(*v) for rn, v in content.items()}
        verified.append(content_norm == ref_map)

    stale = stale.copy()
    stale["content_verified"] = verified
    return stale


def stale_dates_report(stale, prod):
    """
    One row per unique date still needing reprocessing (pass the already-
    filtered "actionable" subset - see main()), with a ready-to-run command
    for that date on the same environment that was checked (--PROD if PROD
    was checked, nothing - i.e. INT - otherwise). Paste straight into a
    shell, or drive a batch reprocessing loop off this CSV.
    """
    if not len(stale):
        return pd.DataFrame(columns=["date", "reprocess_command"])
    prod_flag = " --PROD" if prod else ""
    dates = sorted(stale.date.unique())
    return pd.DataFrame([
        {"date": d,
         "reprocess_command": f"python main_functions/util_reprocess_vhi_warnregions.py "
                              f"{d} {d}{prod_flag}"}
        for d in dates
    ])


def region_summary(data):
    """Per region x suffix: n dates, valid stats for VHI and availability."""
    # rows where the region really had data that day: a real average is
    # always <=100 regardless of the (sometimes rounded-to-0 or, on old
    # un-reprocessed rows, wrongly-100-looking) availability_percentage -
    # so vhi_mean alone identifies "real" rows, see no_data_mask()
    valid = data[data.vhi_mean <= 100]
    summary = valid.groupby(["suffix", "REGION_NR"]).agg(
        n_valid_dates=("vhi_mean", "size"),
        vhi_min=("vhi_mean", "min"),
        vhi_max=("vhi_mean", "max"),
        vhi_mean=("vhi_mean", "mean"),
        avail_min=("availability_percentage", "min"),
        avail_max=("availability_percentage", "max"),
        avail_mean=("availability_percentage", "mean"),
    ).round(1).reset_index()
    n_total = data.groupby(["suffix", "REGION_NR"]).size().rename("n_dates").reset_index()
    return n_total.merge(summary, on=["suffix", "REGION_NR"], how="left")


def build_matrix(data, suffix, dates, regions, column):
    """(region x date) matrix of `column`; NaN where region had no real data."""
    sub = data[data.suffix == suffix].copy()
    # mask the legitimate "no data" convention, see no_data_mask()
    if column == "vhi_mean":
        sub.loc[no_data_mask(sub), "vhi_mean"] = np.nan
    m = sub.pivot_table(index="REGION_NR", columns="date", values=column, aggfunc="first")
    return m.reindex(index=regions, columns=dates)


def plot_overview(data, asset_updates, stale_broken, args, env, dates, regions, names, outfile):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
    from matplotlib.lines import Line2D
    import matplotlib.dates as mdates

    # diverging (polarity around VHI=50): warm pole = stress, cool pole = good,
    # neutral gray midpoint; single-hue sequential for availability
    cmap_vhi = LinearSegmentedColormap.from_list(
        "vhi", [COL_FOREST, "#e8e7e3", COL_VEG])
    cmap_vhi.set_bad(COL_NODATA)
    cmap_av = LinearSegmentedColormap.from_list(
        "avail", ["#f2faf7", "#1baf7a", "#0b5c40"])
    cmap_av.set_bad(COL_NODATA)

    n_sfx = len(args.suffixes)
    fig_h = 4.6 + 5.2 * n_sfx
    fig, axes = plt.subplots(2 + 2 * n_sfx, 1, figsize=(14, fig_h),
                             gridspec_kw={"height_ratios": [0.55, 1.1] + [1.6] * (2 * n_sfx)})
    fig.patch.set_facecolor(COL_SURFACE)
    axes = np.atleast_1d(axes)

    # --- panel 0: staleness check status - which dates genuinely need
    # reprocessing (known bug pattern, or content that was fetched and
    # verified to actually differ from the current data) - NOT the same as
    # "has a stale STAC 'updated' timestamp", see verify_stale_content() ---
    ax = axes[0]
    ax.set_facecolor(COL_SURFACE)
    suffix_row = {"vegetation": 1, "forest": 0}
    status_color = {"known bug pattern": COL_CRITICAL,
                    "content differs from current data": COL_SERIOUS}
    date_pos = {d: i for i, d in enumerate(dates)}
    active_rows = [suffix_row[s] for s in ["vegetation", "forest"] if s in args.suffixes]
    if len(stale_broken):
        # one marker per (date, suffix): the worse of the two reasons if a
        # date/suffix has both (e.g. one format known-bad, another mismatched)
        rank = {"known bug pattern": 0, "content differs from current data": 1}
        worst = (stale_broken.assign(_rank=stale_broken.reason.map(rank))
                 .sort_values("_rank").drop_duplicates(["date", "suffix"]))
        for row in worst.itertuples():
            if row.suffix not in args.suffixes or row.date not in date_pos:
                continue
            ax.add_patch(plt.Rectangle(
                (date_pos[row.date] - 0.4, suffix_row[row.suffix] - 0.4), 0.8, 0.8,
                facecolor=status_color[row.reason], edgecolor="none"))
    ax.set_xlim(-0.6, len(dates) - 0.4)
    ax.set_ylim(-0.7, max(active_rows, default=0) + 0.7)
    ax.set_yticks([suffix_row[s] for s in ["vegetation", "forest"] if s in args.suffixes])
    ax.set_yticklabels([s for s in ["vegetation", "forest"] if s in args.suffixes],
                       fontsize=8, color=COL_TEXT_2)
    ax.set_xticks([])
    ax.set_title(f"Staleness check ({env}) - dates that genuinely still need reprocessing",
                 loc="left", color=COL_TEXT, fontsize=11, fontweight="bold")
    for spine in ax.spines.values():
        spine.set_visible(False)
    status_handles = [
        Line2D([0], [0], marker="s", linestyle="None", markersize=9,
              markerfacecolor=color, markeredgecolor=color, label=label)
        for label, color in status_color.items()
    ]
    ax.legend(handles=status_handles, frameon=False, fontsize=8, loc="upper left",
             bbox_to_anchor=(1.005, 1.15), borderaxespad=0)

    # --- panel 1: asset update timestamps per sensing date, all 3 formats ---
    # color = suffix (identity, matches the rest of the report), shape+size
    # = format - the three files of one date/suffix are usually written
    # within seconds of each other (same publisher run) and would otherwise
    # sit on top of one another, so they are nested largest-to-smallest:
    # csv (large hollow circle) > geojson (medium hollow square) > parquet
    # (small filled triangle)
    ax = axes[1]
    ax.set_facecolor(COL_SURFACE)
    suffix_colors = {"vegetation": COL_VEG, "forest": COL_FOREST}
    format_styles = {
        "csv":     dict(marker="o", s=120, facecolors="none", linewidths=2.2),
        "geojson": dict(marker="s", s=56, facecolors="none", linewidths=1.6),
        "parquet": dict(marker="^", s=26, linewidths=0),
    }
    au = asset_updates.dropna(subset=["updated"])
    for suffix in ["vegetation", "forest"]:
        if suffix not in args.suffixes:
            continue
        color = suffix_colors[suffix]
        for fmt, style in format_styles.items():
            sub = au[(au.suffix == suffix) & (au.format == fmt)]
            if not len(sub):
                continue
            x = pd.to_datetime(sub.date)
            y = pd.to_datetime(sub.updated.str[:19])
            facecolors = style.get("facecolors", color)
            ax.scatter(x, y, marker=style["marker"], s=style["s"],
                      facecolors=facecolors, edgecolors=color,
                      linewidths=style["linewidths"], zorder=3)
    ax.set_title(f"Asset update timestamps ({env}) - color = suffix, shape = format",
                 loc="left", color=COL_TEXT, fontsize=11, fontweight="bold")
    ax.set_ylabel("asset updated", color=COL_TEXT_2, fontsize=9)
    ax.grid(True, alpha=0.25, linewidth=0.6)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.tick_params(colors=COL_TEXT_2, labelsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m.%y"))
    ax.yaxis.set_major_formatter(mdates.DateFormatter("%d.%m.%y"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")

    # two independent legends: color (suffix) and shape (format)
    suffix_handles = [
        Line2D([0], [0], marker="o", linestyle="None", markersize=8,
              markerfacecolor=suffix_colors[s], markeredgecolor=suffix_colors[s],
              label=s)
        for s in ["vegetation", "forest"] if s in args.suffixes
    ]
    format_handles = [
        Line2D([0], [0], marker=style["marker"], linestyle="None", markersize=8,
              markerfacecolor=style.get("facecolors", COL_TEXT_2)
              if style.get("facecolors", COL_TEXT_2) == "none" else COL_TEXT_2,
              markeredgecolor=COL_TEXT_2,
              markeredgewidth=1.6 if style.get("facecolors") == "none" else 0,
              label=fmt)
        for fmt, style in format_styles.items()
    ]
    # placed outside the axes (data can occupy any corner, e.g. a dense row
    # along the top when everything was reprocessed together); bbox_inches=
    # 'tight' on save below keeps them from getting clipped
    leg1 = ax.legend(handles=suffix_handles, frameon=False, fontsize=8,
                     loc="upper left", bbox_to_anchor=(1.005, 1.05),
                     borderaxespad=0, title="suffix", title_fontsize=8)
    ax.add_artist(leg1)
    ax.legend(handles=format_handles, frameon=False, fontsize=8,
             loc="upper left", bbox_to_anchor=(1.005, 0.55),
             borderaxespad=0, title="format", title_fontsize=8)

    # --- heatmap panels ---
    x_idx = np.arange(len(dates))
    ylabels = [f"{r} {names.get(r, '')[:18]}" for r in regions]
    panel = 2
    for suffix in args.suffixes:
        for column, cmap, norm, cb_label in [
            ("vhi_mean", cmap_vhi, TwoSlopeNorm(vmin=0, vcenter=50, vmax=100),
             "VHI mean (0 stress - 100 good)"),
            ("availability_percentage", cmap_av, None, "availability %"),
        ]:
            ax = axes[panel]; panel += 1
            ax.set_facecolor(COL_SURFACE)
            m = build_matrix(data, suffix, dates, regions, column)
            im = ax.imshow(m.values.astype(float), aspect="auto", cmap=cmap,
                           norm=norm, vmin=None if norm else 0,
                           vmax=None if norm else 100, interpolation="nearest")
            # flag violations on the VHI panel (see run_checks() bad_mean)
            if column == "vhi_mean":
                viol = data[(data.suffix == suffix) & (data.vhi_mean > 100)
                            & (data.vhi_mean < VHI_MISSING_DATA)]
                for _, r in viol.iterrows():
                    ax.plot(dates.index(r.date), regions.index(int(r.REGION_NR)),
                            "x", color=COL_TEXT, markersize=5, markeredgewidth=1.5)
            ax.set_title(f"{suffix} - {cb_label}", loc="left", color=COL_TEXT,
                         fontsize=10, fontweight="bold")
            ax.set_yticks(np.arange(len(regions)))
            ax.set_yticklabels(ylabels, fontsize=4.6, color=COL_TEXT_2)
            step = max(1, len(dates) // 20)
            ax.set_xticks(x_idx[::step])
            # 2-digit year prefix (YY-MM-DD): a range spanning >1 year would
            # otherwise show ambiguous, repeating MM-DD labels
            ax.set_xticklabels([d[2:] for d in dates[::step]], fontsize=7,
                               color=COL_TEXT_2, rotation=45, ha="right")
            cb = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
            cb.ax.tick_params(labelsize=7, colors=COL_TEXT_2)
            cb.outline.set_visible(False)

    fig.suptitle(f"swissEO VHI warnregions QA - {env} - {args.start_date} to {args.end_date}"
                 "  (gray = no data, x = check violation)",
                 x=0.01, ha="left", color=COL_TEXT, fontsize=12, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.985])
    # bbox_inches='tight': panel 1's legends sit outside the axes (see
    # above) and would otherwise get clipped at the figure edge
    fig.savefig(outfile, dpi=150, facecolor=COL_SURFACE, bbox_inches="tight")
    plt.close(fig)


def plot_timeseries(data, args, env, dates, regions, names, outfile):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ncols = 7
    nrows = int(np.ceil(len(regions) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(16, 2.0 * nrows),
                             sharex=True, sharey=True)
    fig.patch.set_facecolor(COL_SURFACE)
    x = np.arange(len(dates))

    for ax, region in zip(axes.flat, regions):
        ax.set_facecolor(COL_SURFACE)
        for suffix, color in zip(["vegetation", "forest"], [COL_VEG, COL_FOREST]):
            if suffix not in args.suffixes:
                continue
            m = build_matrix(data, suffix, dates, [region], "vhi_mean")
            ax.plot(x, m.values.ravel(), color=color, linewidth=1.4,
                    marker="o", markersize=2.4, label=suffix)
        ax.set_title(f"{region} {names.get(region, '')[:20]}", fontsize=7,
                     color=COL_TEXT, loc="left")
        ax.set_ylim(0, 105)
        ax.grid(True, alpha=0.2, linewidth=0.5)
        ax.tick_params(labelsize=6, colors=COL_TEXT_2)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
    for ax in axes.flat[len(regions):]:
        ax.axis("off")

    step = max(1, len(dates) // 4)
    for ax in axes[-1]:
        ax.set_xticks(x[::step])
        # 2-digit year prefix (YY-MM-DD), see plot_overview
        ax.set_xticklabels([d[2:] for d in dates[::step]], fontsize=6,
                           rotation=45, ha="right")

    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", frameon=False, fontsize=9)
    fig.suptitle(f"VHI mean per warnregion - {env} - {args.start_date} to {args.end_date}",
                 x=0.01, ha="left", color=COL_TEXT, fontsize=12, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(outfile, dpi=150, facecolor=COL_SURFACE)
    plt.close(fig)


def main():
    args = parse_args()
    env = "PROD" if args.prod else "INT"
    host = HOSTS["prod" if args.prod else "int"]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    tag = f"vhi_warnregions_check_{env}_{args.start_date}_{args.end_date}"

    print(f"Checking {env} (https://{host}) - {args.start_date} to {args.end_date}"
          f" | sets: {', '.join(args.suffixes)} | threads: {args.threads}")

    data, updates, asset_updates, no_item, missing_asset = collect(args, host)
    if not len(data):
        print("No warnregion parquet data found in the given range.")
        sys.exit(1)

    dates = sorted(data.date.unique())
    regions = sorted(data.REGION_NR.astype(int).unique())
    names = load_region_names()

    # ---- checks ----
    violations, signature, no_data_count = run_checks(data)
    stale = check_staleness(asset_updates, args.cutoff_date)
    stale = mark_known_bug(stale, violations, signature)
    # only bother verifying content for rows not already conclusively known-bad -
    # known_bug rows need reprocessing regardless of what the file itself says
    stale = verify_stale_content(stale, data, host)
    # actionable: either matches a known bug pattern, or its content was fetched
    # and genuinely differs from (or couldn't be compared against) the parquet
    # this report is built on - NOT the same as "stale timestamp", see the two
    # functions above for why a stale timestamp alone proves nothing either way
    stale_broken = stale[stale.known_bug | ~stale.content_verified] if len(stale) else stale
    stale_harmless = stale[~stale.known_bug & stale.content_verified] if len(stale) else stale
    # every row in stale_broken has known_bug True and/or content_verified False
    # (see the filter above), so "content differs..." is the only reachable else
    stale_broken = stale_broken.assign(
        reason=np.where(stale_broken.known_bug, "known bug pattern",
                        "content differs from current data")) if len(stale_broken) \
        else stale_broken.assign(reason=[])
    stale_dates = stale_dates_report(stale_broken, args.prod)

    # ---- summary CSV ----
    summary = region_summary(data)
    summary_file = outdir / f"{tag}_summary.csv"
    summary.to_csv(summary_file, index=False)

    # ---- plots ----
    overview_file = outdir / f"{tag}_overview.png"
    ts_file = outdir / f"{tag}_timeseries.png"
    plot_overview(data, asset_updates, stale_broken, args, env, dates, regions, names,
                 overview_file)
    plot_timeseries(data, args, env, dates, regions, names, ts_file)

    # ---- console report ----
    print("\n----- data coverage -----")
    print(f"dates with data: {len(dates)} / requested "
          f"{len(list(daterange(args.start_date, args.end_date)))}")
    if no_item:
        print(f"dates without STAC item ({len(no_item)}): {no_item}")
    if missing_asset:
        print(f"items without parquet asset ({len(missing_asset)}): {missing_asset}")

    print("\n----- update-date distribution (parquet 'updated') -----")
    if len(updates):
        upd = updates.copy()
        upd["updated_day"] = upd.updated.str[:10]
        print(upd.groupby(["updated_day", "suffix"]).size()
                 .rename("n_files").reset_index().to_string(index=False))

    print("\n----- validity checks -----")
    print(f"legitimate no-data rows (vhi_mean == {VHI_MISSING_DATA} sentinel): "
          f"{no_data_count} - not counted as violations")
    if len(violations):
        print(f"VIOLATIONS: {len(violations)}")
        print(violations.to_string(index=False))
    else:
        print(f"no violations (vhi_mean <= 100, or exactly the {VHI_MISSING_DATA} sentinel; "
              "availability within 0..100)")
    if signature:
        print(f"\nWARNING - parameter-mix bug signature (availability==100.0 for >90% "
              f"of regions): {len(signature)}")
        for s in signature:
            print(f"   {s}")

    print(f"\n----- staleness check (csv/geojson/parquet 'updated' before "
          f"{args.cutoff_date}) -----")
    stale_file = stale_dates_file = None
    if len(stale_harmless):
        harmless_dates = sorted(stale_harmless.date.unique())
        print(f"stale timestamp but content verified identical to the current data "
              f"(harmless, no action needed): {len(stale_harmless)} asset(s) across "
              f"{len(harmless_dates)} date(s) [{harmless_dates[0]}..{harmless_dates[-1]}] - "
              f"the STAC backend only bumps 'updated' on a genuine content change, so a file "
              f"that was already correct keeps its old timestamp when reprocessed to "
              f"identical output; fetched and compared against the current data to confirm")
    if len(stale_broken):
        print(f"\nSTALE AND STILL BROKEN: {len(stale_broken)} asset(s) across "
              f"{len(stale_dates)} unique date(s)")
        print(stale_broken[["date", "suffix", "format", "updated", "reason"]]
              .fillna("(never updated)").to_string(index=False))
        stale_file = outdir / f"{tag}_stale.csv"
        stale_broken[["date", "suffix", "format", "updated", "reason"]].to_csv(
            stale_file, index=False)

        stale_dates_file = outdir / f"{tag}_stale_dates.csv"
        stale_dates.to_csv(stale_dates_file, index=False)
        print(f"\n-> {len(stale_dates)} date(s) need reprocessing; "
              f"ready-to-run commands in {stale_dates_file.name}")
    elif not len(stale_harmless):
        print(f"all csv/geojson/parquet assets were updated on/after {args.cutoff_date}")
    else:
        print("\nno further action needed - all stale entries above are harmless")

    print("\n----- outputs -----")
    print(f"summary CSV: {summary_file}")
    if stale_file:
        print(f"stale CSV:   {stale_file}")
        print(f"stale dates: {stale_dates_file}")
    print(f"overview:    {overview_file}")
    print(f"time series: {ts_file}")

    sys.exit(1 if len(violations) or signature or len(stale_broken) else 0)


if __name__ == "__main__":
    main()
