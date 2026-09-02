# -*- coding: utf-8 -*-
r"""
Reprocess the VHI warnregion statistics (CSV / GeoJSON / Parquet) for a date range.

The VHI rasters (vegetation-10m / forest-10m COGs) are ALWAYS read from the
PROD FSDI STAC (data.geo.admin.ch, collection ch.swisstopo.swisseo_vhi_v100),
directly over HTTP - no download needed. For each date in the range the
warnregion statistics are re-extracted with main_extract_warnregions.export()
and the resulting CSV / GeoJSON / Parquet files are uploaded via STAC:

- default: INT   (sys-data.int.bgdi.ch, uses configuration/dev_config.py)
- --PROD:  PROD  (data.geo.admin.ch,    uses configuration/prod_config.py)

Dates are processed in parallel (one worker thread per date, default 3).

Usage (paths are resolved to the repo root, run from anywhere):
    python main_functions/util_reprocess_vhi_warnregions.py 2026-08-01 2026-08-15
    python main_functions/util_reprocess_vhi_warnregions.py 2026-08-10 2026-08-10 --PROD
    python main_functions/util_reprocess_vhi_warnregions.py 2026-08-01 2026-08-15 --dry-run
    python main_functions/util_reprocess_vhi_warnregions.py 2026-08-01 2026-08-15 --threads 6

Options:
    --PROD      upload to PROD (data.geo.admin.ch) instead of INT
    --dry-run   extract only, no upload (implies --keep)
    --keep      keep the generated local files after upload
    --suffixes  which rasters to process (default: vegetation forest)
    --threads   number of dates processed in parallel (default: 3)

Dates where no VHI item exists on PROD (e.g. cloudy days) are skipped.
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Source of the rasters: ALWAYS the PROD FSDI STAC
SOURCE_STAC_API = "https://data.geo.admin.ch/api/stac/v0.9/"
COLLECTION = "ch.swisstopo.swisseo_vhi_v100"
# descriptor for the mean column in the exported files (see satromo_publish.extract_descriptor_mean)
MEAN_TYPE = "vhi"
WARNREGION_FORMATS = [".csv", ".geojson", ".parquet"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Reprocess VHI warnregion statistics from PROD rasters and "
                    "upload them to INT (default) or PROD (--PROD).")
    parser.add_argument("start_date", help="start date YYYY-MM-DD (inclusive)")
    parser.add_argument("end_date", help="end date YYYY-MM-DD (inclusive)")
    parser.add_argument("--PROD", action="store_true", dest="prod",
                        help="upload to PROD (data.geo.admin.ch) instead of INT")
    parser.add_argument("--dry-run", action="store_true",
                        help="extract only, do not upload (implies --keep)")
    parser.add_argument("--keep", action="store_true",
                        help="keep the generated local files after upload")
    parser.add_argument("--suffixes", nargs="+", default=["vegetation", "forest"],
                        choices=["vegetation", "forest"],
                        help="which rasters to process (default: vegetation forest)")
    parser.add_argument("--threads", type=int, default=3,
                        help="number of dates processed in parallel (default: 3)")
    args = parser.parse_args()

    for name in ("start_date", "end_date"):
        try:
            datetime.strptime(getattr(args, name), "%Y-%m-%d")
        except ValueError:
            parser.error(f"{name} must be in YYYY-MM-DD format")
    if args.start_date > args.end_date:
        parser.error("start_date must be <= end_date")
    if args.threads < 1:
        parser.error("--threads must be >= 1")
    return args


def daterange(start_str, end_str):
    day = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    while day <= end:
        yield day.strftime("%Y-%m-%d")
        day += timedelta(days=1)


def get_source_item(date_str):
    """Fetch the PROD STAC item for a date, or None if it does not exist."""
    item_id = date_str + "t235959"
    response = requests.get(
        f"{SOURCE_STAC_API}collections/{COLLECTION}/items/{item_id}")
    if response.status_code == 200:
        return response.json()
    return None


def ensure_target_item(config, mps, source_item, date_str, geocat_id):
    """
    Make sure the target STAC item exists (it may be missing on INT).
    publish_to_stac() only creates items for TIF assets, so for the
    CSV/GeoJSON/Parquet-only upload the item is created here if needed,
    reusing the geometry of the PROD source item.
    """
    item_id = date_str + "t235959"
    target_item_url = (f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}"
                       f"{config.STAC_FSDI_API}collections/{COLLECTION}/items/{item_id}")
    if requests.get(target_item_url).status_code == 200:
        return True

    print(f"{date_str}: target ITEM {item_id} missing - creating")
    # initialize the module globals (user/password) used by upload_item
    mps.determine_run_type()
    mps.initialize_fsdi()
    coordinates = source_item["geometry"]["coordinates"][0]
    item_title = COLLECTION.replace("ch.swisstopo.", "") + "_" + item_id
    payload = mps.item_create_json_payload(
        item_id, coordinates, date_str + "T23:59:59Z", item_title, geocat_id, None)
    if not mps.upload_item(target_item_url, payload):
        print(f"{date_str}: ERROR - could not create target ITEM {item_id}")
        return False
    return True


def process_date(date_str, args, config, mps, extract, params):
    """
    Process one date: extract the warnregion statistics for all requested
    suffixes and upload them. Runs in a worker thread; all filenames contain
    the date, so parallel dates never touch the same files.

    Returns one of 'processed', 'skipped', 'failed'.
    """
    item_id = date_str + "t235959"
    source_item = get_source_item(date_str)
    if source_item is None:
        print(f"{date_str}: no VHI item on PROD - skipped")
        return "skipped"

    # transient remote-read hiccups (GDAL/vsicurl header race under threading,
    # surfacing as NotGeoreferencedWarning which is escalated to an error in
    # main) get one retry before the date is reported as failed
    attempts = 2
    for attempt in range(1, attempts + 1):
        try:
            for suffix in args.suffixes:
                tif_asset = f"{COLLECTION}_mosaic_{item_id}_{suffix}-10m.tif"
                if tif_asset not in source_item["assets"]:
                    print(f"{date_str}: asset {suffix}-10m.tif missing - skipped")
                    continue
                raster_url = source_item["assets"][tif_asset]["href"]

                # same naming convention as the publish pipeline (uppercase T locally,
                # publish_to_stac lowercases the STAC item/asset names)
                item_ts = date_str + "T235959"
                filename = f"{COLLECTION}_{item_ts}_{suffix}-warnregions"

                print(f"{date_str}: extracting {suffix} ...")
                extract.export(
                    raster_url, config.WARNREGIONS, filename,
                    date_str + "T23:59:59Z",
                    params["missing_values"], params["no_data_values"],
                    params["scaling_factor"], MEAN_TYPE)

                if args.dry_run:
                    print(f"{date_str}: dry run - files kept: "
                          + ", ".join(filename + ext for ext in WARNREGION_FORMATS))
                    continue

                if not ensure_target_item(config, mps, source_item, date_str,
                                          params["geocat_id"]):
                    raise RuntimeError(f"target item {item_id} could not be created")

                for ext in WARNREGION_FORMATS:
                    mps.publish_to_stac(filename + ext, item_ts, COLLECTION,
                                        params["geocat_id"])

                if not args.keep:
                    for ext in WARNREGION_FORMATS:
                        if os.path.exists(filename + ext):
                            os.remove(filename + ext)

            print(f"{date_str}: done")
            return "processed"
        except Exception as e:
            if attempt < attempts:
                print(f"{date_str}: attempt {attempt} failed ({e}) - retrying")
            else:
                print(f"{date_str}: FAILED - {e}")
    return "failed"


def main():
    args = parse_args()

    # Resolve everything relative to the repo root (config paths, assets, secrets)
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)
    sys.path.insert(0, str(repo_root))

    # configuration/__init__.py reads sys.argv[1] as the config filename,
    # so the config has to be injected before the first project import
    config_file = "prod_config.py" if args.prod else "dev_config.py"
    sys.argv = [sys.argv[0], config_file]
    import configuration as config
    from main_functions import main_extract_warnregions
    from main_functions import main_publish_stac_fsdi as mps

    # A raster that opens without georeferencing (transient GDAL/vsicurl race
    # under threading) would make mask() silently produce garbage - escalate
    # the warning to an error so the date fails and is retried instead
    import warnings
    from rasterio.errors import NotGeoreferencedWarning
    warnings.filterwarnings("error", category=NotGeoreferencedWarning)

    target = (f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}"
              f"{config.STAC_FSDI_API}collections/{COLLECTION}")
    print(f"Source:  {SOURCE_STAC_API}collections/{COLLECTION} (always PROD)")
    print(f"Target:  {target} ({'PROD' if args.prod else 'INT'})"
          + (" [DRY RUN - no upload]" if args.dry_run else ""))
    print(f"Rasters: {', '.join(args.suffixes)} | threads: {args.threads}")

    params = {
        "missing_values": config.PRODUCT_VHI["missing_data"],
        "no_data_values": config.PRODUCT_VHI["no_data"],
        "scaling_factor": config.PRODUCT_VHI["scaling_factor"],
        "geocat_id": config.PRODUCT_VHI["geocat_id"],
    }

    dates = list(daterange(args.start_date, args.end_date))
    results = {}
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = {pool.submit(process_date, date_str, args, config, mps,
                               main_extract_warnregions, params): date_str
                   for date_str in dates}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

    processed = sorted(d for d, r in results.items() if r == "processed")
    skipped = sorted(d for d, r in results.items() if r == "skipped")
    failed = sorted(d for d, r in results.items() if r == "failed")

    print("\n----- summary -----")
    print(f"processed: {len(processed)} {processed}")
    print(f"skipped (no item): {len(skipped)} {skipped}")
    print(f"failed: {len(failed)} {failed}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
