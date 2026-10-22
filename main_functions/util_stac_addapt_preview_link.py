# -*- coding: utf-8 -*-
r"""
Adapt the 'visual' link (map.geo.admin.ch) of already published STAC items,
without re-uploading any data.

For every item of the collection the expected links are derived from the
product configuration ('visual_link_months' in PRODUCT_NDVIdiff / PRODUCT_NDVIz, see
main_publish_stac_fsdi.has_visual_link):

- 'visual' link expected but missing -> it is added
- 'visual' link present but not expected -> it is removed
- otherwise the item is left untouched

All other links of the item (e.g. preview/thumbnail) are kept as they are.
Only the 'links' of the item are patched (PATCH), assets are not touched.

The environment is selected like in the other util_* scripts:
- default: INT / DEV  (sys-data.int.bgdi.ch, uses configuration/dev_config.py)
- --PROD:  PROD       (data.geo.admin.ch,    uses configuration/prod_config.py)

Usage (paths are resolved to the repo root, run from anywhere):
    python main_functions/util_stac_addapt_preview_link.py --dry-run
    python main_functions/util_stac_addapt_preview_link.py
    python main_functions/util_stac_addapt_preview_link.py --PROD --dry-run
    python main_functions/util_stac_addapt_preview_link.py --PROD
    python main_functions/util_stac_addapt_preview_link.py --PROD --collection swisseo_ndvi_diff_v100

Options:
    --PROD         work on PROD (data.geo.admin.ch) instead of INT
    --collection   collection to fix (default: every product in the config that defines
                   'visual_link_months', i.e. swisseo_ndvi_diff_v100 and swisseo_ndvi_z_v100)
    --date         only fix the item(s) of this day (YYYY-MM-DD)
    --start-date   only fix items from this day on (inclusive)
    --end-date     only fix items up to this day (inclusive)
    --dry-run      only report what would be changed, no write access

Date examples:
    python main_functions/util_stac_addapt_preview_link.py --date 2026-07-31 --dry-run
    python main_functions/util_stac_addapt_preview_link.py --start-date 2026-01-01 --end-date 2026-07-31
"""

import argparse
import os
import sys
from pathlib import Path

import requests

# Links the STAC server generates itself, they must not be sent back
SERVER_LINK_RELS = {"self", "root", "parent", "collection", "item", "items", "alternate"}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Add/remove the 'visual' link of published STAC items "
                    "according to the product configuration (no data re-upload).")
    parser.add_argument("--PROD", action="store_true", dest="prod",
                        help="work on PROD (data.geo.admin.ch) instead of INT")
    parser.add_argument("--collection", default=None,
                        help="collection name (default: all products with 'visual_link_months')")
    parser.add_argument("--date", default=None, metavar="YYYY-MM-DD",
                        help="only fix the item(s) of this day")
    parser.add_argument("--start-date", default=None, metavar="YYYY-MM-DD",
                        help="only fix items from this day on (inclusive)")
    parser.add_argument("--end-date", default=None, metavar="YYYY-MM-DD",
                        help="only fix items up to this day (inclusive)")
    parser.add_argument("--dry-run", action="store_true",
                        help="only report the changes, do not write")
    args = parser.parse_args()
    if args.date:
        args.start_date = args.end_date = args.date
    return args


def iter_items(url, auth):
    """Yields all items of a collection, following the 'next' pagination links."""
    params = {"limit": 100}
    while url:
        response = requests.get(url, params=params, auth=auth)
        response.raise_for_status()
        data = response.json()
        for feature in data.get("features", []):
            yield feature
        url = next((link["href"] for link in data.get("links", [])
                    if link.get("rel") == "next"), None)
        params = None  # the next link already carries its query parameters


def expected_links(mps, feature, collection):
    """
    Returns the links the item should have: the user links already on the item
    (without 'visual') plus the 'visual' link if the configuration wants one.
    """
    item_id = feature["id"]
    props = feature["properties"]
    coordinates = feature["geometry"]["coordinates"][0]

    # Same function as used for publishing -> identical visual link and rule
    payload = mps.item_create_json_payload(
        item_id, coordinates, props["datetime"], props["title"], None, None)
    visual = [link for link in payload["links"] if link["rel"] == "visual"]

    kept = [link for link in feature.get("links", [])
            if link.get("rel") not in SERVER_LINK_RELS and link.get("rel") != "visual"]
    return kept + visual


def main():
    args = parse_args()

    # Resolve everything relative to the repo root (config paths, secrets)
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)
    sys.path.insert(0, str(repo_root))

    # configuration/__init__.py reads sys.argv[1] as the config filename,
    # so the config has to be injected before the first project import
    config_file = "prod_config.py" if args.prod else "dev_config.py"
    sys.argv = [sys.argv[0], config_file]
    import configuration as config
    from main_functions import main_publish_stac_fsdi as mps

    if args.collection:
        products = [args.collection]
    else:
        # all products of the configuration that restrict their visual link
        products = [value["product_name"] for value in
                    (getattr(config, name, None) for name in dir(config))
                    if isinstance(value, dict) and "visual_link_months" in value
                    and "product_name" in value]
    if not products:
        sys.exit("No product with 'visual_link_months' found in " + config_file)

    stac_path = f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}{config.STAC_FSDI_API}"

    mps.determine_run_type()
    mps.initialize_fsdi()
    auth = (mps.user, mps.password)

    for product in products:
        collection = product if product.startswith("ch.swisstopo.") else "ch.swisstopo." + product
        process_collection(args, mps, auth, f"{stac_path}collections/{collection}", collection)


def process_collection(args, mps, auth, collection_url, collection):
    print(f"Target: {collection_url} ({'PROD' if args.prod else 'INT'})"
          + (" [DRY RUN - no write]" if args.dry_run else ""))

    checked = changed = failed = 0
    for feature in iter_items(collection_url + "/items", auth):
        # ISO dates compare correctly as strings (YYYY-MM-DD prefix of the datetime)
        item_day = feature["properties"]["datetime"][:10]
        if (args.start_date and item_day < args.start_date) or \
                (args.end_date and item_day > args.end_date):
            continue

        checked += 1
        item_id = feature["id"]
        has_visual = any(link.get("rel") == "visual" for link in feature.get("links", []))
        links = expected_links(mps, feature, collection)
        wants_visual = any(link["rel"] == "visual" for link in links)

        if has_visual == wants_visual:
            continue

        action = "add visual link" if wants_visual else "remove visual link"
        changed += 1
        print(f"{item_id}: {action}")
        if args.dry_run:
            continue

        response = requests.patch(f"{collection_url}/items/{item_id}",
                                  json={"links": links}, auth=auth)
        if response.status_code // 200 != 1:
            failed += 1
            print(f"  FAILED ({response.status_code}): {response.text}")

    print(f"Done: {checked} items checked, {changed} to change"
          f"{'' if args.dry_run else f', {failed} failed'}.")


if __name__ == "__main__":
    main()
