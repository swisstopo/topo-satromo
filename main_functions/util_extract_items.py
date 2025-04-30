
"""
STAC Date Extraction Script

This script extracts dates from items in a STAC (SpatioTemporal Asset Catalog) API collection
and exports them to a CSV file.

Adapted from the STAC deletion script by David Oesch
"""

import pystac_client
import csv
from typing import  Set
import logging
from datetime import datetime
from urllib.parse import urljoin
import os
import sys
# Dynamisch Projekt-Root ermitteln und zu sys.path hinzufügen
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import configuration as config

COLLECTION_ID = 'ch.swisstopo.swisseo_vhi_v100'
STORAGE_PATH = os.getcwd() + '/'


def setup_stac_client(url: str) -> pystac_client.Client:
    """
    Initialize and setup STAC client with required conformance

    Args:
        url (str): STAC API endpoint URL

    Returns:
        pystac_client.Client: Configured STAC client
    """
    client = pystac_client.Client.open(url)
    client.add_conforms_to("COLLECTIONS")
    client.add_conforms_to("ITEM_SEARCH")
    return client


def get_collection(client: pystac_client.Client, collection_id: str):
    """
    Retrieve a specific collection

    Args:
        client (pystac_client.Client): STAC client
        collection_id (str): ID of the collection to retrieve

    Returns:
        Collection: The requested STAC collection
    """
    try:
        return client.get_collection(collection_id)
    except Exception as e:
        logging.error(f"Error retrieving collection {collection_id}: {str(e)}")
        raise


def extract_date_from_item(item) -> str:
    """
    Extract date from a STAC item.
    First tries the datetime property, then falls back to properties.

    Args:
        item: STAC item

    Returns:
        str: Date in YYYY-MM-DD format or None if not found
    """
    date_str = None

    # Try to get date from item.datetime
    if hasattr(item, 'datetime') and item.datetime:
        return item.datetime.strftime("%Y-%m-%d")

    # Try to get date from properties.datetime
    if hasattr(item, 'properties') and item.properties:
        if "datetime" in item.properties:
            date_prop = item.properties["datetime"]

            # Handle different date formats
            try:
                if isinstance(date_prop, str):
                    date_obj = datetime.fromisoformat(date_prop.replace('Z', '+00:00'))
                    return date_obj.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                # If there's an error parsing the date, try to extract just the date part
                if isinstance(date_prop, str) and len(date_prop) >= 10:
                    return date_prop[:10]

    # Try other common date fields
    if hasattr(item, 'properties') and item.properties:
        date_fields = ["date", "created", "updated", "observed", "acquisition_date"]
        for field in date_fields:
            if field in item.properties:
                try:
                    date_prop = item.properties[field]
                    if isinstance(date_prop, str):
                        date_obj = datetime.fromisoformat(date_prop.replace('Z', '+00:00'))
                        return date_obj.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    pass

    return None


def collect_dates_from_collection(collection) -> Set[str]:
    """
    Get all dates from all items in a collection

    Args:
        collection: STAC collection

    Returns:
        Set[str]: Set of unique dates in YYYY-MM-DD format
    """
    dates = set()

    try:
        # Get items from collection
        items = collection.get_items()

        for item in items:
            date_str = extract_date_from_item(item)
            if date_str:
                dates.add(date_str)

    except Exception as e:
        logging.error(f"Error collecting dates from collection {collection.id}: {str(e)}")

    return dates


def export_dates_to_csv(dates: Set[str], collection_id: str):
    """
    Export a set of dates to a CSV file

    Args:
        dates (Set[str]): Set of dates to export
        collection_id (str): Collection ID for the filename

    Returns:
        str: Path to the created CSV file
    """
    output_filename = os.path.join(STORAGE_PATH,'tools',f"available_{collection_id}.csv")

    # Sort dates for consistent output
    sorted_dates = sorted(list(dates))

    try:
        with open(output_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['date'])  # Header
            for date in sorted_dates:
                writer.writerow([date])

        return output_filename

    except Exception as e:
        logging.error(f"Error exporting dates to CSV: {str(e)}")
        raise


def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Construct base URL from config
    base_url = f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}{config.STAC_FSDI_API}"
    collection_id = COLLECTION_ID

    try:
        # Initialize STAC client
        logger.info(f"Connecting to STAC API at: {base_url}")
        client = setup_stac_client(base_url)
        logger.info("STAC client initialized successfully")

        # Get the collection
        logger.info(f"Retrieving collection: {collection_id}")
        collection = get_collection(client, collection_id)

        if not collection:
            logger.error(f"Collection {collection_id} not found")
            return

        # Get all dates from the collection
        logger.info(f"Extracting dates from collection: {collection_id}")
        dates = collect_dates_from_collection(collection)

        logger.info(f"Found {len(dates)} unique dates")

        # Export dates to CSV
        if dates:
            output_file = export_dates_to_csv(dates, collection_id)
            logger.info(f"Successfully exported {len(dates)} dates to {output_file}")
        else:
            logger.warning(f"No dates found in collection {collection_id}")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise


if __name__ == "__main__":
    print("Starting STAC date extraction...")
    main()
    print("Done!")