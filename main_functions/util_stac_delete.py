"""
STAC Asset and Item Deletion Script

This script deletes assets and items from a STAC (SpatioTemporal Asset Catalog) API. It first deletes all assets
associated with an item and then deletes the item itself if all its assets were successfully deleted.

Author: David Oesch
Date: 2025-02-18
License: MIT License

Usage:
    python util_stac_delete.py

Dependencies:
    - pystac_client
    - requests
    - logging
    - json
    - urllib.parse

Functions:
    - load_credentials(config_path: str) -> tuple
    - setup_stac_client(url: str) -> pystac_client.Client
    - get_swisseo_collections(client: pystac_client.Client, collection_del: str) -> Generator
    - get_collection_items_assets(collection) -> List[Dict]
    - delete_asset(base_url: str, collection_id: str, item_id: str, asset_key: str, auth: tuple) -> bool
    - delete_item(base_url: str, collection_id: str, item_id: str, auth: tuple) -> bool
    - delete_items_and_assets(base_url: str, items_assets: List[Dict], auth: tuple) -> Dict[str, List[str]]
    - main() -> Dict[str, List[str]]

Example:
    To run the script, simply execute:
    python util_stac_delete.py
"""

import pystac_client
from typing import Dict, List, Generator
import logging
import requests
from urllib.parse import urljoin
import json


def load_credentials(config_path: str) -> tuple:
    """
    Load FSDI credentials from config file

    Args:
        config_path (str): Path to the config file

    Returns:
        tuple: (username, password)
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return (config['FSDI']['username'], config['FSDI']['password'])
    except Exception as e:
        logging.error(f"Error loading credentials: {str(e)}")
        raise

def setup_stac_client(url) -> pystac_client.Client:
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

def get_swisseo_collections(client: pystac_client.Client,collection_del) -> Generator:
    """
    Retrieve all SwissEO collections

    Args:
        client (pystac_client.Client): STAC client

    Returns:
        Generator: Generator of SwissEO collections
    """
    return (
        collection for collection in client.get_collections()
        if collection_del in collection.id.lower()
    )

def get_collection_items_assets(collection) -> List[Dict]:
    """
    Get all assets from all items in a collection

    Args:
        collection: STAC collection

    Returns:
        List[Dict]: List of dictionaries containing item ID and its assets
    """
    items_assets = []

    for item in collection.get_items():
        item_assets = {
            'item_id': item.id,
            'collection_id': collection.id,
            'assets': {}
        }

        for asset_key, asset in item.get_assets().items():
            item_assets['assets'][asset_key] = {
                'href': asset.href,
                'type': asset.media_type,
                'roles': asset.roles if hasattr(asset, 'roles') else []
            }

        items_assets.append(item_assets)

    return items_assets

def delete_asset(base_url: str, collection_id: str, item_id: str, asset_key: str, auth: tuple) -> bool:
    """
    Delete a specific asset from an item

    Args:
        base_url (str): Base URL of the STAC API
        collection_id (str): Collection ID
        item_id (str): Item ID
        asset_key (str): Asset key to delete
        auth (tuple): Authentication credentials (username, password)

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    delete_url = urljoin(base_url, f"collections/{collection_id}/items/{item_id}/assets/{asset_key}")
    print("deleting asset: ", asset_key)
    try:
        response = requests.delete(delete_url, auth=auth)
        return response.status_code in [200, 204]
    except Exception as e:
        logging.error(f"Error deleting asset {asset_key} from item {item_id}: {str(e)}")
        return False

def delete_item(base_url: str, collection_id: str, item_id: str, auth: tuple) -> bool:
    """
    Delete a specific item from a collection

    Args:
        base_url (str): Base URL of the STAC API
        collection_id (str): Collection ID
        item_id (str): Item ID
        auth (tuple): Authentication credentials (username, password)

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    delete_url = urljoin(base_url, f"collections/{collection_id}/items/{item_id}")
    print("deleting item: ", item_id)
    try:
        response = requests.delete(delete_url, auth=auth)
        return response.status_code in [200, 204]
    except Exception as e:
        logging.error(f"Error deleting item {item_id}: {str(e)}")
        return False

def delete_items_and_assets(base_url: str, items_assets: List[Dict], auth: tuple) -> Dict[str, List[str]]:
    """
    Delete all assets and items in the correct order

    Args:
        base_url (str): Base URL of the STAC API
        items_assets (List[Dict]): List of items and their assets to delete
        auth (tuple): Authentication credentials (username, password)

    Returns:
        Dict[str, List[str]]: Summary of successful and failed deletions
    """
    results = {
        'successful_asset_deletions': [],
        'failed_asset_deletions': [],
        'successful_item_deletions': [],
        'failed_item_deletions': []
    }

    for item in items_assets:
        item_id = item['item_id']
        collection_id = item['collection_id']

        # First delete all assets for this item
        all_assets_deleted = True
        for asset_key in item['assets'].keys():
            success = delete_asset(base_url, collection_id, item_id, asset_key, auth)
            if success:
                results['successful_asset_deletions'].append(f"{item_id}/{asset_key}")
            else:
                results['failed_asset_deletions'].append(f"{item_id}/{asset_key}")
                all_assets_deleted = False

        # Only delete the item if all its assets were successfully deleted
        if all_assets_deleted:
            success = delete_item(base_url, collection_id, item_id, auth)
            if success:
                results['successful_item_deletions'].append(item_id)
            else:
                results['failed_item_deletions'].append(item_id)
        else:
            results['failed_item_deletions'].append(item_id)
            logging.warning(f"Skipping item deletion for {item_id} due to failed asset deletions")

    return results

def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    base_url="https://sys-data.int.bgdi.ch/api/stac/v0.9/"
    config_path = r"C:\temp\satromo-dev\secrets\stac_fsdi-int.json"
    COLLECTION_DEL="ch.swisstopo.swisseo_vhi_v100"




    try:
        # Load credentials
        auth = load_credentials(config_path)
        logger.info("Credentials loaded successfully")

        # Initialize STAC client
        client = setup_stac_client(base_url)
        logger.info("STAC client initialized successfully")

        # Get all assets from all collections
        all_assets = []

        for collection in get_swisseo_collections(client,COLLECTION_DEL):
            logger.info(f"Processing collection: {collection.id}")

            try:
                collection_assets = get_collection_items_assets(collection)
                all_assets.extend(collection_assets)
                logger.info(f"Found {len(collection_assets)} items in collection {collection.id}")

            except Exception as e:
                logger.error(f"Error processing collection {collection.id}: {str(e)}")
                continue

        logger.info(f"Total number of items to process: {len(all_assets)}")

        # Delete assets and items
        results = delete_items_and_assets(base_url, all_assets, auth)

        # Log summary
        logger.info("Deletion Summary:")
        logger.info(f"Successfully deleted assets: {len(results['successful_asset_deletions'])}")
        logger.info(f"Failed asset deletions: {len(results['failed_asset_deletions'])}")
        logger.info(f"Successfully deleted items: {len(results['successful_item_deletions'])}")
        logger.info(f"Failed item deletions: {len(results['failed_item_deletions'])}")

        return results

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    results = main()

    print("done")