import pystac_client
import os
import requests
import json
import re
import time
import rasterio
import pyproj
import sys
from datetime import datetime
from urllib.parse import urlparse
from pystac import Asset
from tqdm import tqdm
sys.path.append(os.path.join(os.path.dirname(__file__), 'main_functions')) # the import was not working without this line, thus directly adding the path
import main_multipart_upload_via_api

"""
STAC Collection Migration Script for Swiss Federal Geoportal

This script migrates STAC (SpatioTemporal Asset Catalog) items and their associated 
assets from an internal BGDI STAC catalog to the production STAC API.

Main Functionality:
- Connects to internal BGDI STAC catalog and production geo.admin.ch STAC API
- Searches for items within a specified date range and geographic bounding box
- Creates STAC items in production if they don't already exist
- Downloads assets from internal catalog and uploads them to production

Usage:
Modify the configuration variables at the top of the script:
- collection_name: STAC collection identifier
- geocat_id: Metadata catalog identifier  
- start_date/end_date: Temporal filter for items
- aoi: Area of interest bounding box [west, south, east, north]

Prerequisites:
- Access to internal BGDI STAC catalog
- Valid credentials for production STAC API

Author: stj, swisstopo
Date: 25.09.2025
"""

# Defining variables that will change according to the product to be moved
collection_name = 'ch.swisstopo.swisseo_ndvi_z_v100'
geocat_id = '07f332fb-f728-4120-b6f1-488631555296'
start_date = '2025-09-01'
end_date = '2025-10-15'
aoi = [5.5, 45.5, 11, 48]  # Bounding box for Switzerland
current = 'current'  # None or 'current'


# Configuration
PROD_SECRETS = os.path.join("secrets", "stac_fsdi-prod.json")
with open(PROD_SECRETS, "r") as json_file:
    config_data = json.load(json_file)
user = os.environ.get('STAC_USER', config_data["FSDI"]["username"])
password = os.environ.get(
    'STAC_PASSWORD', config_data["FSDI"]["password"])

# Creating the STAC client for the internal BGDI STAC
stac_int = pystac_client.Client.open('https://sys-data.int.bgdi.ch/api/stac/v0.9/')
stac_prod = 'https://data.geo.admin.ch/api/stac/v0.9'
stac_prod_main = 'https://data.geo.admin.ch'

# Due to the swisstopo STAC implementation, we need to add the conformance classes
stac_int.add_conforms_to('COLLECTIONS')
stac_int.add_conforms_to('ITEM_SEARCH')

# Defining the collection
collection = stac_int.get_collection(collection_name)

# Getting the items in the collection
items = collection.get_items()
item_count = sum(1 for _ in collection.get_items())

# How many items are in the collection?
print(f"Number of items in the collection {collection_name}: {item_count}")

# Choosing a subset of items
item_search = stac_int.search(
    bbox = aoi,
    datetime = start_date + '/' + end_date,
    collections = [collection_name],
)
items = list(item_search.items())

# Define the LV95 and WGS84 coordinate systems
lv95 = pyproj.CRS.from_epsg(2056)  # LV95 EPSG code
wgs84 = pyproj.CRS.from_epsg(4326)  # WGS84 EPSG code

# Create transformer objects
transformer_lv95_to_wgs84 = pyproj.Transformer.from_crs(
    lv95, wgs84, always_xy=True)

def is_existing(stac_item_path):
    """
    Checks if a STAC item exists.

    This function sends a GET request to the provided `stac_item_path` and checks the status code of the response. If the status code is in the 200 range, it returns True, indicating that the STAC item exists. Otherwise, it returns False.

    Args:
        stac_item_path (str): The path of the STAC item to check.

    Returns:
        bool: True if the STAC item exists, False otherwise.
    """
    response = requests.get(url=stac_item_path)

    if response.status_code // 200 == 1:
        return True
    else:
        return False

def download(asset: Asset, directory: str = None, chunk_size: int = 1024 * 16, **request_options) -> str:
    """
    Smart download STAC Item asset.
    This method uses a checksum validation and a progress bar to monitor download status.
    """
    if directory is None:
        directory = ''

    response = requests.get(asset.href, stream=True, **request_options)
    output_file = os.path.join(directory, urlparse(asset.href)[2].split('/')[-1])
    os.makedirs(directory, exist_ok=True)
    total_bytes = int(response.headers.get('content-length', 0))
    with tqdm.wrapattr(open(output_file, 'wb'), 'write', miniters=1, total=total_bytes, desc=os.path.basename(output_file)) as fout:
        for chunk in response.iter_content(chunk_size=chunk_size):
            fout.write(chunk)


def item_create_json_payload(id, coordinates, dt_iso8601, title, geocat_id, current):
    """
    Creates a JSON payload for a STAC item.

    This function creates a dictionary with the provided arguments and additional static data. The dictionary can be used as a JSON payload in a request to create a STAC item.

    Args:
        id (str): The ID of the STAC item.
        coordinates (list): The coordinates of the STAC item.
        dt_iso8601 (str): The datetime of the STAC item in ISO 8601 format.
        title (str): The title of the STAC item.
        geocat_id (str): The Geocat ID of the STAC item.
        current (str): If not None, indicates the 'current' substring should be used to determine the title.

    Returns:
        dict: A dictionary representing the JSON payload for the STAC item.
    """
    domain = stac_prod_main + "/"

    # define "current" use case
    if current is not None:
        product = id
    else:
        # Define regex patterns to match the date and 't'
        iso_pattern = r'_\d{4}-\d{2}-\d{2}t\d{6}$'
    
        # Try to remove ISO format first
        product = re.sub(iso_pattern, '', title)
    
    thumbnail_url = (domain + "ch.swisstopo." + product + "/" +
                     id + "/thumbnail.jpg")

    payload = {
        "id": id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [coordinates],
        },
        "properties": {
            "datetime": dt_iso8601,  # "%Y-%m-%dT%H:%M:%SZ"
            "title": title
        },
        "links": [
            {
                "href": "https://map.geo.admin.ch/index.html?layers=WMS||" + title + "||https://wms.geo.admin.ch/?item=" + id + "||ch.swisstopo."+ product,
                "rel": "visual"
            },
            {
                "href": thumbnail_url,
                "rel": "preview",
                "type": "image/jpeg",
                "title": "Thumbnail"
            }
        ]
    }

    return payload

def upload_item(item_path, item_payload):
    """
    Uploads a STAC item.

    This function sends a PUT request to the provided `item_path` with the provided `item_payload` as JSON data. If the status code of the response is in the 200 range, it returns True, indicating that the upload was successful. Otherwise, it returns False.

    Args:
        item_path (str): The path where the STAC item should be uploaded.
        item_payload (dict): The JSON payload of the STAC item.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        response = requests.put(
            url=item_path,
            json=item_payload,
            # proxies={"https": proxy.guess_proxy()},
            # verify=False,
            # auth=HTTPBasicAuth(user, password)
            auth=(user, password)
        )

        if response.status_code // 200 == 1:
            return True
        else:
            print(response.json())
            return False
    except Exception as e:
        print(f"An error occurred in upload_item: {e}")


def asset_create_title(asset, current):
    """
    Creates a title for a STAC asset.

    This function extracts the date from the provided `asset` string using a regular expression, finds the text after the date, removes the file extension, and converts the result to uppercase to create a title for the STAC asset.

    Args:
        asset (str): The string from which to create the title.
        current (str): If not None, indicates the 'current' substring should be used to determine the title.

    Returns:
        str: The created title.
    """
    if asset == "thumbnail.jpg":
        return "THUMBNAIL"
    else:
        # use case "current"
        if current is not None:
            # Regular expression to match the  current pos
            match = re.search(r'current', asset)
        else:
            # Regular expression to match the ISO 8601 date format
            match = re.search(r'\d{4}-\d{2}-\d{2}t\d{6}', asset)

        if match is None:
            # No date pattern found - this shouldn't happen with your expected formats
            raise ValueError(f"No recognized date pattern found in asset name: {asset}")
        
        # Find the position of the first underscore after the date
        underscore_pos = asset.find('_', match.end())

        # Extract the text after the date
        text_after_date = asset[underscore_pos + 1:]

        # Remove the file extension
        filename_without_extension = text_after_date.rsplit('.', 1)[0]

        # Deal with warnregions: asset name needs to contain the file extension
        # Extract the file extension
        file_extension = os.path.splitext(asset)[1]

        # Check if "warnregions" is present in filename_without_extension and if the file extension is ".csv", ".geojson", or ".parquet"
        if "warnregions" in filename_without_extension and file_extension.lower() in [".csv", ".geojson", ".parquet"]:
            filename_without_extension = filename_without_extension + \
                "-"+file_extension.lstrip('.')

        # Convert to uppercase
        filename_uppercase = filename_without_extension.upper()

        return filename_uppercase

def rename_asset_for_current(asset_name, current):
    """
    Renames an asset file to replace the date string with 'current'.
    
    Args:
        asset_name (str): The original asset filename
        current (str): If not None, indicates this is a 'current' asset
    
    Returns:
        str: The renamed asset filename with 'current' replacing the date
    """
    if current is None:
        return asset_name
    
    # Replace the ISO date pattern (YYYY-MM-DDtHHMMSS) with 'current'
    iso_pattern = r'\d{4}-\d{2}-\d{2}t\d{6}'
    renamed_asset = re.sub(iso_pattern, 'current', asset_name)
    
    return renamed_asset

def asset_create_json_payload(id, asset_type, current):
    """
    Creates a JSON payload for a STAC asset.

    This function creates a dictionary with the provided arguments and additional static data.
    The dictionary can be used as a JSON payload in a request to create a STAC asset.
    JSON TIF and CSV type supported

    Args:
        id (str): The ID of the STAC asset.
        asset_type (str): The type of the STAC asset.
        current (str): If not None, indicates the 'current' substring should be used to determine the title.

    Returns:
        dict: A dictionary representing the JSON payload for the STAC asset.
    """
    title = asset_create_title(id, current)
    if asset_type == "TIF":
        gsd = re.findall(r'\d+', title)
        payload = {
            "id": id,
            "title": title,
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "proj:epsg": 2056,
            "eo:gsd": int(gsd[0])
        }
    elif asset_type == "JSON":
        payload = {
            "id": id,
            "title": title,
            "type": "application/json"
        }
    elif asset_type == "GEOJSON":
        payload = {
            "id": id,
            "title": title,
            "type": "application/geo+json"
        }
    elif asset_type == "CSV":
        payload = {
            "id": id,
            "title": title,
            "type": "text/csv"
        }
    elif asset_type == "PARQUET":
        payload = {
            "id": id,
            "title": title,
            "type": "application/vnd.apache.parquet"
        }
    else:
        asset_type == "JPEG"
        payload = {
            "id": id,
            "title": title,
            "type": "image/jpeg"
        }
    return payload

def create_asset(stac_asset_url, payload):
    """
    Creates a STAC asset.

    This function sends a PUT request to the provided `stac_asset_url` with the provided `payload` as JSON data. If the status code of the response is in the 200 range, it returns True, indicating that the creation was successful. Otherwise, it returns False.

    Args:
        stac_asset_url (str): The URL where the STAC asset should be created.
        payload (dict): The JSON payload of the STAC asset.

    Returns:
        bool: True if the creation was successful, False otherwise.
    """
    # Maximum number of retries
    max_retries = 3
    # Delay between retries in seconds
    delay = 20
    # Flag to indicate success or failure
    success = False

    for attempt in range(max_retries):
        try:
            # Send PUT request
            response = requests.put(
                url=stac_asset_url,
                auth=(user, password),
                json=payload
            )

            # Check the status code
            if response.status_code == 200 or response.status_code == 201:
                try:
                    # Try to decode the JSON response
                    # data = response.json()
                    #print(data)
                    success = True
                    break
                except requests.exceptions.JSONDecodeError as e:
                    print("Error decoding JSON:", e)
                    print("Response content:", response.text)
            else:
                print(
                    f"Attempt {attempt + 1}: Received status code {response.status_code}")
                print("Response content:", response.text)
                if attempt < max_retries - 1:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)

        except requests.exceptions.RequestException as e:
            # Handle other request-related exceptions
            print(f"An error occurred: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)

    if not success:
        print("Failed to receive a successful response after multiple attempts.")
        return False

    return True

# -----------------------------------------------------

# Iterating through the items
for item in items:
    print(f"Current item: '{item.properties.get('title')}")

    assets = item.assets
    print(f"Assets in the item: {list(assets.keys())}")

    # Defining item name
    if current is not None:
        item_name = collection_name.replace('ch.swisstopo.', '')
        prod_item_id = item_name  # Use the same name for the item ID in production
    else:
        item_name = collection_name.replace('ch.swisstopo.', '') + "_" + item.id
        prod_item_id = item.id  # Use the original item.id

    # Get item path
    item_path = f'collections/{collection_name}/items/{prod_item_id}'

    # Check if item exists in the production STAC, if not, create it
    if not is_existing(f"{stac_prod}/{item_path}"):
        print(f"ITEM object {prod_item_id}: does not yet exist ... creating")

        # Get a TIF asse to get bounds from
        tif_asset = None
        for asset in assets.values():
            asset_name = asset.href.split('/')[-1]
            extension = asset_name.split('.')[-1]
            if extension.lower() in ['tif', 'tiff']:
                tif_asset = asset
                break
        
        if tif_asset is None:
            print(f"No TIF asset found for item {prod_item_id}. Skipping item creation.")
            continue

        try:
            # Creating the payload, getting the bounds and opening the GeoTIFF file
            with rasterio.open(tif_asset.href) as ds:
                # Get the bounds of the raster
                left, bottom, right, top = ds.bounds

            # Create a list of coordinates (in this case, a rectangle)
            coordinates_lv95 = [
                [left, bottom],
                [right, bottom],
                [right, top],
                [left, top],
                [left, bottom]
            ]

            # Convert your coordinates
            coordinates_wgs84 = [transformer_lv95_to_wgs84.transform(
                *coord) for coord in coordinates_lv95]

            # Parse datetime
            dt = datetime.strptime(item.id, '%Y-%m-%dt%H%M%S')
            dt_iso8601 = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            payload = item_create_json_payload(
                prod_item_id, coordinates_wgs84, dt_iso8601, item_name, geocat_id, current)

            if upload_item((stac_prod + '/' + item_path), payload):
                print(f"ITEM object {prod_item_id}: created successfully")
            else:
                print(f"ITEM object {prod_item_id}: creation FAILED")
                continue  # Skip processing assets if item creation failed
        
        except Exception as e:
            print(f"An error occurred creating object {item}: {e}")
            continue  # Skip to next item
    else:
        print(f"ITEM object {prod_item_id}: exists ... skipping creation")

    # Processing assets for the item
    for asset in assets.values():
        download(asset, 'temp')
        asset_name = asset.href.split('/')[-1] 

        # Rename asset for 'current' use case
        prod_asset_name = rename_asset_for_current(asset_name, current)        
        
        # Get asset paths
        asset_path = f'collections/{collection_name}/items/{prod_item_id}/assets/{prod_asset_name}'
        local_asset_path = os.path.join('temp', asset_name)

        # Get the file extension and determine asset type
        extension = asset_name.split('.')[-1]
        if extension.lower() == 'csv':
            asset_type = 'CSV'
        elif extension.lower() == 'json':
            asset_type = 'JSON'
        elif extension.lower() == 'jpg':
            asset_type = 'JPEG'
        elif extension.lower() == 'geojson':
            asset_type = 'GEOJSON'
        elif extension.lower() == 'parquet':
            asset_type = 'PARQUET'
        else:
            asset_type = 'TIF'
    
        # Uploading the asset to the production STAC
        # Check if the asset already exists in the production STAC
        if is_existing(f"{stac_prod}/{collection_name}/{item_name}/{prod_asset_name}"):
            print(f"ASSET object {prod_asset_name}: exists ... overwriting")
        else:
            print(f"ASSET object {prod_asset_name}: does not yet exist ... preparing")
        
        # create asset payload
        payload = asset_create_json_payload(prod_asset_name, asset_type, current)

        # Create Asset
        if not create_asset((stac_prod + '/' + asset_path), payload):
             print(f"ASSET object {prod_asset_name}: creation FAILED")
        
        # Define environment
        env = 'prod'

        # Upload Asset
        if not main_multipart_upload_via_api.multipart_upload(env, collection_name, prod_item_id, prod_asset_name, local_asset_path, user, password, force=True, verbose=False):
            print(f"ASSET object {prod_asset_name}: upload FAILED")

        print("FSDI update done: " +
            f"{stac_prod}/{collection_name}/{prod_item_id}/{prod_asset_name}")
        
