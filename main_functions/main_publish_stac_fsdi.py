import os
import hashlib
from base64 import b64encode
#from urllib.parse import urlparse
import requests
import multihash
from hashlib import md5
import platform
import json
import rasterio
from rasterio.transform import from_bounds
from datetime import datetime
import pyproj
import re
import time
import configuration as config
from main_functions import main_multipart_upload_via_api


"""
This script is used to publish geospatial data to the Swiss Federal Spatial Data Infrastructure (FSDI) using the SpatioTemporal Asset Catalog (STAC) API.

The script handles the following tasks:
- Determines the run type (development or production) based on the existence of the SECRET on the local machine file.
- Initializes FSDI authentication.
- Checks if an item exists in the STAC collection.
- Creates a new item in the STAC collection if it does not exist.
- Checks if an asset exists in the STAC item.
- Creates a new asset in the STAC item if it does not exist.
- Uploads the asset data to the STAC item.

The script supports multipart upload for large files and single part upload for smaller files.

References:
- Create a ITEM: https://data.geo.admin.ch/api/stac/static/spec/v0.9/apitransactional.html#tag/Data-Management/operation/putFeature
- Create a ASSET: https://data.geo.admin.ch/api/stac/static/spec/v0.9/apitransactional.html#tag/Data-Management/operation/putAsset
- Example upload: https://data.geo.admin.ch/api/stac/static/spec/v0.9/apitransactional.html#section/Example
- Rayman inspiration: https://github.com/geoadmin/tool-lubis/blob/81bfd2cf346a59e4ffc982f6b8dc8538b181bc9f/maintenance/scheduled/fra_stac/fra_stac.py
"""

# TODO:
# if_exists throws once in a while a 404 even if the path does exist, now solved with if asset_type is TIF then we do create item...not the proper way: Since cloudfront is behine it we should  ask for the file twice so the dataset is in teh cache


# Multipart upload
part_size_mb = 100
attempts = 5

# Define the LV95 and WGS84 coordinate systems
lv95 = pyproj.CRS.from_epsg(2056)  # LV95 EPSG code
wgs84 = pyproj.CRS.from_epsg(4326)  # WGS84 EPSG code

# Create transformer objects
transformer_lv95_to_wgs84 = pyproj.Transformer.from_crs(
    lv95, wgs84, always_xy=True)


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file and the platform.

    This function checks if the file `config.FSDI_SECRETS` exists. If it does, it sets the global variable `run_type` to 2 (indicating a DEV environment) and `os_name` to the current operating system's name. If the file does not exist, it sets `run_type` to 1 (indicating a PROD environment).

    Args:
        None

    Returns:
        None
    """
    global run_type
    global os_name

    # Get the operating system name
    os_name = platform.system()

    # get secrets

    if os.path.exists(config.FSDI_SECRETS):
        run_type = 2
        # print("\nType 2 run PUBLISHER: We are on DEV")

    else:
        run_type = 1
        # print("\nType 1 run PUBLISHER: We are on FSDI INT")


def initialize_fsdi():
    """
    Initialize FSDI authentication.

    This function authenticates FSDI STAC API either using a service account key file or GitHub secrets depending on the run type.

    Args:
        None

    Returns:
        None
    """
    global user
    global password

    # DEV
    if run_type == 2:
        # get u/o using JSON file
        with open(config.FSDI_SECRETS, "r") as json_file:
            config_data = json.load(json_file)

        user = os.environ.get('STAC_USER', config_data["FSDI"]["username"])
        password = os.environ.get(
            'STAC_PASSWORD', config_data["FSDI"]["password"])

    # PROD (github action)
    else:
        user = os.environ['FSDI_STAC_USER']
        password = os.environ['FSDI_STAC_PASSWORD']


def is_existing(stac_item_path):
    """
    Checks if a STAC item exists.

    This function sends a GET request to the provided `stac_item_path` and checks the status code of the response. If the status code is in the 200 range, it returns True, indicating that the STAC item exists. Otherwise, it returns False.

    Args:
        stac_item_path (str): The path of the STAC item to check.

    Returns:
        bool: True if the STAC item exists, False otherwise.
    """
    response = requests.get(
        url=stac_item_path,
        # proxies={"https": proxy.guess_proxy()},
        # verify=False,
        # auth=(user, password),
        # headers=headers,
    )

    if response.status_code // 200 == 1:
        return True
    else:
        return False


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
    # TODO Add stac_path Parse the URL
    # parsed_url = urlparse(stac_path)
    # Extract the domain
    # domain = parsed_url.netloc
    domain = "https://"+config.STAC_FSDI_HOSTNAME+"/"

    # define "current" use case
    if current is not None:
        product = id
    else:
        # Define regex patterns to match the date and 't'
        iso_pattern = r'_\d{4}-\d{2}-\d{2}t\d{6}$'
    
        # Try to remove ISO format first
        product = re.sub(iso_pattern, '', title)
    
    thumbnail_url = (domain+"ch.swisstopo."+product+"/" +
                     id+"/thumbnail.jpg")

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
                "href": "https://map.geo.admin.ch/index.html?layers=WMS||"+title+"||https://wms.geo.admin.ch/?item="+id+"||ch.swisstopo."+product,
                # "href": "https://cms.geo.admin.ch/Topo/umweltbeobachtung/satromocogviewer.html?url="+domain+"ch.swisstopo."+product+"/" +
                # id+"/ch.swisstopo."+product+"_mosaic_"+id+"_bands-10m.tif",
                "rel": "visual"
            },
            {
                "href": thumbnail_url,
                "rel": "preview"
            }

            # {
            #     "href": "https://scihub.copernicus.eu/twiki/pub/SciHubWebPortal/TermsConditions/Sentinel_Data_Terms_and_Conditions.pdf",
            #     "rel": "license",
            #     "title": "Contains modified Copernicus Sentinel data ["+id[:4]+"]"
            # },
            # {
            #     "href": "https://www.geocat.ch/geonetwork/srv/eng/catalog.search#/metadata/"+geocat_id,
            #     "rel": "describedby"
            # }
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
                    data = response.json()
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



def publish_to_stac(raw_asset, raw_item, collection, geocat_id, current=None):
    """
    Publishes a STAC asset.

    This function determines the run type, initializes FSDI authentication, checks if the STAC item exists and creates it if it doesn't, checks if the STAC asset exists and overwrites it if it does, and finally uploads the STAC asset.

    Args:
        raw_asset (str): The filename of the raw asset to publish.
        raw_item (str): The raw item associated with the asset.
        collection (str): The collection to which the asset belongs.
        geocat_id (str): The Geocat ID of the asset.
        current (str): If not None, indicates the 'current' substring should be used to determine the title.

    Returns:
        None
    """
    # Test if we are on Local DEV Run or if we are on PROD
    determine_run_type()

    # Get FSDI credentials
    initialize_fsdi()

    # STAC FSDI only allows lower case item, so we rename it here temporarely
    item = raw_item.lower()
    asset = raw_asset.lower()
    os.rename(raw_asset, asset)

    if not collection.startswith('ch.swisstopo.'):
        collection = 'ch.swisstopo.' + collection
        
    if current is not None:
        item_title = collection.replace('ch.swisstopo.', '')
        item = item_title
    else:
        item_title = collection.replace('ch.swisstopo.', '')+"_" + item

    item_path = f'collections/{collection}/items/{item}'
    # Get path
    asset_path = f'collections/{collection}/items/{item}/assets/{asset}'
    stac_path = f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}{config.STAC_FSDI_API}"

    # Get the file extension
    extension = asset.split('.')[-1]

    # Assign different values based on the extension
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

    # ITEM
    #############

    # Check if ITEM exists, if not create it first

    # if is_existing(stac_path+item_path):
    #     print(f"ITEM object {stac_path+item_path}: exists")
    # else:
        try:
            if asset_type == 'TIF':
                print(f"ITEM object {item}: creating")
                # Create payload
                # Getting the bounds
                # Open the GeoTIFF file
                with rasterio.open(asset) as ds:
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

                # Check if raw_item ends with "240000", since python does not recognize the newest version of ISO8601 of October 2022: "An amendment was published in October 2022 featuring minor technical clarifications and attempts to remove ambiguities in definitions. The most significant change, however, was the reintroduction of the "24:00:00" format to refer to the instant at the end of a calendar day."

                # if raw_item.endswith('240000'):
                #     raw_item_fix = raw_item[:-6] + '235959'
                #     # Date: Convert the string to a datetime object
                #     dt = datetime.strptime(raw_item_fix, '%Y-%m-%dT%H%M%S')

                #     # Adjust the formatting accordingly
                #     dt_iso8601 = dt.strftime('%Y-%m-%dT23:59:59Z')
                # else:
                # Date: Convert the string to a datetime object
                dt = datetime.strptime(raw_item, '%Y-%m-%dT%H%M%S')

                # Convert the datetime object back to a string in the desired format
                dt_iso8601 = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                payload = item_create_json_payload(
                    item, coordinates_wgs84, dt_iso8601, item_title, geocat_id, current)

                upload_item(stac_path+item_path, payload)

        except Exception as e:
            print(f"An error occurred creating object {item}: {e}")

    # ASSET
    #############

    # Check if ASSET exists, if not upload it

    if is_existing(f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}/{collection}/{item}/{asset}"):
        print(f"ASSET object {asset}: exists ... overwriting")
    else:
        print(f"ASSET object {asset}: does not exist preparing...")

    # create asset payload
    payload = asset_create_json_payload(asset, asset_type, current)

    # Create Asset
    if not create_asset(stac_path+asset_path, payload):
        print(f"ASSET object {asset}: creation FAILED")

    # Define environment
    env = "int" if ".int." in config.STAC_FSDI_HOSTNAME else "prod"

    # Upload ASSET
    if not main_multipart_upload_via_api.multipart_upload(env, collection, item, asset, asset, user, password, force=True,verbose=False):
        print(f"ASSET object {asset}: upload FAILED")


    print("FSDI update done: " +
          f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}/{collection}/{item}/{asset}")

    # rename it back to the orginal name for further processing
    os.rename(asset, raw_asset)