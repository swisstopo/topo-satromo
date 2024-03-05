import os
import hashlib
from base64 import b64encode
from urllib.parse import urlparse
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

import configuration as config

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

    # INT (github action)
    else:
        # TODO add PROD PW in GA and in config when going live
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
        # Define a regex pattern to match the date and 't'
        pattern = r'_\d{4}-\d{2}-\d{2}t\d{6}$'
        product = re.sub(pattern, '', title)
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
                "href": "https://tinyurl.com/sat54?url="+domain+"ch.swisstopo."+product+"/" +
                id+"/ch.swisstopo."+product+"_mosaic_"+id+"_bands-10m.tif",
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


def asset_create_title(asset,current):
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
        #use case "current"
        if current is not None:
            # Regular expression to match the  current pos
            match = re.search(r'current', asset)
        else:
            # Regular expression to match the ISO 8601 date format
            match = re.search(r'\d{4}-\d{2}-\d{2}t\d{6}', asset)

        # Find the position of the first underscore after the date
        underscore_pos = asset.find('_', match.end())

        # Extract the text after the date
        text_after_date = asset[underscore_pos + 1:]

        # Remove the file extension
        filename_without_extension = text_after_date.rsplit('.', 1)[0]

        # Convert to uppercase
        filename_uppercase = filename_without_extension.upper()

        return filename_uppercase


def asset_create_json_payload(id, asset_type,current):
    """
    Creates a JSON payload for a STAC asset.

    This function creates a dictionary with the provided arguments and additional static data. The dictionary can be used as a JSON payload in a request to create a STAC asset.
    JSON TIF and CSV type supported

    Args:
        id (str): The ID of the STAC asset.
        asset_type (str): The type of the STAC asset.
        current (str): If not None, indicates the 'current' substring should be used to determine the title.

    Returns:
        dict: A dictionary representing the JSON payload for the STAC asset.
    """
    title = asset_create_title(id,current)
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
    elif asset_type == "CSV":
        payload = {
            "id": id,
            "title": title,
            "type": "text/csv"
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
    response = requests.put(
        url=stac_asset_url,
        auth=(user, password),
        # auth=HTTPBasicAuth(user, password),
        # proxies={"https": proxy.guess_proxy()},
        # verify=False,
        json=payload
    )
    if response.status_code // 200 == 1:
        return True
    else:
        print(response.json())
        return False


def upload_asset_multipart(stac_asset_filename, stac_asset_url, part_size=part_size_mb * 1024 ** 2):
    """
    Uploads a STAC asset in multiple parts.

    This function prepares a multipart upload by calculating the SHA256 and MD5 hashes of the parts of the file at `stac_asset_filename`. It then creates a multipart upload, uploads the parts using the presigned URLs, and completes the upload. If any step fails, it returns False. Otherwise, it returns True.

    Args:
        stac_asset_filename (str): The filename of the STAC asset to upload.
        stac_asset_url (str): The URL where the STAC asset should be uploaded.
        part_size (int, optional): The size of each part in bytes. Defaults to `part_size_mb * 1024 ** 2`.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    # 1. Prepare multipart upload
    sha256 = hashlib.sha256()
    md5_parts = []
    with open(stac_asset_filename, "rb") as fd:
        while True:
            data = fd.read(part_size)
            if data in (b"", ""):
                break
            sha256.update(data)
            md5_parts.append({"part_number": len(
                md5_parts) + 1, "md5": b64encode(md5(data).digest()).decode("utf-8")})
    checksum_multihash = multihash.to_hex_string(
        multihash.encode(sha256.digest(), "sha2-256"))

    # 2. Create a multipart upload
    response = requests.post(
        url=stac_asset_url + "/uploads",
        auth=(user, password),
        # auth=HTTPBasicAuth(user, password),
        # proxies={"https": proxy.guess_proxy()},
        # verify=False,
        json={"number_parts": len(
            md5_parts), "md5_parts": md5_parts, "checksum:multihash": checksum_multihash}
    )
    if response.status_code // 200 == 1:
        upload_id = response.json()["upload_id"]
        urls = response.json()["urls"]
    else:
        return False

    # 3. Upload the part using the presigned url
    parts = []
    with open(stac_asset_filename, "rb") as fd:
        for url in urls:
            data = fd.read(part_size)
            for attempt in range(attempts):
                try:
                    response = requests.put(
                        url=url["url"],
                        # proxies={"https": proxy.guess_proxy()},
                        # verify=False,
                        data=data,
                        headers={
                            "Content-MD5": md5_parts[url["part"] - 1]["md5"]},
                        timeout=part_size_mb * 2
                    )
                    if response.status_code // 200 == 1:
                        parts.append(
                            {"etag": response.headers["ETag"], "part_number": url["part"]})
                        print(
                            f'Part {url["part"]}/{len(urls)} of File {os.path.basename(stac_asset_filename)} uploaded after attempt {attempt + 1}')
                        break
                    elif attempt == attempts - 1:
                        return False
                except Exception as e:
                    print(e)

    # 4. Complete the upload
    response = requests.post(
        url=stac_asset_url + f"/uploads/{upload_id}/complete",
        # proxies={"https": proxy.guess_proxy()},
        # verify=False,
        auth=(user, password),
        json={"parts": parts}
    )
    if response.status_code // 200 == 1:
        return True
    else:
        return False


def upload_asset(stac_asset_filename, stac_asset_url):
    """
    Uploads a STAC asset.

    This function prepares a singlepart upload by calculating the SHA256 and MD5 hashes of the file at `stac_asset_filename`. It then creates a multipart upload, uploads the part using the presigned URL, and completes the upload. If any step fails, it returns False. Otherwise, it returns True.

    Args:
        stac_asset_filename (str): The filename of the STAC asset to upload.
        stac_asset_url (str): The URL where the STAC asset should be uploaded.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    # 1. Prepare singlepart upload
    with open(stac_asset_filename, 'rb') as fd:
        data = fd.read()

    checksum_multihash = multihash.to_hex_string(
        multihash.encode(hashlib.sha256(data).digest(), 'sha2-256'))
    md5 = b64encode(hashlib.md5(data).digest()).decode('utf-8')

    # 2. Create a multipart upload
    response = requests.post(
        stac_asset_url + "/uploads",
        auth=(user, password),
        json={
            "number_parts": 1,
            "md5_parts": [{
                "part_number": 1,
                "md5": md5
            }],
            "checksum:multihash": checksum_multihash
        }
    )
    upload_id = response.json()['upload_id']

    # 2. Upload the part using the presigned url
    response = requests.put(
        response.json()['urls'][0]['url'], data=data, headers={'Content-MD5': md5})
    etag = response.headers['ETag']

    # 3. Complete the upload
    response = requests.post(
        f"{stac_asset_url}/uploads/{upload_id}/complete",
        auth=(user, password),
        json={'parts': [{'etag': etag, 'part_number': 1}]}
    )

    if response.status_code // 200 == 1:
        return True
    else:
        return False


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
    else:
        asset_type = 'TIF'

    # ITEM
    #############

    # Check if ITEM exists, if not create it first

    if is_existing(stac_path+item_path):
        print(f"ITEM object {stac_path+item_path}: exists")
    else:
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

                # Date: Convert the string to a datetime object
                dt = datetime.strptime(raw_item, '%Y-%m-%dT%H%M%S')

                # Convert the datetime object back to a string in the desired format
                dt_iso8601 = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                payload = item_create_json_payload(
                    item, coordinates_wgs84, dt_iso8601, item_title, geocat_id,current)

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
    payload = asset_create_json_payload(asset, asset_type,current)

    # Create Asset
    if not create_asset(stac_path+asset_path, payload):
        print(f"ASSET object {asset}: creation FAILED")

    # Upload ASSET
    if asset_type == 'TIF':
        print("TIF asset - Multipart upload")
        if not upload_asset_multipart(asset, stac_path+asset_path):
            print(f"ASSET object {asset}: upload FAILED")
    else:
        print(asset_type+" single part upload")
        if not upload_asset(asset, stac_path+asset_path):
            print(f"ASSET object {asset}: upload FAILED")
    print("FSDI update done: " +
          f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}/{collection}/{item}/{asset}")

    # rename it back to the orginal name for further processing
    os.rename(asset, raw_asset)
