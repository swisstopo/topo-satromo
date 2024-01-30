import os
import hashlib
from base64 import b64encode

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

# Refernces
# create a ITEM https://data.geo.admin.ch/api/stac/static/spec/v0.9/apitransactional.html#tag/Data-Management/operation/putFeature
# create a ASSSET https://data.geo.admin.ch/api/stac/static/spec/v0.9/apitransactional.html#tag/Data-Management/operation/putAsset
# example upload https://data.geo.admin.ch/api/stac/static/spec/v0.9/apitransactional.html#section/Example

#  Elias magic Script: https://github.com/geoadmin/tool-lubis/blob/81bfd2cf346a59e4ffc982f6b8dc8538b181bc9f/maintenance/scheduled/fra_stac/fra_stac.py
# ITEM json  https://data.geo.admin.ch/api/stac/v0.9/collections/ch.swisstopo.lubis-luftbilder_schwarzweiss/items/lubis-luftbilder_schwarzweiss_000-000-003 based on https://data.geo.admin.ch/browser/index.html#/collections/ch.swisstopo.lubis-luftbilder_schwarzweiss/items/lubis-luftbilder_schwarzweiss_000-000-003?.language=en
# ASSET JSON see as well lubis

# TODO:
# if_exists throws once in a while a 404 even if the path does exist, now solved with if asset_type is TIF then we do create item...not the proper way

# Multipart upload
part_size_mb = 5
attempts = 5

# Define the LV95 and WGS84 coordinate systems
lv95 = pyproj.CRS.from_epsg(2056)  # LV95 EPSG code
wgs84 = pyproj.CRS.from_epsg(4326)  # WGS84 EPSG code

# Create transformer objects
transformer_lv95_to_wgs84 = pyproj.Transformer.from_crs(
    lv95, wgs84, always_xy=True)


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file. And determine platform

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    global os_name

    # Get the operating system name
    os_name = platform.system()

    # Set SOURCE , DESTINATION and MOUNTPOINTS

    if os.path.exists(config.FSDI_SECRETS):
        run_type = 2
        # print("\nType 2 run PUBLISHER: We are on DEV")

    else:
        run_type = 1
        print("\nType 1 run PUBLISHER: We are on INT")


def initialize_fsdi():
    """
    Initialize FSDI authentication.

    This function authenticates FSDI STAC API either using a service account key file
    or GitHub secrets depending on the run type.

    Returns:
    None
    """
    global user
    global password

    if run_type == 2:
        # Initialize FSDI using service account key file

        # Authenticate using the service account key file
        with open(config.FSDI_SECRETS, "r") as json_file:
            config_data = json.load(json_file)

        user = os.environ.get('STAC_USER', config_data["FSDI"]["username"])
        password = os.environ.get(
            'STAC_PASSWORD', config_data["FSDI"]["password"])

    else:
        # TODO Initialize FSDI using GitHub secrets; add PROD PW in GA and in config when going live
        user = os.environ['FSDI_STAC_USER']
        password = os.environ['FSDI_STAC_PASSWORD']


def is_existing(stac_item_path):

    response = requests.get(
        url=stac_item_path,
        # proxies={"https": proxy.guess_proxy()},
        # verify=False,
        auth=(user, password),
        # headers=headers,
    )

    if response.status_code // 200 == 1:
        # if 200 <= response.status_code < 300 or response.status_code == 403:  # since it might exist but no acces
        return True
    else:
        return False


def item_create_json_payload(id, coordinates, dt_iso8601, title, geocat_id):
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
                "href": "https://www.swisstopo.admin.ch/en/home/meta/conditions/geodata.html",
                "rel": "license",
                "title": "Opendata Federal Office of Topography swisstopo"
            },
            {
                "href": "https://scihub.copernicus.eu/twiki/pub/SciHubWebPortal/TermsConditions/Sentinel_Data_Terms_and_Conditions.pdf",
                "rel": "license",
                "title": "Legal notice on the use of Copernicus Sentinel Data and Service Information"
            },
            {
                "href": "https://www.geocat.ch/geonetwork/srv/eng/catalog.search#/metadata/"+geocat_id,
                "rel": "describedby"
            }
        ]
    }

    return payload


def upload_item(item_path, item_payload):
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


def asset_create_title(asset):
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


def asset_create_json_payload(id, asset_type):
    title = asset_create_title(id)
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
    else:
        asset_type == "CSV"
        payload = {
            "id": id,
            "title": title,
            "type": "text/csv"
        }
    return payload


def create_asset(stac_asset_url, payload):
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


def publish_to_stac(raw_asset, raw_item, collection, geocat_id):

    # Test if we are on Local DEV Run or if we are on PROD
    determine_run_type()

    # Get FSDI credentials
    initialize_fsdi()

    # STAC FSDI only allows lower case item, so we rename it here temporarely
    item = raw_item.lower()
    asset = raw_asset.lower()
    os.rename(raw_asset, asset)

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
                with rasterio.open(raw_asset) as ds:
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
                    item, coordinates_wgs84, dt_iso8601, item_title, geocat_id)

                if upload_item(stac_path+item_path, payload):
                    print(f"ITEM object {item}: succesfully created")
                else:
                    print(f"ITEM object {item}: creation FAILED")
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
    payload = asset_create_json_payload(asset, asset_type)

    # Create Asset
    if create_asset(stac_path+asset_path, payload):
        print(f"ASSET object {asset}: successfully created")
    else:
        print(f"ASSET object {asset}: creation FAILED")

    # Upload ASSET
    if asset_type == 'TIF':
        print("TIF asset - Multipart upload")
        if upload_asset_multipart(asset, stac_path+asset_path):
            print(f"ASSET object {asset}: succesfully uploaded")
        else:
            print(f"ASSET object {asset}: upload FAILED")
    else:
        print(asset_type+" single part upload")
        if upload_asset(asset, stac_path+asset_path):
            print(f"ASSET object {asset}: succesfully uploaded")
        else:
            print(f"ASSET object {asset}: upload FAILED")
    print("FSDI update completed: " +
          f"{config.STAC_FSDI_SCHEME}://{config.STAC_FSDI_HOSTNAME}/{collection}/{item}/{asset}")

    # rename it back to upper case for further processing
    os.rename(asset, raw_asset)
