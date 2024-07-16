from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
import ee
import json
from datetime import datetime, timedelta, timezone

"""
Overview
This script initializes Google Earth Engine (GEE) and Google Drive authentication, converts Day-Of-Year (DOY) to datetime strings,
and manages the copying and updating of assets within Google Earth Engine. 

Dependencies:
- oauth2client.service_account
- pydrive.auth
- earthengine-api (ee)
- json
- datetime
- time

Configuration Files:
- config: Path to the service account key file.
- asset_list: Path to the text file containing asset names.

Functions:
- doy_to_datetime_string(doy, year=2023, hour=10, minute=0):
    Converts a day-of-year (DOY) to a datetime string in the format 'YYYY-MM-DDTHH:MM:SS.SSSZ' for the specified year.

- initialize_gee_and_drive():
    Initializes Google Earth Engine (GEE) and Google Drive authentication using the service account key file.
    Authenticates and initializes GEE, verifying the setup with a test image.

Usage:
- The script reads asset names from a text file.
- Copies each asset from a source account to a destination account in Google Earth Engine.
- Updates asset properties with the corresponding start and end times.

      
"""


config = r'C:\temp\topo-satromo\secrets\xxx.secret'
asset_list = r'C:\temp\topo-satromo\main_functions\asset_list.txt'


def doy_to_datetime_string(doy, year=2023, hour=10, minute=0):
    """
    Converts a day-of-year (DOY) to a datetime string in the format 'YYYY-MM-DDTHH:MM:SS.SSSZ' for the specified year.

    Args:
        doy (int): Day of year.
        year (int): Year to convert DOY into.
        hour (int): Hour of the day.
        minute (int): Minute of the hour.

    Returns:
        str: Corresponding datetime string in UTC.
    """
    # Convert DOY to a datetime object
    date_time = datetime(year, 1, 1, hour, minute) + timedelta(days=doy - 1)

    # Set timezone to UTC
    date_time = date_time.replace(tzinfo=timezone.utc)

    # Format the datetime object to a string in the desired format
    date_time_str = date_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    return date_time_str


def initialize_gee_and_drive():
    """
    Initializes Google Earth Engine (GEE) and Google Drive based on the run type.

    If the run type is 2, initializes GEE and authenticates using the service account key file.
    If the run type is 1, initializes GEE and authenticates using secrets from GitHub Action.

    Prints a success or failure message after initializing GEE.

    Note: This function assumes the required credentials and scopes are properly set.

    Returns:
        None
    """
    # Set scopes for Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]

    # Initialize GEE and authenticate using the service account key file

    # Read the service account key file
    with open(config, "r") as f:
        data = json.load(f)

    # Authenticate with Google using the service account key file
    gauth = GoogleAuth()
    gauth.service_account_file = config
    gauth.service_account_email = data["client_email"]
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
        gauth.service_account_file, scopes=scopes
    )

    # Initialize Google Earth Engine
    credentials = ee.ServiceAccountCredentials(
        gauth.service_account_email, gauth.service_account_file
    )
    ee.Initialize(credentials)

    # Test if GEE initialization is successful
    image = ee.Image("NASA/NASADEM_HGT/001")
    title = image.get("title").getInfo()

    if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
        print("GEE initialization successful")
    else:
        print("GEE initialization FAILED")


# Authenticate with GEE and GDRIVE
initialize_gee_and_drive()

# Read asset names from the text file into a list
with open(asset_list, 'r') as file:
    asset_names = file.readlines()

# Remove whitespace and newline characters from the asset names
asset_names = [name.strip() for name in asset_names]

# Define the Earth Engine username for the source account


# Define the Earth Engine username for the destination account
destination_username = "satromo-prod"

# Copy each asset from the source to the destination and set properties
for asset_name in asset_names:
    # Construct the source and destination asset paths
    source_asset = "projects/geetest-386915/assets/terrain_shadow/{}".format(
        asset_name)
    destination_asset = 'projects/{}/assets/col/TERRAINSHADOW_SWISS/{}'.format(
        destination_username, asset_name)

    # Check if the destination asset already exists
    try:
        ee.data.getAsset(destination_asset)
        # If the asset exists, remove it
        ee.data.deleteAsset(destination_asset)
        print(f"Deleted existing asset: {destination_asset}")
    except ee.EEException:
        # If the asset does not exist, continue
        pass

    # Copy the asset from the source to the destination
    ee.data.copyAsset(source_asset, destination_asset)

    # Extract DOY from the asset name (assuming asset name is just a number representing DOY)
    doy = int(asset_name)

    start_time = doy_to_datetime_string(doy, hour=10)
    end_time = doy_to_datetime_string(doy, hour=11)

    # Set the properties on the copied asset
    ee.data.updateAsset(destination_asset, {
                        'start_time': start_time}, ['start_time'])
    ee.data.updateAsset(destination_asset, {
                        'end_time': end_time}, ['end_time'])

    print("Copied and set time  asset:", asset_name)
