from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
import ee
import json
import os
# Assuming configuration.py is in ../configuration directory you can create the list with https://code.earthengine.google.com/765436590a4353699e9455d75f25bd45 


config = r'C:\temp\topo-satromo\secrets\geetest-credentials-int.secret'
asset_list = r'C:\temp\topo-satromo\main_functions\asset_list.txt'

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

# Delete assets from ImageCollection
for asset_name in asset_names:
        try:
            # Delete the asset
            ee.data.deleteAsset(asset_name)
            print(f'Deleted asset: {asset_name}')
        except Exception as e:
            print(f'Error deleting asset: {asset_name}, {e}')
