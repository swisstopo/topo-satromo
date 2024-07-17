from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
import ee
import os
import json
import time

"""
This script performs the following tasks:
1. Initializes Google Earth Engine (GEE) .
2. Copies each asset from a source location to a destination location.
3. Updates metadata property for each asset.
4. Deletes temporary assets after processing.
"""

config = r'[...]\secrets\[...].secret'

###########################################################################################################
# Parameters
# assets to change
asset_list = r'[...]\main_functions\asset_list.txt'
# name and value of the property that shall be added
prop_name = 'pixel_size_meter'
prop_value = 10
###########################################################################################################

def initialize_gee_and_drive():
    """
    Initializes Google Earth Engine (GEE) and Google Drive based on the run type.

    Initializes GEE and authenticates using the service account key file.

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


###########################################################################################################
# Adding properties to existing GEE assets
###########################################################################################################

# IMPORT
# assets to be changed
# Read asset names from the text file into a list
with open(asset_list, 'r') as file:
    asset_names = file.readlines()

# Copy each asset from the source to the destination
for asset_name in asset_names:
    # Load the image
    asset_id = asset_name.replace('\n', '')
    image = ee.Image(asset_id)

        # Set metadata property 'cloud_mask_threshold'
    image_new = image.set(prop_name, prop_value)

    # Export the image and overwrite the existing asset
    task = ee.batch.Export.image.toAsset(
        image=image_new,
        description='Export Asset',
        maxPixels=1e10,
        assetId=asset_id + "_tempmove"
    )
    task.start()

    # Wait for the export to complete
    print('Export operation in progress...')
    while task.active():
        time.sleep(30)

    # Check the export status
    if task.status()['state'] == 'COMPLETED':
        print('Export operation completed.')
        updated_asset = ee.Image(asset_id)
        print('Updated Asset:')
        print(asset_id)
    else:
        print('Export operation failed or not yet completed.')
        print('Export operation status:', task.status())

    # Copy the temporary asset to the original asset and delete the temporary asset
    ee.data.copyAsset(asset_id+"_tempmove", asset_id, allowOverwrite=True)
    ee.data.deleteAsset(asset_id+"_tempmove")

