from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
import ee
import os
import json
# Assuming configuration.py is in ../configuration directory
import configuration as config

def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2
        print("\nType 2 run PROCESSOR: We are on a local machine")
    else:
        run_type = 1
        print("\nType 1 run PROCESSOR: We are on GitHub")


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

    if run_type == 2:
        # Initialize GEE and authenticate using the service account key file

        # Read the service account key file
        with open(config.GDRIVE_SECRETS, "r") as f:
            data = json.load(f)

        # Authenticate with Google using the service account key file
        gauth = GoogleAuth()
        gauth.service_account_file = config.GDRIVE_SECRETS
        gauth.service_account_email = data["client_email"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )
    else:
        # Run other code using secrets from GitHub Action
        # This script is running on GitHub
        gauth = GoogleAuth()
        google_client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
        google_client_secret = json.loads(google_client_secret)
        gauth.service_account_email = google_client_secret["client_email"]
        google_client_secret_str = json.dumps(google_client_secret)

        # Write the JSON string to a temporary key file
        gauth.service_account_file = "keyfile.json"
        with open(gauth.service_account_file, "w") as f:
            f.write(google_client_secret_str)

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

# Test if we are on Local DEV Run or if we are on PROD
determine_run_type()

# Authenticate with GEE and GDRIVE
initialize_gee_and_drive()


###########################################################################################################
# Adding properties to existing GEE assets
###########################################################################################################
# PARAMETERS
# name of the property that shall be added
prop_name = 'pixel_size_meter'
prop_value = 10

# IMPORT
# asset to be changed
asset_path = 'projects/satromo-int/assets/COL_S2_SR_HARMONIZED_SWISS/'
asset_name = 'S2-L2A_mosaic_2023-10-09T103931_bands-10m'
asset = asset_path + asset_name

input_image = ee.Image(asset)

# ADD PROPERTIES
output_image = input_image.set(prop_name, prop_value)

# # DELETE OLD ASSET
# # needed because the GEE doesn't allow direct modification of metadata
# def delete_asset(asset):
#     try:
#         ee.data.deleteAsset(asset)
#         print(f'Asset {asset} deleted successfully.')
#     except Exception as e:
#         print(f'Error deleting asset {asset}:', e)

# # Call the function to delete the asset
# delete_asset(asset)

# EXPORT
# to replace the asset just deleted with a copy of itself (with the missing properties added)

task = ee.batch.Export.image.toAsset(
    image = output_image,
    description = asset_name,
    assetId = asset + '_test',
    scale = 10,
    crs ='EPSG:2056',
    maxPixels = 1e10
)

task.start()

print(f"Export task started with ID: {task.id}")