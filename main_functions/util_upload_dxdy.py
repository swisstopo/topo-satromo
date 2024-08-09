import ee
import os
import argparse
import numpy as np
from datetime import datetime, timezone
from pydrive.auth import GoogleAuth
import json
import time
import subprocess
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
import re

"""
Header:
-------
Script Name: util_upload_dxdy.py
Description: This script automates the process of uploading a dx dy mosaic image to Google Cloud Storage (GCS) and  then to Google Earth Engine (GEE) collection. It checks the environment (local or production), initializes the necessary credentials, and processes the upload and configuration of the image asset.

Introduction:
-------------
The primary purpose of this script is to streamline the workflow for uploading and managing mosaic images within the Google Cloud and Google Earth Engine ecosystems. This script is particularly made for uploading coregistration shift matrix for s2 to be used in GEE.

Content:
--------
1. determine_run_type():
    - Determines the run environment (local or production).
    - Initializes Google Cloud and Google Earth Engine credentials.
2. upload_dx_dy_mosaic_for_single_date(day_to_process, collection):
    - Uploads a specified GeoTIFF file to Google Cloud Storage.
    - Configures and processes the image within Google Earth Engine.
    - Manages metadata, checks for existing assets, and starts task execution for upload to GEE colleaction as asset.

Example:
----------
python util_upload_dxdy.py -d "/path/to/your/dx file.tif"



"""


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    # Set scopes for Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]

    if os.path.exists(config_GDRIVE_SECRETS):
        run_type = 2

        # Read the service account key file
        with open(config_GDRIVE_SECRETS, "r") as f:
            data = json.load(f)

        # Authenticate with Google using the service account key file
        gauth = GoogleAuth()
        gauth.service_account_file = os.path.join(
            "secrets", "geetest-credentials.secret")
        gauth.service_account_email = data["client_email"]
        print("\nType 2 run PROCESSOR: We are on a local machine")
    else:
        run_type = 1
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
        print("\nType 1 run PROCESSOR: We are on GitHub")

    # Test if GEE initialization is successful
    # Initialize Google Earth Engine
    credentials = ee.ServiceAccountCredentials(
        gauth.service_account_email, gauth.service_account_file
    )
    ee.Initialize(credentials)

    image = ee.Image("NASA/NASADEM_HGT/001")
    title = image.get("title").getInfo()

    if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
        print("GEE initialization successful")
    else:
        print("GEE initialization FAILED")

    # Initialize GCS
    global storage_client
    storage_client = storage.Client.from_service_account_json(
        gauth.service_account_file)


def upload_dx_dy_mosaic_for_single_date(day_to_process: str, collection: str) -> None:
    """
    upload a dX dy  mosaic for a single date to Google Cloud Storage and Google Earth Engine.

    Args:
        day_to_process (str): The date to process (format: YYYY-MM-DD).
        collection (str): The collection name for the asset.
        task_description (str): Description of the task.
    """

    # CONFIGURATION START
    # --------------------
    # Filename
    file_name = os.path.basename(day_to_process)

    # Timestamp
    timestamp = re.search(r'(\d{4}-\d{2}-\d{2}T\d{6})',
                          file_name).group(0).upper()

    # EPSG traget colllection: shoudl be EPSG:32632
    epsg = 'EPSG:32632'

    # assetname
    # asset_name = re.sub(r'.*?(s2-sr.*)\.tif', r'\1', file_name)
    asset_name = "S2-SR_mosaic_"+timestamp+"_registration-10m"

    # GCS Google cloud storage bucket
    bucket_name = "s2_sr_registration_swiss"

    # new_band_names =
    band_names = ['reg_dx', 'reg_dy']

    # Switch to Wait till upload is complete
    wait_for_upload = True

    # Switch to delete local file
    delete_local = True

    # CONFIGURATION END
    # --------------------

    print("processing DXDY of: "+timestamp)

    # Merging the corresponding DX and DY into one file

    command = ["gdalbuildvrt",
               "-separate",
               asset_name+".vrt",
               day_to_process,
               day_to_process.replace('_dx', '_dy')
               ]
    # print(command)
    result = subprocess.run(command, check=True,
                            capture_output=True, text=True)
    # print(result)

    command = ["gdal_translate",
               "-of", "COG",
               #    "-co", "TILING_SCHEME=GoogleMapsCompatible",
               "-co", "COMPRESS=DEFLATE",
               "-co", "PREDICTOR=2",
               "-co", "NUM_THREADS=ALL_CPUS",
               asset_name+".vrt",
               asset_name+".tif",
               ]
    # print(command)
    result = subprocess.run(command, check=True,
                            capture_output=True, text=True)
    # print(result)

    # check if we are on a local machine or on github
    determine_run_type()

    # initialize_bucket(bucket_name)
    bucket = storage_client.bucket(bucket_name)

    # Upload the file to the bucket
    try:
        blob = bucket.blob(asset_name+".tif")
        blob.upload_from_filename(
            asset_name+".tif")
        print("SUCCESS: uploaded to gs://"+bucket_name+"/"+asset_name+".tif")

        # delete file on GCS
        print("Starting export task "+asset_name+" to GEE ...")
    except Exception as e:
        # Handle any exceptions raised during the upload process
        print(f"ERROR: uploading file to GCS: {e}")

    # Load the GeoTIFF file as an Earth Engine Image
    image = ee.Image.loadGeoTIFF(
        f"gs://{bucket_name}/"+asset_name+".tif")

    # rename band
    image = image.rename(band_names)

    # Parse the datetime in UTC
    date_time = datetime.strptime(
        timestamp + "Z", '%Y-%m-%dT%H%M%SZ')
    date_time = date_time.replace(tzinfo=timezone.utc)

    # Set metadata properties

    image = image.set({
        # Convert to milliseconds Start timestamp
        'system:time_start': int(date_time.timestamp()) * 1000,
        # Assuming single timestamp Convert to milliseconds End timestamp
        'system:time_end': int(date_time.timestamp()) * 1000,
        # Set the name of the image
        'system:name': asset_name+".tif",
        # Set the date
        'date': timestamp,
        # Method
        'method': "AROSICS",
        # Version
        'product_version': "v1.0.0",
        # Arosics orig_name X
        'orig_file_dx': file_name,
        # Arosics orig_name Y
        'orig_file_dy': file_name.replace('_dx', '_dy'),
    })

    # Check if the asset already exists
    asset_exists = ee.data.getInfo(collection+"/"+asset_name)

    # If the asset exists, delete it, otherwise upload failes
    if asset_exists:
        ee.data.deleteAsset(collection+"/"+asset_name)
        print(f"Deleted existing asset: {asset_name}")

    # Export the image to the asset folder
    task = ee.batch.Export.image.toAsset(
        image,
        scale=10,
        description="regDX_regDY_"+timestamp,
        crs=epsg,
        maxPixels=1e10,
        assetId=collection+"/"+asset_name
    )  # force Enable overwrite
    task.start()

    if wait_for_upload is True:
        # Wait for 60 seconds
        time.sleep(60)

        # Check the export task status:
        # If bulk upload is required: no while loop is required, you need to parse thet stats in dict and then check for FAILED tasks
        while task.active():
            print("Export task "+asset_name+" is still active. Waiting...")
            time.sleep(60)  # Check status every 5 seconds

        # If the task is completed, continue with cleaning up GCS
        if task.status()['state'] == 'COMPLETED':
            # Continue with your code here
            pass
            print("SUCCESS: uploaded to collection "+collection+" finished")

            # delete file on GCS
            blob.delete()

        # If the task has failed, print the error message
        elif task.status()['state'] == 'FAILED':
            error_message = task.status()['error_message']
            print("ERROR: Export task "+asset_name +
                  " failed with error message:", error_message)

    # remove the local file
    if delete_local is True:
        os.remove(asset_name+".tif")
        os.remove(asset_name+".vrt")
    else:
        pass


if __name__ == "__main__":
    global config_GDRIVE_SECRETS
    config_GDRIVE_SECRETS = r'C:\temp\topo-satromo\secrets\geetest-credentials-int.secret'

    # Define the default path for `day_to_process`
    default_day_to_process = os.path.join(
        'C:', 'temp', 'temp', '2023-10', 'S2-L2A-mosaic_2023-10-24T104019_registration_swiss-10m_dx.tif')
    collection = "projects/satromo-int/assets/COL_S2_SR_DXDY"

    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Process DX DY for upload.")
    parser.add_argument('-d', '--day', type=str, default=default_day_to_process,
                        help="Path to the dx file. Defaults to the file defined in the code.")
    args = parser.parse_args()

    # Use the argument if provided, otherwise fall back to the default
    day_to_process = args.day

    # Call the function with the appropriate day_to_process value
    upload_dx_dy_mosaic_for_single_date(day_to_process, collection)
