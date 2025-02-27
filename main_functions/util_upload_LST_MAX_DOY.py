import ee
import netCDF4
import subprocess
import os
import sys
sys.path.append(os.path.abspath(r"C:\temp\topo-satromo\step0_processors"))
sys.path.append(os.path.abspath(r"C:\temp\topo-satromo"))
# from step0_utils import write_asset_as_empty
from main_functions import main_utils
import numpy as np
from datetime import datetime, timezone
from pydrive.auth import GoogleAuth
import json
import time
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
import tempfile
"""
util_upload_LST_MAX_DOY.py

This script contains functions to determine the run environment, generate LST mosaics for a single day of the year (DOY),
and upload the generated GeoTIFF files to Google Cloud Storage (GCS) and Google Earth Engine (GEE).

Functions:
----------
1. determine_run_type():
    - Determines the run environment (local or production).
    - Initializes Google Cloud and Google Earth Engine credentials.

2. generate_lst_mosaic_for_single_doy(doy_to_process, collection, netcdf_path, task_description, set_timeframe, set_scale, tiffpath, percentiles):
    - Generates a MSG MFG LST mosaic for a single date and uploads it to Google Cloud Storage and Google Earth Engine.
    - Manages metadata, checks for existing assets, and starts task execution for upload to GEE collection as an asset.

3. export_doy_bands_to_geotiff(netcdf_path, output_path, percentiles, doy):
    - Exports all 4 percentile bands for a specific day of year (DOY) from a NetCDF file to a GeoTIFF using subprocess calls to GDAL utilities.
    - Multiplies values by 100 and converts to UInt16.

Example Usage:
--------------
python util_upload_LST_MAX_DOY.py -d "/path/to/your/dx file.tif"


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
            "secrets", "geetest-credentials-int.secret")
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




def generate_lst_mosaic_for_single_doy(doy_to_process: str, collection: str,netcdf_path, task_description: str,set_timeframe,set_scale, tiffpath,percentiles) -> None:
    """
    Generates a MSG MFG LST mosaic for a single date and uploads it to Google Cloud Storage and Google Earth Engine.

    Args:
        doy_to_process (str): The day of year to process (format: DDD).
        collection (str): The collection name for the asset.
        netcdf_path (str): Path to the NetCDF file.
        task_description (str): Description of the task.
        set_timeframe (int): Timeframe in days.
        set_scale (int): Scale for the asset.
        tiffpath (str): Path to save the output GeoTIFF.
        percentiles (list): List of percentiles to process.
    """

    # Band name
    # Define new band names based on percentiles
    percentiles = [0.02, 0.05, 0.95, 0.98]
    band_names = [f"p{int(p * 100):02d}" for p in percentiles]  # Ensures 2-digit formatting

    # GCS Google cloud storage bucket
    bucket_name = "viirs_lst_meteoswiss"

    # GEE asset NAME prefix
    asset_prefix = "LST_Stats_DOY"

    # Switch to Wait till upload is complete
    # if set to false, The deletion of the file on GCS (delete blob) has to be implemented yet
    wait_for_upload = True

    output_path = tiffpath+doy_to_process+"_percentiles_CH1903.tif"
    # Create the TIFF dataset
    export_doy_bands_to_geotiff(netcdf_path, output_path, percentiles, doy_to_process)

    # check if we are on a local machine or on github
    determine_run_type()

    # initialize_bucket(bucket_name)
    bucket = storage_client.bucket(bucket_name)

    # upload local file to GCS
    # gs_path = upload_to_gcs("M_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif", "M_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif")
    # Upload the file to the bucket
    try:
        blob = bucket.blob("M_LST_"+doy_to_process +
                           "_percentiles_CH1903.tif")
        blob.upload_from_filename(
            output_path)
        print(f"File M_LST_"+doy_to_process +
                           "_percentiles_CH1903.tif" +
              " uploaded to gs://"+bucket_name+"/M_LST_"+doy_to_process+"_percentiles_CH1903.tif")
    except Exception as e:
        # Handle any exceptions raised during the upload process
        print(f"Error uploading file to GCS: {e}")

    # Define the asset name for Earth Engine
    asset_name = collection+"/"+asset_prefix+doy_to_process.zfill(3)

    # Load the GeoTIFF file as an Earth Engine Image
    image = ee.Image.loadGeoTIFF(
        f"gs://{bucket_name}/M_LST_"+doy_to_process+"_percentiles_CH1903.tif")

    # rename band
    image = image.rename(band_names)

    # Fixed start and end dates
    start_date = "1991-01-01"  # Start date (fixed)
    end_date = "2020-12-31"    # End date (fixed)

    # Parse the datetime in UTC for start date
    start_datetime = datetime.strptime(start_date + "T00:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    start_datetime = start_datetime.replace(tzinfo=timezone.utc)

    # Convert to milliseconds since epoch for 'system:time_start'
    start_timestamp = int(start_datetime.timestamp()) * 1000  # Convert seconds to milliseconds

    # Parse the datetime in UTC for end date
    end_datetime = datetime.strptime(end_date + "T23:59:59Z", '%Y-%m-%dT%H:%M:%SZ')
    end_datetime = end_datetime.replace(tzinfo=timezone.utc)

    # Convert to milliseconds since epoch for 'system:time_end'
    end_timestamp = int(end_datetime.timestamp()) * 1000  # Convert seconds to milliseconds

    # Getting swisstopo Processor Version
    processor_version = main_utils.get_github_info()

    # Set metadata properties

    image = image.set({
        # Convert to milliseconds Start timestamp
        'system:time_start': start_timestamp,
        # Assuming single timestamp Convert to milliseconds End timestamp
        'system:time_end': end_timestamp,
        # Set the name of the image
        'system:name': asset_prefix+doy_to_process.zfill(3),
        # Set the date
        'doy': doy_to_process,
         # Orig filename
        'orig_filename': os.path.basename(netcdf_path),
        # Set the date
        'SWISSTOPO_PROCESSOR': processor_version['GithubLink'],
        'SWISSTOPO_RELEASE_VERSION': processor_version['ReleaseVersion'],
        # Set the scale
        'scale': set_scale,
        # timeframeInDays
        'timeframeInDays': set_timeframe

    })

    # Check if the asset already exists
    asset_exists = ee.data.getInfo(asset_name)

    # If the asset exists, delete it, otherwise upload fails
    if asset_exists:
        ee.data.deleteAsset(asset_name)
        print(f"Deleted existing asset: {asset_name}")

    # Export the image to the asset folder
    task = ee.batch.Export.image.toAsset(
        image, assetId=asset_name,
        description=task_description)  # force Enable overwrite
    task.start()

    if wait_for_upload is True:
        # Wait for 15 seconds
        time.sleep(15)

        # Check the export task status:
        # If bulk upload is required: no while loop is required, you need to parse thet stats in dict and then check for FAILED tasks
        while task.active():
            print("Export task is still active. Waiting...")
            time.sleep(5)  # Check status every 5 seconds

        # If the task is completed, continue with cleaning up GCS
        if task.status()['state'] == 'COMPLETED':
            # Continue with your code here
            pass
            print("upload finished:" + asset_prefix+doy_to_process )

            # delete file on GCS
            blob.delete()

        # If the task has failed, print the error message
        elif task.status()['state'] == 'FAILED':
            error_message = task.status()['error_message']
            print("Export task failed with error message:", error_message)

    # remove the local file
    os.remove(output_path)


def export_doy_bands_to_geotiff(netcdf_path, output_path, percentiles, doy):
    """
    Export all 4 percentile bands for a specific day of year (DOY) from a NetCDF file to a GeoTIFF
    using subprocess calls to GDAL utilities. Multiplies values by 100 and converts to UInt16.

    Parameters:
    -----------
    netcdf_path : str
        Path to the NetCDF file
    output_path : str
        Path where the output GeoTIFF will be saved
    percentiles : list
        List of percentiles to process
    doy : int
        The day of year to extract (1-366)
    """
    # Open the NetCDF file to get information
    dataset = netCDF4.Dataset(netcdf_path, 'r')



    # Create a temporary directory for intermediate files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Process each percentile separately
        temp_files = []

        for i, percentile in enumerate(percentiles):
            # Calculate the band index (starting from 1)
            band_idx = (int(doy) - 1) * 4 + (i + 1)

            # Create a temporary file for this band
            temp_file = os.path.join(temp_dir, f"doy{doy}_percentile{percentile}.tif")

            # Create an intermediate file for scaled data
            temp_scaled_file = os.path.join(temp_dir, f"doy{doy}_percentile{percentile}_scaled.tif")

            # Construct gdal_translate command to extract the specific band
            gdal_translate_cmd = [
                'gdal_translate',
                '-b', str(band_idx),
                '-a_srs', 'EPSG:4326',  # Source coordinate system
                netcdf_path,
                temp_file
            ]

            print(f"Extracting band {band_idx} (DOY {doy}, percentile {percentile})")

            # Execute gdal_translate
            result = subprocess.run(gdal_translate_cmd, check=True,
                                   capture_output=True, text=True)

            # Process with gdal_calc to multiply by 100 and convert to UInt16
            gdal_calc_cmd = [
                'gdal_calc',
                '-A', temp_file,
                '--outfile=' + temp_scaled_file,
                '--calc=numpy.uint16(A*100)',  # Multiply by 100 and convert to UInt16
                '--NoDataValue=0',
                '--type=UInt16'
            ]

            print(f"Scaling band {band_idx} by 100 and converting to UInt16")

            result = subprocess.run(gdal_calc_cmd, check=True,
                                  capture_output=True, text=True)

            if result.stderr and 'Warning' not in result.stderr:
                print(f"Warning in gdal_calc: {result.stderr}")

            temp_files.append(temp_scaled_file)

        # Now merge all 4 scaled band files into one multi-band GeoTIFF
        temp_merged = os.path.join(temp_dir, "merged.tif")
        gdal_buildvrt_cmd = [
            'gdalbuildvrt',
            '-separate',
            os.path.join(temp_dir, "merged.vrt"),
        ] + temp_files

        # Execute gdalbuildvrt
        subprocess.run(gdal_buildvrt_cmd, check=True,
                       capture_output=True, text=True)

        # Convert VRT to GeoTIFF with UInt16 type
        gdal_translate_vrt_cmd = [
            'gdal_translate',
            '-ot', 'UInt16',  # Ensure output type is UInt16
            os.path.join(temp_dir, "merged.vrt"),
            temp_merged
        ]

        subprocess.run(gdal_translate_vrt_cmd, check=True,
                      capture_output=True, text=True)

        # Finally, warp the merged file to EPSG:2056
        gdalwarp_cmd = [
            'gdalwarp',
            '-s_srs', 'EPSG:4326',
            '-t_srs', 'EPSG:2056',
            '-of', 'COG',
            '-srcnodata', '0',
            '-co', 'COMPRESS=DEFLATE',
            '-co', 'PREDICTOR=2',
            '-ot', 'UInt16',  # Ensure output type is UInt16
            '-r', 'bilinear',  # Resampling method
            temp_merged,
            output_path
        ]

        print(f"Reprojecting to EPSG:2056 and saving to {output_path}")
        result = subprocess.run(gdalwarp_cmd, check=True,
                               capture_output=True, text=True)

        # For debugging
        if result.stderr:
            print(f"Warning: {result.stderr}")

    # Close the dataset
    dataset.close()

    print(f"GeoTIFF with 4 percentile bands for DOY {doy} created at: {output_path}")
    print(f"Bands contain percentiles: {percentiles} (multiplied by 100 and stored as UInt16)")

if __name__ == "__main__":
    global config_GDRIVE_SECRETS
    config_GDRIVE_SECRETS = r'C:\temp\topo-satromo\secrets\geetest-credentials-int.secret'

    # Set parameters for processing
    collection = "projects/satromo-int/assets/2004-2020_LST_MAX_AS_SWISS"
    percentiles = [0.02, 0.05, 0.95, 0.98]
    doy_to_process="1" # add a loop
    set_timeframe=15
    set_scale=100
    task_description=str(doy_to_process)+" 2004-2021_LST_MAX_AS_SWISS"
    tiff_path = r"C:\temp\temp\output_doy"
    netcdf_path = r"C:\temp\geosatclim\ch.meteoswiss.geosatclim.custom_delivery_to_swisstopo_by_huv.percentiles_prelim\LST_percentiles\msg.LST1max_percentile.H_ch02.lonlat.nc"



    #generate_lst_mosaic_for_single_doy(doy_to_process, collection,netcdf_path, task_description,set_timeframe,set_scale,tiff_path,percentiles)

        # Loop through all days of the year (1-366 to include leap years)
    for doy_to_process in range(1, 367):
        task_description = f"{doy_to_process} 2004-2021_LST_MAX_AS_SWISS"
        print(f"Processing day {doy_to_process}...")
        
        # Call your processing function for each day
        generate_lst_mosaic_for_single_doy(
            str(doy_to_process),  # Convert to string as your function seems to expect a string
            collection,
            netcdf_path,
            task_description,
            set_timeframe,
            set_scale,
            tiff_path,
            percentiles
        )






