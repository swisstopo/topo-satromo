import ee
from main_functions import main_utils
from main_functions import util_create_LSTMAX
from .step0_utils import write_asset_as_empty
import netCDF4
import subprocess
import rasterio
import os
import numpy as np
from datetime import datetime, timezone
import requests
import urllib.request
import configuration as config
from pydrive.auth import GoogleAuth
import json
import time
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage

# Processing pipeline for DAILY NETCDF for daily LandSurfce  mosaics over Switzerland.

##############################
# INTRODUCTION
# This script provides a tool to Access and upload Landsurface (LST) data over Switzerland to GEE.
# It uses CMS SAF data provided by  MeteoSwiss  LST as
# ->  DAILY FILES
# to be stored as SATROMO assets and  to calculate VCI and TCI and combine them to the VHI. The CM SAF data are owned by EUMETSAT and are
# available to all users free of charge and with no conditions to use. If you wish to use these products,
# EUMETSAT's copyright credit must be shown by displaying the words "Copyright (c) (2022) EUMETSAT" under/in
# each of these SAF Products used in a project or shown in a publication or website.

##############################
# CONTENT
# The switches enable/disable the execution of individual steps in this script.

# This script includes the following steps:
# 1. Determine the run type (local or GitHub).
# 2. Get NetCDF information from the specified file.
# 3. Export NetCDF band data to GeoTIFF format.
# 4. Generate MSG LST mosaic for a single date.
# 5. Upload the GeoTIFF file to Google Cloud Storage (GCS).
# 6. Export the image to Google Earth Engine as an asset.

###########################################
# FUNCTIONS


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    # Set scopes for Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]

    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2

        # Read the service account key file
        with open(os.path.join("secrets", "geetest-credentials.secret"), "r") as f:
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


def get_netcdf_info(file_path, epoch_time=None):
    """
    Extracts information from a NetCDF file.

    Args:
        file_path (str): Path to the NetCDF file.
        epoch_time (int, optional): Specific epoch time to extract data for. Defaults to None.

    Returns:
        dict: A dictionary containing global attributes, dimensions, and variables of the NetCDF file.
    """

    # Open the NetCDF file
    dataset = netCDF4.Dataset(file_path, 'r')

    # Extract global attributes
    global_attributes = {attr: dataset.getncattr(
        attr) for attr in dataset.ncattrs()}

    # Extract dimensions
    dimensions = {dim: len(dataset.dimensions[dim])
                  for dim in dataset.dimensions}

    # Extract variables and their attributes
    variables = {}
    for var_name in dataset.variables:
        var = dataset.variables[var_name]
        variables[var_name] = {
            'dimensions': var.dimensions,
            'shape': var.shape,
            'attributes': {attr: var.getncattr(attr) for attr in var.ncattrs()},
            'data': var[:].tolist()
        }

    # If epoch_time is provided, find the index corresponding to that time
    time_var = dataset.variables['time']
    time_index = None
    if epoch_time is not None:
        for i, t in enumerate(time_var[:]):
            if t == epoch_time:
                time_index = i
                break

    # Close the dataset if no epoch_time is provided
    if epoch_time is None:
        dataset.close()
        return {
            'global_attributes': global_attributes,
            'dimensions': dimensions,
            'variables': variables
        }

    # If epoch_time is provided and found in the file, get the variables for that time
    if time_index is not None:
        time_variables = {}
        for var_name in dataset.variables:
            var = dataset.variables[var_name]
            var_data = var[time_index].tolist()
            time_variables[var_name] = {
                'dimensions': var.dimensions,
                'shape': var.shape,
                'attributes': {attr: var.getncattr(attr) for attr in var.ncattrs()},
                'data': var_data
            }

        # Close the dataset
        dataset.close()
        return {
            'global_attributes': global_attributes,
            'dimensions': dimensions,
            'time': epoch_time,
            'variables': time_variables
        }
    else:
        # Close the dataset
        dataset.close()
        return {
            'global_attributes': global_attributes,
            'dimensions': dimensions,
            'time': epoch_time,
            'variables': None
        }


def export_netcdf_band_to_geotiff(file_path, time_value, output_tiff):
    """
    Exports a specific band from a NetCDF file to a GeoTIFF file.

    Args:
        file_path (str): Path to the NetCDF file.
        time_value (int): Specific time value to extract the band for.
        output_tiff (str): Path to the output GeoTIFF file.


    Example usage
         file_path = 'msg.LST_PMW.H_ch02.lonlat_20240412000000.nc'
         output_tiff = 'msg.LST_PMW.H_ch02.lonlat_20240412000000.tif'
         time_value = 1712944800  # Example time value
    """

    # Open the NetCDF file
    dataset = netCDF4.Dataset(file_path, 'r')

    # Extract the time variable
    time_var = dataset.variables['time']

    # Find the index corresponding to the specified time value
    time_index = None
    for i, t in enumerate(time_var[:]):
        if t == time_value:
            time_index = i
            break

    if time_index is None:
        raise ValueError("Time value not found in the NetCDF file.")

    # Extract the specific band for the given time
    lst_var = dataset.variables['LST_PMW']
    lst_band = lst_var[time_index, :, :]

    # Write the band to a temporary GeoTIFF file
    temp_nc_file = 'temp.nc'
    with netCDF4.Dataset(temp_nc_file, 'w') as temp_dataset:
        # Create dimensions
        temp_dataset.createDimension('lon', lst_band.shape[1])
        temp_dataset.createDimension('lat', lst_band.shape[0])

        # Create variables
        lon_var = temp_dataset.createVariable('lon', 'f4', ('lon',))
        lon_var[:] = dataset.variables['lon'][:]
        lat_var = temp_dataset.createVariable('lat', 'f4', ('lat',))
        lat_var[:] = dataset.variables['lat'][:]
        lst_temp_var = temp_dataset.createVariable(
            'LST', 'f4', ('lat', 'lon',))
        lst_temp_var[:] = lst_band

        # Set variable attributes
        lst_temp_var.units = lst_var.units
        lst_temp_var.long_name = lst_var.long_name

    # Use gdal_translate to export the temporary NetCDF file to GeoTIFF
    temp_tiff = 'temp.tif'
    command = [
        'gdal_translate',
        f'NETCDF:"{temp_nc_file}":LST',
        temp_tiff
    ]
    # print(result.stdout))

    result = subprocess.run(command, check=True,
                            capture_output=True, text=True)
    # print(result.stdout)
    print(result.stderr)

    # Multiply the band values by 100 and change the data type to 16-bit integer using Rasterio
    scaled_tiff = 'scaled_temp.tif'

    # Open the temporary GeoTIFF file
    with rasterio.open(temp_tiff) as src:
        # Read the band data and the nodata value
        data = src.read(1)
        nodata_value = src.nodata

        # Set nodata values to 0
        data[data == nodata_value] = 0

        # Multiply the data by 100
        scaled_data = np.clip(data * 100, -32767, 32767).astype(np.int16)

        # Update the profile to set the data type to int16 and compress using LZW
        profile = src.profile
        profile.update(
            dtype=rasterio.int16,
            compress='lzw',
            nodata=None  # Reset nodata value
        )

        # Write the scaled data to a new GeoTIFF file
        with rasterio.open(scaled_tiff, 'w', **profile) as dst:
            dst.write(scaled_data, 1)

    # Reproject the GeoTIFF to EPSG:2056
    command = [
        'gdalwarp',
        '-s_srs', 'EPSG:4326',
        '-t_srs', 'EPSG:2056',
        '-of', 'COG',
        # '-cutline', config.BUFFER,
        '-srcnodata', '0',
        '-co', 'COMPRESS=DEFLATE',
        '-co', 'PREDICTOR=2',
        scaled_tiff,
        output_tiff
    ]

    result = subprocess.run(command, check=True,
                            capture_output=True, text=True)
    # print(result.stdout)
    print(result.stderr)

    # Clean up the temporary files
    os.remove(temp_nc_file)
    os.remove(temp_tiff)
    os.remove(scaled_tiff)

    # Close the dataset
    dataset.close()


def generate_msg_lst_mosaic_for_single_date(day_to_process: str, collection: str, task_description: str) -> None:
    """
    Generates a MSG LST mosaic for a single date and uploads it to Google Cloud Storage and Google Earth Engine.

    Args:
        day_to_process (str): The date to process (format: YYYY-MM-DD).
        collection (str): The collection name for the asset.
        task_description (str): Description of the task.
    """
    # CONFIGURATION START
    # --------------------


     # WORKING WITH 1 FILE PER DAY (operational delivery)
    # netcdf files: download data from data.geo.admin.ch location , check if file exist
    raw_filename = day_to_process.replace("-", "")+"000000.nc"

    # WORKING WITH 1 FILE PER MONTH (one time delivery)
    # modified_date_str = day_to_process.replace("-", "")
    # Replace the day part (DD) with "01"
    # raw_filename = modified_date_str[:6] + "01"+"000000.nc"

    data_import_url = "https://data.geo.admin.ch/ch.meteoschweiz.landoberflaechentemperatur/msg.LST_PMW.H_ch02.lonlat_"+raw_filename
    # data_import_url = "https://data.geo.admin.ch/ch.meteoschweiz.landoberflaechentemperatur/MSG2004-2023/msg.LST_PMW.H_ch02.lonlat_"+raw_filename

    # Set LST_HOUR based on the condition
    LST_hour =  11

    # Band name
    band_name = "LST_PMW"

    # GCS Google cloud storage bucket
    bucket_name = "viirs_lst_meteoswiss"

    # GEE asset NAME prefix
    asset_prefix = "MSG_METEOSWISS_mosaic_"

    # Switch to Wait till upload is complete
    # if set to false, The deletion of the file on GCS (delete blob) has to be implemented yet
    wait_for_upload = True

    # CONFIGURATION END
    # --------------------


    # Convert the string to an ee.Date object

    # Send a HEAD request to check if the file exists
    try:
        # Send a HEAD request to check if the file exists
        response = requests.head(data_import_url)

    # Check if the response status code indicates the file exists (status code 200)
        if response.status_code == 200:

            #  download the file
            urllib.request.urlretrieve(data_import_url, raw_filename)

        else:
            write_asset_as_empty(
                collection, day_to_process, 'No candidate scene')
            return False  # File does not exist
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return False  # An error occurred

    # info_raw_file = get_netcdf_info(raw_filename)

    # Split the date string by the hyphen delimiter
    year, month, day = day_to_process.split('-')

    # we use the 1100 UTC LST dataset
    epochtime = int(datetime(int(year), int(month),
                    int(day), LST_hour, 00, 00).timestamp())

    # Create the TIFF dataset
    export_netcdf_band_to_geotiff(
        raw_filename, epochtime, "MSG_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif")

    # Get the metdata for the epoch
    info_raw_file = get_netcdf_info(raw_filename, epoch_time=epochtime)

    # check if we are on a local machine or on github
    determine_run_type()

    # initialize_bucket(bucket_name)
    bucket = storage_client.bucket(bucket_name)

    # upload local file to GCS
    # gs_path = upload_to_gcs("MSG_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif", "MSG_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif")
    # Upload the file to the bucket
    try:
        blob = bucket.blob("MSG_LST_"+day_to_process +
                           "T"+str(LST_hour)+"0000.tif")
        blob.upload_from_filename(
            "MSG_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif")
        print(f"File MSG_LST_"+day_to_process+"T"+str(LST_hour) +
              "0000.tif uploaded to gs://"+bucket_name+"/MSG_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif")
    except Exception as e:
        # Handle any exceptions raised during the upload process
        print(f"Error uploading file to GCS: {e}")

    # Define the asset name for Earth Engine
    asset_name = config.PRODUCT_VHI['LST_current_data']+"/"+asset_prefix+day_to_process + \
        "T"+str(LST_hour)+"0000"+'_bands-1721m'

    # Load the GeoTIFF file as an Earth Engine Image
    image = ee.Image.loadGeoTIFF(
        f"gs://{bucket_name}/MSG_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif")

    # rename band
    image = image.rename([band_name])

    # Parse the datetime in UTC
    date_time = datetime.strptime(
        day_to_process + "T" + str(LST_hour).zfill(2) + "0000Z", '%Y-%m-%dT%H%M%SZ')
    date_time = date_time.replace(tzinfo=timezone.utc)

    # Set metadata properties

    image = image.set({
        # Convert to milliseconds Start timestamp
        'system:time_start': int(date_time.timestamp()) * 1000,
        # Assuming single timestamp Convert to milliseconds End timestamp
        'system:time_end': int(date_time.timestamp()) * 1000,
        # Set the name of the image
        'system:name': asset_prefix+day_to_process+"T"+str(LST_hour)+"0000"+'_bands-1721m',
        # Set the date
        'date': day_to_process,
        # HourMin Sec
        'hour': str(LST_hour),
        # Orig filename
        'orig_filename': os.path.basename(data_import_url),
        # Set the date
        'spacecraft_name': info_raw_file['global_attributes']['platform'],
        # Set the date
        'long_name': info_raw_file['variables']['LST_PMW']['attributes']['long_name'],
        # Version
        'product_version': info_raw_file['variables']['LST_PMW']['attributes']['version'],
        # record Status
        'netcdf_dim_time': str(info_raw_file['time']),
        # date created
        'date_created': info_raw_file['global_attributes']['date_created'],
        # Set the no data value, you can add more properties like baselines  etc
        # '_FillValue': str(info_raw_file['variables']['LST_PMW']['attributes']['_FillValue'])
        'no_data': str(config.PRODUCT_MSG_CLIMA["no_data"])
    })

    # Check if the asset already exists
    asset_exists = ee.data.getInfo(asset_name)

    # If the asset exists, delete it, otherwise upload failes
    if asset_exists:
        ee.data.deleteAsset(asset_name)
        print(f"Deleted existing asset: {asset_name}")

    # Export the image to the asset folder
    task = ee.batch.Export.image.toAsset(
        image,
        assetId=asset_name,
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
            print("upload finished:" + asset_prefix+day_to_process +
                  "T"+str(LST_hour)+"0000"+'_bands-1721m')

            # delete file on GCS
            blob.delete()

        # If the task has failed, print the error message
        elif task.status()['state'] == 'FAILED':
            error_message = task.status()['error_message']
            print("Export task failed with error message:", error_message)

    # remove the local file
    os.remove("MSG_LST_"+day_to_process+"T"+str(LST_hour)+"0000.tif")
    os.remove(raw_filename)
    return True
