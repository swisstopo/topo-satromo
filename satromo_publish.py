# -*- coding: utf-8 -*-
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
import csv
import json
import os
import ee
import configuration as config
from collections import OrderedDict
import subprocess
import glob
import platform
import re
import requests
import time
from datetime import datetime
from collections import defaultdict
from google.cloud import storage
from main_functions import main_thumbnails, main_publish_stac_fsdi, main_extract_warnregions


# Set the CPL_DEBUG environment variable to enable verbose output
# os.environ["CPL_DEBUG"] = "ON"


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file. And determine platform

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    global GDRIVE_SOURCE
    global S3_DESTINATION
    global GDRIVE_MOUNT
    global os_name

    # Get the operating system name
    os_name = platform.system()

    # Set SOURCE , DESTINATION and MOUNTPOINTS

    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2
        print("\nType 2 run PUBLISHER: We are on a local machine")
        GDRIVE_SOURCE = config.GDRIVE_SOURCE_DEV
        GDRIVE_MOUNT = config.GDRIVE_MOUNT_DEV
        S3_DESTINATION = config.S3_DESTINATION_DEV
    else:
        run_type = 1
        print("\nType 1 run PUBLISHER: We are on Github")
        GDRIVE_SOURCE = config.GDRIVE_SOURCE_INT
        GDRIVE_MOUNT = config.GDRIVE_MOUNT_INT
        S3_DESTINATION = config.S3_DESTINATION_INT


def initialize_gee_and_drive():
    """
    Initialize Google Earth Engine (GEE), RCLONE and Google Drive authentication.

    This function authenticates GEE and Google Drive either using a service account key file
    or GitHub secrets depending on the run type.

    Returns:
    None
    """

    scopes = ["https://www.googleapis.com/auth/drive"]


    if run_type == 2:
        # Initialize GEE and Google Drive using service account key file

        # Authenticate using the service account key file
        with open(config.GDRIVE_SECRETS, "r") as f:
            service_account_key = json.load(f)

        # Authenticate Google Drive
        gauth = GoogleAuth()
        gauth.service_account_file = config.GDRIVE_SECRETS
        gauth.service_account_email = service_account_key["client_email"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )

        rclone_config_file = config.RCLONE_SECRETS
        google_secret_file = config.GDRIVE_SECRETS



    else:
        # Initialize GEE and Google Drive using GitHub secrets

        # Authenticate using the provided secrets from GitHub Actions
        gauth = GoogleAuth()
        google_client_secret = json.loads(
            os.environ.get('GOOGLE_CLIENT_SECRET'))
        gauth.service_account_email = google_client_secret["client_email"]
        gauth.service_account_file = "keyfile.json"
        with open(gauth.service_account_file, "w") as f:
            f.write(json.dumps(google_client_secret))
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )

        # Write rclone config to a file
        rclone_config = os.environ.get('RCONF_SECRET')
        rclone_config_file = "rclone.conf"
        with open(rclone_config_file, "w") as f:
            f.write(rclone_config)


        # Write GDRIVE Secrest config to a file
        google_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
        google_secret_file = "keyfile.json"
        with open(google_secret_file, "w") as f:
            f.write(google_secret)


        # Create mountpoint GDRIVE
        command = ["mkdir", GDRIVE_MOUNT]
        print(command)
        result = subprocess.run(command, check=True)

        if config.GDRIVE_TYPE != "GCS":
            # GDRIVE Mount
            command = ["rclone", "mount", "--config", "rclone.conf",  # "--allow-other",
                    os.path.join(GDRIVE_SOURCE), GDRIVE_MOUNT, "--vfs-cache-mode","full"]
        else:
            # GCS Mount
            command = ["rclone", "mount", "--config", "rclone.conf",
                    GDRIVE_SOURCE+config.GCLOUD_BUCKET, GDRIVE_MOUNT, "--gcs-bucket-policy-only" ]
            # add path on Bucket to drive
            #GDRIVE_MOUNT=os.path.join(GDRIVE_MOUNT,config.GCLOUD_BUCKET)
        print(command)
        subprocess.Popen(command)

    # Create the Google Drive client
    global drive
    drive = GoogleDrive(gauth)

    # Create the Google Drive client
    global storage_client
    storage_client = storage.Client.from_service_account_json(
            gauth.service_account_file)

    # Initialize EE
    credentials = ee.ServiceAccountCredentials(
        gauth.service_account_email, gauth.service_account_file
    )
    ee.Initialize(credentials)

    # Test EE initialization
    image = ee.Image("NASA/NASADEM_HGT/001")
    title = image.get("title").getInfo()
    if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
        print("GEE initialization successful")
    else:
        print("GEE initialization FAILED")


def initialize_drive():
    """
    Re-Initialize Google Drive since it times out.

    This function authenticates  Google Drive either using a service account key file
    or GitHub secrets depending on the run type.

    Returns:
    None
    """

    scopes = ["https://www.googleapis.com/auth/drive"]

    if run_type == 2:
        # Initialize  Google Drive using service account key file

        # Authenticate using the service account key file
        with open(config.GDRIVE_SECRETS, "r") as f:
            service_account_key = json.load(f)

        # Authenticate Google Drive
        gauth = GoogleAuth()
        gauth.service_account_file = config.GDRIVE_SECRETS
        gauth.service_account_email = service_account_key["client_email"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )

    else:
        # Initialize Google Drive using GitHub secrets

        # Authenticate using the provided secrets from GitHub Actions
        gauth = GoogleAuth()
        google_client_secret = json.loads(
            os.environ.get('GOOGLE_CLIENT_SECRET'))
        gauth.service_account_email = google_client_secret["client_email"]
        gauth.service_account_file = "keyfile.json"
        with open(gauth.service_account_file, "w") as f:
            f.write(json.dumps(google_client_secret))
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )

    # Create the Google Drive client
    global drive
    drive = GoogleDrive(gauth)


def download_and_delete_file(file):
    """
    DEV/local machine only Download a file from Google Drive and delete it afterwards.

    Parameters:
    file (GoogleDriveFile): Google Drive file object to download and delete.

    Returns:
    None
    """

    # Download the file to local machine
    file.GetContentFile(os.path.join(config.RESULTS, file["title"]))
    print(f"File {file['title']} downloaded.")

    # Delete the file
    file.Delete()
    print(f"File {file['title']} DELETED on Google Drive.")


def move_files_with_rclone(source, destination, move=True):
    """
    #TO DO : is obsolote, we need to delete file, no backup needed on INT S3 location

    Move files using the rclone command.

    Parameters:
    source (str): Source path of the files to be moved.
    destination (str): Destination path to move the files to.

    Returns:
    None
    """
    # Run rclone command to move files
    # See hint https://forum.rclone.org/t/s3-rclone-v-1-52-0-or-after-permission-denied/21961/2

    # Uncomment below for backup
    # ..........................

    # if run_type == 2:
    #     rclone = os.path.join("secrets", "rclone")
    #     rclone_conf = os.path.join("secrets", "rclone.conf")
    # else:
    #     rclone = "rclone"
    #     rclone_conf = "rclone.conf"

    # if move == True:
    #     command = [rclone, "move", "--config", rclone_conf, "--s3-no-check-bucket",
    #                source, destination]
    # else:
    #     command = [rclone, "copy", "--config", rclone_conf, "--s3-no-check-bucket",
    #                source, destination]
    # subprocess.run(command, check=True)

    # if move == True:
    #      print("SUCCESS: moved " + source + " to " + destination)
    # else:
    #      print("SUCCESS: copied " + source + " to " + destination)

    # Comment below for backup
    # ..........................
    if move == True:
        os.remove(source)
        print("SUCCESS: deleted " + source)
    else:
        print("keeping file:"+source)


def merge_files_with_gdal_warp(source):
    """
    Merge with GDAL

    Parameters:
    source (str): Source filename .

    Returns:
    None
    """

    # check local disk disk space
    if os_name == "Windows":
        print("This is a Windows operating system, make sure you have enough disk space.")
    else:
        command = ["df", "-h"]
        print(command)
        result = subprocess.run(command, check=True,
                                capture_output=True, text=True)
        print(result)

    # Get the list of all quadrant files matching the pattern
    file_list = sorted(glob.glob(os.path.join(
        GDRIVE_MOUNT, source+"*.tif")))

    # under Windows Replace double backslashes with single backslashes in the file list
    if os_name == "Windows":
        file_list = [filename.replace('\\\\', '\\') for filename in file_list]

    # Write the file names to _list.txt
    with open(source+"_list.txt", "w") as file:
        file.writelines([f"{filename}\n" for filename in file_list])

    # run gdal vrt
    command = ["gdalbuildvrt",
               "-input_file_list", source+"_list.txt", source+".vrt",
               "--config", "GDAL_CACHEMAX", "9999",
               "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
               "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES",
               # "-vrtnodata", str(config.NODATA),
               # "-srcnodata", str(config.NODATA),
               ]
    # print(command)
    result = subprocess.run(command, check=True,
                            capture_output=True, text=True)
    # print(result)

    # run gdal translate
    command = ["gdalwarp",
               # rename to source+"_merged.tif" when doing reprojection afterwards
               source+".vrt", source+".tif",
               "-of", "COG",
               "-cutline", config.BUFFER,
               "-dstnodata",  str(config.NODATA),
               # "-srcnodata", str(config.NODATA),
               # "-co", "NUM_THREADS=ALL_CPUS",
               "-co", "BIGTIFF=YES",
               # "--config", "GDAL_CACHEMAX", "9999",
               # "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
               "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES",
               # otherwise use compress=LZW
               # https://kokoalberti.com/articles/geotiff-compression-optimization-guide/ and https://digital-geography.com/geotiff-compression-comparison/
               "-co", "COMPRESS=DEFLATE",
               "-co", "PREDICTOR=2",
               # "-r", "near", #enforce nearest with cutline
               ]
    # print(command)
    try:
        result = subprocess.run(command, check=True,
                                capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occured in gdalwarp process: {e}")
        print("Output")
        print(e.output)
        raise

    # print(result)

    # For Debugging uncomment  below
    # print("Standard Output:")
    # print(result.stdout)
    # print("Standard Error:")
    # print(result.stderr)

    print("SUCCESS: merged " + source+".tif")
    return (source+".tif")


def extract_value_from_csv(filename, search_string, search_col, col_result):
    try:
        with open(filename, "r") as file:
            reader = csv.DictReader(file)

            for row in reader:
                if row[search_col] == search_string:
                    return row[col_result]

            print(
                f"Entry not found for '{search_string}' in column '{search_col}'")
    except FileNotFoundError:
        print("File not found.")

    return None


def write_update_metadata(filename, filemeta):
    # Use a regular expression pattern to find everything after the date
    match = re.search(r"(.*?\d{4}-\d{2}-\d{2}T\d{6})_(.*)", filename)

    if match:
        # Everything before and including the date
        file_prefix = match.group(1)

        # Everything after the date
        band_name = match.group(2)
        band_name = band_name.upper()

    # Construct the file path
    file_path = os.path.join(file_prefix + "_metadata.json")

    # Initialize metadata as an empty dictionary
    metadata = {}

    # Check if the file exists
    if os.path.exists(file_path):
        # If the file exists, open it and load the JSON data
        with open(file_path, 'r') as f:
            metadata = json.load(f)

    # Check if 'm-10' is in metadata
    if band_name not in metadata:
        metadata[band_name] = {}
        # Copy the data

        metadata[band_name]['BANDS'] = filemeta['GEE_BANDS']
        metadata[band_name]['PROPERTIES'] = filemeta['SWISSTOPO']
        metadata[band_name]['SOURCE_COLLECTION'] = filemeta['GEE_ID'] if 'GEE_ID' in filemeta else filemeta['GEE_PROPERTIES']['collection']
        metadata[band_name]['SOURCE_COLLECTION_PROPERTIES'] = filemeta['GEE_PROPERTIES']
        metadata[band_name]['GEE_VERSION'] = filemeta['GEE_VERSION'] if 'GEE_VERSION' in filemeta else None
        for key in filemeta:
            # Check if "*WARNREGIONS*" is part of the key
            if "WARNREGIONS" in key:
                metadata[key] = {}
                # Copy the value associated with the matching key to metadata under the key "WARNREGIONS"
                metadata[key] = filemeta[key]

        # Write the updated data back to the JSON file
        with open(file_path, 'w') as json_file:
            json.dump(metadata, json_file)

        # upload consolidated META JSON file to FSDI STAC
        main_publish_stac_fsdi.publish_to_stac(
            file_path, metadata[band_name]['PROPERTIES']['ITEM'], metadata[band_name]['PROPERTIES']['PRODUCT'], metadata[band_name]['PROPERTIES']['GEOCATID'])

        # Create a current version and upload file to FSDI STAC, only if the latest item on STAC is newer or of the same age
        collection = metadata[band_name]['PROPERTIES']['PRODUCT']
        result = extract_and_compare_datetime_from_url(config.STAC_FSDI_SCHEME+"://"+config.STAC_FSDI_HOSTNAME+config.STAC_FSDI_API +
                                                       "collections/"+collection+"/items/"+collection.replace("ch.swisstopo.", ""), metadata[band_name]['PROPERTIES']['ITEM'])
        if result == True:
            file_merged_current = re.sub(
                r'\d{4}-\d{2}-\d{2}T\d{6}', 'current', file_path)
            # Rename the file
            os.rename(file_path, file_merged_current)

            # Publish  current dataset to stac
            main_publish_stac_fsdi.publish_to_stac(
                file_merged_current, metadata[band_name]['PROPERTIES']['ITEM'], metadata[band_name]['PROPERTIES']['PRODUCT'], metadata[band_name]['PROPERTIES']['GEOCATID'], current=True)

            # Rename the file back
            os.rename(file_merged_current, file_path)


def delete_gdrive(file):
    # Attempt to delete the file with retries, since gdrive once in a while returns a error 500
    for attempt in range(3):  # Try up to 3 times
        try:
            file.Delete()
            print(f"File {file['title']} DELETED on Google Drive.")
            break  # Exit the loop if the deletion was successful
        except Exception as e:
            print(
                f"Attempt {attempt + 1} to delete file {file['title']} failed with error: {e}")
            if attempt < 2:  # If not the last attempt, wait before retrying
                time.sleep(8)  # Wait for 5 seconds before retrying
            else:
                print(
                    f"Failed to delete file {file['title']} after 3 attempts.")


def clean_up_gdrive(filename):
    """
    Deletes files in Google Drive that match the given filename.Writes Metadata of processing results

    Args:
        filename (str): The name of the file to be deleted.

    Returns:
        None
    """
    #  Find the file in Google Drive by its name
    # file_list = drive.ListFile({
    #     "q": "title contains '"+filename+"' and trashed=false"
    # }).GetList()
    # The  approach above does not work if there are a lot of files

    # TODO GCS HERE:: List forl all files
    if config.GDRIVE_TYPE != "GCS":
        filtered_files = drive.ListFile({"q": "trashed=false"}).GetList()
        file_list = [
            file for file in filtered_files if filename in file['title']]
    else:
        # TODO GCS HERE:: List forl all files
        # initialize_bucket(bucket_name)
        bucket = storage_client.bucket(config.GCLOUD_BUCKET)
        blobs = bucket.list_blobs()
        file_list = []
        for blob in blobs:
            if filename in blob.name:
                file_list.append(blob.name)

    # Check if the file is found
    if len(file_list) > 0:

        # Iterate through the files and delete them
        for file in file_list:

            # Get the current Task id
            if config.GDRIVE_TYPE != "GCS":
                file_on_drive = file['title']
            else:
                file_on_drive = file
            file_task_id = extract_value_from_csv(
                config.GEE_RUNNING_TASKS, file_on_drive.replace(".tif", ""), "Filename", "Task ID")

            # Check task status
            file_task_status = ee.data.getTaskStatus(file_task_id)[0]

            # Get the product and item
            file_product, file_item = extract_product_and_item(
                file_task_status['description'])

            # Delete file on gdrive with muliple attempt
            if config.GDRIVE_TYPE != "GCS":
                delete_gdrive(file)
            else:
                # Get the blob (file) object
                blob = bucket.blob(file)

                # Delete the blob
                blob.delete()

                print(f"File {file} deleted from bucket.")

            # Add DATA GEE PROCESSING info to stats
            write_file(file_task_status, config.GEE_COMPLETED_TASKS)

            # Remove the line from the RUNNING tasks file
            delete_line_in_file(config.GEE_RUNNING_TASKS, file_task_id)

        # read metadata from json
        with open(os.path.join(
                config.PROCESSING_DIR, filename + "_metadata.json"), 'r') as f:
            existing_data = json.load(f)

        # Add prefix and convert keys to uppercase
        file_task_status = {
            f"POSTPROCESSING_{key.upper()}": value for key, value in file_task_status.items()}

        # Add file_task_status to the "SWISSTOPO" list
        existing_data["SWISSTOPO"].update(file_task_status)

        # Write the updated data back to the JSON file
        with open(os.path.join(
                config.PROCESSING_DIR, filename + "_metadata.json"), 'w') as json_file:
            json.dump(existing_data, json_file)

        # Write and upload consolidated META JSON file to FSDI STAC
        write_update_metadata(filename, existing_data)

       # Copy  consolidated META JSON file to SATROMO INT
        move_files_with_rclone(os.path.join(
            existing_data['SWISSTOPO']['PRODUCT']+"_mosaic_"+existing_data['SWISSTOPO']['ITEM']+"_metadata.json"), os.path.join(S3_DESTINATION, file_product, existing_data['SWISSTOPO']['ITEM']), move=False)

     # delete JSON Description of asset

        if os.path.exists(os.path.join(
                config.PROCESSING_DIR, filename+"_metadata.json")):
            os.remove(os.path.join(config.PROCESSING_DIR,
                      filename+"_metadata.json"))

        # Update Status in RUNNING tasks file
        replace_running_with_complete(
            config.LAST_PRODUCT_UPDATES, file_product)

        # Clean up GDAL temporary files

        # VRT file, Pattern for .vrt files
        vrt_pattern = f"*{existing_data['SWISSTOPO']['ITEM']}*.vrt"
        vrt_files = glob.glob(vrt_pattern)
        [os.remove(file_path)
         for file_path in vrt_files if os.path.exists(file_path)]

        # Pattern for _list.txt files
        list_txt_pattern = f"*{existing_data['SWISSTOPO']['ITEM']}*_list.txt"
        list_files = glob.glob(list_txt_pattern)
        [os.remove(file_path)
         for file_path in list_files if os.path.exists(file_path)]
    else:
        # No files found
        print("No files found in GDRIVE to delete and move for "+filename)
    return


def write_file(input_dict, output_file):
    """
    Write a dictionary to a CSV file. If the file exists, the data is appended
    to it. If the file does not exist, a new file is created with a header.

    Parameters:
    input_dict (dict): Dictionary to be written to file.
    output_file (str): Path of the output file.

    Returns:
    None
    """
    append_or_write = "a" if os.path.isfile(output_file) else "w"
    with open(output_file, append_or_write, encoding="utf-8", newline='') as f:
        dict_writer = csv.DictWriter(f, fieldnames=list(input_dict.keys()),
                                     delimiter=",", quotechar='"',
                                     lineterminator="\n")
        if append_or_write == "w":
            dict_writer.writeheader()
        dict_writer.writerow(input_dict)
    return


def delete_line_in_file(filepath, stringtoremove):
    """
    Delete lines containing a specific string from a file.

    Parameters:
    filepath (str): Path of the file to modify.
    stringtoremove (str): String to search for and remove from the file.

    Returns:
    None
    """
    with open(filepath, "r+") as file:
        lines = file.readlines()
        file.seek(0)
        file.truncate()
        for line in lines:
            if stringtoremove not in line.strip() and line.strip():
                file.write(line)
            elif not line.strip():
                file.write("\n")


def extract_product_and_item(task_description):
    """
    Extract the product and item information from a task description.

    Parameters:
    task_description (str): Description of the task containing product and item information.

    Returns:
    tuple: A tuple containing the extracted product and item information.
    """

    product = task_description.split("_mosaic_")[0]
    item = task_description

    return product, item


def replace_running_with_complete(input_file, item):
    """
    Replace 'RUNNING' with 'complete' in the specific item line of an input file.

    Parameters:
    input_file (str): Path to the input file.
    item (str): Item to identify the line to be modified.

    Returns:
    None
    """
    output_lines = []
    with open(input_file, 'r') as f:
        for line in f:
            if line.startswith(item):
                line = line.replace('RUNNING', 'complete')
            output_lines.append(line)

    with open(input_file, 'w') as f:
        f.writelines(output_lines)


def extract_and_compare_datetime_from_url(url, iso_string):
    """
    Extracts the datetime value from a given STAC ITEM JSON URL and compares it with a provided ISO string.

    Args:
        url (str): The URL to fetch JSON data from.
        iso_string (str): The ISO 8601 datetime string for comparison.

    Returns:
        bool: True if the extracted datetime value is on the same day or newer than the provided ISO string; False otherwise.
    """
    response = requests.get(url)  # Fetch the JSON data from the URL
    if response.status_code == 200:
        data = response.json()  # Parse the JSON data
        # Extract the "datetime" value
        datetime_value = data['properties']['datetime']

        # Parse the datetime value from the JSON response
        extracted_datetime = datetime.strptime(
            datetime_value, '%Y-%m-%dT%H:%M:%SZ')

        # Parse the ISO string
        iso_datetime = datetime.strptime(iso_string[:10], '%Y-%m-%d')

        # Extract dates from both datetime objects
        extracted_date = extracted_datetime.date()
        iso_date = iso_datetime.date()

        # Compare the dates
        return extracted_date <= iso_date
    else:
        print("Failed to fetch data from the URL:", response.status_code)
        return False


def check_substrings_presence(file_merged, substring_to_check, additional_substrings):
    """
    Check if the main substring and at least one of the additional substrings are present in the file_merged string.

    Args:
    - file_merged (str): The string to check.
    - substring_to_check (str): The main substring to check for.
    - additional_substrings (list of str): List of additional substrings to check for.

    Returns:
    - bool: True if the main substring and at least one of the additional substrings are present, False otherwise.
    """
    # Check if the main substring is present in the string
    if substring_to_check in file_merged:
        # Check if any of the additional substrings are also present
        additional_substring_found = any(
            substring in file_merged for substring in additional_substrings)
        return additional_substring_found
    else:
        return False


def get_product_info(filename):
    """
    Checks the asset size and missing_data value of the product defined in the configuration.

    Args:
    - filename (str): The filename containing the product name to match.

    Returns:
    - tuple: (asset_size, missing_data) if a matching product is found,
             otherwise None.
    """
    # Iterate through all items in the config file
    for product_name in dir(config):
        # Get the product dictionary
        product_info = getattr(config, product_name)

        # Check if it's a dictionary and has the 'product_name' key
        if isinstance(product_info, dict) and 'product_name' in product_info:
            if product_info['product_name'] in filename:
                # Return the expected asset size and missing_data value
                return (product_info.get('asset_size'), product_info.get('missing_data'),product_info.get('no_data'))

    print("No matching product found in the configuration.")
    return None  # Return None if no matching product is found

def extract_descriptor_mean(input_string):
    """
    Extracts the descriptor substring from an input string formatted with 'swisseo_' and '_v' markers.

    Args:
        input_string (str): The input string containing the descriptor.

    Returns:
        str: The extracted descriptor between 'swisseo_' and '_v'. Returns "no-name" if the pattern is not found.
    """
    match = re.search(r'swisseo_(.*?)_v\d{3}', input_string)
    if match:
        return match.group(1)
    return "no-name"


if __name__ == "__main__":

    # Test if we are on a local machine or if we are on Github
    determine_run_type()

    # Authenticate with GEE and GDRIVE
    initialize_gee_and_drive()

    # empty temp files on GDrive
    if config.GDRIVE_TYPE != "GCS":
        file_list = drive.ListFile({'q': "trashed=true"}).GetList()
        for file in file_list:
            # Delete file on gdrive with muliple attempt
            delete_gdrive(file)
            # print('GDRIVE TRASH: Deleted file: %s' % file['title'])

    # Read the status file
    with open(config.GEE_RUNNING_TASKS, "r") as f:
        lines = f.readlines()

    # Get the unique filename
    unique_filenames = set()

    for line in lines[1:]:  # Start from the second line
        _, filename = line.strip().split(',')
        # Take the part before "quadrant"
        filename = filename.split('quadrant')[0]
        unique_filenames.add(filename.strip())

    unique_filenames = sorted(list(unique_filenames))

    # Step 1: Group by date
    grouped_files = defaultdict(list)

    for filename in unique_filenames:
        # Extract the date part
        date_part = filename.split('_mosaic_')[1].split('T')[0]
        # Group the filenames by date
        grouped_files[date_part].append(filename)

    # Step 2: Create the unique_filename_day list
    unique_filename_day = [sorted(day_list) for day_list in grouped_files.values()]

    # Step 3: Loop through unique_filename_day, Start the processing and remove groups from unique_filename
    for group in unique_filename_day:
        print("Date:",
              group[0].split('_mosaic_')[1].split('T')[0], "checking export status")

        # Check  if each quandrant is complete then process
        # Iterate over unique filenames

        # Set asset counter to 0
        all_assets = 0
        for filename in group:

            # Keep track of completion status
            all_completed = True

            # You need to change this if we have more than 4 quadrants
            for quadrant_num in range(1, 5):
                # Construct the filename with the quadrant
                full_filename = filename + "quadrant" + str(quadrant_num)

                # Find the corresponding task ID in the lines list
                task_id = None
                for line in lines[1:]:
                    if full_filename in line:
                        task_id = line.strip().split(",")[0]
                        break

                if task_id:
                    # Check task status
                    task_status = ee.data.getTaskStatus(task_id)[0]

                if task_status["state"] != "COMPLETED":
                    # Task is not completed
                    all_completed = False
                    print(f"{full_filename} - {task_status['state']}")

            # Check overall completion status of files

            if all_completed:
                all_assets = all_assets + 1

                # Check overall completion status of all assets for date and get Product No_Data
                asset_size, product_missing_data, product_no_data = get_product_info(filename)
                if all_assets == asset_size:
                    print(" ... checking status of asset: "+filename)
                    print(" --> ",
                          group[0].split('_mosaic_')[1].split('T')[0], "all assets exported and READY ...")

                    for filename in group:

                        print(filename+" starting processing ... ")

                        # read metadata from json
                        with open(os.path.join(
                                config.PROCESSING_DIR, (filename+"_metadata.json")), 'r') as f:
                            metadata = json.load(f)

                        # Set the buffer based on orbit or use Switzerland wide buffer
                        if 'GEE_PROPERTIES' in metadata and 'SENSING_ORBIT_NUMBER' in metadata['GEE_PROPERTIES']:
                            config.BUFFER = os.path.join("assets", "ch_buffer_5000m_2056_" + str(
                                metadata['GEE_PROPERTIES']['SENSING_ORBIT_NUMBER']) + ".shp")
                        else:
                            config.BUFFER = os.path.join(
                                "assets", "ch_buffer_5000m.shp")

                        # merge files

                        file_merged = merge_files_with_gdal_warp(filename)

                        # check if there is a need to create thumbnail , if yes create it
                        thumbnail = main_thumbnails.create_thumbnail(
                            file_merged, metadata['SWISSTOPO']['PRODUCT'])

                        # upload file to FSDI STAC

                        main_publish_stac_fsdi.publish_to_stac(
                            file_merged, metadata['SWISSTOPO']['ITEM'], metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['GEOCATID'])

                        # Define  mean type
                        mean_type = extract_descriptor_mean(filename)

                        # Warnregions:
                        # swisseo-vhi warnregions: create

                        # Check if we deal with VHI Vegetation or Forest files
                        if check_substrings_presence(file_merged, metadata['SWISSTOPO']['PRODUCT'], ['vegetation-10m.tif', 'forest-10m.tif','vegetation-30m.tif', 'forest-30m.tif']) is True:
                            print("Extracting warnregions stats...")
                            warnregionfilename = metadata['SWISSTOPO']['PRODUCT']+"_"+metadata['SWISSTOPO']['ITEM'] + \
                                "_" + \
                                file_merged[file_merged.rfind(
                                    "_") + 1:file_merged.rfind("-")]+"-warnregions"

                            # Extracting warnregions

                            main_extract_warnregions.export(file_merged, config.WARNREGIONS, warnregionfilename,
                                                            metadata['SWISSTOPO']['DATEITEMGENERATION']+"T23:59:59Z", product_missing_data, product_no_data, mean_type)

                            # Pushing  CSV , GEOJSON and PARQUET
                            warnformats = [".csv", ".geojson", ".parquet"]  #
                            for format in warnformats:
                                main_publish_stac_fsdi.publish_to_stac(
                                    warnregionfilename+format, metadata['SWISSTOPO']['ITEM'], metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['GEOCATID'])
                                # Define the new metadata entry
                                new_entry_key = (file_merged[file_merged.rfind(
                                    "_") + 1:file_merged.rfind("-")] + "-warnregions" + format.replace(".", "-")).upper()
                                new_entry_value = {
                                    "PRODUCT": metadata['SWISSTOPO']['PRODUCT'],
                                    "ITEM": metadata['SWISSTOPO']['ITEM'],
                                    "ASSET": warnregionfilename + format,
                                    "SOURCE": file_merged,
                                    "format": format,
                                    "regionId": "RegionID",
                                    mean_type+"Mean": mean_type.upper()+" Mean Region",
                                    "availabilityPercentage": "percentage of available pixels with information within region"
                                }

                                # Update the metadata dictionary with the new entry
                                metadata[new_entry_key] = new_entry_value

                                # Write the updated metadata back to the JSON file
                                with open(os.path.join(
                                        config.PROCESSING_DIR, file_merged.replace(".tif", "_metadata.json")), 'w') as f:
                                    json.dump(metadata, f)

                        # Create a current version and upload file to FSDI STAC, only if the latest item on STAC is newer or of the same age
                        collection = metadata['SWISSTOPO']['PRODUCT']
                        result = extract_and_compare_datetime_from_url(config.STAC_FSDI_SCHEME+"://"+config.STAC_FSDI_HOSTNAME+config.STAC_FSDI_API +
                                                                       "collections/"+collection+"/items/"+collection.replace("ch.swisstopo.", ""), metadata['SWISSTOPO']['ITEM'])
                        if result == True:
                            print("Newest dataset detected: updating CURRENT")

                            file_merged_current = re.sub(
                                r'\d{4}-\d{2}-\d{2}T\d{6}', 'current', file_merged)
                            # Rename the file
                            os.rename(file_merged, file_merged_current)

                            # Publish  current dataset to stac
                            main_publish_stac_fsdi.publish_to_stac(
                                file_merged_current, metadata['SWISSTOPO']['ITEM'], metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['GEOCATID'], current=True)

                            # Publish  current thumbnail if a thumbnail is required
                            if thumbnail is not False:
                                main_publish_stac_fsdi.publish_to_stac(
                                    thumbnail, metadata['SWISSTOPO']['ITEM'], metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['GEOCATID'], current=True)

                            # Rename the file back
                            os.rename(file_merged_current, file_merged)

                            # Pushing Warnregions CSV , GEOJSON and PARQUET
                            if check_substrings_presence(file_merged, metadata['SWISSTOPO']['PRODUCT'], ['vegetation-10m.tif', 'forest-10m.tif','vegetation-30m.tif', 'forest-30m.tif']) is True:
                                # create filepath
                                warnregionfilename_current = re.sub(
                                    r'\d{4}-\d{2}-\d{2}T\d{6}', 'current', warnregionfilename)
                                for format in warnformats:

                                    # Rename the file
                                    os.rename(warnregionfilename+format,
                                              warnregionfilename_current+format)

                                    # Publish  current dataset to stac
                                    main_publish_stac_fsdi.publish_to_stac(
                                        warnregionfilename_current+format, metadata['SWISSTOPO']['ITEM'], metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['GEOCATID'], current=True)

                                    # Rename the file back
                                    os.rename(warnregionfilename_current +
                                              format, warnregionfilename+format)

                        # move file to INT STAC : in case reproejction is done here: move file_reprojected
                        move_files_with_rclone(
                            file_merged, os.path.join(S3_DESTINATION, metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['ITEM']))

                        # Pushing Warnregions CSV , GEOJSON and PARQUET
                        if check_substrings_presence(file_merged, metadata['SWISSTOPO']['PRODUCT'], ['vegetation-10m.tif', 'forest-10m.tif','vegetation-30m.tif', 'forest-30m.tif']) is True:
                            for format in warnformats:
                                move_files_with_rclone(
                                    warnregionfilename+format, os.path.join(S3_DESTINATION, metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['ITEM']))

                        # Upload and move thumbnail if a thumbnail is required
                        if thumbnail is not False:
                            main_publish_stac_fsdi.publish_to_stac(
                                thumbnail, metadata['SWISSTOPO']['ITEM'], metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['GEOCATID'])
                            move_files_with_rclone(
                                thumbnail, os.path.join(S3_DESTINATION, metadata['SWISSTOPO']['PRODUCT'], metadata['SWISSTOPO']['ITEM']))

                        # clean up GDrive and local drive, move JSON to STAC
                        # Re -Test if we are on a local machine or if we are on Github: Redo, since GDRIVE might have a timeout
                        determine_run_type()

                        # Authenticate with GDRIVE
                        initialize_drive()

                        # os.remove(file_merged
                        clean_up_gdrive(filename)

                        # Remove each filename from the original group and list
                        unique_filenames.remove(filename)

                else:
                    print(" ... checking status of asset: "+filename)

    # delete consolidated META file
    [os.remove(file) for file in glob.glob("*_metadata.json")]

    # Last step
    if run_type == 1:
        # Remove the key file so It wont be commited
        os.remove("keyfile.json")
        os.remove("rclone.conf")

    if config.GDRIVE_TYPE == "DRIVE":
        # empty temp files on GDrive
        file_list = drive.ListFile({'q': "trashed=true"}).GetList()
        for file in file_list:
            # Delete file on gdrive with muliple attempt
            delete_gdrive(file)
            # print('GDRIVE TRASH: Deleted file: %s' % file['title'])
    print("PUBLISH Process done.")
