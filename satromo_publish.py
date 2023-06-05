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


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2
        print("\nType 2 run PROCESSOR: We are on DEV")
    else:
        run_type = 1
        print("\nType 1 run PROCESSOR: We are on PROD")


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

    # Create the Google Drive client
    global drive
    drive = GoogleDrive(gauth)

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


def move_files_with_rclone(source, destination):
    """
    Move files using the rclone command.

    Parameters:
    source (str): Source path of the files to be moved.
    destination (str): Destination path to move the files to.

    Returns:
    None
    """
    # Run rclone command to move files
    # See hint https://forum.rclone.org/t/s3-rclone-v-1-52-0-or-after-permission-denied/21961/2
    command = ["rclone", "move", "--config", "rclone.conf", "--s3-no-check-bucket",
               source, destination]
    subprocess.run(command, check=True)

    print("SUCCESS: moved " + source + " to " + destination)


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


def write_file_meta(input_dict, output_file):
    """
    Read the existing CSV file, append the input dictionary, and export it as a new CSV file.

    Parameters:
    input_dict (dict): Dictionary to be appended to the CSV file.
    output_file (str): Path of the output CSV file.

    Returns:
    None
    """
    existing_data = OrderedDict()
    if os.path.isfile(output_file):
        with open(output_file, "r", encoding="utf-8", newline='') as f:
            reader = csv.reader(f)
            existing_data = OrderedDict(zip(next(reader), next(reader)))

    existing_data.update(input_dict)

    with open(output_file, "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter=",", quotechar='"',
                            lineterminator="\n")

        writer.writerow(list(existing_data.keys()))
        writer.writerow(list(existing_data.values()))


def extract_product_and_item(task_description):
    """
    Extract the product and item information from a task description.

    Parameters:
    task_description (str): Description of the task containing product and item information.

    Returns:
    tuple: A tuple containing the extracted product and item information.
    """
    product_start_index = task_description.index('P:') + 2
    product_end_index = task_description.index(' I:')
    product = task_description[product_start_index:product_end_index]

    item_start_index = task_description.index('I:') + 2
    item = task_description[item_start_index:]

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


if __name__ == "__main__":

    # Test if we are on Local DEV Run or if we are on PROD
    determine_run_type()

    # Authenticate with GEE and GDRIVE
    initialize_gee_and_drive()

    # Read the status file

    with open(config.GEE_RUNNING_TASKS, "r") as f:
        lines = f.readlines()

    # Process each line
    for line in lines[1:]:

        # Get the task ID
        task_id = line.strip().split(",")[0]

        # Get the corresponding second entry
        filename = line.strip().split(",")[1]

        # Check task status
        task_status = ee.data.getTaskStatus(task_id)[0]

        if task_status["state"] == "COMPLETED":
            #  Find the file in Google Drive by its name
            file_list = drive.ListFile(
                {"q": "title contains '"+filename+"'"}).GetList()

            # Check if the file is found
            if len(file_list) > 0:

                # Iterate through the files and delete them
                # Get the product and item
                product, item = extract_product_and_item(
                    task_status['description'])

                for file in file_list:

                    # Local / DEV
                    if run_type == 2:
                        # local machine run
                        # Download DATA

                        download_and_delete_file(file)

                    # Github /PROD
                    else:
                        # add move to s3 etc

                        move_files_with_rclone(
                            config.GDRIVE_SOURCE+file["title"], os.path.join(config.S3_DESTINATION, product))

                    # Add DATA GEE PROCESSING info to stats
                    write_file(task_status, config.GEE_COMPLETED_TASKS)

                    # Remove the line from the RUNNING tasks file
                    delete_line_in_file(config.GEE_RUNNING_TASKS, task_id)

                    # only after the first oof a set of data is done
                    if filename.endswith("quadrant1"):
                        # Add DATA GEE PROCESSING info to Metadata of item,

                        write_file_meta(task_status, os.path.join(
                            config.PROCESSING_DIR, item+".csv"))

                        # On Prod move data to S3 Destination
                        if run_type == 1:
                            move_files_with_rclone(os.path.join(
                                config.PROCESSING_DIR, item+".csv"), os.path.join(config.S3_DESTINATION, product))

                        # Update Status in RUNNING tasks file
                        replace_running_with_complete(
                            config.LAST_PRODUCT_UPDATES, product)

            else:
                print(filename+" not found in Google Drive.")
                delete_line_in_file(config.GEE_RUNNING_TASKS, task_id)
        elif task_status["state"] in ["FAILED", "CANCELLED"]:
            print("Export task failed or was cancelled.")
            task_run = False

    # Last step
    if run_type == 1:
        # Remove the key file so It wont be commited
        os.remove("keyfile.json")
        os.remove("rclone.conf")
    print("done!!!")
