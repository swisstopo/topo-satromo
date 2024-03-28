from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
import ee
import json
# Assuming configuration.py is in ../configuration directory

import configuration as config


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
    with open(config.GDRIVE_SECRETS, "r") as f:
        data = json.load(f)

    # Authenticate with Google using the service account key file
    gauth = GoogleAuth()
    gauth.service_account_file = config.GDRIVE_SECRETS
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

# Function to filter tasks by their status


def list_incomplete_tasks():
    tasks = ee.batch.Task.list()
    incomplete_tasks = [task for task in tasks if task.status()[
        'state'] != 'COMPLETED']
    return incomplete_tasks


# Authenticate with GEE and GDRIVE
initialize_gee_and_drive()

# Function to filter tasks by their status
def list_incomplete_tasks():
    tasks = ee.batch.Task.list()
    incomplete_tasks = [task for task in tasks if task.status()['state'] != 'COMPLETED']
    return incomplete_tasks

# List all incomplete tasks
incomplete_tasks = list_incomplete_tasks()

# Print the list of incomplete tasks
for task in incomplete_tasks:
    print("Task ID:", task.id)
    print("Status:", task.status()['state'])
    print("Description:", task.config['description'])
    print("---------------------------------------------")


# def list_tasks_by_status(status):
#     tasks = ee.batch.Task.list()
#     filtered_tasks = [task for task in tasks if task.status()[
#         'state'] == status]
#     return filtered_tasks

# # List all completed tasks
# completed_tasks = list_tasks_by_status('COMPLETED')

# # Print the list of completed tasks
# for task in completed_tasks:
#     print("Task ID:", task.id)
#     print("Status:", task.status()['state'])
#     print("Description:", task.config['description'])
#     print("---------------------------------------------")
