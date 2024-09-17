"""
This script is designed to work with Google Earth Engine (GEE) and Google Drive. It authenticates GEE and Google Drive, lists assets from a GEE collection, extracts timestamps from filenames, identifies missing timestamps based on a specified date range, and optionally exports the missing timestamps to a CSV fil


Modules Used

    import ee: Google Earth Engine API for interacting with GEE resources.
    import os: Module for interacting with the operating system, particularly for directory and file operations.
    import re: Regular expressions module for string pattern matching and extraction.
    import datetime and import timedelta: Modules for handling dates and time arithmetic.
    import pandas as pd: Pandas library, commonly used for data manipulation (imported but not used in this script).
    import csv: Module for CSV file operations.
    from pydrive.auth import GoogleAuth: Google Drive authentication module.
    from oauth2client.service_account import ServiceAccountCredentials: OAuth2 credentials for authenticating service accounts.

Functions

    initialize_gee_and_drive(credentials_file)
        Initializes Google Earth Engine and Google Drive authentication using the provided service account credentials.
        Args:
            credentials_file (str): Path to the service account credentials JSON file.
        Returns:
            True if initialization is successful, False otherwise.
    list_assets(path)
        Lists assets in the specified GEE collection.
        Args:
            path (str): The path to the GEE collection.
        Returns:
            A list of assets in the collection.
    extract_timestamps(directory_path, start_date, end_date)
        Extracts timestamps from filenames in the specified directory that match a specific pattern and are within the given date range.
        Args:
            directory_path (str): The path to the directory containing the files.
            start_date (str): The start date in YYYY-MM-DD format.
            end_date (str): The end date in YYYY-MM-DD format.
        Returns:
            A list of timestamps extracted from the filenames.
    find_missing_assets(source_timestamps, asset_names)
        Identifies timestamps that are present in the source_timestamps but missing from the asset_names list.
        Args:
            source_timestamps (list): List of timestamps to check.
            asset_names (list): List of asset names containing dates.
        Returns:
            A sorted list of missing timestamps.
    doy_to_date(doy, year=2023)
        Converts a day-of-year (DOY) to a date for the specified year.
        Args:
            doy (int): The day of the year (1-365).
            year (int): The year for the conversion (default is 2023).
        Returns:
            A datetime object representing the date.
    export_to_csv(missing_numbers, filename)
        Exports a list of missing timestamps to a CSV file.
        Args:
            missing_numbers (list): List of missing timestamps.
            filename (str): The name of the CSV file to create.
        Returns:
            None.

Usage

    Initialization:
        The script starts by initializing GEE and Google Drive authentication using the provided credentials file.

    Asset Listing:
        It lists all assets in the specified GEE collection path.

    Timestamp Extraction:
        Timestamps are extracted from files in the specified directory that match the date range.

    Finding Missing Assets:
        The script checks if there are any timestamps missing between the source files and the GEE assets.

    Exporting Missing Assets:
        If any timestamps are missing, they are exported to a CSV file.

    Output:
        The script outputs a message indicating whether synchronization is "OK" or "NOT OK" based on the presence of missing assets.

Example:
    $ python util_checkassets.py

    Ensure to update the 'credentials_file' and "data_path" and 'collection_path' variables accordingly.
"""
import ee
import os
import re
from datetime import datetime
import pandas as pd
from pydrive.auth import GoogleAuth
from oauth2client.service_account import ServiceAccountCredentials
import csv
from datetime import datetime, timedelta


def initialize_gee_and_drive(credentials_file):
    """
    Initializes Google Earth Engine (GEE) and Google Drive authentication.

    Args:
        credentials_file (str): Path to the service account credentials JSON file.

    Returns:
        bool: True if initialization is successful, False otherwise.
    """
    # Set scopes for Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]

    try:
        # Initialize Google Earth Engine
        ee.Initialize()

        # Authenticate with Google Drive
        gauth = GoogleAuth()
        gauth.service_account_file = credentials_file
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )
        gauth.ServiceAuth()

        print("Google Earth Engine and Google Drive authentication successful.")
        return True

    except Exception as e:
        print(
            f"Failed to initialize Google Earth Engine and Google Drive: {str(e)}")
        return False


# Function to list assets in a collection
def list_assets(path):
    asset_list = ee.data.listAssets({'parent': path})
    return asset_list.get('assets', [])

# Function to extract numbers from asset names


def extract_timestamps(directory_path, start_date, end_date):

    # Initialize an empty list to store the timestamps
    timestamps = []

    # Loop through all files in the directory
    for file_name in os.listdir(directory_path):
        # Check if the file ends with '10m_dx.tif'
        if file_name.endswith('10m_dx.tif'):
            # Retrieve the date from the filename using regex
            match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{6})', file_name)
            if match:
                timestamp = match.group(0).upper()
                timestamps.append(timestamp)
    return timestamps

# Function to determine missing numbers between 1 and 365


def find_missing_assets(source_timestamps, asset_names):
    # Extract dates from asset names
    asset_dates = set()
    for asset in asset_names:
        match = re.search(
            r'S2-SR_mosaic_(\d{4}-\d{2}-\d{2}T\d{6})_registration-10m', asset)
        if match:
            asset_date = match.group(1).upper()
            asset_dates.add(asset_date)

    # Find missing timestamps
    missing_timestamps = [
        timestamp for timestamp in source_timestamps if timestamp not in asset_dates]

    return sorted(missing_timestamps)

# Function to convert day-of-year to date for the year 2023


def doy_to_date(doy, year=2023):
    return datetime(year, 1, 1) + timedelta(days=doy - 1)

# Function to export missing numbers and corresponding dates to CSV


def export_to_csv(missing_numbers, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Missing Date'])
        for number in missing_numbers:
            writer.writerow([number])


if __name__ == "__main__":
    # Path to the service account credentials JSON file
    credentials_file = r'C:\path\xxx'

    start_date = "2023-10-01"
    end_date = "2023-10-10"

    # Specify the originla data path
    data_path = r'\\path\2023-10'

    # Specify the collection path
    collection_path = 'projects/satromo-432405/assets/COL_S2_SR_DXDY'

    # Initialize Google Earth Engine and Google Drive authentication
    if initialize_gee_and_drive(credentials_file):
        asset_list = list_assets(collection_path)
        asset_names = [asset['name'] for asset in asset_list]
        source_timestamps = extract_timestamps(data_path, start_date, end_date)
        missing_assets = find_missing_assets(source_timestamps, asset_names)

        # Check if missing_assets is empty
        if not missing_assets:
            print("OK sync for "+start_date+" to "+end_date +
                  " for "+data_path+" and "+collection_path)
        else:
            print("NOT OK: Missing assets:", missing_assets)
            # Specify the output CSV file name
            csv_filename = 'missing_asset_numbers.csv'
            export_to_csv(missing_assets, csv_filename)

            print(f'Missing asset  exported to {csv_filename}')
