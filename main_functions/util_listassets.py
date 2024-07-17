"""
Script for initializing Google Earth Engine (GEE) and Google Drive authentication,
listing assets in a GEE collection, extracting numbers from asset names, finding
missing numbers, converting day-of-year to date, and exporting missing numbers
and corresponding dates to a CSV file.

Modules:
    import ee: Google Earth Engine API.
    import pandas as pd: Pandas library for data manipulation (not used in this script but commonly useful).
    import csv: Module for CSV file operations.
    import datetime: Module for date and time manipulation.
    import timedelta: Module for date and time arithmetic.
    import GoogleAuth from pydrive.auth: Google Drive authentication.
    import ServiceAccountCredentials from oauth2client.service_account: OAuth2 credentials for service accounts.

Functions:
    initialize_gee_and_drive(credentials_file):
        Initializes GEE and Google Drive authentication.
    list_assets(path):
        Lists assets in the specified GEE collection.
    extract_numbers(asset_names):
        Extracts numbers from asset names.
    find_missing_numbers(numbers):
        Determines missing numbers between 1 and 365.
    doy_to_date(doy, year=2023):
        Converts day-of-year to a date for the specified year.
    export_to_csv(missing_numbers, filename):
        Exports missing numbers and corresponding dates to a CSV file.

Usage:
    This script should be run as the main module. It will:
    1. Initialize GEE and Google Drive authentication.
    2. List assets in the specified collection.
    3. Extract numbers from asset names.
    4. Determine missing numbers between 1 and 365.
    5. Export missing numbers and corresponding dates to a CSV file.

Example:
    $ python util_listassets.py

    Ensure to update the 'credentials_file' and 'collection_path' variables accordingly.
"""
import ee
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


def extract_numbers(asset_names):
    numbers = []
    for name in asset_names:
        # Extract the number assuming the asset name contains digits
        parts = name.split('/')
        asset_name = parts[-1]
        num = ''.join(filter(str.isdigit, asset_name))
        if num:
            numbers.append(int(num))
    return numbers

# Function to determine missing numbers between 1 and 365


def find_missing_numbers(numbers):
    all_numbers = set(range(1, 366))
    present_numbers = set(numbers)
    missing_numbers = all_numbers - present_numbers
    return sorted(missing_numbers)

# Function to convert day-of-year to date for the year 2023


def doy_to_date(doy, year=2023):
    return datetime(year, 1, 1) + timedelta(days=doy - 1)

# Function to export missing numbers and corresponding dates to CSV


def export_to_csv(missing_numbers, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Missing Number', 'Date'])
        for number in missing_numbers:
            date = doy_to_date(number).strftime('%Y-%m-%d')
            writer.writerow([number, date])


if __name__ == "__main__":
    # Path to the service account credentials JSON file
    credentials_file = r'C:\temp\topo-satromo\secrets\xxx'

    # Specify the collection path
    collection_path = 'projects/geetest-386915/assets/terrain_shadow/'

    # Initialize Google Earth Engine and Google Drive authentication
    if initialize_gee_and_drive(credentials_file):
        asset_list = list_assets(collection_path)
        asset_names = [asset['name'] for asset in asset_list]
        numbers = extract_numbers(asset_names)
        missing_numbers = find_missing_numbers(numbers)

        # Specify the output CSV file name
        csv_filename = 'missing_asset_numbers.csv'
        export_to_csv(missing_numbers, csv_filename)
        breakpoint()
        print(f'Missing asset numbers exported to {csv_filename}')
