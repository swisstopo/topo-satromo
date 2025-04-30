#!/usr/bin/python3.10
"""
Script for harvesting .kml information for Sentinel-2 acquisition plans from from sentinel.esa.int.
Extracts user-defined Area of Interest (AOI) polygons from the acquisition plan, stores it as CSV
Inspired by https://github.com/hevgyrt/harvest_sentinel_acquisition_plans/

USAGE:
    python get_acquisition.py

Author: David Oesch
Date: 2024-09-26
"""

import datetime
import os
import glob
import urllib.request as ul
from datetime import timedelta
import pandas as pd  # type: ignore
from lxml import html

from util_extract_acquisition_plans_s2 import \
    extract_S2_entries  # in-house developed method


def merge_aoi_files(directory, output_file):
    """
    Merges all CSV files ending with '_AOI.csv' from the specified directory, filtering out entries
    with an Acquisition Date older than today.

    Args:
        directory (str): The directory where the _AOI.csv files are stored.
        output_file (str): The path to the output CSV file.

    Returns:
        bool: True if the merge was successful, False if no files were processed.
    """
    # List to store all data from CSVs
    merged_data = []
    # Get today's date
    today = datetime.datetime.now().date()

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith("_AOI.csv"):
            filepath = os.path.join(directory, filename)
            print(f"Processing {filename}...")

            # Read the CSV file into a DataFrame
            df = pd.read_csv(filepath)

            # Extract Acquisition Date from the ObservationTimeStart column
            df['Acquisition Date'] = pd.to_datetime(df['ObservationTimeStart']).dt.date
            # Filter out rows where Acquisition Date is older than today minus 2 days
            df = df[df['Acquisition Date'] >= today - timedelta(days=2)]

            # If there are valid rows after filtering
            if not df.empty:
                # Calculate Publish Date as Acquisition Date + 3 days
                df['Publish Date'] = df['Acquisition Date'] + timedelta(days=3)

                # Keep only necessary columns and rename them
                df = df[['Acquisition Date', 'Publish Date', 'OrbitRelative', 'Platform']]
                df.rename(columns={'OrbitRelative': 'Orbit'}, inplace=True)

                # Append the processed DataFrame to the list
                merged_data.append(df)

    # Concatenate all DataFrames if any valid data exists
    if merged_data:
        result_df = pd.concat(merged_data)

        # Sort the DataFrame by Acquisition Date
        result_df.sort_values(by='Acquisition Date', inplace=True)

        # Save the merged data to the output CSV file
        result_df.to_csv(output_file, index=False)

        print(f"Merged file saved as {output_file}.")
        return True
    else:
        print("No valid _AOI.csv files with future or today's dates found.")
        return False


def download_and_extract_kml(satellite, file_url, output_filename, output_path, extract_area=False):
    """
    Download and store .kml file, and extract user-defined AOI if specified.

    Args:
        satellite (str): Name of the satellite (e.g., 'Sentinel-2').
        file_url (str): URL to download the .kml file.
        output_filename (str): Name to store the .kml file as.
        output_path (str): Directory where the file will be saved.
        extract_area (bool): Whether to extract AOI data from the file.

    Returns:
        bool: True if extraction succeeds, False otherwise.
    """
    kml_file_path = os.path.join(output_path, output_filename + '.kml')

    # Download the .kml file
    ul.urlretrieve(file_url, filename=kml_file_path)
    print(f"Successfully downloaded {file_url}")

    if extract_area and satellite == "Sentinel-2":
        # Extract AOI from the .kml file using in-house method
        entries = extract_S2_entries(file_url.split('/')[-1][:3].upper(), kml_file_path, output_filename + '_AOI.csv', output_path, POLYGON_WKT)
        if not entries:
            print(f"Failed to extract AOI from {output_filename}")
            return False
        print(f"Successfully extracted AOI from {file_url}")
    return True

def parse_kml_elements(li_elements, url_kml_prefix):
    """
    Parses KML file URLs from a list of <li> elements and constructs a dictionary mapping filenames to their full URLs.

    Args:
        li_elements (list): A list of <li> HTML elements. Each <li> element may contain a child element with an 'href' attribute pointing to a KML file.
        url_kml_prefix (str): The base URL to be prepended to the 'href' attribute in order to construct the full URL for each KML file.

    Returns:
        dict: A dictionary where keys are KML filenames extracted from the 'href' attribute and values are their corresponding full URLs.

    """
    kml_dict = {}
    for li in li_elements:
        for c in li.getchildren():
            if 'href' in c.attrib:
                href = c.attrib['href']
                if href.startswith('/documents'):
                    if href.endswith('00'):
                        kml_dict[href.split('/')[-1]] = str(url_kml_prefix + href)
                    else:
                        for i in range(len(href.split('/'))):
                            if href.split('/')[-i].endswith('kml'):
                                kml_dict[href.split('/')[-i]] = str(url_kml_prefix + href)

    return kml_dict

def get_latest_kml(kml_dict):
    """
    Find the latest available .kml file based on date from the given dictionary.

    Args:
        kml_dict (dict): Dictionary of available .kml files with their URLs.

    Returns:
        str: Filename of the latest .kml file, or None if not found.
    """
    today = datetime.datetime.now()
    date_format = '%Y%m%dT%H%M%S'
    latest_key = None

    for key in kml_dict:
        file_dates = key.split('_')
        start_date = datetime.datetime.strptime(file_dates[-2], date_format)
        end_date = datetime.datetime.strptime(file_dates[-1].split('.')[0], date_format)

        if start_date < today < end_date:
            if latest_key is None or end_date > datetime.datetime.strptime(latest_key.split('_')[-1].split('.')[0], date_format):
                latest_key = key

    return latest_key

# URLs and paths
S2_URL = 'https://sentinel.esa.int/web/sentinel/missions/sentinel-2/acquisition-plans'
URL_KML_PREFIX = 'https://sentinel.esa.int'
STORAGE_PATH = os.getcwd() + '/'

# Polygon defining the Area of Interest (AOI)
POLYGON_WKT = "POLYGON((5.96 46.13,6.03 46.66,6.91 47.52,8.56 47.90,9.78 47.65,9.91 47.17,10.70 46.96,10.60 46.47,10.08 46.11,9.06 45.74,7.13 45.77,5.96 46.13))"# This is Switzerland


# Fetch and parse the Sentinel-2 acquisition plans page
s2_tree = html.parse(ul.urlopen(S2_URL))

liElementsS2A = []
liElementsS2B = []
liElementsS2C = []
for tree in [s2_tree]:
    bodyElement = tree.findall('./')[1]

    for div in bodyElement.find(".//div[@class='sentinel-2a']"):
        for li in div.findall('.//li'):
            liElementsS2A.append(li)

    for div in bodyElement.find(".//div[@class='sentinel-2b']"):
        for li in div.findall('.//li'):
            liElementsS2B.append(li)

    for div in bodyElement.find(".//div[@class='sentinel-2c']"):
        for li in div.findall('.//li'):
            liElementsS2C.append(li)

# Extract .kml file links for Sentinel-2A and Sentinel-2B and Sentinel-2C
kml_dict_s2a = parse_kml_elements(liElementsS2A, URL_KML_PREFIX)
kml_dict_s2b = parse_kml_elements(liElementsS2B, URL_KML_PREFIX)
kml_dict_s2c = parse_kml_elements(liElementsS2C, URL_KML_PREFIX)

# Find the latest .kml file for Sentinel-2A and Sentinel-2B and Sentinel-2C
s2a_key = get_latest_kml(kml_dict_s2a)
s2b_key = get_latest_kml(kml_dict_s2b)
s2c_key = get_latest_kml(kml_dict_s2c)

# Download and process the .kml files for Sentinel-2A and Sentinel-2B and Sentinel-2C
S2A_OK = download_and_extract_kml('Sentinel-2', kml_dict_s2a[s2a_key], 'S2A_acquisition_plan', STORAGE_PATH, extract_area=True) if s2a_key else False
S2B_OK = download_and_extract_kml('Sentinel-2', kml_dict_s2b[s2b_key], 'S2B_acquisition_plan', STORAGE_PATH, extract_area=True) if s2b_key else False
S2C_OK = download_and_extract_kml('Sentinel-2', kml_dict_s2c[s2c_key], 'S2C_acquisition_plan', STORAGE_PATH, extract_area=True) if s2c_key else False

# Merge the three files, add publish date and remove dates older than today
MERGE_OK = merge_aoi_files(STORAGE_PATH,os.path.join(STORAGE_PATH,'tools','acquisitionplan.csv'))

# Report success or failure
if not (S2A_OK and S2B_OK and S2C_OK and MERGE_OK):
    print(f"")
    print(f"**********************")
    print(f"Sentinel-2A: {'Success' if S2A_OK else 'no planned aquisitions'}")
    print(f"Sentinel-2B: {'Success' if S2B_OK else 'no planned aquisitions'}")
    print(f"Sentinel-2C: {'Success' if S2C_OK else 'no planned aquisitions'}")
    print(f"Merge: {'Success' if MERGE_OK else 'Failed'}")
else:
    print("\nAll Sentinel-2 downloads and operations completed successfully.")
# Clean pattern
# Pattern for files to delete
patterns = ["S2A_acquisition_plan*", "S2B_acquisition_plan*", "S2C_acquisition_plan*"]

# Iterate through the patterns
for pattern in patterns:
    # Find all files matching the pattern
    matching_files = glob.glob(os.path.join(STORAGE_PATH, pattern))

    # Delete each matching file
    for file in matching_files:
        try:
            os.remove(file)
            #print(f"Deleted: {file}")
        except OSError as e:
            print(f"Error deleting {file}: {e}")