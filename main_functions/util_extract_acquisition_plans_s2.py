#!/usr/bin/python3.10
"""
Script for extracting polygons within a defined AOI from Sentinel-2 .kml files.

USAGE:
    python extract_acquisition_plans_s2.py

This script extracts information from Sentinel-2 acquisition plans based on
a given Area of Interest (AOI), defined as a polygon in WKT format. It outputs
the extracted data as a CSV file.
Inspired by https://github.com/hevgyrt/harvest_sentinel_acquisition_plans/

Author: David Oesch
Date: 2024-09-26
"""

from lxml import etree as ET  # Using lxml for better parsing capabilities
import os
import csv
from shapely.geometry import Polygon
from shapely.wkt import loads  # For loading WKT into a Shapely geometry

def extract_S2_entries(platform, infile, outfile, outpath, polygon_wkt):
    """
    Extracts Sentinel-2 entries from a .kml file that intersect with a defined AOI.

    Args:
        platform (str): Name of the satellite platform (e.g., 'Sentinel-2A').
        infile (str): Input .kml file path.
        outfile (str): Name of the output CSV file.
        outpath (str): Directory where the output CSV will be saved.
        polygon_wkt (str): AOI polygon in WKT format.

    Returns:
        bool: True if extraction succeeds, False otherwise.
    """

    # Create the AOI geometry from the WKT string
    aoi_polygon = loads(polygon_wkt)

    # Parse the KML file
    tree = ET.parse(infile)
    root = tree.getroot()

    # Define KML namespace
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}

    # Prepare output list for extracted entries
    output_entries = []

    # Iterate over all Placemark elements in the KML file
    for placemark in root.findall('.//kml:Placemark', ns):
        # Filter based on 'Timeliness' being 'NOMINAL'
        timeliness = placemark.find('.//kml:Data[@name="Timeliness"]/kml:value', ns)
        if timeliness is not None and timeliness.text == 'NOMINAL':
            # Extract coordinates for the Polygon within the Placemark
            coordinates_str = placemark.find('.//kml:Polygon//kml:coordinates', ns)
            if coordinates_str is not None:
                # Create a Polygon from the coordinates
                coordinates = [
                    tuple(map(float, coord.split(',')))[:2]  # Convert coordinates to (longitude, latitude)
                    for coord in coordinates_str.text.strip().split()
                ]
                placemark_polygon = Polygon(coordinates)

                # Check if the Placemark's polygon intersects with the AOI
                if aoi_polygon.intersects(placemark_polygon):
                    # Extract relevant ExtendedData fields
                    observation_time_start = placemark.find('.//kml:Data[@name="ObservationTimeStart"]/kml:value', ns)
                    orbit = placemark.find('.//kml:Data[@name="OrbitRelative"]/kml:value', ns)

                    # Add the extracted data to the output list
                    if observation_time_start is not None and orbit is not None:
                        output_entries.append({
                            'ObservationTimeStart': observation_time_start.text,
                            'OrbitRelative': orbit.text,
                            'Platform': platform
                        })

    # Save results to a CSV file
    try:
        with open(os.path.join(outpath, outfile), 'w', newline='') as f:
            # Define CSV headers
            fieldnames = ['ObservationTimeStart', 'OrbitRelative', 'Platform']

            # Create a DictWriter object and write the header
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            # Write each entry (row) in the output_entries list
            for entry in output_entries:
                writer.writerow(entry)

        print(f"Extraction completed. {len(output_entries)} entries written to {outfile}.")
        return True

    except Exception as e:
        print(f"Could not write {outfile}: {e}")
        return False

def main():
    """
    Main function to run the extraction script.
    """

    # Parameters for the extraction
    platform = 'Sentinel-2A'  # Example platform
    infile = 'S2A_acquisition_plan.kml'  # Input .kml file
    outfile = 'S2A_extracted_entries.csv'  # Output CSV file name
    outpath = os.getcwd()  # Output directory
    polygon_wkt = "POLYGON((5.96 46.13,6.03 46.66,6.91 47.52,8.56 47.90,9.78 47.65,9.91 47.17,10.70 46.96,10.60 46.47,10.08 46.11,9.06 45.74,7.13 45.77,5.96 46.13))"  # Example AOI polygon

    # Perform the extraction
    success = extract_S2_entries(platform, infile, outfile, outpath, polygon_wkt)

    if success:
        print("Script completed successfully.")
    else:
        print("Script encountered an error.")

if __name__ == '__main__':
    main()
