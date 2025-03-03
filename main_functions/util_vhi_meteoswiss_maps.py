"""
MeteoSwiss VHI Maps Generator

This script generates annual maps for Vegetation Health Index (VHI) based on meteoswiss.vhi.weekly.csv
For traceability, I add columns “startday” and “endday”. These indicate exactly what days are used to compute the weekly average. This is because while the weeks follow calendar weeks, the processing around 31 Dec / 1. Jan has two twists:
1)	If the first week would contain days from the previous year, those days are ignored
2)	The last week also includes any remaining days until Dec. 31


“nanfrac” is fraction of missing values (0-1). Pixels that are always NA (e.g. lakes) do not contribute to nanfrac.
The maps are created for each month and week within a specified date range and saved as PNG files.

Author: David Oesch
Date: 2025-02-20
License: MIT License

Usage:
    python util_vhi_meteoswiss_maps.py

Dependencies:
    - pandas
    - geopandas
    - matplotlib
    - os

Functions:
    - get_color_for_vhi(vhi: float) -> str
    - process_date_data(date_data: pd.DataFrame, regions: gpd.GeoDataFrame, data_type: str) -> gpd.GeoDataFrame
    - create_annual_maps(output_folder: str, regions_shapefile: str, vhi_csv_path: str, start_year: int, end_year: int)
    - main()

Example:
    To run the script, simply execute:
    python util_cdi_vhi_maps_v2.py
"""


import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import os


# Constants
THRESHOLD_AVAILABILITY = 20  # Not used since  nanfrac is always 0

# Color mappings
VHI_COLOR_MAPPING = [
    ((0, 10), '#b56a29'),
    ((10, 20), '#ce8540'),
    ((20, 30), '#f5cd85'),
    ((30, 40), '#fff5ba'),
    ((40, 50), '#cbffca'),
    ((50, 60), '#52bd9f'),
    ((60, 100), '#0470b0'),
    ((110, 110), '#b3b6b7')
]


def get_color_for_vhi(vhi):
    """
    Determine color based on VHI value
    Args:
        vhi (float): VHI value
        nanfrac (float): Fraction of missing values (0-100)
    """
    # Check if data quality is sufficient (nanfrac should be low enough)
    # if nanfrac >= (100 - THRESHOLD_AVAILABILITY):
    #     return '#ffffff'

    if pd.isna(vhi):
        return '#ffffff'

    for (lower, upper), color in VHI_COLOR_MAPPING:
        if lower <= vhi <= upper:
            return color
    return '#ffffff'



def process_date_data(date_data, regions):
    """
    Process either VHI

    Args:
        date_data (pd.DataFrame): Data for a specific date
        regions (gpd.GeoDataFrame): Regions base geometry

    """
    processed_data = date_data.copy()


    processed_data.loc[:, 'color'] = processed_data.apply(
        lambda row: get_color_for_vhi(row['VHI']),
        axis=1)

    # Merge with regions
    merged_data = regions.merge(
        processed_data[['Region_ID', 'color']],
        left_on='REGION_NR',
        right_on='Region_ID',
        how='left'
    )

    # Fill any missing colors with white
    merged_data.loc[:, 'color'] = merged_data['color'].fillna('#ffffff')


    return merged_data

def create_annual_maps(output_folder, regions_shapefile, vhi_csv_path,  start_year=2018, end_year=2022):
    """Create annual maps for VHI."""
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)

    # Load base data
    print("\nLoading base data...")
    regions = gpd.read_file(regions_shapefile)
    regions['REGION_NR'] = pd.to_numeric(regions['REGION_NR'], errors='coerce')

    # Load and preprocess VHI data
    print("\nLoading VHI data...")
    vhi_data = pd.read_csv(vhi_csv_path, sep=';')
    vhi_data['Datum'] = pd.to_datetime(vhi_data['Datum'])
    vhi_data['Region_ID'] = pd.to_numeric(vhi_data['Region_ID'], errors='coerce')
    vhi_data['VHI'] = pd.to_numeric(vhi_data['VHI'], errors='coerce')
    vhi_data['nanfrac'] = pd.to_numeric(vhi_data['nanfrac'], errors='coerce')

    # Filter data for our date range
    vhi_data = vhi_data[
        (vhi_data['Datum'].dt.year >= start_year) &
        (vhi_data['Datum'].dt.year <= end_year)
    ].copy()


    years = list(range(start_year, end_year + 1))

    # Create  figures
    for data_type in ['VHI']:
        print(f"\nProcessing {data_type} maps...")
        fig, axes = plt.subplots(nrows=48, ncols=len(years), figsize=(5 * len(years), 100))
        axes = axes.flatten()

        data_source = vhi_data

        for col, year in enumerate(years):
            print(f"Processing year {year}...")
            for month in range(1, 13):
                # Get dates for this month
                month_mask = (data_source['Datum'].dt.year == year) & (data_source['Datum'].dt.month == month)
                month_data = data_source[month_mask].copy()
                dates = sorted(month_data['Datum'].unique())

                for week in range(4):
                    ax_idx = col + len(years) * ((month - 1) * 4 + week)
                    ax = axes[ax_idx]

                    if week < len(dates):
                        date = dates[week]
                        date_mask = (month_data['Datum'] == date)
                        date_data = month_data[date_mask].copy()

                        merged_data = process_date_data(date_data, regions.copy())

                        # Plot the data
                        merged_data.plot(
                            ax=ax,
                            color=merged_data['color'],
                            edgecolor='black',
                            linewidth=0.5
                        )
                        ax.set_title(date.strftime('%Y-%m-%d'), fontsize=15)
                    else:
                        # Empty plot for missing weeks
                        regions.plot(ax=ax, color='#ffffff', edgecolor='black', linewidth=0.5)
                        ax.set_title(f"{year}-{month:02d} Week {week+1}", fontsize=15)

                    ax.axis('off')

        # Add overall title and save
        fig.suptitle(f"{data_type} Maps {start_year}-{end_year}", fontsize=16)
        output_path = os.path.join(output_folder, f"{data_type.lower()}_annual_maps_{start_year}_{end_year}.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved {data_type} maps to {output_path}")

def main():
    output_folder = r"C:\temp\output_maps"
    regions_shapefile = r"C:\temp\satromo-dev\assets\warnregionen_vhi_2056.shp"
    vhi_csv_path = r"C:\temp\temp\meteoswiss.vhi.weekly.csv"  # Update this path


    create_annual_maps(
        output_folder=output_folder,
        regions_shapefile=regions_shapefile,
        vhi_csv_path=vhi_csv_path,
        start_year=2018,
        end_year=2022
    )

if __name__ == "__main__":
    main()