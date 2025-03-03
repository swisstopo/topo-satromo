import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from fsspec.implementations.http import HTTPFileSystem
from datetime import datetime
import os
import numpy as np

"""
CDI and VHI Maps Generator

This script generates annual maps for Vegetation Health Index (VHI) and Combined Drought Indicator (CDI) data.
The maps are created for each month and week within a specified date range and saved as PNG files.
Be aware of the shift we need since for CDI_1991-01-01_2022-12-31_RegionaleHochwasserregionen.csv Regions

Author: David Oesch
Date: 2025-02-25
License: MIT License

Usage:
    python util_cdi_vhi_maps.py

Dependencies:
    - pandas
    - geopandas
    - matplotlib
    - fsspec
    - os
    - numpy

Functions:
    - get_color_for_cdi(cdi: float) -> str
    - get_color_for_vhi(vhi: float, availability: float) -> str
    - process_cdi_data(date_data: pd.DataFrame, regions: gpd.GeoDataFrame) -> gpd.GeoDataFrame
    - get_vhi_from_geoparquet(date: datetime) -> gpd.GeoDataFrame
    - create_annual_maps(output_folder: str, regions_shapefile: str, cdi_csv_path: str, shift_region: int, start_year: int, end_year: int)
    - main()

Example:
    To run the script, simply execute:
    python util_cdi_vhi_maps.py
"""

# Constants remain the same
THRESHOLD_AVAILABILITY = 20
STAC_PATH = "https://sys-data.int.bgdi.ch/"
SURFACE_TYPE = "vegetation"

# Color mappings remain the same
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

CDI_COLOR_MAPPING = {
    1: '#6EAAC8',
    2: '#FFF5BA',
    3: '#F5CD84',
    4: '#CD853F',
    5: '#965123'
}

def get_color_for_cdi(cdi):
    """Determine color based on CDI value with NaN handling."""
    if pd.isna(cdi):
        return '#ffffff'  # White for missing values
    try:
        cdi_int = int(cdi)
        return CDI_COLOR_MAPPING.get(cdi_int, '#ffffff')
    except (ValueError, TypeError):
        return '#ffffff'  # White for invalid values

# VHI functions remain the same as they were working correctly
def get_color_for_vhi(vhi, availability):
    """Determine color based on VHI value and availability threshold."""
    if availability <= THRESHOLD_AVAILABILITY:
        return '#ffffff'

    for (lower, upper), color in VHI_COLOR_MAPPING:
        if lower <= vhi <= upper:
            return color
    return '#ffffff'

def process_cdi_data(date_data, regions):
    """
    Process CDI data for a specific date and merge with regions.

    Args:
        date_data (pd.DataFrame): CDI data for a specific date
        regions (gpd.GeoDataFrame): Regions base geometry

    Returns:
        gpd.GeoDataFrame: Processed and merged data with colors
    """
    # Create a fresh copy of the data
    processed_data = date_data.copy()

    # Create color column
    processed_data.loc[:, 'color'] = processed_data['CDI'].apply(get_color_for_cdi)

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

def get_vhi_from_geoparquet(date):
    """Fetch VHI data from geoparquet for a specific date."""
    date_str = date.strftime('%Y-%m-%d')
    url = f"{STAC_PATH}ch.swisstopo.swisseo_vhi_v100/{date_str}t235959/ch.swisstopo.swisseo_vhi_v100_{date_str}t235959_{SURFACE_TYPE}-warnregions.parquet"

    try:
        filesystem = HTTPFileSystem()
        gdf = gpd.read_parquet(url, filesystem=filesystem)

        processed_gdf = gdf.copy()
        processed_gdf = processed_gdf.rename(columns={
            'REGION_NR': 'region_id',
            'vhi_mean': 'vhi',
            'availability_percentage': 'availability'
        })

        processed_gdf['region_id'] = pd.to_numeric(processed_gdf['region_id'], errors='coerce')
        processed_gdf['vhi'] = pd.to_numeric(processed_gdf['vhi'], errors='coerce')
        processed_gdf['availability'] = pd.to_numeric(processed_gdf['availability'], errors='coerce')
        processed_gdf['color'] = processed_gdf.apply(
            lambda row: get_color_for_vhi(row['vhi'], row['availability']),
            axis=1
        )
        processed_gdf['date'] = pd.to_datetime(date)

        return processed_gdf

    except Exception as e:
        print(f"No VHI data for {date_str}: {str(e)}")
        return None

def create_annual_maps(output_folder, regions_shapefile, cdi_csv_path, shift_region,start_year=2018, end_year=2022):
    """Create annual maps for both VHI and CDI data."""
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)

    # Load base data
    print("Loading base data...")
    regions = gpd.read_file(regions_shapefile)

    # Load and preprocess CDI data
    print("Loading CDI data...")
    cdi_data = pd.read_csv(cdi_csv_path, sep=';',encoding="latin1)")

    # Convert date column to datetime
    cdi_data['Datum'] = pd.to_datetime(cdi_data['Datum'])

    # Convert numeric columns
    cdi_data['CDI'] = pd.to_numeric(cdi_data['CDI'], errors='coerce')
    cdi_data['Region_ID'] = pd.to_numeric(cdi_data['Region_ID']+shift_region, errors='coerce')

    # Filter CDI data for our date range
    cdi_data = cdi_data[
        (cdi_data['Datum'].dt.year >= start_year) &
        (cdi_data['Datum'].dt.year <= end_year)
    ].copy()  # Make a copy to avoid chained assignment warnings

    years = list(range(start_year, end_year + 1))

    # Create separate figures for VHI and CDI
    for data_type in [ "VHI",'CDI']:
        print(f"\nProcessing {data_type} maps...")
        fig, axes = plt.subplots(nrows=48, ncols=len(years), figsize=(5 * len(years), 100))
        axes = axes.flatten()

        for col, year in enumerate(years):
            print(f"Processing year {year}...")
            for month in range(1, 13):
                # Get dates for this month
                month_mask = (cdi_data['Datum'].dt.year == year) & (cdi_data['Datum'].dt.month == month)
                month_data = cdi_data[month_mask].copy()
                dates = sorted(month_data['Datum'].unique())

                for week in range(4):
                    ax_idx = col + len(years) * ((month - 1) * 4 + week)
                    ax = axes[ax_idx]

                    if week < len(dates):
                        date = dates[week]

                        if data_type == 'VHI':
                            # Process VHI data (unchanged)
                            date_data = get_vhi_from_geoparquet(date)
                            if date_data is not None:
                                regions_copy = regions.copy()
                                merged_data = regions_copy.merge(
                                    date_data[['region_id', 'color']],
                                    left_on='REGION_NR',
                                    right_on='region_id',
                                    how='left'
                                )
                                merged_data.loc[:, 'color'] = merged_data['color'].fillna('#ffffff')
                            else:
                                merged_data = regions.copy()
                                merged_data.loc[:, 'color'] = '#ffffff'
                        else:
                            # Process CDI data
                            date_mask = (month_data['Datum'] == date)
                            date_cdi = month_data[date_mask].copy()
                            merged_data = process_cdi_data(date_cdi, regions.copy())

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
                        ax.set_title(f"{year}-{month:02d} Week {week+1}", fontsize=6)

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
    cdi_csv_path = r"C:\temp\temp\CDI_1991-01-01_2022-12-31_RegionaleHochwasserregionen.csv"
    shift_region=30
    print("****************")
    print("adding to region: "+str(shift_region))
    print("****************")

    create_annual_maps(
        output_folder=output_folder,
        regions_shapefile=regions_shapefile,
        cdi_csv_path=cdi_csv_path,
        shift_region=shift_region,
        start_year=2018,
        end_year=2022
    )

if __name__ == "__main__":
    main()