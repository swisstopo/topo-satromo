import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from fsspec.implementations.http import HTTPFileSystem
import os
from datetime import datetime

THRESHOLD_AVAILABILITY = 20
STAC_PATH = "https://sys-data.int.bgdi.ch/"
SURFACE_TYPE = "forest"

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

def process_cdi_data(date_data, regions):
    """
    Process CDI data for a specific date and merge with regions.
    """
    processed_data = date_data.copy()
    processed_data.loc[:, 'color'] = processed_data['CDI'].apply(get_color_for_cdi)
    merged_data = regions.merge(
        processed_data[['Region_ID', 'color']],
        left_on='REGION_NR',
        right_on='Region_ID',
        how='left'
    )
    merged_data.loc[:, 'color'] = merged_data['color'].fillna('#ffffff')
    return merged_data

def create_august_cdi_maps(output_folder, regions_shapefile, cdi_csv_path, shift_region, start_year=1991, end_year=2018):
    """Create CDI maps for all August data between start_year and end_year."""
    os.makedirs(output_folder, exist_ok=True)
    print("Loading base data...")
    regions = gpd.read_file(regions_shapefile)
    print("Loading CDI data...")
    cdi_data = pd.read_csv(cdi_csv_path, sep=';', encoding="latin1")
    cdi_data['Datum'] = pd.to_datetime(cdi_data['Datum'])
    cdi_data['CDI'] = pd.to_numeric(cdi_data['CDI'], errors='coerce')
    cdi_data['Region_ID'] = pd.to_numeric(cdi_data['Region_ID'] + shift_region, errors='coerce')
    cdi_data = cdi_data[(cdi_data['Datum'].dt.month == 8) & (cdi_data['Datum'].dt.year >= start_year) & (cdi_data['Datum'].dt.year <= end_year)]
    years = sorted(cdi_data['Datum'].dt.year.unique())

    fig, axes = plt.subplots(nrows=len(years), ncols=4, figsize=(20, 5 * len(years)))
    axes = axes.flatten()

    for idx, year in enumerate(years):
        year_data = cdi_data[cdi_data['Datum'].dt.year == year]
        dates = sorted(year_data['Datum'].unique())

        for week in range(4):
            ax_idx = idx * 4 + week
            ax = axes[ax_idx]

            if week < len(dates):
                date = dates[week]
                date_data = year_data[year_data['Datum'] == date]
                merged_data = process_cdi_data(date_data, regions.copy())
                merged_data.plot(ax=ax, color=merged_data['color'], edgecolor='black', linewidth=0.5)
                ax.set_title(date.strftime('%Y-%m-%d'), fontsize=10)
            else:
                regions.plot(ax=ax, color='#ffffff', edgecolor='black', linewidth=0.5)
                ax.set_title(f"{year} Week {week+1}", fontsize=8)
            ax.axis('off')

    fig.suptitle(f"CDI Maps for August {start_year}-{end_year}", fontsize=16)
    output_path = os.path.join(output_folder, f"cdi_august_maps_{start_year}_{end_year}.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved CDI maps to {output_path}")

def get_color_for_vhi(vhi, availability):
    """Determine color based on VHI value and availability threshold."""
    if availability <= THRESHOLD_AVAILABILITY:
        return '#ffffff'
    for (lower, upper), color in VHI_COLOR_MAPPING:
        if lower <= vhi <= upper:
            return color
    return '#ffffff'

def get_vhi_from_geoparquet(date):
    """Fetch VHI data from geoparquet for a specific date."""
    date_str = date.strftime('%Y-%m-%d')
    url = f"{STAC_PATH}ch.swisstopo.swisseo_vhi_v100/{date_str}t235959/ch.swisstopo.swisseo_vhi_v100_{date_str}t235959_{SURFACE_TYPE}-warnregions.parquet"
    try:
        filesystem = HTTPFileSystem()
        gdf = gpd.read_parquet(url, filesystem=filesystem)
        gdf = gdf.rename(columns={
            'REGION_NR': 'region_id',
            'vhi_mean': 'vhi',
            'availability_percentage': 'availability'
        })
        gdf['color'] = gdf.apply(lambda row: get_color_for_vhi(row['vhi'], row['availability']), axis=1)
        gdf['date'] = date
        return gdf
    except Exception as e:
        print(f"No VHI data for {date_str}: {str(e)}")
        return None

def create_vhi_maps(output_folder, regions_shapefile, start_year=1991, end_year=2018):
    """Create VHI maps for August 1st of each year."""
    os.makedirs(output_folder, exist_ok=True)
    regions = gpd.read_file(regions_shapefile)
    for year in range(start_year, end_year + 1):
        date = datetime(year, 8, 1)
        print(f"Processing VHI map for {date.strftime('%Y-%m-%d')}...")
        vhi_data = get_vhi_from_geoparquet(date)
        if vhi_data is not None:
            merged_data = regions.merge(vhi_data[['region_id', 'color']], left_on='REGION_NR', right_on='region_id', how='left')
            merged_data['color'] = merged_data['color'].fillna('#ffffff')
            fig, ax = plt.subplots(figsize=(8, 10))
            merged_data.plot(ax=ax, color=merged_data['color'], edgecolor='black', linewidth=0.5)
            ax.set_title(f"VHI Map {year}-08-01", fontsize=15)
            ax.axis('off')
            output_path = os.path.join(output_folder, f"vhi_map_{SURFACE_TYPE }_{year}_08_01.png")
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved VHI map to {output_path}")

def main():
    output_folder = r"C:\temp\output_vhi_maps"
    regions_shapefile = r"C:\temp\satromo-dev\assets\warnregionen_vhi_2056.shp"
    create_vhi_maps(output_folder, regions_shapefile, start_year=1991, end_year=2018)
    cdi_csv_path = r"C:\temp\temp\CDI_1991-01-01_2022-12-31_RegionaleHochwasserregionen.csv"
    shift_region = 30

    print("****************")
    print("Adding to region: " + str(shift_region))
    print("****************")

    create_august_cdi_maps(
        output_folder=output_folder,
        regions_shapefile=regions_shapefile,
        cdi_csv_path=cdi_csv_path,
        shift_region=shift_region,
        start_year=1991,
        end_year=2018
    )

if __name__ == "__main__":
    main()
