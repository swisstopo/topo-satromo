import pystac_client
import rasterio
import geopandas as gpd
from pyproj import CRS, Transformer
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
import os
from datetime import datetime
from tqdm import tqdm
import time
import logging
from functools import wraps
import requests
from collections import defaultdict
"""
util_treenet_extract.py

This module provides utility functions for extracting and processing
tree network data from various geospatial sources. It includes functions
for constructing URLs, handling API retries, and other helper functions.
ons
 works with data  delivered by the Swiss Federal Office for the Environment (BAFU) as part of the TreeNet project.
 adds new columns to the resulting CSV file:
        - `mask_value`: The value from the mask data indicating the presence or absence of cloud / shadow, based on the MASK-10m bansd of SwissEO S2-SR.
        - `vhi_value`: The Vegetation Health Index value extracted from the VHI data from FOREST-10m band of SwissEO VHI.


"""

import pystac_client
import rasterio
import geopandas as gpd
from pyproj import CRS, Transformer
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
import os
from datetime import datetime
from tqdm import tqdm
import time
import logging
from functools import wraps
import requests
from collections import defaultdict

# Previous helper functions remain the same
def construct_url(datetime_str):
    base_url = "https://data.geo.admin.ch/ch.swisstopo.swisseo_vhi_v100/"
    timestamp = f"{datetime_str}t235959"
    file_name = f"ch.swisstopo.swisseo_vhi_v100_mosaic_{timestamp}_forest-10m.tif"
# Previous helper functions remain the same
def construct_url(datetime_str):
    base_url = "https://data.geo.admin.ch/ch.swisstopo.swisseo_vhi_v100/"
    timestamp = f"{datetime_str}t235959"
    file_name = f"ch.swisstopo.swisseo_vhi_v100_mosaic_{timestamp}_forest-10m.tif"
    full_url = f"{base_url}{timestamp}/{file_name}"
    return full_url

def retry_on_api_error(max_retries=3, delay=10):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except pystac_client.exceptions.APIError as e:
                    retries += 1
                    logging.warning(f"API Error (Attempt {retries}/{max_retries}): {str(e)}")
                    if retries == max_retries:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator

class VHIExtractor:
    # VHIExtractor class implementation remains the same
    def __init__(self, catalog):
        self.catalog = catalog
        self.mask_cache = {}
        self.vhi_cache = {}
        self.transformer = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_epsg(2056), always_xy=True)
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    # Previous methods remain the same...
    # (get_mask_url, get_vhi_url, process_coordinates, check_mask, check_vhi)
    @retry_on_api_error(max_retries=20, delay=10)
    def get_mask_url(self, datetime_str):
        if datetime_str not in self.mask_cache:
            try:
                search = self.catalog.search(
                    collections=["ch.swisstopo.swisseo_s2-sr_v100"],
                    datetime=datetime_str
                )
                items = list(search.items())
                if len(items) == 0:
                    self.logger.warning(f"No items found for date {datetime_str}")
                    self.mask_cache[datetime_str] = None
                else:
                    masks_10m_key = next((key for key in items[0].assets.keys() if key.endswith('_masks-10m.tif')), None)
                    if masks_10m_key is None:
                        self.logger.warning(f"No mask file found for date {datetime_str}")
                        self.mask_cache[datetime_str] = None
                    else:
                        self.mask_cache[datetime_str] = items[0].assets[masks_10m_key].href
            except Exception as e:
                self.logger.error(f"Error getting mask URL for {datetime_str}: {str(e)}")
                self.mask_cache[datetime_str] = None
        return self.mask_cache[datetime_str]

    def get_vhi_url(self, datetime_str):
        if datetime_str not in self.vhi_cache:
            try:
                url = construct_url(datetime_str)
                response = requests.head(url, timeout=10)
                self.vhi_cache[datetime_str] = url if response.status_code == 200 else None
                if response.status_code != 200:
                    self.logger.warning(f"VHI URL not available for {datetime_str}: Status {response.status_code}")
            except Exception as e:
                self.logger.error(f"Error checking VHI URL for {datetime_str}: {str(e)}")
                self.vhi_cache[datetime_str] = None
        return self.vhi_cache[datetime_str]

    def process_coordinates(self, lon, lat):
        try:
            return self.transformer.transform(lon, lat)
        except Exception as e:
            self.logger.error(f"Error transforming coordinates ({lon}, {lat}): {str(e)}")
            return None, None

    def check_mask(self, lon, lat, datetime_str, mask_url):
        if mask_url is None:
            return 120

        try:
            x, y = self.process_coordinates(lon, lat)
            if x is None or y is None:
                return 120

            with rasterio.open(mask_url) as src:
                if not (0 <= x < src.bounds.right and 0 <= y < src.bounds.top):
                    self.logger.warning(f"Coordinates ({x}, {y}) outside S2-SR MASK raster bounds")
                    return 120

                py, px = src.index(x, y)

                if (0 <= py < src.height and 0 <= px < src.width):
                    window = ((py, py+1), (px, px+1))
                    mask_data = src.read(2, window=window)
                    if mask_data.size > 0:
                        return mask_data[0, 0]
                    else:
                        self.logger.warning(f"Empty mask data for coordinates ({x}, {y})")
                        return 120
                else:
                    self.logger.warning(f"Invalid S2-SR MASK pixel coordinates: py={py}, px={px}")
                    return 120

        except Exception as e:
            self.logger.error(f"Error reading mask for coordinates ({lon}, {lat}): {str(e)}")
            return 120

    def check_vhi(self, lon, lat, datetime_str, vhi_url):
        if vhi_url is None:
            return 120

        try:
            x, y = self.process_coordinates(lon, lat)
            if x is None or y is None:
                return 120

            with rasterio.open(vhi_url) as src:
                if not (0 <= x < src.bounds.right and 0 <= y < src.bounds.top):
                    self.logger.warning(f"Coordinates ({x}, {y}) outside raster bounds")
                    return 120

                py, px = src.index(x, y)

                if (0 <= py < src.height and 0 <= px < src.width):
                    window = ((py, py+1), (px, px+1))
                    vhi_data = src.read(1, window=window)
                    if vhi_data.size > 0:
                        return vhi_data[0, 0]
                    else:
                        self.logger.warning(f"Empty VHI data for coordinates ({x}, {y})")
                        return 120
                else:
                    self.logger.warning(f"Invalid pixel coordinates: py={py}, px={px}")
                    return 120

        except Exception as e:
            self.logger.error(f"Error reading VHI for coordinates ({lon}, {lat}): {str(e)}")
            return 120

def process_csv(input_file, output_file, extractor):
    # Normalize paths
    input_file = os.path.normpath(input_file)
    output_file = os.path.normpath(output_file)

    # Create temp file path
    temp_file = output_file.replace('.csv', '_temp.csv')

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Initialize logging
    logging.info(f"Starting processing of {input_file}")

    try:
        # Read CSV
        df = pd.read_csv(input_file, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(input_file, encoding='latin-1', low_memory=False)

    # Sort by DOY
    df = df.sort_values('doy')

    # Create DATETIME column
    df['DATETIME'] = pd.to_datetime(df['year'].astype(str) + '-' + df['doy'].astype(str).str.zfill(3), format='%Y-%j')
    df['DATETIME'] = df['DATETIME'].dt.strftime('%Y-%m-%d')

    # Initialize new columns
    df['swissEOVHI'] = None
    df['swissEOMASK'] = None

    # Get unique dates
    unique_dates = df['DATETIME'].unique()
    total_rows = len(df)
    processed_rows = 0

    # Check if temp file exists and load progress
    if os.path.exists(temp_file):
        temp_df = pd.read_csv(temp_file)
        processed_dates = temp_df['DATETIME'].unique()
        df.loc[df['DATETIME'].isin(processed_dates), 'swissEOVHI'] = temp_df['swissEOVHI']
        df.loc[df['DATETIME'].isin(processed_dates), 'swissEOMASK'] = temp_df['swissEOMASK']
        unique_dates = [d for d in unique_dates if d not in processed_dates]
        processed_rows = len(temp_df)
        logging.info(f"Resuming from previous progress. {len(processed_dates)} days already processed.")

    with tqdm(total=total_rows - processed_rows, desc="Processing data") as pbar:
        for date in unique_dates:
            try:
                # Get URLs for this date once
                mask_url = extractor.get_mask_url(date)
                vhi_url = extractor.get_vhi_url(date)

                # Process all rows for this date
                date_mask = df['DATETIME'] == date
                date_df = df[date_mask]

                for idx, row in date_df.iterrows():
                    df.at[idx, 'swissEOMASK'] = extractor.check_mask(
                        row['tree_xcor'], row['tree_ycor'], date, mask_url)
                    df.at[idx, 'swissEOVHI'] = extractor.check_vhi(
                        row['tree_xcor'], row['tree_ycor'], date, vhi_url)
                    pbar.update(1)
                    processed_rows += 1

                # Save progress after each day
                df.to_csv(temp_file, index=False)
                logging.info(f"Progress saved after processing date {date}. {processed_rows}/{total_rows} rows processed.")

            except Exception as e:
                logging.error(f"Error processing date {date}: {str(e)}")
                continue

    # Final save to output file
    df.to_csv(output_file, index=False)

    # Clean up temp file
    if os.path.exists(temp_file):
        os.remove(temp_file)

    logging.info(f"Processing completed. Final output saved to {output_file}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('vhi_extraction.log'),
            logging.StreamHandler()
        ]
    )

    # Connect to the STAC API
    catalog = pystac_client.Client.open("https://data.geo.admin.ch/api/stac/v0.9/")
    catalog.add_conforms_to("COLLECTIONS")
    catalog.add_conforms_to("ITEM_SEARCH")

    # Create extractor instance
    extractor = VHIExtractor(catalog)
    # Process years 2017-2024
    for year in range(2018, 2024):
        input_file = fr'C:\temp\BAFU_TreeNet_Signals_2017_2024\TN_{year}.csv'
        output_file = fr'C:\temp\satromo-dev\output\TN_{year}_swisseo.csv'
        try:
            process_csv(input_file, output_file, extractor)
        except Exception as e:
            logging.error(f"Fatal error during processing: {str(e)}")

    # # Process specific year
    # year = "2022_test"
    # input_file = fr'C:\temp\BAFU_TreeNet_Signals_2017_2024\TN_{year}.csv'
    # output_file = fr'C:\temp\satromo-dev\output\TN_{year}_swisseo.csv'
    # process_csv(input_file, output_file, extractor)