import geopandas as gpd
import rasterio
from rasterio.mask import mask
import shapely
# from rasterio.plot import show
import json
import numpy as np
# import pandas as pd
"""
Extract raster statistics for each polygon in a shapefile, calculate the percentage of data availability, and export
the results to CSV, GeoJSON, and GeoParquet formats.

Dependencies:
- geopandas
- rasterio
- numpy

Usage:
Ensure all necessary dependencies are installed.
Update the file paths and parameters as needed.
Run the script.
"""

# Read the shapefile into a GeoDataFrame
gdf = gpd.read_file(
    r"C:\temp\temp\BAFU_Grossregionen_erweitert_mitDoubs_EPSG2056\BAFU_Grossregionen_erweitert_mitDoubs_EPSG2056.shp")

# Open the raster file from URL
raster_url = 'https://data.geo.admin.ch/ch.swisstopo.swisseo_vhi_v100/swisseo_vhi_v100/swisseo_vhi_v100_current_forest_bands-10m.tif'

# missing data values
missing_values = 110

# date
dateISO8601 = "2024-03-07T24:00:00Z"

# filename
filename = "ch.swisstopo.swisseo_vhi_v100_2024-03-07t240000_forest_region-39"


# ----------------------------------------
def export(raster_url, shape_file, filename, dateISO8601, missing_values):
    # Parameters
    regionnr = "REGION_NR"
    vhimean = "vhi_mean"
    availpercen = "availability_percentage"

    gdf = gpd.read_file(shape_file)

    # Open the raster file from URL using rasterio
    with rasterio.open(raster_url) as src:
        # Iterate over each polygon
        raster_values = []
        availability_percentages = []
        for idx, row in gdf.iterrows():

            # Extract the geometry of the polygon
            geom = row['geometry']
            region = row[regionnr]
            region_name = row['Name']
            try:
                # Use rasterio to mask the raster with the polygon
                out_image, out_transform = mask(src, [geom], crop=True)
                # Extract the raster values
                values = out_image[0].flatten()

                # Count the number of cells with the missing values
                missing_values_count = np.count_nonzero(
                    values == missing_values)

                # Remove NoData values (255) and missing data values (110)
                valid_values = values[(values != src.nodata)
                                      & (values != missing_values)]
                # Count the number of cells with the valid values
                # valid_values_count = np.count_nonzero(valid_values)
                valid_values_count = valid_values.size

                # Store the mean value (or any other statistic you're interested in)
                min_value = np.min(valid_values)
                max_value = np.max(valid_values)
                mean_value = np.mean(valid_values)

                # Round mean value to the nearest integer
                mean_value_rounded = round(mean_value)

                # Calculate percentage of availability
                total_cells = valid_values_count+missing_values_count
                availability_percentage = (
                    valid_values_count / total_cells) * 100

                # Round availability percentage to two decimal places
                availability_percentage_rounded = round(
                    availability_percentage, 1)
                # Append rounded mean value and availability percentage to lists
                raster_values.append(mean_value_rounded)
                availability_percentages.append(
                    availability_percentage_rounded)

                # Print statistics
                print(f"Region {region}: {region_name}")
                print(f"  Min value: {min_value}")
                print(f"  Max value: {max_value}")
                print(f"  Mean value: {mean_value_rounded}")
                print(
                    f"  Percentage of availability: {availability_percentage:.1f}%")
            except ValueError:
                # Handle empty intersection (assign missing_values)
                raster_values.append(missing_values)
                availability_percentages.append(missing_values)

    # Add raster values and availability percentages to the GeoDataFrame
    gdf[vhimean] = raster_values
    gdf[availpercen] = availability_percentages

    # Save selected columns of the GeoDataFrame to a CSV file
    gdf[[regionnr, vhimean, availpercen]].to_csv(
        filename + '.csv', index=False)

    # Remove the "Name" column from the GeoDataFrame
    gdf.drop(columns=['Name'], inplace=True)

    # Convert "REGION_NR" and "vhi_mean" columns to UInt8 datatype
    gdf[regionnr] = gdf[regionnr].astype(int)
    gdf[vhimean] = gdf[vhimean].astype(int)
    #print(gdf.dtypes)

    # Round the coordinates to 0 decimals resulting in approx 0.2m displacement of the vertexes
    gdf.geometry = shapely.wkt.loads(
        shapely.wkt.dumps(gdf.geometry, rounding_precision=0))

    # Export the converted GeoDataFrame to a geoparquet file
    gdf.to_parquet(filename+'.parquet', compression="gzip")

    # Convert the GeoDataFrame to WGS84 (EPSG:4326)
    gdf_wgs84 = gdf.to_crs(epsg=4326)

    # Round the coordinates to 5 decimals resulting in approx 0.2-0.5m differences
    gdf_wgs84.geometry = shapely.wkt.loads(
        shapely.wkt.dumps(gdf_wgs84.geometry, rounding_precision=5))

    # Construct the GeoJSON dictionary without features
    geojson_dict = {
        "type": "FeatureCollection",
        "global_date": dateISO8601,
        "crs": {"type": "name", "properties": {"name": "https://www.opengis.net/def/crs/OGC/1.3/CRS84"}},
        "features": gdf_wgs84.__geo_interface__["features"]
    }

    # Export the GeoJSON dictionary to a file
    with open(filename+'.geojson', 'w') as outfile:
        json.dump(geojson_dict, outfile)


