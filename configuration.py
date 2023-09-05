# -*- coding: utf-8 -*-
import os

# GitHub repository
GITHUB_OWNER = "swisstopo"
GITHUB_REPO = "topo-satromo"

# Google Drive secrets
GDRIVE_SECRETS = os.path.join("secrets", "geetest-credentials.secret")
RCLONE_SECRETS = os.path.join("secrets", "rclone.conf")

# File and directory paths
GEE_RUNNING_TASKS = os.path.join("processing", "running_tasks.csv")
GEE_COMPLETED_TASKS = os.path.join("tools", "completed_tasks.csv")
PROCESSING_DIR = "processing"
LAST_PRODUCT_UPDATES = os.path.join("tools", "last_updates.csv")
GDRIVE_MOUNT = "localgdrive"
# DEV
GDRIVE_SOURCE_DEV = "geedrivetest:"
S3_DESTINATION_DEV = os.path.join(
    "gees3test:cms.geo.admin.ch", "test", "topo", "umweltbeobachtung")
# INT
GDRIVE_SOURCE_INT = "geedriveINT:"
S3_DESTINATION_INT = os.path.join(
    "s3INT:satromoint", "data")


# General GEE parameters
SHARD_SIZE = 256

# Development environment parameters
RESULTS = os.path.join("results")  # Local path for results

# General product parameters
# Coordinate Reference System (EPSG:4326 for WGS84, EPSG:2056 for CH1903+, see epsg.io)
OUTPUT_CRS = "EPSG:2056"
# Desired buffer in m width around ROI, e.g., 25000, this defines the final extent
BUFFER = os.path.join("tools", "ch_buffer_5000m.shp")
# Switzerland border with 10km buffer: [5.78, 45.70, 10.69, 47.89] , Schönbühl [ 7.471940, 47.011335, 7.497431, 47.027602] Martigny [ 7.075402, 46.107098, 7.100894, 46.123639]
# is not the final extent is defined by buffer above
ROI_RECTANGLE = [7.075402, 46.107098, 7.100894, 46.123639]
ROI_BORDER_BUFFER = 5000  # Buffer around Switzerland
NODATA = 9999  # No data values

# NDVI product parameters
PRODUCT_NDVI_MAX = {
    "prefix": "Sentinel_NDVI-MAX_SR_CloudFree_crop",
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "temporal_coverage": "30",  # Days
    "spatial_scale_export": "10",  # Meters
    "band_names": [{'NIR': "B8", 'RED': "B4"}],
    "product_name": "NDVI-MAX"
}

PRODUCT_S2_LEVEL_2A = {
    "prefix": "S2_L2A_SR",
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "temporal_coverage": "1",  # Days
    "spatial_scale_export": "10",  # Meters
    "spatial_scale_export_qa60": "60",  # Meters
    "product_name": "S2_LEVEL_2A"
}
