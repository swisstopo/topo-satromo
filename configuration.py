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
GDRIVE_SOURCE = "geedrivetest:"
GDRIVE_MOUNT = "localgdrive"
S3_DESTINATION = os.path.join(
    "gees3test:cms.geo.admin.ch", "test", "topo", "umweltbeobachtung")

# General GEE parameters
SHARD_SIZE = 256

# Development environment parameters
RESULTS = os.path.join("results")  # Local path for results

# General product parameters
# Coordinate Reference System (EPSG:4326 for WGS84, EPSG:2056 for CH1903+, see epsg.io)
OUTPUT_CRS = "EPSG:2056"
BUFFER = os.path.join("tools","ch_buffer_5000m.shp")  # Desired buffer in m width around ROI, e.g., 25000

# Switzerland border with 10km buffer: [5.78, 45.70, 10.69, 47.89] , Schönbühl [ 7.471940, 47.011335, 7.497431, 47.027602] Martigny [ 7.075402, 46.107098, 7.100894, 46.123639]
ROI_RECTANGLE = [ 7.075402, 46.107098, 7.100894, 46.123639]
NODATA = -9999  # No data values

# NDVI product parameters
PRODUCT_NDVI_MAX = {
    "prefix": "Sentinel_NDVI-MAX_SR_CloudFree_crop",
    "image_collection": "COPERNICUS/S2_HARMONIZED",
    "temporal_coverage": "30",  # Days
    "spatial_scale_export": "10",  # Meters
    "band_names": [{'NIR': "B8", 'RED': "B4"}],
    "product_name": "NDVI-MAX"
}
