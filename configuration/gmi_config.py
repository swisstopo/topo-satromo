# -*- coding: utf-8 -*-
import os

# GitHub repository
GITHUB_OWNER = "GilliM"
GITHUB_REPO = "topo-satromo"

# Google Drive secrets
GDRIVE_SECRETS = os.path.join("secrets", "geetest-credentials.secret")
RCLONE_SECRETS = os.path.join("secrets", "rclone.conf")

# File and directory paths
GEE_RUNNING_TASKS = os.path.join("processing", "running_tasks.csv")
GEE_COMPLETED_TASKS = os.path.join("tools", "completed_tasks.csv")
EMPTY_ASSET_LIST = os.path.join("tools", "step0_empty_assets.csv")
PROCESSING_DIR = "processing"
LAST_PRODUCT_UPDATES = os.path.join("tools", "last_updates.csv")
# DEV
GDRIVE_SOURCE= "satromo_exolabs:"
# under Windows, add \\ to escape the backslash like r'Y:\\'
GDRIVE_MOUNT= "localgdrive"
# under Windows, add \\ to escape the backslash like r'X:\\'
S3_DESTINATION= 'exolabs_s3:/satromo'
# INT
GDRIVE_SOURCE_INT = "geedriveINT:"
GDRIVE_MOUNT_INT = "localgdrive"
S3_DESTINATION_INT = os.path.join("s3INT:satromoint", "data")


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
ROI_RECTANGLE = [5.78, 45.70, 10.69, 47.89]
ROI_BORDER_BUFFER = 5000  # Buffer around Switzerland
NODATA = 9999  # No data values


## PRODUCTS and INDICES ###

# NDVI product parameters
PRODUCT_NDVI_MAX = {
    "prefix": "Sentinel_NDVI-MAX_SR_CloudFree_crop",
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "temporal_coverage": 30,  # Days
    "spatial_scale_export": 10,  # Meters
    "band_names": [{'NIR': "B8", 'RED': "B4"}],
    "product_name": "NDVI-MAX",
    # "step0_collection": "projects/satromo-exolabs/assets/col2"
}

PRODUCT_S2_LEVEL_2A = {
    "prefix": "S2_L2A_SR",
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 10,  # Meters
    "spatial_scale_export_qa60": 60,  # Meters
    "product_name": "S2_LEVEL_2A",
    # "step0_collection": "projects/satromo-exolabs/assets/col2"
}

PRODUCT_S2_LEVEL_1C = {
    "prefix": "S2_L1C_TOA",
    "image_collection": "COPERNICUS/S2_HARMONIZED",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 10,  # Meters
    "spatial_scale_export_mask60": 60,
    "product_name": "S2_LEVEL_1C",
    "step0_collection": "projects/satromo-exolabs/assets/col1"
}

PRODUCT_NDVI_MAX_TOA = {
    "prefix": "Sentinel_NDVI-MAX_TOA_CloudFree_crop",
    "image_collection": "COPERNICUS/S2_HARMONIZED",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 10,  # Meters
    "band_names": [{'NIR': "B8", 'RED': "B4"}],
    "product_name": "NDVI-MAX_TOA",
    "step0_collection": "projects/satromo-exolabs/assets/col1"
}

# dictionary used to manage custom asset,
# for example to clear old images not used anymore.
step0 = {
    'projects/satromo-exolabs/assets/col1': {
        # cleaning_older_than: 2 # entry used to clean assets
    }
}


## STAC ###

STAC_FOLDER = "stac-collection"
STAC_BASE_URL = "https://d29cp2gnktw6by.cloudfront.net/" #Use the AWS Cloudfront distribution instead of  "https://satromoint.s3.eu-central-2.amazonaws.com/"
STAC_PRODUCT = ["S2_LEVEL_2A", "NDVI-MAX"]
# under Windows, add \\ to escape the backslash like r'X:\\'
STAC_DESTINATION_DEV = r'X:\\'
# INT
GDRIVE_SOURCE_INT = "geedriveINT:"
GDRIVE_MOUNT_INT = "localgdrive"
STAC_DESTINATION_INT = "s3INT:satromoint"
