# -*- coding: utf-8 -*-
import os

# General variables
# --------------------------

# GitHub repository
GITHUB_OWNER = "swisstopo"
GITHUB_REPO = "topo-satromo"

# Secrets
GDRIVE_SECRETS = os.path.join("secrets", "geetest-credentials.secret")
RCLONE_SECRETS = os.path.join("secrets", "rclone.conf")
FSDI_SECRETS = os.path.join("secrets", "stac_fsdi.json")

# File and directory paths
GEE_RUNNING_TASKS = os.path.join("processing", "running_tasks.csv")
GEE_COMPLETED_TASKS = os.path.join("tools", "completed_tasks.csv")
EMPTY_ASSET_LIST = os.path.join("tools", "step0_empty_assets.csv")
PROCESSING_DIR = "processing"
LAST_PRODUCT_UPDATES = os.path.join("tools", "last_updates.csv")
# DEV
GDRIVE_SOURCE_DEV = "geedrivetest:"
# under Windows, add \\ to escape the backslash like r'Y:\\'
GDRIVE_MOUNT_DEV = r'Y:\\'
# under Windows, add \\ to escape the backslash like r'X:\\'
S3_DESTINATION_DEV = r'X:\\'
# INT
GDRIVE_SOURCE_INT = "geedriveINT:"
GDRIVE_MOUNT_INT = "localgdrive"
S3_DESTINATION_INT = os.path.join("s3INT:satromoint", "data")


# General GEE parameters

# TODO: check if needed
SHARD_SIZE = 256

# Development environment parameters
RESULTS = os.path.join("results")  # Local path for results

# General product parameters
# ---------------------------

# Coordinate Reference System (EPSG:4326 for WGS84, EPSG:2056 for CH1903+, see epsg.io)
OUTPUT_CRS = "EPSG:2056"

# Desired buffer in m width around ROI, e.g., 25000, this defines the final extent
# TODO: check if needed in context with step0
BUFFER = os.path.join("tools", "ch_buffer_5000m.shp")
OVERVIEW_LAKES = os.path.join("assets", "overview_lakes_2056.shp")
OVERVIEW_RIVERS = os.path.join("assets", "overview_rivers_2056.shp")

# Switzerland border with 10km buffer: [5.78, 45.70, 10.69, 47.89] , Schönbühl [ 7.471940, 47.011335, 7.497431, 47.027602] Martigny [ 7.075402, 46.107098, 7.100894, 46.123639]
# Defines the initial extent to search for image tiles This is not the final extent is defined by BUFFER
# TODO: check if needed in context with step0
ROI_RECTANGLE = [5.78, 45.70, 10.69, 47.89]
ROI_BORDER_BUFFER = 5000  # Buffer around Switzerland

# No data value
NODATA = 9999


## PRODUCTS, INDICES and custom COLLECTIONS ###
# ---------------------------
# See https://github.com/swisstopo/topo-satromo/tree/main?tab=readme-ov-file#configuration-in-_configpy for details
# TL;DR : First define in A) PRODUCTS, INDICES: for step0 (cloud, shadow, co-register, mosaic) the TOA SR data  custom  "step0_collection" to be generated / used
# then

# A) PRODUCTS, INDICES
# ********************

#  ch.swisstopo.swisseo_s2-sr
PRODUCT_S2_LEVEL_2A = {
    # "prefix": "S2_L2A_SR",
    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "geocat_id": "7ae5cd5b-e872-4719-92c0-dc2f86c4d471",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 10,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "spatial_scale_export_mask": 10,
    "product_name": "ch.swisstopo.swisseo_s2-sr_v100",
    # "step0_collection": "projects/satromo-int/assets/COL_S2_SR_HARMONIZED_SWISS"
}

# V1 – Trockenstress
PRODUCT_V1 = {
    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "geocat_id": "bc4d0e6b-e92e-4f28-a7d2-f41bf61e98bc",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 10,  # Meters
    "band_names": [{'NIR': "B8", 'RED': "B4"}],
    "product_name": "ch.swisstopo.swisseo_vhi_v100",
    # "step0_collection": "projects/satromo-int/assets/COL_S2_SR_HARMONIZED_SWISS"
}

# TEST datasets
# TEST NDVI
PRODUCT_NDVI_MAX = {
    # "prefix": "Sentinel_NDVI-MAX_SR_CloudFree_crop",
    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "temporal_coverage": 3,  # Days
    "spatial_scale_export": 10,  # Meters
    "band_names": [{'NIR': "B8", 'RED': "B4"}],
    "product_name": "NDVI-MAX",
    # "step0_collection": "projects/satromo-int/assets/COL_S2_SR_HARMONIZED_SWISS"
}

# TEST S2 -TOA: TEST
PRODUCT_S2_LEVEL_1C = {
    # "prefix": "S2_L1C_TOA",
    "image_collection": "COPERNICUS/S2_HARMONIZED",
    "temporal_coverage": 30,  # Days
    "spatial_scale_export": 10,  # Meters
    "spatial_scale_export_mask": 60,
    "product_name": "S2_LEVEL_1C",
    # "step0_collection": "projects/geetest-386915/assets/col_s2_toa"
}

# TEST S2 -TOA- NDVI p
PRODUCT_NDVI_MAX_TOA = {
    # "prefix": "Sentinel_NDVI-MAX_TOA_CloudFree_crop",
    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S2_HARMONIZED",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 1,  # Meters
    "band_names": [{'NIR': "B8", 'RED': "B4"}],
    "product_name": "NDVI-MAX_TOA",
    # "step0_collection": "projects/geetest-386915/assets/col_s2_toa"
}

#  ch.swisstopo.swisseo_l57-sr
PRODUCT_L57_LEVEL_2 = {
    # "prefix": "S2_L2A_SR",
    # TODO: check if needed in context with step0
    "image_collection": "LANDSAT/LT05/C02/T1_L2",
    "geocat_id": "tbd",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 30,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "product_name": "ch.swisstopo.swisseo_l57-sr_v100",
    # "step0_collection": "projects/satromo-int/assets/COL_LANDSAT_SR_SWISS"
}

#  ch.swisstopo.swisseo_l57-sr
PRODUCT_L57_LEVEL_1 = {
    # "prefix": "S2_L2A_SR",
    # TODO: check if needed in context with step0
    "image_collection": "LANDSAT/LT05/C02/T1_TOA",
    "geocat_id": "tbd",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 30,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "product_name": "ch.swisstopo.swisseo_l57-toa_v100",
    "step0_collection": "projects/satromo-int/assets/COL_LANDSAT_TOA_SWISS"
}

# B custom COLLECTION
# ********************
# Contains dictionary used to manage custom collection (asset) in GEE,
# for example to clear old images not used anymore.

# Configure the dict containing
# -  the name of the custom collection (asset) in GEE, (eg: projects/satromo-int/assets/COL_S2_SR_HARMONIZED_SWISS )
# -  the function to process the raw data for teh collection (eg:step0_processor_s2_sr.generate_s2_sr_mosaic_for_single_date )

# Make sure that the products above use the corresponding custom collection (assets)

step0 = {
    # 'projects/satromo-exolabs/assets/col_s2_toa': {
    #    'step0_function': 'step0_processor_s2_toa.generate_s2_toa_mosaic_for_single_date',
    #    # cleaning_older_than: 2 # entry used to clean assets
    # },
    'projects/satromo-int/assets/COL_S2_SR_HARMONIZED_SWISS': {
        'step0_function': 'step0_processor_s2_sr.generate_s2_sr_mosaic_for_single_date'
        # cleaning_older_than: 2 # entry used to clean assets
    },
    'projects/satromo-int/assets/COL_LANDSAT_SR_SWISS': {
        'step0_function': 'step0_processor_l57_sr.generate_l57_sr_mosaic_for_single_date'
        # cleaning_older_than: 2 # entry used to clean assets
    },
    'projects/satromo-int/assets/COL_LANDSAT_TOA_SWISS': {
        'step0_function': 'step0_processor_l57_toa.generate_l57_toa_mosaic_for_single_date'
        # cleaning_older_than: 2 # entry used to clean assets
    }
}


# STAC Integration
# ---------------

STAC_FOLDER = "stac-collection"
# Use the AWS Cloudfront distribution instead of  "https://satromoint.s3.eu-central-2.amazonaws.com/"
STAC_BASE_URL = "https://d29cp2gnktw6by.cloudfront.net/"
STAC_PRODUCT = ["S2_LEVEL_2A", "NDVI-MAX"]

# under Windows, add \\ to escape the backslash like r'X:\\'
STAC_DESTINATION_DEV = r'X:\\'

GDRIVE_SOURCE_INT = "geedriveINT:"
GDRIVE_MOUNT_INT = "localgdrive"
STAC_DESTINATION_INT = "s3INT:satromoint"

# STAC FSDI
# ---------------
STAC_FSDI_SCHEME = 'https'
STAC_FSDI_HOSTNAME = 'sys-data.int.bgdi.ch'
STAC_FSDI_API = '/api/stac/v0.9/'
