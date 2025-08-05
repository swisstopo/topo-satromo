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
CMS_SECRETS = os.path.join("secrets", "INT_CMS.json")

# File and directory paths
GEE_RUNNING_TASKS = os.path.join("processing", "running_tasks.csv")
GEE_COMPLETED_TASKS = os.path.join("tools", "completed_tasks.csv")
EMPTY_ASSET_LIST = os.path.join("tools", "step0_empty_assets.csv")
PROCESSING_DIR = "processing"
LAST_PRODUCT_UPDATES = os.path.join("tools", "last_updates.csv")
# Set GDRIVE Type: GCS for Google Cloud Storage and DRIVE for Google Drive
GDRIVE_TYPE = "GCS"
# Set GCS Bucket name of Google Cloud Storage
GCLOUD_BUCKET = "satromo_export"
# Local Machine
GDRIVE_SOURCE_DEV = "geedrivetest:"
# under Windows, add \\ to escape the backslash like r'Y:\\'
# GDRIVE_MOUNT_DEV = r'Y:\\' #for DRIVE
GDRIVE_MOUNT_DEV = r'M:\\satromo_export'  # for GCS
# under Windows, add \\ to escape the backslash like r'X:\\'
S3_DESTINATION_DEV = r'X:\\'

# GITHUB
# if using GDRIVE
# GDRIVE_SOURCE_INT = "geedriveINT:"
# if using GCS
GDRIVE_SOURCE_INT = "gcsPROD:"
GDRIVE_MOUNT_INT = "localgdrive"
S3_DESTINATION_INT = os.path.join("s3INT:satromoint", "data")

# CMS to store tools files
CMS_BUCKET = "satromoint"

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
BUFFER = os.path.join("assets", "ch_buffer_5000m.shp")
OVERVIEW_LAKES = os.path.join("assets", "overview_lakes_2056.shp")
OVERVIEW_RIVERS = os.path.join("assets", "overview_rivers_2056.shp")
WARNREGIONS = os.path.join("assets", "warnregionen_vhi_2056.shp")


# Switzerland border with 10km buffer: [5.78, 45.70, 10.69, 47.89] , Schönbühl [ 7.471940, 47.011335, 7.497431, 47.027602] Martigny [ 7.075402, 46.107098, 7.100894, 46.123639]
# Defines the initial extent to search for image tiles This is not the final extent is defined by BUFFER
# TODO: check if needed in context with step0
ROI_RECTANGLE = [5.78, 45.70, 10.69, 47.89]
ROI_BORDER_BUFFER = 5000  # Buffer around Switzerland

# No data value TODO : needs to be defined per product
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
    "asset_size": 5,
    "spatial_scale_export_mask": 10,
    "product_name": "ch.swisstopo.swisseo_s2-sr_v100",
    "no_data": 9999,
    #"step0_collection": "projects/satromo-int/assets/COL_S2_SR_HARMONIZED_SWISS"
}

# VHI – Trockenstress
PRODUCT_VHI = {
    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "geocat_id": "bc4d0e6b-e92e-4f28-a7d2-f41bf61e98bc",
    "temporal_coverage": 7,  # Days
    "spatial_scale_export": 10,  # Meters
    "product_name": "ch.swisstopo.swisseo_vhi_v100",
    "no_data": 255,
    "missing_data": 110,
    "asset_size": 2,
    'NDVI_reference_data': 'projects/satromo-prod/assets/col/1991-2020_NDVI_SWISS',
    'LST_reference_data': 'projects/satromo-prod/assets/col/1991-2020_LST_SWISS',
    'LST_current_data': 'projects/satromo-prod/assets/col/LST_SWISS',
    "step1_collection": 'projects/satromo-int/assets/VHI_SWISS',
    "step0_collection": 'projects/satromo-prod/assets/col/S2_SR_HARMONIZED_SWISS'
}

# VHI Historic – Trockenstress
PRODUCT_VHI_HIST = {
    # TODO: check if needed in context with step0
    "image_collection": [{"LANDSAT/LT05/C02/T1_L2", "LANDSAT/LE07/C02/T1_L2", "LANDSAT/LC08/C02/T1_L2"}],
    "geocat_id": "bc4d0e6b-e92e-4f28-a7d2-f41bf61e98bc",
    "temporal_coverage": 1,  # Months
    "spatial_scale_export": 30,  # Meters
    "product_name": "ch.swisstopo.swisseo_vhi_v100",
    "no_data": 255,
    "missing_data": 110,
    "asset_size": 2,
    'NDVI_reference_data': 'projects/satromo-prod/assets/col/1991-2020_NDVI_SWISS_M',
    'LST_reference_data': 'projects/satromo-prod/assets/col/1991-2020_LST_SWISS_M',
    'LST_current_data': 'projects/satromo-prod/assets/col/LST_SWISS',
    "step1_collection": 'projects/satromo-prod/assets/col/VHI_HIST_SWISS',
    # "step0_collection": 'projects/satromo-prod/assets/col/LST_SWISS'
}

# NDVI z-score (vitality anomaly)
PRODUCT_NDVIz = {
    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "geocat_id": "07f332fb-f728-4120-b6f1-488631555296",
    "temporal_coverage": 2,  # Months
    "spatial_scale_export": 10,  # Meters
    "product_name": "ch.swisstopo.swisseo_ndvi_z_v100",
    "no_data": 32701,
    "missing_data": 32700,
    "asset_size": 1,
    'NDVI_reference_data': 'projects/satromo-prod/assets/col/1991-2020_NDVI_SWISS_MM',
    "step1_collection": 'projects/satromo-int/assets/COL_NDVIz_SWISS',
    "step0_collection": 'projects/satromo-prod/assets/col/S2_SR_HARMONIZED_SWISS'
}

# NDVI difference (vitality anomaly)
PRODUCT_NDVIdiff = {
    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S2_SR_HARMONIZED",
    "geocat_id": "b144c4ba-971b-4e17-b809-47ee38ecfa26",
    "temporal_coverage": 2,  # Months
    "spatial_scale_export": 10,  # Meters
    "product_name": "ch.swisstopo.swisseo_ndvi_diff_v100",
    "no_data": 32701,
    "missing_data": 32700,
    "asset_size": 1,
    "step1_collection": 'projects/satromo-int/assets/COL_NDVIdiff_SWISS',
    #"step0_collection": 'projects/satromo-prod/assets/col/S2_SR_HARMONIZED_SWISS'
}

# # MSG – MeteoSchweiz
# PRODUCT_MSG = {
#     #
#     # this is  placeholder, needed for the step0 function,
#     "image_collection": "METEOSCHWEIZ/MSG",
#     "temporal_coverage": 7,  # Days
#     "product_name": "ch.meteoschweiz.landoberflaechentemperatur",
#     "no_data": 0,
#     'step0_collection': 'projects/satromo-int/assets/LST_CLIMA_SWISS'
# }

# MSG – MeteoSchweiz: only used for repreocessing
PRODUCT_MSG_CLIMA = {
    #
    # this is  placeholder, needed for the step0 function,
    "image_collection": "METEOSCHWEIZ/MSG",
    "temporal_coverage": 7,  # Days
    "product_name": "ch.meteoschweiz.landoberflaechentemperatur",
    "no_data": 0,
    # 'step0_collection': 'projects/satromo-int/assets/LST_MAX_AS_SWISS'
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
    "no_data": 255,
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
    "no_data": 9999,
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
    "no_data": 255,
    # "step0_collection": "projects/geetest-386915/assets/col_s2_toa"
}

#  ch.swisstopo.swisseo_l57-sr
PRODUCT_L57_LEVEL_2 = {

    # TODO: check if needed in context with step0
    "image_collection": "LANDSAT/LT05/C02/T1_L2",
    "geocat_id": "tbd",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 30,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "product_name": "ch.swisstopo.swisseo_l57-sr_v100",
    "no_data": 9999,
    # "step0_collection": "projects/satromo-int/assets/COL_LANDSAT_SR_SWISS"
}

#  ch.swisstopo.swisseo_l57-toa
PRODUCT_L57_LEVEL_1 = {

    # TODO: check if needed in context with step0
    "image_collection": "LANDSAT/LT05/C02/T1_TOA",
    "geocat_id": "tbd",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 30,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "product_name": "ch.swisstopo.swisseo_l57-toa_v100",
    "no_data": 9999,
    # "step0_collection": "projects/satromo-int/assets/COL_LANDSAT_TOA_SWISS"
}

#  ch.swisstopo.swisseo_l57-sr
PRODUCT_L89_LEVEL_2 = {

    # TODO: check if needed in context with step0
    "image_collection": "LANDSAT/LC08/C02/T1_L2",
    "geocat_id": "tbd",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 30,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "product_name": "ch.swisstopo.swisseo_l89-sr_v100",
    "no_data": 9999,
    # "step0_collection": "projects/satromo-int/assets/COL_LANDSAT_SR_SWISS"
}

#  ch.swisstopo.swisseo_l89-toa
PRODUCT_L89_LEVEL_1 = {

    # TODO: check if needed in context with step0
    "image_collection": "LANDSAT/LC08/C02/T1_TOA",
    "geocat_id": "tbd",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 30,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "product_name": "ch.swisstopo.swisseo_l89-toa_v100",
    "no_data": 9999,
    # "step0_collection": "projects/satromo-int/assets/COL_LANDSAT_TOA_SWISS"
}

#  ch.swisstopo.swisseo_s3-toa
PRODUCT_S3_LEVEL_1 = {

    # TODO: check if needed in context with step0
    "image_collection": "COPERNICUS/S3/OLCI",
    "geocat_id": "tbd",
    "temporal_coverage": 1,  # Days
    "spatial_scale_export": 300,  # Meters # TODO: check if needed in context with step0
    # Meters # TODO: check if needed in context with step0
    "product_name": "ch.swisstopo.swisseo_s3-toa_v100",
    "no_data": 9999,
    # "step0_collection": "projects/satromo-int/assets/COL_S3_TOA_SWISS"
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
    'projects/satromo-prod/assets/col/S2_SR_HARMONIZED_SWISS': {
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
    },
    'projects/satromo-int/assets/COL_LANDSAT_SR_SWISS': {
        'step0_function': 'step0_processor_l89_sr.generate_l89_sr_mosaic_for_single_date'
        # cleaning_older_than: 2 # entry used to clean assets
    },
    'projects/satromo-int/assets/COL_LANDSAT_TOA_SWISS': {
        'step0_function': 'step0_processor_l89_toa.generate_l89_toa_mosaic_for_single_date'
        # cleaning_older_than: 2 # entry used to clean assets
    },
    'projects/satromo-int/assets/COL_S3_TOA_SWISS': {
        'step0_function': 'step0_processor_s3_toa.generate_s3_toa_mosaic_for_single_date'
        # cleaning_older_than: 2 # entry used to clean assets
    },
    'projects/satromo-int/assets/LST_MAX_AS_SWISS': {
        'step0_function': 'step0_processor_msg_lst.generate_msg_lst_mosaic_for_single_date'
        # cleaning_older_than: 2 # entry used to clean assets
    },
    'projects/satromo-int/assets/LST_MAX_AS_SWISS': {
        'step0_function': 'step0_processor_msg_lst_clima.generate_msg_lst_mosaic_for_single_date'
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

# GDRIVE_SOURCE_INT = "geedriveINT:"
# GDRIVE_MOUNT_INT = "localgdrive"
# STAC_DESTINATION_INT = "s3INT:satromoint"

# STAC FSDI
# ---------------
STAC_FSDI_SCHEME = 'https'
STAC_FSDI_HOSTNAME = 'sys-data.int.bgdi.ch'
STAC_FSDI_API = '/api/stac/v0.9/'