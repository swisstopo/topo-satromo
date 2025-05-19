import ee
import datetime
from datetime import timedelta
import configuration as config
from main_functions import main_utils
from step0_processors.step0_utils import write_asset_as_empty
from step0_processors.step0_processor_msg_lst import generate_msg_lst_mosaic_for_single_date

# Processing pipeline for forest vitality anomalies (NDVI z-score) over Switzerland

##############################
# INTRODUCTION
# This script provides a tool to process NDVI z-score data over Switzerland.
# It uses reference data for NDVI (derived from Landsat data for the climate reference period) 
# stored as SATROMO assets and the current NDVI data from Sentinel-2.

##############################
# CONTENT

# This script includes the following steps:
# 1. Loading the NDVI reference data data
# 2. Calculating the NDVI for a two-month period for the current time frame
# 3. Calculating z-scores for NDVI
# 4. Mask for forests
# 5. Exporting the resulting NDVI z-score data

###########################################
# FUNCTIONS