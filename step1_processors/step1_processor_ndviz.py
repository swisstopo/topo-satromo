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

# This function loads the reference NDVI data (statistical value derived per two-month window from 1991-2020)
def loadNdviRefData(month):
    """
    Loads the reference NDVI data for a two month window.

    Args:
        month (int): Latter month, e.g. 8 -> July & August

    Returns:
        ee.Image: Reference NDVI image adjusted for offset and scale.
    """
    m2 = ee.String(ee.Number(month).format('%02d')).getInfo()  # 1 -> 01
    asset_name = config.PRODUCT_VHI['NDVI_reference_data'] + \
        '/NDVI_Stats_MM' + m2
    NDVIref = ee.Image(asset_name)
    # back to float
    NDVIref = NDVIref.float()
    # Get offset and scale values
    offset = ee.Number(NDVIref.get('offset'))
    scale = ee.Number(NDVIref.get('scale'))
    # Create an image with a constant value equal to the offset
    offsetImage = ee.Image.constant(offset)
    # Subtract the offset, then divide by the scale
    NDVIref = NDVIref.subtract(offsetImage).divide(scale)
    return NDVIref

# This function loads the current NDVI data
def loadNdviCurrentData(image):
    """
    Loads the current NDVI data from Sentinel-2 imagery.

    Args:
        image (ee.ImageCollection): Sentinel-2 image collection.

    Returns:
        ee.Image: Combined image with median NDVI and pixel count bands
    """
    # Apply the cloud and terrain shadow mask within the S2 image collection
    def applyMasks(image):
        image = image.updateMask(image.select('terrainShadowMask').lt(65))
        image = image.updateMask(image.select('cloudAndCloudShadowMask').eq(0))
        image = image.updateMask(image.select('ndsi').lt(0.43))
        return image
    S2_col_masked = image.map(applyMasks)

    # Calculate NDVI for each image
    def calculate_ndvi(image):
        ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi')
        return ndvi
    
    ndvi_col = S2_col_masked.map(calculate_ndvi)
    
    # Calculate data availability with explicit projection
    data_availability = (ndvi_col.reduce(ee.Reducer.count())
                        .rename('pixel_count')
                        .reproject(crs='EPSG:2056', scale=10))
    
    # Calculate median NDVI
    ndvi_median = ndvi_col.median().rename('median')
    
    # Combine median NDVI with pixel count
    return ee.Image.cat(ndvi_median, data_availability)

# This function calculates a normal z-score
def calculate_z_score(current_vi, ref_vi):
    """
    Calculate "normal" z-score.
    
    Args:
        current_vi (ee.Image): Current vegetation index image with 'median' band
        ref_vi (ee.Image): Reference vegetation index image with 'median' and 'std' bands
    
    Returns:
        ee.Image: Z-score image with metadata
    """
    # Z = (score - mean) / standard deviation
    z_method = '(currentMedian-refMedian)/(refStD)'
    
    zscore = (current_vi.select('median')
              .subtract(ref_vi.select('median'))
              .divide(ref_vi.select('std'))
              .rename('zscore'))
    
    zscore = zscore.multiply(100)
    zscore = zscore.set('zscore_method', z_method)
    
    return zscore

# This function calculates a modified z-score
def calculate_mod_z_score(current_vi, ref_vi):
    """
    Calculate modified z-score using waldmonitoring.ch method.
    
    Args:
        current_vi (ee.Image): Current vegetation index image with 'median' band
        ref_vi (ee.Image): Reference vegetation index image with 'median' and 'medianAD' bands
    
    Returns:
        ee.Image: Modified z-score image with metadata
    """
    # mod z-score ----- waldmonitoring.ch 
    # (https://github.com/HAFL-WWI/Digital-Forest-Monitoring/blob/main/methods/use-case3/1_ndvi_anomaly_gee_script.js#L156)
    z_method = '(currentMedian-refMedian)*constant/(MedianAD)'
    constant = 0.6745
    
    zscore = (current_vi.select('median')
              .subtract(ref_vi.select('median'))
              .multiply(constant)
              .divide(ref_vi.select('medianAD'))
              .rename('zscore'))
    
    zscore = zscore.multiply(100)  # scaling factor
    zscore = zscore.set({
        'zscore_method': z_method,
        'constant': constant
    })
    
    return zscore