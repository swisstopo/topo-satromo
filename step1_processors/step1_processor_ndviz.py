import ee
import datetime
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

# This function creates a two-month period datetime object
def create_two_month_period_datetime(date_string):
    """
    Creates a two-month time period using datetime objects for robust date handling.
    
    Args:
        date_string (str): Date in 'YYYY-MM' format (e.g., '2023-08')
    
    Returns:
        tuple: (start_date, end_date) as strings in 'YYYY-MM-DD' format
    """
    # Parse the input date
    year, month = map(int, date_string.split('-'))
    
    # Calculate start date (previous month, 1st day)
    if month == 1:
        start_date = datetime(year - 1, 12, 1)
    else:
        start_date = datetime(year, month - 1, 1)
    
    # Calculate end date (next month, 1st day)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# Function to calculate NDSI
def add_ndsi(image_10m, image_20m):
    # Select the green band (10m) and SWIR band (20m)
    green = image_10m.select('B3')  # Adjust according to your band's naming convention

    # Get the acquisition date of the current image
    acq_date = image_10m.get('system:time_start')
    # Filter S2_col_20m to find the corresponding SWIR band based on the acquisition date
    swir = image_20m.filter(ee.Filter.eq('system:time_start', acq_date)).first().select('B11')  # SWIR band

    # Calculate NDSI
    ndsi = green.subtract(swir).divide(green.add(swir)).rename('ndsi')

    # Add NDSI band to the image
    return image_10m.addBands(ndsi)

# this function processes the VHI data
def process_PRODUCT_NDVIz(roi, collection_ready, date_str):
    """
    Processes swissEO NDVI z-score data for Switzerland.

    Args:
        roi (ee.Geometry): Region of interest.
        collection_ready (str): Name of the image collection.
        date_str (str): Date (of the end month) in string format 'YYYY-MM'.

    Returns:
        None
    """

    ##############################
    # MASK
    # Mask for the forest
    forest_mask = ee.Image(
        'projects/satromo-prod/assets/res/ch_bafu_lebensraumkarte_mask_forest_epsg32632')

    ##############################
    # SWITCHES
    # The switches enable / disable the execution of individual steps in this script
    exportForestAsset = True    # options: True, False
    exportForestDrive = False    # options: True, False
    modZScore = True            # options: True, False - defines if the modiefied (or the "normal") z-score is calculated

    ##############################
    # SPACE
    aoi = roi

    ##############################
    # PRODUCT
    product_name = config.PRODUCT_NDVIz['product_name']
    print("********* processing {} *********".format(product_name))

    ##############################
    # TIME
    start_date, end_date = create_two_month_period_datetime(date_str)

    ##############################
    # Sentinel S2 SR Data
    S2_col = ee.ImageCollection(collection_ready) \
        .filterDate(start_date, end_date) \
        .filterBounds(aoi) \
        .filter(ee.Filter.stringEndsWith('system:index', '10m'))

    S2_col_20m = ee.ImageCollection(collection_ready) \
        .filterDate(start_date, end_date) \
        .filterBounds(aoi) \
        .filter(ee.Filter.stringEndsWith('system:index', '20m'))

    # Get information about the available sensor data for the range
    # Get the number of images in the filtered collection
    image_count = S2_col.size().getInfo()

    if image_count == 0:
        write_asset_as_empty(
            config.PRODUCT_NDVIz['step1_collection'], date_str, 'No S2 SR data available')
        return

    # Test if NDVIz GEE Asset already exists? If not, it will be created.
    NDVIz_col = ee.ImageCollection(config.PRODUCT_NDVIz['step1_collection']) \
        .filterMetadata('system:index', 'contains', date_str) \
        .filterBounds(aoi)
    NDVIz_count = NDVIz_col.size().getInfo()
    if NDVIz_count == 0:

            # TEST VHI empty asset? VHI in empty_asset list? then skip
            if main_utils.is_date_in_empty_asset_list(config.PRODUCT_VHI['step1_collection'], current_date_str):
                return

            # Get information about the available sensor data for the range
            # Get the number of images in the filtered collection
            image_count = S2_col.size().getInfo()

            if image_count == 0:
                write_asset_as_empty(
                    config.PRODUCT_VHI['step1_collection'], current_date_str, 'No S2 SR data available')
                return
    
    else:
        print(date_str +' is already in ' +
            config.PRODUCT_NDVIz['step1_collection'])

        # Load from GEE Asset
        NDVIz = ee.Image(NDVIz_col.filter(ee.Filter.stringContains('system:index', date_str)).first())

    if exportForestDrive is True:
        # Generate the filename
        filename = config.PRODUCT_NDVIz['product_name'] + \
            '_mosaic_' + date_str + '_forest-10m'
        main_utils.prepare_export(roi, date_str, filename, config.PRODUCT_NDVIz['product_name'],
                                config.PRODUCT_VHI['spatial_scale_export'], NDVIz,
                                sensor_stats, date_str)