import ee
import configuration as config
from datetime import datetime
from dateutil.relativedelta import relativedelta
from main_functions import main_utils
from step0_processors.step0_utils import write_asset_as_empty

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

    # Create list with indices of all used data
    NDVI_index_list = ndvi_col.aggregate_array('system:index')
    NDVI_index_list = NDVI_index_list.join(',')
    
    # Calculate data availability with explicit projection
    data_availability = (ndvi_col.reduce(ee.Reducer.count())
                        .rename('pixel_count')
                        .reproject(crs='EPSG:2056', scale=10))
    
    # Calculate median NDVI
    ndvi_median = ndvi_col.median().rename('median')
    # Combine median NDVI with pixel count
    ndvi_median.addBands(data_availability)
    
    return ndvi_median, NDVI_index_list

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

# This function creates a XX-month period datetime object
def create_time_period_datetime(date_string, temporal_coverage_months):
    """
    Creates a XX-month time period using datetime objects for robust date handling.
    No matter the date's day, it will always create a time window for the previous
    full XX months (e.g. if the date is 2024-08-15 and the temporal coverage 2, it 
    will create a time window from June 1st to August 1st).
    
    Args:
        date_string (str): Date in 'YYYY-MM-DD' format
        temporal_coverage_months (int): Number of months for the time window
    
    Returns:
        tuple: (start_date, end_date_excl, end_date_incl) as strings in 'YYYY-MM-DD' format
    """
    # Parse the input date
    year, month, day = map(int, date_string.split('-'))
    
    # Create a datetime object for the input date (using 1st day of the month)
    input_date = datetime(year, month, 1)
    
    # Calculate start date (go back by temporal_coverage_months)
    start_date = input_date - relativedelta(months=+temporal_coverage_months)
    
    # End date: 1st of input month - for filtering the first day outside the period is needed
    end_date_excl = input_date

    # End date: last of time period - for naming the product
    end_date_incl = input_date - relativedelta(days=+1)
    
    return start_date.strftime('%Y-%m-%d'), end_date_excl.strftime('%Y-%m-%d'), end_date_incl.strftime('%Y-%m-%d')

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
    start_date, end_date_filter, end_date_name = create_time_period_datetime(date_str, config.PRODUCT_NDVIz['temporal_coverage'])
    print(f"Processing period: {start_date} to {end_date_name} (inclusive)")  
    print(f"Filtering data until: {end_date_filter} (exclusive)")

    ##############################
    # Sentinel S2 SR Data
    S2_col = ee.ImageCollection(collection_ready) \
        .filterDate(start_date, end_date_filter) \
        .filterBounds(aoi) \
        .filter(ee.Filter.stringEndsWith('system:index', '10m'))

    S2_col_20m = ee.ImageCollection(collection_ready) \
        .filterDate(start_date, end_date_filter) \
        .filterBounds(aoi) \
        .filter(ee.Filter.stringEndsWith('system:index', '20m'))

    ##############################
    # TESTS

    # Get Sensor info -> which S2 is available
    sensor_stats = main_utils.get_collection_info(S2_col)

    # Get information about the available sensor data for the range
    # Get the number of images in the filtered collection
    image_count = S2_col.size().getInfo()

    if image_count == 0:
        write_asset_as_empty(
            config.PRODUCT_NDVIz['step1_collection'], date_str, 'No S2 SR data available')
        return

    # Check if all required data is available (processed or within empty asset list)
    all_available, missing_dates = main_utils.check_collection_data_availability(
    'S2_SR_HARMONIZED_SWISS', '2024-05-01', '2024-05-31', ['bands-10m', 'bands-20m'])

    if all_available:
        print(f"✅ All required data is available. Processing can proceed...")

        # # Test if NDVIz GEE Asset already exists? If not, it will be created.
        # NDVIz_col = ee.ImageCollection(config.PRODUCT_NDVIz['step1_collection']) \
        #     .filterMetadata('system:index', 'contains', date_str) \
        #     .filterBounds(aoi)
        # NDVIz_count = NDVIz_col.size().getInfo()
        # if NDVIz_count == 0:

        # Test if NDVIz is in empty asset list? If yes, then skip.
        if main_utils.is_date_in_empty_asset_list(config.PRODUCT_NDVIz['step1_collection'], date_str):
            return

            ###########################################
            # PROCESSING
            # Map the function over the S2_col collection
            S2_col = add_ndsi(S2_col, S2_col_20m)

            # Load/Calculate NDVI data
            month = int(date_str.split('-')[1])
            NDVIref = loadNdviRefData(month)
            NDVIj, NDVI_index_list = loadNdviCurrentData(S2_col) # bands: median, pixel_count

            # Calculate VCI
            if modZScore is True:
                zscore = calculate_mod_z_score(NDVIj, NDVIref)
                print('--- Modified z-score calculated ---')
            else:
                zscore = calculate_z_score(NDVIj, NDVIref)
                print('--- Z-score calculated ---')

            # converting the data type
            zscore = zscore.int16()
            data_availability = NDVIj.select('pixel_count').int8()

            # add no data value for when one of the datasets is unavailable
            zscore = zscore.unmask(config.PRODUCT_NDVIz['missing_data'])

            # Mask for forest
            zscore = zscore.updateMask(forest_mask.eq(1))
            data_availability = data_availability.updateMask(forest_mask.eq(1))

            # combine both outputs in one ee.Image
            NDVIz = zscore.addBands(data_availability)

            # Set data properties
            # Getting swisstopo Processor Version
            processor_version = main_utils.get_github_info()
            # Earth Engine version
            ee_version = ee.__version__

            # set properties to the product to be exported
            NDVIz = NDVIz.set({
                'scale': 100,
                'system:time_start': start_date.millis(),
                'system:time_end': end_date.millis(),
                'temporal_coverage': config.PRODUCT_NDVIz['temporal_coverage'],
                'missing_data': config.PRODUCT_NDVIz['missing_data'],
                'no_data': config.PRODUCT_NDVIz['no_data'],
                'SWISSTOPO_PROCESSOR': processor_version['GithubLink'],
                'SWISSTOPO_RELEASE_VERSION': processor_version['ReleaseVersion'],
                'collection': collection_ready,
                'S2-SR_index_list': NDVI_index_list,
                'NDVI_reference_data': config.PRODUCT_NDVIz['NDVI_reference_data'],
                'GEE_api_version': ee_version,
                'pixel_size_meter': 10,
            })

            ##############################
            # EXPORT

            # define the export aoi
            aoi_exp = aoi

            # SWITCH export - forest (Asset)
            if exportForestAsset is True:
                task_description = 'NDVIz_SWISS_' + date_str
                print('Launching NDVIz export for forests')
                # Export asset
                task = ee.batch.Export.image.toAsset(
                    image = NDVIz.clip(aoi_exp),
                    scale = 10,
                    description = task_description + '_FOREST_10m',
                    crs = 'EPSG:2056',
                    region = aoi_exp,
                    maxPixels = 1e10,
                    assetId = config.PRODUCT_NDVIz['step1_collection'] +
                        '/' + task_description + '_FOREST_10m',
                )
                task.start()
        
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
    
        else:
            print(f"❌ PROCESSING ABORTED: Missing data for the following dates:")
        for date in missing_dates:
            print(f"   - {date}")
        print(f"\nTotal missing dates: {len(missing_dates)}")
        print(f"Please ensure all required assets are available before running the processing.")

