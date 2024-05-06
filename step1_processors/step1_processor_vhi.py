import ee
import datetime
import configuration as config
from main_functions import main_utils

# Processing pipeline for daily vegetation health index (VHI) mosaics over Switzerland

##############################
# INTRODUCTION
# This script provides a tool to process vegetation health index (VHI) data over Switzerland.
# It uses reference data for NDVI and LST stored as SATROMO assets and the current NDVI and
# LST data to calculate VCI and TCI and combine them to the VHI.

##############################
# CONTENT
# The switches enable / disable the execution of individual steps in this script

# This script includes the following steps:
# 1. Loading the NDVI and LST data
# 2. Calculating the VCI from a specific date and the NDVI reference
# 3. Calculating the TCI from a specific date and the LST reference
# 4. Combining the VCI and TCI to generate the VHI
# 5. Mask for forest or all vegetation
# 6. Exporting the resulting VHI

###########################################
# FUNCTIONS

# This function loads the reference NDVI data (statistical value derived per DOY from 1991-2020)


def loadNdviRefData(doy):
    """
    Loads the reference NDVI data for a specific day of year (DOY).

    Args:
        doy (int): Day of year.

    Returns:
        ee.Image: Reference NDVI image adjusted for offset and scale.
    """
    doy3 = ee.String(ee.Number(doy).format('%03d')).getInfo()  # 1 -> 001
    asset_name = config.PRODUCT_V1['NDVI_reference_data'] + \
        '/NDVI_Stats_DOY' + doy3
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
    Takes the most recent pixels from the ee.ImageCollection.

    Args:
        image (ee.ImageCollection): Sentinel-2 image collection.

    Returns:
        Tuple[ee.Image, str, int]: Tuple containing NDVI image, index list, and scene count.
    """
    # Apply the cloud and terrain shadow mask within the S2 image collection
    def applyMasks(image):
        image = image.updateMask(image.select('terrainShadowMask').eq(0))
        image = image.updateMask(image.select('cloudAndCloudShadowMask').eq(0))
        return image
    S2_col_masked = image.map(applyMasks)

    # Sort the collection by time in descending order
    sortedCollection = S2_col_masked.sort('system:time_start', False)
    # Create list with indices of all used data
    NDVI_index_list = sortedCollection.aggregate_array('system:index')
    NDVI_index_list = NDVI_index_list.join(',')
    NDVI_scene_count = sortedCollection.size()
    # Create a mosaic using the latest pixel values
    latestMosaic = sortedCollection.mosaic()
    # Calculate NDVI for the mosaic
    NDVIj = latestMosaic.normalizedDifference(['B8', 'B4']).rename('ndvi')
    return NDVIj, NDVI_index_list, NDVI_scene_count

# This function loads the reference LST data (statistical value derived per DOY from 2012-2020)


def loadLstRefData(doy):
    """
    Loads the reference Land Surface Temperature (LST) data for a specific day of year (DOY).

    Args:
        doy (int): Day of year.

    Returns:
        ee.Image: Reference LST image adjusted for scale.
    """
    doy3 = ee.String(ee.Number(doy).format('%03d')).getInfo()  # 1 -> 001
    asset_name = config.PRODUCT_V1['LST_reference_data'] + \
        '/LST_Stats_DOY' + doy3
    LSTref = ee.Image(asset_name)
    # back to float
    LSTref = LSTref.float()
    # Get scale value
    scale = ee.Number(LSTref.get('scale'))
    # Divide by the scale
    LSTref = LSTref.divide(scale)
    return LSTref

# Helper function to extract the values from specific bits


def bitwiseExtract(input, fromBit, toBit):
    """
    Extracts values from specific bits in an input integer.

    Args:
        input (ee.Number): Input integer.
        fromBit (int): Starting bit position.
        toBit (int): Ending bit position.

    Returns:
        ee.Number: Extracted value.
    """
    maskSize = ee.Number(1).add(toBit).subtract(fromBit)
    mask = ee.Number(1).leftShift(maskSize).subtract(1)
    return input.rightShift(fromBit).bitwiseAnd(mask)

# function to mask clouds from a "NOAA/VIIRS/001/VNP21A1D" dataset


def maskCloudsAndLowQuality(image):
    """
    Masks clouds and low-quality pixels in a VIIRS dataset.

    Args:
        image (ee.Image): VIIRS image.

    Returns:
        ee.Image: Masked image.
    """
    # extract the quality band
    QC = image.select('QC')
    # only keep pixels from the input image where
    # Bits 0-1 = 0 (Pixel produced, good quality, no further QA info necessary)
    qaMask = bitwiseExtract(QC, 0, 1).eq(0)
    image = image.updateMask(qaMask)
    return image

# This function loads the current LST data


def loadLstCurrentData(date, d, aoi):
    """
    Loads the current Land Surface Temperature (LST) data from VIIRS imagery.
    Takes the most recent pixels from the ee.ImageCollection.

    Args:
        date (ee.Date): Date of interest.
        d (int): Number of days to cover in the time window.
        aoi (ee.Geometry): Area of interest.

    Returns:
        Tuple[ee.Image, str, int]: Tuple containing LST image, index list, and scene count.
    """
    start_date = date.advance((-1*d), 'day')
    end_date = date.advance(1, 'day')
    LST_col = ee.ImageCollection("NOAA/VIIRS/001/VNP21A1D") \
        .filterDate(start_date, end_date) \
        .filterBounds(aoi)
    # apply cloud and low quality masks
    LST_col_masked = LST_col.map(maskCloudsAndLowQuality)
    # Sort the collection by time in descending order
    sortedCollection = LST_col_masked.sort('system:time_start', False)
    # Create list with indices of all used data
    LST_index_list = sortedCollection.aggregate_array('system:index')
    LST_index_list = LST_index_list.join(',')
    LST_scene_count = sortedCollection.size()
    # Create a mosaic using the latest pixel values
    latestMosaic = sortedCollection.mosaic()
    # Calculate NDVI for the mosaic
    LSTj = latestMosaic.select('LST_1KM').rename('lst')
    return LSTj, LST_index_list, LST_scene_count


def process_PRODUCT_V1(roi, collection_ready, current_date_str):
    """
    Processes swissEO VHI data for Switzerland.

    Args:
        roi (ee.Geometry): Region of interest.
        collection_ready (str): Name of the image collection.
        current_date_str (str): Current date in string format.

    Returns:
        None
    """
    ##############################
    # SWITCHES
    # The switches enable / disable the execution of individual steps in this script

    exportVegetationAsset = True
    exportForestAsset = True
    # options: True, False
    exportVegetationDrive = True
    exportForestDrive = True
    # options: True, False
    workWithPercentiles = True
    # options: True, False - defines if the p05 and p95 percentiles of the reference data sets are used,
    # otherwise the min and max will be used (False)

    ##############################
    # PRODUCT
    product_name = config.PRODUCT_V1['product_name']
    print("********* processing {} *********".format(product_name))

    ##############################
    # TIME
    current_date = ee.Date(current_date_str)
    # To advance the start date by d days to cover the time window defined in 'temporal_coverage'
    d = int(config.PRODUCT_V1['temporal_coverage'])-1
    # get day of year
    doy = (ee.Number(current_date.getRelative('day', 'year')).add(
        1).mod(365)).add(365).mod(365)
    start_date = current_date.advance((-1*d), 'day')
    end_date = current_date.advance(1, 'day')

    # /
    # PARAMETERS
    alpha = 0.5

    if workWithPercentiles is True:
        CI_method = '5th_and_95th_percentile'
    else:
        CI_method = 'min_and_max'

    ##############################
    # SPACE
    aoi = roi

    ##############################
    # MASKS
    # Mask for vegetation
    vegetation_mask = ee.Image(
        'projects/satromo-prod/assets/res/ch_bafu_lebensraumkarte_mask_vegetation_epsg32632')
    # Mask for the forest
    forest_mask = ee.Image(
        'projects/satromo-prod/assets/res/ch_bafu_lebensraumkarte_mask_forest_epsg32632')

    ##############################
    # DATA
    S2_col = ee.ImageCollection(collection_ready) \
        .filterDate(start_date, end_date) \
        .filterBounds(aoi) \
        .filter(ee.Filter.stringEndsWith('system:index', '10m'))

    ###########################################
    # PRE-PROCESSING
    # Get information about the available sensor data for the range
    sensor_stats = main_utils.get_collection_info(S2_col)

    # Check if there is new sensor data compared to the stored dataset
    if main_utils.check_product_update(config.PRODUCT_V1['product_name'], sensor_stats[1]) is True:
        print("new imagery from: " + sensor_stats[1])

        ###########################################
        # PROCESSING
        # Load NDVI for VCI calculation
        NDVIref = loadNdviRefData(doy)
        NDVIj, NDVI_index_list, NDVI_scene_count = loadNdviCurrentData(S2_col)

        # Calculate VCI
        if workWithPercentiles is True:
            VCI = NDVIj.subtract(NDVIref.select('p05')).divide(NDVIref.select(
                'p95').subtract(NDVIref.select('p05'))).multiply(100).rename('vci')
            print(
                '--- VCI calculated (with 5th and 95th percentile reference values) ---')
        else:
            VCI = NDVIj.subtract(NDVIref.select('min')).divide(NDVIref.select(
                'max').subtract(NDVIref.select('min'))).multiply(100).rename('vci')
            print('--- VCI calculated (with min and max reference values) ---')

        # Load LST for TCI calculation
        LSTref = loadLstRefData(doy)
        LSTj, LST_index_list, LST_scene_count = loadLstCurrentData(
            current_date, d, aoi)

        # Calculate TCI
        if workWithPercentiles is True:
            TCI = LSTj.subtract(LSTref.select('p05')).divide(LSTref.select(
                'p95').subtract(LSTref.select('p05'))).multiply(100).rename('tci')
            print(
                '--- TCI calculated (with 5th and 95th percentile reference values) ---')
        else:
            TCI = LSTj.subtract(LSTref.select('min')).divide(LSTref.select(
                'max').subtract(LSTref.select('min'))).multiply(100).rename('tci')
            print('--- TCI calculated (with min and max reference values) ---')

        # Calculate VHI
        VHI = VCI.multiply(alpha).add(TCI.multiply(1-alpha)).rename('vhi')
        print('--- VHI calculated ---')

        # converting the data type (to UINT8) and force data range (to [0 100])
        VHI = VHI.uint8().clamp(0, 100)

        # add no data value for when one of the datasets is unavailable
        VHI = VHI.unmask(config.PRODUCT_V1['missing_data'])

        # Set data properties
        # Getting swisstopo Processor Version
        processor_version = main_utils.get_github_info()
        # Earth Engine version
        ee_version = ee.__version__

        # set properties to the product to be exported
        VHI = VHI.set({
            'doy': doy,
            'alpha': alpha,
            'temporal_coverage': config.PRODUCT_V1['temporal_coverage'],
            'missing_data': config.PRODUCT_V1['missing_data'],
            'no_data': config.PRODUCT_V1['no_data'],
            'SWISSTOPO_PROCESSOR': processor_version['GithubLink'],
            'SWISSTOPO_RELEASE_VERSION': processor_version['ReleaseVersion'],
            'collection': collection_ready,
            'system:time_start': current_date.advance((-1*d), 'day').millis(),
            'system:time_end': current_date.millis(),
            'NDVI_reference_data': config.PRODUCT_V1['NDVI_reference_data'],
            'NDVI_index_list': NDVI_index_list,
            'NDVI_scene_count': NDVI_scene_count,
            'LST_reference_data': config.PRODUCT_V1['LST_reference_data'],
            'LST_index_list': LST_index_list,
            'LST_scene_count': LST_scene_count,
            'VCI_and_TCI_calculated_with': CI_method,
            'GEE_api_version': ee_version,
            'pixel_size_meter': 10,
        })

        # mask vegetation
        VHI_vegetation = VHI
        VHI_vegetation = VHI_vegetation.updateMask(vegetation_mask.eq(1))
        # add the no data value to all masked pixels
        VHI_vegetation = VHI_vegetation.unmask(config.PRODUCT_V1['no_data'])

        # mask forest
        VHI_forest = VHI
        VHI_forest = VHI_forest.updateMask(forest_mask.eq(1))
        # add the no data value to all masked pixels
        VHI_forest = VHI_forest.unmask(config.PRODUCT_V1['no_data'])

        # Define item Name
        timestamp = datetime.datetime.strptime(current_date_str, '%Y-%m-%d')
        timestamp = timestamp.strftime('%Y-%m-%dT235959')

        ##############################
        # EXPORT

        # define the export aoi
        aoi_exp = aoi

        # SWITCH export - vegetation (Asset)
        task_description = 'VHI_SWISS_' + current_date_str
        if exportVegetationAsset is True:
            print('Launching VHI export for vegetation')
            # Export asset
            task = ee.batch.Export.image.toAsset(
                image=VHI_vegetation.clip(aoi_exp),
                scale=10,
                description=task_description + '_VEGETATION_10m',
                crs='EPSG:2056',
                region=aoi_exp,
                maxPixels=1e10,
                assetId=config.PRODUCT_V1['step1_collection'] +
                    '/' + task_description + '_VEGETATION_10m',
            )
            task.start()

        # SWITCH export - forest (Asset)
        if exportForestAsset is True:
            print('Launching VHI export for forests')
            # Export asset
            task = ee.batch.Export.image.toAsset(
                image=VHI_forest.clip(aoi_exp),
                scale=10,
                description=task_description + '_FOREST_10m',
                crs='EPSG:2056',
                region=aoi_exp,
                maxPixels=1e10,
                assetId=config.PRODUCT_V1['step1_collection'] +
                    '/' + task_description + '_FOREST_10m',
            )
            task.start()

        # SWITCH export (Drive)
        if exportVegetationDrive is True:
            # Generate the filename
            filename = config.PRODUCT_V1['product_name'] + \
                '_mosaic_' + timestamp + '_vegetation-10m'
            main_utils.prepare_export(roi, timestamp, filename, config.PRODUCT_V1['product_name'],
                                      config.PRODUCT_V1['spatial_scale_export'], VHI_vegetation,
                                      sensor_stats, current_date_str)

        if exportForestDrive is True:
            # Generate the filename
            filename = config.PRODUCT_V1['product_name'] + \
                '_mosaic_' + timestamp + '_forest-10m'
            main_utils.prepare_export(roi, timestamp, filename, config.PRODUCT_V1['product_name'],
                                      config.PRODUCT_V1['spatial_scale_export'], VHI_forest,
                                      sensor_stats, current_date_str)
