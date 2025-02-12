import dateutil.parser
import ee
import math
import datetime
from datetime import timedelta
import dateutil
from dateutil import tz
from dateutil.relativedelta import relativedelta
import configuration as config
from main_functions import main_utils
from step0_processors.step0_utils import write_asset_as_empty
from step0_processors.step0_processor_msg_lst import generate_msg_lst_mosaic_for_single_date

# Processing pipeline for monthly vegetation health index (VHI) mosaics over Switzerland

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
# 2. Calculating the VCI from a specific month and the NDVI reference
# 3. Calculating the TCI from a specific month and the LST reference
# 4. Combining the VCI and TCI to generate the VHI
# 5. Mask for forest or all vegetation
# 6. Exporting the resulting VHI


###########################################
# FUNCTIONS

def watermask():
    """
    Water mask
    The water mask is used to limit a buffering operation on the cast shadow mask.
    Here, it helps to better distinguish between dark areas and water bodies.
    This distinction is also used to limit the cloud shadow propagation.
    EU-Hydro River Network Database 2006-2012 data is derived from this data source:
    https:#land.copernicus.eu/en/products/eu-hydro/eu-hydro-river-network-database#download
    processing: reprojected in QGIS to epsg32632
    """
    # Simplified and buffered shapefile of Switzerland to simplify processing
    aoi = ee.FeatureCollection(
        "projects/satromo-prod/assets/res/CH_boundaries_buffer_5000m_epsg32632").geometry()
    # Lakes
    # lakes = ee.FeatureCollection("users/michaelbrechbuehler/eu-hydro")
    lakes = ee.FeatureCollection("projects/satromo-prod/assets/res/CH_inlandWater")

    # vector-to-image conversion based on the area attribute
    lakes_img = lakes.reduceToImage(
        properties=['AREA'],
        reducer=ee.Reducer.first()
    )

    # Make a binary mask and clip to area of interest
    lakes_binary = lakes_img.gt(0).unmask().clip(aoi)
    # # Map.addLayer(lakes_binary, {min:0, max:1}, 'lake mask', False)

    # Rivers
    rivers = ee.FeatureCollection("projects/satromo-prod/assets/res/CH_RiverNet")
    # print('rivers',rivers.first())
    # Make an image out of the land area attribute.
    rivers_img = rivers.reduceToImage(
        properties=['AREA_GEO'],
        reducer=ee.Reducer.first()
    )

    # Make a binary mask and clip to area of interest
    rivers_binary = rivers_img.gt(0).unmask().clip(aoi)
    # # Map.addLayer(rivers_binary, {min:0, max:1}, 'river mask', False)

    # combine both water masks
    water_binary = rivers_binary.Or(lakes_binary)
    # # Map.addLayer(water_binary, {min:0, max:1}, 'water mask', False)
    return water_binary

# -----------------------------------------
# Functions for processing Landsat data
# -----------------------------------------

# which Landsat sensor does the used data originate from?
def get_collection_strings(merged_collection):
    """
    Identifies which Landsat collections are present in a merged ImageCollection
    using collection filtering instead of aggregations.
    
    Args:
        merged_collection: ee.ImageCollection - Merged collection of Landsat images
        
    Returns:
        list: List of collection ID strings present in the merged collection
    """
    # Define the possible collection IDs
    collection_ids = [
        'LANDSAT/LT05/C02/T1_L2',
        'LANDSAT/LE07/C02/T1_L2',
        'LANDSAT/LC08/C02/T1_L2'
    ]
    
    found_collections = []
    
    # Check for each collection's presence using size()
    for collection_id in collection_ids:
        # Filter images that contain this collection ID in their system:id
        filtered = merged_collection.filter(
            ee.Filter.stringContains('system:id', collection_id)
        )
        
        # If the filtered collection has any images, add this collection_id
        if filtered.size().getInfo() > 0:
            found_collections.append(collection_id)
    
    return found_collections

# This function masks clouds & cloud shadows based on the QA quality bands of Landsat
def maskCloudsAndShadowsLsr(image):
    """
    Masks clouds and cloud shadows in Landsat Surface Reflectance (SR) images based on quality bands.
    This function applies cloud and cloud shadow masking using the QA_PIXEL and QA_RADSAT bands.
    It also applies scaling factors to optical and thermal bands, detects dark pixels,
    projects cloud shadows based on solar position, and combines various masks.

    Args:
        image (ee.Image): Input Landsat SR image.

    Returns:
        ee.Image: Processed image with clouds and shadows masked, scaled bands,
                  and additional mask bands added.

    Note:
        This function assumes working in a UTM projection for shadow projection calculations.
        This function assumes band names according to Landsat 5 or 7.
    """
    water_binary = watermask()

    if isinstance(image, ee.ImageCollection):
        image = image.first()

    # DETECT CLOUDS
    qa = image.select('QA_PIXEL')
    # See https:#www.usgs.gov/media/files/landsat-8-9-olitirs-collection-2-level-2-data-format-control-book

    # Bit 0 - Fill
    # Bit 1 - Dilated Cloud
    # Bit 2 - Cirrus
    # Bit 3 - Cloud

    qaMask = qa.bitwiseAnd(int('1111', 2)).eq(0).Not()
    saturationMask = image.select('QA_RADSAT').eq(0)

    # Apply the scaling factors to the appropriate bands.
    opticalBands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    thermalBands = image.select('ST_B.*').multiply(0.00341802).add(149.0)

    # DETECT CLOUD SHADOWS
    # Find dark pixels in the image (used in the upcoming addTerrainShadow function)
    darkPixels = opticalBands.select(['SR_B4', 'SR_B5', 'SR_B7']).reduce(ee.Reducer.sum()).lt(0.25) \
        .subtract(water_binary).clamp(0, 1).rename('darkPixels')

    # get the solar position
    meanAzimuth = ee.Number(image.get('SUN_AZIMUTH'))
    meanZenith = ee.Number(90).subtract(
        ee.Number(image.get('SUN_ELEVATION')))

    # Project shadows from clouds. This step assumes we're working in a UTM projection.
    shadowAzimuth = ee.Number(90).subtract(ee.Number(meanAzimuth))
    # shadow distance is tied to the solar zenith angle (minimum shadowDistance is 30 pixel)
    shadowDistance = ee.Number(meanZenith).multiply(
        0.7).floor().int().max(30)

    # With the following algorithm, cloud shadows are projected.
    isCloud = qaMask.directionalDistanceTransform(
        shadowAzimuth, shadowDistance)
    isCloud = isCloud.reproject(
        crs=image.select('SR_B2').projection(), scale=100)

    cloudShadow = isCloud.select('distance').mask()

    # combine projectedShadows & darkPixel and buffer the cloud shadow
    cloudShadow = cloudShadow.And(darkPixels).focalMax(
        100, 'circle', 'meters', 1, None)

    # combined mask for clouds and cloud shadows
    cloudAndCloudShadowMask = cloudShadow.Or(qaMask)

    # add bands & apply the masks
    image = image.addBands(opticalBands, None, True).multiply(10000) \
        .addBands(thermalBands, None, True) \
        .updateMask(cloudAndCloudShadowMask.Not()) \
        .updateMask(saturationMask) \
        .addBands(cloudAndCloudShadowMask.rename('cloudAndCloudShadowMask')) \
        .addBands(image.select(['QA_PIXEL', 'QA_RADSAT'])) \
        .addBands(darkPixels) \
        .copyProperties(image, image.propertyNames())

    return image


# This function detects snow cover
def addNDSI_L(image):
    """
    Calculates and adds the Normalized Difference Snow Index (NDSI) band to a Landsat image.
    This function computes the NDSI using the green (SR_B2) and short-wave infrared (SR_B5) bands
    of Landsat imagery. The NDSI is useful for detecting snow cover.

    Args:
        image (ee.Image): Input Landsat surface reflectance image.

    Returns:
        ee.Image: The input image with an additional 'ndsi' band.

    Note:
        The NDSI is calculated as (Green - SWIR) / (Green + SWIR).
        Higher NDSI values generally indicate a higher likelihood of snow cover.
        This function assumes band names according to Landsat 5 or 7.
    """
    # select the green and swir band
    green = image.select('SR_B2')
    swir = image.select('SR_B5')

    # calculate NDSI
    ndsi = green.subtract(swir).divide(green.add(swir)).rename('ndsi')

    # add NDSI band to the image
    image = image.addBands(ndsi)

    return image


# This function detects terrain shadows
def addTerrainShadow(image):
    """
    Detects and adds a terrain shadow mask to an image based on solar position and topography.

    This function calculates terrain shadows using a digital elevation model (DEM) and the image's
    solar position. It then refines the shadow mask by considering dark pixels, buffering the
    shadow areas, and excluding water bodies.

    Args:
        image (ee.Image): Input image with 'SUN_AZIMUTH', 'SUN_ELEVATION', and 'darkPixels' properties.

    Returns:
        ee.Image: The input image with an additional 'terrainShadowMask' band.

    Note:
        The terrain shadow mask is refined using a buffer and dark pixel information.
    """
    DEM_sa3d = ee.Image("projects/satromo-prod/assets/res/SwissALTI3d_20kmBuffer_epsg32632")
    water_binary = watermask()

    if isinstance(image, ee.ImageCollection):
        image = image.first()

    # get the solar position
    meanAzimuth = ee.Number(image.get('SUN_AZIMUTH'))
    meanZenith = ee.Number(90).subtract(
        ee.Number(image.get('SUN_ELEVATION')))

    # Define dark pixels
    darkPixels = image.select('darkPixels')

    # Terrain shadow
    terrainShadow = ee.Terrain.hillShadow(
        DEM_sa3d, meanAzimuth, meanZenith, 100, True)
    terrainShadow = terrainShadow.Not()  # invert the binaries

    # buffering the terrain shadow
    terrainShadow_buffer = terrainShadow.focalMax(
        200, 'circle', 'meters', 1, None)

    # removing extracting the terrain shadow buffer
    shadowBuffer = terrainShadow_buffer.subtract(terrainShadow)

    # removing dark water pixels from the buffer (as water is part of the darkPixels class, we exclude it from the buffer)
    shadowBuffer = shadowBuffer.subtract(water_binary).clamp(0, 1)

    # add the new buffer
    terrainShadow_bufferNoWater = terrainShadow.add(
        shadowBuffer).clamp(0, 1)

    # combining castShadow and darkPixels (only from the buffer region)
    terrainShadow_darkPixels = terrainShadow_bufferNoWater.And(
        darkPixels).Or(terrainShadow).rename('terrainShadowMask')

    # add the additonal terrainShadow band
    image = image.addBands(terrainShadow_darkPixels)

    return image


# This function calculates the illumination condition during the time of image acquisition
def topoCorr_L(img):
    """
    Calculates and adds the illumination condition to a Landsat image based on topography.

    This function computes the illumination condition (IC) using the image's solar position
    and a digital elevation model (DEM). It considers both slope and aspect in the calculation.
    The function adds several new bands to the image: 'TC_illumination' (total illumination condition),
    'cosZ' (cosine of solar zenith angle), 'cosS' (cosine of slope), and 'slope'.

    Args:
        img (ee.Image): Input Landsat image with 'SUN_AZIMUTH' and 'SUN_ELEVATION' properties.

    Returns:
        ee.Image: The input image with added illumination condition and related bands.

    Note:
        The calculation uses radians for angular measurements.
    """
    DEM_sa3d = ee.Image("projects/satromo-prod/assets/res/SwissALTI3d_20kmBuffer_epsg32632")

    if isinstance(img, ee.ImageCollection):
        img = img.first()

    # get the solar position
    meanAzimuth = ee.Number(img.get('SUN_AZIMUTH'))
    meanZenith = ee.Number(90).subtract(
        ee.Number(img.get('SUN_ELEVATION')))

    # Extract image metadata about solar position and covert from degree to radians
    SZ_rad = ee.Image.constant((meanZenith).multiply(math.pi).divide(180))
    SA_rad = ee.Image.constant((meanAzimuth).multiply(math.pi).divide(180))

    # Creat terrain layers and covert from degree to radians
    slp = ee.Terrain.slope(DEM_sa3d)
    slp_rad = ee.Terrain.slope(DEM_sa3d).multiply(math.pi).divide(180)
    asp_rad = ee.Terrain.aspect(DEM_sa3d).multiply(math.pi).divide(180)

    # Calculate the Illumination Condition (IC)
    # slope part of the illumination condition
    cosZ = SZ_rad.cos()
    cosS = slp_rad.cos()
    slope_illumination = cosS.select('slope').multiply(cosZ)

    # aspect part of the illumination condition
    sinZ = SZ_rad.sin()
    sinS = slp_rad.sin()
    cosAziDiff = (SA_rad.subtract(asp_rad)).cos()
    aspect_illumination = sinZ.multiply(sinS).multiply(cosAziDiff)

    # full illumination condition (IC)
    ic = slope_illumination.add(aspect_illumination)

    # Add the illumination condition to original image
    img_plus_ic = ee.Image(
        img.addBands(ic.rename('TC_illumination')).addBands(cosZ.rename('cosZ')).addBands(cosS.rename('cosS')).addBands(
            slp.rename('slope')))
    return img_plus_ic

# This function applies the sun-canopy-sensor+C topographic correction (Soenen et al. 2005)
def topoCorr_SCSc_L(img):
    """
    Applies the sun-canopy-sensor+C (SCSc) topographic correction to a Landsat image.

    This function implements the SCSc topographic correction method (Soenen et al. 2005)
    to adjust reflectance values in mountainous terrain. It corrects for illumination
    differences due to topography in specified bands.

    Args:
        img (ee.Image): Input Landsat image with added illumination condition bands
                        (output from topoCorr_L function).

    Returns:
        ee.Image: Topographically corrected image with original properties and additional 'TC_mask' band.

    Note:
        - This function assumes the input image has 'slope', 'TC_illumination', and other bands
          added by the topoCorr_L function.
        - It applies correction to bands 'SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'.
        - Pixels with slope < 5 degrees or TC_illumination < 0.1 are masked from correction.
        - This function assumes band names according to Landsat 5 or 7.
    """
    img_plus_ic = img

    # masking flat, shadowed, and incorrect pixels (these get excluded from the topographic correction)
    mask = img_plus_ic.select('slope').gte(5) \
        .And(img_plus_ic.select('TC_illumination').gte(0.1)) \
        .And(img_plus_ic.select('SR_B4').gt(-0.1))
    img_plus_ic_mask = ee.Image(img_plus_ic.updateMask(mask))

    # Specify Bands to topographically correct
    bandList = ee.List(
        ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'])

    def check_band_validity(image, band):
        band_stats = image.select([band]).reduceRegion(
            reducer=ee.Reducer.count(),
            geometry=image.geometry(),
            scale=30,
            maxPixels=1e9
        )
        return ee.Number(band_stats.get(band)).gt(0)

    valid_bands = bandList.map(lambda band: ee.Algorithms.If(
        check_band_validity(img_plus_ic_mask, band),
        band,
        None
    )).removeAll([None])

    empty_bands = bandList.map(lambda band: ee.Algorithms.If(
        check_band_validity(img_plus_ic_mask, band),
        None,
        band
    )).removeAll([None])

    # This function quantifies the linear relation between illumination and reflectance and corrects for it
    def apply_SCSccorr(band):
        """
        Applies the SCSc correction to a single band of a Landsat image.

        This function is used within topoCorr_SCSc_L to perform the actual correction
        calculations for each specified band. It computes the linear relationship between
        illumination and reflectance, then applies the SCSc correction.

        Args:
            band (str): Name of the band to correct.

        Returns:
            ee.Image: Single-band image with SCSc correction applied.

        Note:
            This function is intended to be used as a mapping function within topoCorr_SCSc_L
            and relies on variables defined in that outer scope.
        """
        out = img_plus_ic_mask.select('TC_illumination', band).reduceRegion(
            # Compute coefficients': a(slope), b(offset), c(b/a)
            reducer=ee.Reducer.linearFit(),
            geometry=ee.Geometry(img.geometry().buffer(-5000)),
            # trim off the outer edges of the image for linear relationship
            scale=30,
            maxPixels=1e6,
            bestEffort=True,
            tileScale=16
        )
        out_c = ee.Number(out.get('offset')).divide(
            ee.Number(out.get('scale')))

        # Apply the SCSc correction
        SCSc_output = img_plus_ic_mask.expression("((image * (cosB * cosZ + cvalue)) / (ic + cvalue))", {
            'image': img_plus_ic_mask.select([band]),
            'ic': img_plus_ic_mask.select('TC_illumination'),
            'cosB': img_plus_ic_mask.select('cosS'),
            'cosZ': img_plus_ic_mask.select('cosZ'),
            'cvalue': out_c
        })
        return ee.Image(SCSc_output)

    # List all bands without topographic correction (to be added to the TC image)
    bandsWithoutTC = ee.List(
        ['ST_B6', 'cloudAndCloudShadowMask', 'QA_PIXEL', 'QA_RADSAT', 'terrainShadowMask', 'ndsi'])

    # Add all bands and properties to the TC bands
    img_SCSccorr = ee.ImageCollection.fromImages(
        valid_bands.map(apply_SCSccorr)).toBands().rename(valid_bands)

    img_SCSccorr = img_SCSccorr.addBands(
        img_plus_ic.select(empty_bands))

    img_SCSccorr = img_SCSccorr.addBands(
        img_plus_ic.select(bandsWithoutTC))

    img_SCSccorr = img_SCSccorr.copyProperties(
        img_plus_ic, img_plus_ic.propertyNames())

    # Flatten both lists into one
    bandList_IC = ee.List([valid_bands, empty_bands, bandsWithoutTC]).flatten()
    # Unmask the uncorrected pixel using the original image
    return ee.Image(img_SCSccorr).unmask(img_plus_ic.select(bandList_IC)).addBands(mask.rename('TC_mask'))


# -----------------------------------------
# Functions for VHI generation
# -----------------------------------------
# This function loads the reference NDVI data (statistical value derived per MOY from 1991-2020)
def loadNdviRefData(moy):
    """
    Loads and processes NDVI reference data for a specific month of the year.

    This function retrieves NDVI reference data from a predefined asset, converts it to
    float values, and applies scaling and offset corrections.

    Args:
        moy (int): Month of the year (1-12).

    Returns:
        ee.Image: Processed NDVI reference data for the specified month.

    Note:
        - This function assumes the presence of a 'config' object with a 'PRODUCT_VHI_HIST'
          dictionary containing 'NDVI_reference_data' key.
        - The asset name is constructed using the month number (zero-padded to two digits).
        - The function applies offset and scale corrections based on properties stored
          in the original asset.
    """
    moy2 = ee.String(ee.Number(moy).format('%02d')).getInfo()  # 1 -> 01
    asset_name = config.PRODUCT_VHI_HIST['NDVI_reference_data'] + \
        '/NDVI_Stats_M' + moy2
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


# This function calculates the NDVI
def calculateNDVI_L(image):
    """
    Calculates the Normalized Difference Vegetation Index (NDVI) for Landsat imagery.

    This function applies masks for terrain shadows, clouds, cloud shadows, and snow,
    then calculates NDVI using the red and near-infrared bands. It also provides
    information about the scenes used in the calculation.

    Args:
        image (ee.ImageCollection): Input Landsat image collection with required bands and masks.

    Returns:
        ee.Image: Single-band image containing NDVI values.

    Note:
        This function assumes the input images have 'terrainShadowMask', 'cloudAndCloudShadowMask',
        'ndsi', 'SR_B3' (red), and 'SR_B4' (NIR) bands.
        This function assumes band names according to Landsat 5 or 7.
    """
    # Apply the terrain, cloud and snow mask within the Landsat image collection
    image = image.updateMask(image.select('terrainShadowMask').eq(0))
    image = image.updateMask(image.select('cloudAndCloudShadowMask').eq(0))
    image = image.updateMask(image.select('ndsi').lt(0.43))

    # select the red and nir band
    red = image.select('SR_B3')
    nir = image.select('SR_B4')

    # calculate ndvi
    ndvi = nir.subtract(red).divide(nir.add(red)).rename('ndvi')

    return ndvi


# This function loads the reference LST data (statistical value derived per MOY from 2012-2020)
def loadLstRefData(moy):
    """
    Loads and processes Land Surface Temperature (LST) reference data for a specific month of the year.

    This function retrieves LST reference data from a predefined asset, converts it to
    float values, and applies scaling correction. The reference data is derived from
    statistical values per month of year (MOY) from 2012-2020.

    Args:
        moy (int): Month of the year (1-12).

    Returns:
        ee.Image: Processed LST reference data for the specified month.

    Note:
        - This function assumes the presence of a 'config' object with a 'PRODUCT_VHI_HIST'
          dictionary containing 'LST_reference_data' key.
        - The asset name is constructed using the month number (zero-padded to two digits).
        - The function applies scale correction based on a property stored in the original asset.
    """
    moy2 = ee.String(ee.Number(moy).format('%02d')).getInfo()  # 1 -> 01
    asset_name = config.PRODUCT_VHI_HIST['LST_reference_data'] + \
        '/LST_Stats_M' + moy2
    LSTref = ee.Image(asset_name)
    # back to float
    LSTref = LSTref.float()
    # Get scale value
    scale = ee.Number(LSTref.get('scale'))
    # Divide by the scale
    LSTref = LSTref.divide(scale)
    return LSTref


# this function processes the VHI data
def process_PRODUCT_VHI_HIST(roi, current_date_str):
    """
    Process and generate the Vegetation Health Index (VHI) product.

    This function processes Landsat satellite data to calculate the Vegetation Health Index (VHI)
    for a given region of interest (ROI) and date. It combines the Vegetation Condition Index (VCI)
    and Temperature Condition Index (TCI) to produce the VHI.

    Args:
        roi (ee.Geometry): The region of interest for processing.
        current_date_str (str): The current date in string format.

    Returns:
        None

    The function performs the following main steps:
    1. Loads and preprocesses Landsat satellite data.
    2. Calculates NDVI (Normalized Difference Vegetation Index) and LST (Land Surface Temperature).
    3. Computes VCI and TCI using reference data.
    4. Combines VCI and TCI to produce the VHI.
    5. Exports the resulting VHI products for vegetation and forest areas.

    The function also includes several configurable parameters and switches for customizing the
    processing and export options.
    """

    ##############################
    # PRODUCT
    product_name = config.PRODUCT_VHI_HIST['product_name']
    print("********* processing {} *********".format(product_name))

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
    # SPACE
    aoi = roi

    ##############################
    # TIME
    d = int(config.PRODUCT_VHI_HIST['temporal_coverage'])

    current_date = dateutil.parser.parse(current_date_str).replace(tzinfo=tz.tzutc(), hour=0, minute=0, second=0, microsecond=0)

    start_date = current_date.replace(day = 1) - relativedelta(months = (d-1))

    first_of_month = current_date.replace(day = 1)
    end_date = first_of_month + relativedelta(months=1) - timedelta(seconds=1)

    timestamp = first_of_month.strftime('%Y-%m-%dT235959')

    ##############################
    # PARAMETERS
    alpha = 0.5

    if workWithPercentiles is True:
        CI_method = '5th_and_95th_percentile'
    else:
        CI_method = 'min_and_max'

    ##############################
    # MASKS
    # Vegetation masks
    vegetation_mask = ee.Image(
        'projects/satromo-prod/assets/res/ch_bafu_lebensraumkarte_mask_vegetation_epsg32632')
    # Mask for the forest
    forest_mask = ee.Image(
        'projects/satromo-prod/assets/res/ch_bafu_lebensraumkarte_mask_forest_epsg32632')


    ##############################
    # LANDSAT SR DATA
    # Landsat 5 (1984-2013)
    L5_sr = ee.ImageCollection("LANDSAT/LT05/C02/T1_L2") \
        .filter(ee.Filter.bounds(aoi)) \
        .filter(ee.Filter.calendarRange(start_date.year, end_date.year, 'year')) \
        .filter(ee.Filter.calendarRange(start_date.month, end_date.month, 'month')) \
        .filter(ee.Filter.lt('GEOMETRIC_RMSE_MODEL', 15)) \
        .filter(ee.Filter.eq('IMAGE_QUALITY', 9)) \
        .select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'ST_B6', 'QA_PIXEL', 'QA_RADSAT'])

    # Landsat 7 (1999-...)
    L7_sr = ee.ImageCollection("LANDSAT/LE07/C02/T1_L2") \
        .filter(ee.Filter.bounds(aoi)) \
        .filter(ee.Filter.calendarRange(start_date.year, end_date.year, 'year')) \
        .filter(ee.Filter.calendarRange(start_date.month, end_date.month, 'month')) \
        .filter(ee.Filter.lt('GEOMETRIC_RMSE_MODEL', 15)) \
        .filter(ee.Filter.eq('IMAGE_QUALITY', 9)) \
        .select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'ST_B6', 'QA_PIXEL', 'QA_RADSAT'])

    # Landsat 8 (2013-...)
    L8_sr = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2") \
        .filter(ee.Filter.bounds(aoi)) \
        .filter(ee.Filter.calendarRange(start_date.year, end_date.year, 'year')) \
        .filter(ee.Filter.calendarRange(start_date.month, end_date.month, 'month')) \
        .filter(ee.Filter.lt('GEOMETRIC_RMSE_MODEL', 15)) \
        .filter(ee.Filter.eq('IMAGE_QUALITY_OLI', 9)) \
        .select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10', 'QA_PIXEL', 'QA_RADSAT'])

    # Rename the bands for Landsat 8 to correspond to the band names of Landsat 5 and 7
    L8_sr = L8_sr.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10', 'QA_PIXEL', 'QA_RADSAT'],
                         ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'ST_B6', 'QA_PIXEL', 'QA_RADSAT'])

    # Merge to one single ee.ImageCollection containing all the Landsat images
    L_sr = L5_sr.merge(L7_sr).merge(L8_sr)


    # TEST VHI GEE: VHI GEE Asset already exists ?? if not 2 assets, in GEE then geneerate assets and export
    VHI_col = ee.ImageCollection(config.PRODUCT_VHI_HIST['step1_collection']) \
        .filterMetadata('system:index', 'contains', current_date_str) \
        .filterBounds(aoi)
    VHI_count = VHI_col.size().getInfo()
    if VHI_count == 0:

        ###########################################
        # PROCESSING

        # ----- Landsat data (pre-processing) -----
        L_sr = L_sr.map(maskCloudsAndShadowsLsr) \
            .map(addNDSI_L) \
            .map(addTerrainShadow) \
            .map(topoCorr_L).map(topoCorr_SCSc_L)

        # get collection info
        sensor_stats = main_utils.get_collection_info_landsat(L_sr)

        # ----- NDVI -----
        # add the NDVI
        NDVI = L_sr.map(calculateNDVI_L)

        # Get the total number and indices of all images used for the NDVI generation
        # Create list with indices of all used data
        NDVI_index_list = L_sr.aggregate_array('system:index')
        NDVI_index_list = NDVI_index_list.join(',')
        NDVI_scene_count = L_sr.size().getInfo()

	    # get current NDVI
        NDVIj = NDVI.mean()

	    # get reference NDVI
        NDVIref = loadNdviRefData(end_date.month)


        # ----- LST -----
        # get current LST
        LST_col = ee.ImageCollection("projects/satromo-prod/assets/col/LST_SWISS") \
            .filter(ee.Filter.bounds(aoi)) \
            .filter(ee.Filter.calendarRange(start_date.year, end_date.year, 'year')) \
            .filter(ee.Filter.calendarRange(start_date.month, end_date.month, 'month'))

        # Sort the collection by time in descending order
        sortedCollection = LST_col.sort('system:time_start', False)

        # Create list with indices of all used data
        LST_index_list = sortedCollection.aggregate_array('system:index')
        LST_index_list = LST_index_list.join(',')
        LST_scene_count = sortedCollection.size().getInfo()

        # Create a mosaic from the LST data
        LST_mosaic = LST_col.mean()
        LST_scale = ee.Number(100)
        LSTj = LST_mosaic.divide(LST_scale)

        # get reference LST
        LSTref = loadLstRefData(end_date.month)


        # ----- VHI -----
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
        VHI = VHI.unmask(config.PRODUCT_VHI_HIST['missing_data'])

        # Set data properties
        # Getting swisstopo Processor Version
        processor_version = main_utils.get_github_info()
        # Earth Engine version
        ee_version = ee.__version__

        # Get the collections used
        collections = get_collection_strings(L_sr)
        # Extract the set from the list and join the elements
        collection_string = ', '.join(collections)

        # set properties to the product to be exported
        
        VHI = VHI.set({
            'moy': end_date.month,
            'alpha': alpha,
            'temporal_coverage': config.PRODUCT_VHI_HIST['temporal_coverage'],
            'missing_data': config.PRODUCT_VHI_HIST['missing_data'],
            'no_data': config.PRODUCT_VHI_HIST['no_data'],
            'SWISSTOPO_PROCESSOR': processor_version['GithubLink'],
            'SWISSTOPO_RELEASE_VERSION': processor_version['ReleaseVersion'],
            'collection': collection_string,
            'system:time_start': int(datetime.datetime.timestamp(start_date) *1000),
            'system:time_end': int(datetime.datetime.timestamp(end_date) * 1000),
            'NDVI_reference_data': config.PRODUCT_VHI_HIST['NDVI_reference_data'],
            'NDVI_index_list': NDVI_index_list,
            'NDVI_scene_count': NDVI_scene_count,
            'LST_reference_data': config.PRODUCT_VHI_HIST['LST_reference_data'],
            'LST_index_list': LST_index_list,
            'LST_scene_count': LST_scene_count,
            'VCI_and_TCI_calculated_with': CI_method,
            'GEE_api_version': ee_version,
            'pixel_size_meter': 30,
        })

        # mask vegetation
        VHI_vegetation = VHI
        VHI_vegetation = VHI_vegetation.updateMask(vegetation_mask.eq(1))
        # add the no data value to all masked pixels
        VHI_vegetation = VHI_vegetation.unmask(config.PRODUCT_VHI_HIST['no_data'])

        # mask forest
        VHI_forest = VHI
        VHI_forest = VHI_forest.updateMask(forest_mask.eq(1))
        # add the no data value to all masked pixels
        VHI_forest = VHI_forest.unmask(config.PRODUCT_VHI_HIST['no_data'])


        ##############################
        # EXPORT

        # define the export aoi
        aoi_exp = aoi


        # SWITCH export - vegetation (Asset)
        task_description = 'VHI_SWISS_' + datetime.datetime.strftime(first_of_month,'%Y-%m-%d')
        if exportVegetationAsset is True:
            print('Launching VHI export for vegetation')
            # Export asset
            task = ee.batch.Export.image.toAsset(
                    image=VHI_vegetation.clip(aoi_exp),
                    scale=30,
                    description=task_description + '_VEGETATION_30m',
                    crs='EPSG:2056',
                    region=aoi_exp,
                    maxPixels=1e10,
                    assetId=config.PRODUCT_VHI_HIST['step1_collection'] + \
                        '/' + task_description + '_VEGETATION_30m',
            )
        task.start()

        # SWITCH export - forest (Asset)
        if exportForestAsset is True:
            print('Launching VHI export for forests')
            # Export asset
            task = ee.batch.Export.image.toAsset(
                    image=VHI_forest.clip(aoi_exp),
                    scale=30,
                    description=task_description + '_FOREST_30m',
                    crs='EPSG:2056',
                    region=aoi_exp,
                    maxPixels=1e10,
                    assetId=config.PRODUCT_VHI_HIST['step1_collection'] + \
                        '/' + task_description + '_FOREST_30m',
            )
            task.start()

    else:
        print(current_date_str+' is already in ' +
            config.PRODUCT_VHI_HIST['step1_collection'])

        # Load from GEE Asset
        VHI_forest = ee.Image(VHI_col.filter(ee.Filter.stringContains('system:index', 'FOREST')).first())
        VHI_vegetation = ee.Image(VHI_col.filter(ee.Filter.stringContains('system:index', 'VEGETATION')).first())

    # SWITCH export (Drive/GCS)
    if exportVegetationDrive is True:
        # Generate the filename
        ee_string = ee.String(config.PRODUCT_VHI_HIST['product_name'])
        py_string = ee_string.getInfo()
        py_timestamp = timestamp
        filename = py_string + '_mosaic_' + py_timestamp + '_vegetation-30m'
        main_utils.prepare_export(roi, py_timestamp, filename, config.PRODUCT_VHI_HIST['product_name'],
                                config.PRODUCT_VHI_HIST['spatial_scale_export'], VHI_vegetation,
                                sensor_stats, current_date_str)

    if exportForestDrive is True:
        # Generate the filename
        ee_string = ee.String(config.PRODUCT_VHI_HIST['product_name'])
        py_string = ee_string.getInfo()
        py_timestamp = timestamp
        filename = py_string + '_mosaic_' + py_timestamp + '_forest-30m'
        main_utils.prepare_export(roi, py_timestamp, filename, config.PRODUCT_VHI_HIST['product_name'],
                                config.PRODUCT_VHI_HIST['spatial_scale_export'], VHI_forest,
                                sensor_stats, current_date_str)

