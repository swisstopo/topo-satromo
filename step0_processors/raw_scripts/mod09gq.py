import ee
import math
ee.Initialize()

# Pre-processing pipeline for daily MODIS Terra surface reflectance observations
# in the red and NIR spectral bands in 250 m (MODIS product: MOD09GQ) over Switzerland


##############################
# INTRODUCTION
#
# This script provides a tool to preprocess MODIS product MOD09GQ over Switzerland.
# It can mask clouds and cloud shadows, detect terrain shadows,
# topographically correct images and export the results.
#


##############################
# CONTENT

# This script includes the following steps:
# 1. Masking clouds and cloud shadows
# 2. Detecting terrain shadows
# 3. Applying a topographic correction (SCSc-correction) to the spectral bands
# 4. Exporting spectral bands, additional layers and relevant properties
#
# The script is set up to export one image per day.


##############################
# SWITCHES
# The switches enable / disable the execution of individual steps in this script

cloudMasking = True  # options': True, False - defines if individual clouds and cloud shadows are masked
terrainShadowDetection = True  # options': True, False - defines if a cast shadow mask is applied
topoCorrection = True  # options': True, False - defines if a topographic correction is applied to the image swath

# Export switches
exportAllToAsset = False  # options': True, False - defines if image with all bands is exported as an asset
export250mBands = True  # options': True, 'False - defines if 250 m spectral bands are exported': 'sur_refl_b01', 'sur_refl_b02'
exportMasks = True  # options': True, 'False - defines if masks are exported': 'terrainShadowMask','cloudAndCloudShadowMask', 'TC_mask', 'clouds_MOD09GA_state_1km'
exportTSF = True  # options': True, 'False - defines if the terrain shadow layer is exported': 'terrainShadowFraction'
exportQAbands = True  # options': True, 'False - defines if MODIS quality bands are exported': 'QC_250m', 'num_observations'
exportProperties = True  # options': True, False - defines if image properties are exported

##############################
# TIME
end_date = ee.Date('2023-07-11')  # ee.Date(Date.now())
start_date = end_date.advance(-1, 'day')  # this script is set up to export only one mosaic image per day

##############################
# SPACE

# Official swisstopo boundaries
# source: https:#www.swisstopo.admin.ch/de/geodata/landscape/boundaries3d.html#download
# processing: layer Landesgebiet dissolved  in QGIS and reprojected to epsg32632
aoi_CH = ee.FeatureCollection("users/wulf/SATROMO/swissBOUNDARIES3D_1_5_TLM_LANDESGEBIET_dissolve_epsg32632").geometry()

# Simplified and buffered shapefile of Switzerland to simplify processing
aoi_CH_simplified = ee.FeatureCollection("users/wulf/SATROMO/CH_boundaries_buffer_5000m_epsg32632").geometry()
# clipping on complex shapefiles costs more processing resources and can cause memory issues


##############################
# VISUALISATION
vis_fci = {'bands': ['sur_refl_b01', 'sur_refl_b02', 'sur_refl_b01'], 'min': 0, 'max': [3000, 4000, 3000]}

##############################
# ELEVATION DATA

# Copernicus DEM GLO-30: Global 30m Digital Elevation Model
# https:#developers.google.com/earth-engine/datasets/catalog/COPERNICUS_DEM_GLO30#description
# alternative to SwissALTI3d as it has a lower spatial resolution
# and therefore reproject for the topographic correction operates faster at lower costs
# furthermore, reduceResolution provides an error on too many input pixels per output pixel at 10m
DEM_cop = ee.ImageCollection('COPERNICUS/DEM/GLO30') \
    .filterBounds(aoi_CH_simplified) \
    .select('DEM')
proj = DEM_cop.first().select('DEM').projection()
DEM = DEM_cop.mosaic().setDefaultProjection(proj)

##############################
# SATELLITE DATA

# MOD09GQ.061
# https:#developers.google.com/earth-engine/datasets/catalog/MODIS_061_MOD09GQ
MOD09GQ = ee.ImageCollection("MODIS/061/MOD09GQ") \
    .filter(ee.Filter.date(start_date, end_date))

# MOD09GA.061
# https:#developers.google.com/earth-engine/datasets/catalog/MODIS_061_MOD09GA
MOD09GA = ee.ImageCollection("MODIS/061/MOD09GA") \
    .filter(ee.Filter.date(start_date, end_date))

# Merge qa bands from other collections to this MOD09GQ collection
MOD09GQ = MOD09GQ.linkCollection(MOD09GA, ['state_1km', 'sur_refl_b06', 'sur_refl_b07', 'SolarZenith', 'SolarAzimuth'])

# print('MOD09GQ size', MOD09GQ.size())
print('MOD09GQ first', MOD09GQ.first())

# Map.addLayer(MOD09GA, {'bands': ['sur_refl_b07',  'sur_refl_b02',  'sur_refl_b01'], 'min': 0, 'max': [3000, 4000, 2000]}, 'MOD09GA original', False)
# Map.addLayer(MOD09GQ, {'bands': ['sur_refl_b01',  'sur_refl_b02',  'sur_refl_b01'], 'min': 0, 'max': [3000, 4000, 3000]}, 'MOD09GQ original', False)


###########################
# WATER MASK
# The water mask is used to limit a buffering operation on the cast shadow mask.
# Here, it helps to better distinguish between dark areas and water bodies.
# This distinction is also used to limit the cloud shadow propagation.
# EU-Hydro River Network Database 2006-2012 data is derived from this data source:
# https:#land.copernicus.eu/en/products/eu-hydro/eu-hydro-river-network-database#download
# processing: reprojected in QGIS to epsg32632

# Lakes
# lakes = ee.FeatureCollection("users/michaelbrechbuehler/eu-hydro")
lakes = ee.FeatureCollection("users/wulf/SATROMO/CH_inlandWater")

# vector-to-image conversion based on the area attribute
lakes_img = lakes.reduceToImage(
    properties=['AREA'],
    reducer=ee.Reducer.first()
)

# Make a binary mask and clip to area of interest
lakes_binary = lakes_img.gt(0).unmask().clip(aoi_CH_simplified)
# # Map.addLayer(lakes_binary, {min:0, max:1}, 'lake mask', False)


# Rivers
rivers = ee.FeatureCollection("users/wulf/SATROMO/CH_RiverNet")
# print('rivers',rivers.first())
# Make an image out of the land area attribute.
rivers_img = rivers.reduceToImage(
    properties=['AREA_GEO'],
    reducer=ee.Reducer.first()
)
# Make a binary mask and clip to area of interest
rivers_binary = rivers_img.gt(0).unmask().clip(aoi_CH_simplified)
# # Map.addLayer(rivers_binary, {min:0, max:1}, 'river mask', False)

# combine both water masks
water_binary = rivers_binary.Or(lakes_binary)


# # Map.addLayer(water_binary, {min:0, max:1}, 'water mask', False)


##############################
# FUNCTIONS


# This function adds the useful-pixel-percentage (excluding clouds, cloud shadows, terrain shadows) as a property to each image
def addMaskedPixelCount(image):
    # define cloudy and shaded pixel
    image_mask = image.select('cloudAndCloudShadowMask').gt(0)

    # Count the number of all non-masked pixels
    statsMasked = image_mask.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=image.geometry().intersection(aoi_CH_simplified),
        scale=100,
        bestEffort=True,
        maxPixels=1e10,
        tileScale=4
    )
    dataPixels = statsMasked.getNumber('cloudAndCloudShadowMask')

    # define all pixel
    image_all = image.select('cloudAndCloudShadowMask').gte(0)

    # Count the number of all pixels
    statsAll = image_all.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=image.geometry().intersection(aoi_CH_simplified),
        scale=100,
        bestEffort=True,
        maxPixels=1e10,
        tileScale=4)
    allPixels = statsAll.getNumber('cloudAndCloudShadowMask')

    # Calculate the percentages and add the properties
    percMasked = (dataPixels.divide(allPixels)).multiply(1000).round().divide(10)
    percData = ee.Number(100).subtract(percMasked)

    return image.set({
        'percentData': percData,  # percentage of useful pixel
        'percentMasked': percMasked  # less useful pixels including clouds, cloud shadows and terrain shadows
    })


##############################
# SOLAR GEOMETRY

# This functions calculates the median solar position angles from the MOD09GA bands and adds them as additional properties
def setSunAngles(image):
    # Extract the median solar zentih angle
    # the median is helpful as diffent image swath can be combined in the final MODIS composite image
    statsSolarZenith = image.select('SolarZenith').reduceRegion(
        reducer=ee.Reducer.median(),
        geometry=aoi_CH_simplified,
        scale=1000,
        bestEffort=True,
        maxPixels=1e10,
        tileScale=4
    )
    numSolarZenith = statsSolarZenith.getNumber('SolarZenith').divide(100)

    # Extract the median solar azimuth angle
    statsSolarAzimuth = image.select('SolarAzimuth').reduceRegion(
        reducer=ee.Reducer.median(),
        geometry=aoi_CH_simplified,
        scale=1000,
        bestEffort=True,
        maxPixels=1e10,
        tileScale=4
    )
    numSolarAzimuth = statsSolarAzimuth.getNumber('SolarAzimuth').divide(100)

    # add the numbers as image properties
    return image.set('SolarZenith', numSolarZenith) \
        .set('SolarAzimuth', numSolarAzimuth)


# map the function
MOD09GQ = MOD09GQ.map(setSunAngles)


##############################
# CLOUD MASKING

# This function masks clouds & cloud shadows using the QA quality bands of Landsat
def maskCloudsAndShadowsMOD(image):
    # get the solar position
    meanAzimuth = ee.Number(image.get('SolarAzimuth'))
    meanZenith = ee.Number(image.get('SolarZenith'))

    # extract the MOD09GA state_1km-QA_band
    QA = image.select('state_1km')

    # Make a mask to get bit 10, the internal_cloud_algorithm_flag bit.
    qaCloud = QA.bitwiseAnd(1 << 10).eq(0)

    # Find dark pixels but exclude lakes and rivers (otherwise projected shadows would cover large parts of water bodies)
    darkPixels = image.select(['sur_refl_b02', 'sur_refl_b06', 'sur_refl_b07']).reduce(ee.Reducer.sum()).lt(
        2700).subtract(water_binary).clamp(0, 1)

    # Project shadows from clouds. This step assumes we're working in a UTM projection.
    shadowAzimuth = ee.Number(90).subtract(ee.Number(meanAzimuth))
    # shadow distance is tied to the solar zenith angle (minimum shadowDistance is 10 pixel)
    shadowDistance = ee.Number(meanZenith).multiply(0.4).floor().int().max(10)

    # With the following algorithm, cloud shadows are projected.
    isCloud = qaCloud.directionalDistanceTransform(shadowAzimuth, shadowDistance)
    isCloud = isCloud.reproject(crs=image.select('sur_refl_b01').projection(), scale=1000)

    cloudShadow = isCloud.select('distance').mask()

    # combine projectedShadows & darkPixel and buffer the cloud shadow
    cloudShadow = cloudShadow.And(darkPixels)

    # combined mask for clouds and cloud shadows
    cloudAndCloudShadowMask = cloudShadow.Or(qaCloud.Not())

    # apply the masks
    image = image.updateMask(qaCloud) \
        .updateMask(cloudAndCloudShadowMask.Not()) \
        .addBands(qaCloud.Not().rename('clouds_MOD09GA_state_1km')) \
        .addBands(cloudAndCloudShadowMask.rename(['cloudAndCloudShadowMask'])) \
        .copyProperties(image, image.propertyNames())

    return image


# SWITCH
if cloudMasking is True:
    print('--- Cloud and cloud shadow masking applied ---')

    # apply the masking function
    MOD09GQ = MOD09GQ.map(maskCloudsAndShadowsMOD) \
        .map(addMaskedPixelCount)
    print('MOD09GQ cloud masked - first', MOD09GQ.first())
    # Map.addLayer(MOD09GQ, vis_fci, 'MOD09GQ cloud masked', True)


##############################
# TERRAIN SHADOWS

# This function detects terrain shadows
def addTerrainShadow(image):
    # get the solar position
    meanAzimuth = ee.Number(image.get('SolarAzimuth'))
    meanZenith = ee.Number(image.get('SolarZenith'))

    # Terrain shadow
    terrainShadow = ee.Terrain.hillShadow(DEM, meanAzimuth, meanZenith, 500, True)
    terrainShadow = terrainShadow.Not().rename('terrainShadowMask')  # invert the binaries

    # Get information about the MODIS projection.
    imageProjection = image.select('sur_refl_b01').projection()

    # Get the terrain shadow mask at MODIS scale and projection.
    # This band contains the per pixel shadow fraction
    terrainShadowMean = terrainShadow \
        .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1024) \
        .reproject(crs=imageProjection) \
        .multiply(100).int8() \
        .rename('terrainShadowFraction')

    # add the additonal terrainShadow bands
    image = image.addBands(terrainShadow) \
        .addBands(terrainShadowMean)

    return image


# SWITCH
if terrainShadowDetection is True:
    print('--- Terrain shadow detection applied ---')

    # apply the terrain shadow function
    MOD09GQ = MOD09GQ.map(addTerrainShadow)

    print('MOD09GQ terrain shadow - first', MOD09GQ.first())
    # # Map.addLayer(MOD09GQ.select('terrainShadowMask'), {}, 'MOD09GQ terrain shadow mask', False)
    # Map.addLayer(MOD09GQ.select('terrainShadowFraction'), {}, 'MOD09GQ terrain shadow fraction', False)


##############################
# TOPOGRAPHIC CORRECTION
# This step compensates for the effects of terrain elevation, slope, and solar illumination variations.
# The method is based on Soenen et al. 2005 (https:#ieeexplore.ieee.Org/document/1499030)

# This function calculates the illumination condition during the time of image acquisition
def topoCorr_MOD(img):
    # get the solar position
    meanAzimuth = ee.Number(img.get('SolarAzimuth'))
    meanZenith = ee.Number(img.get('SolarZenith'))

    # Extract image metadata about solar position and covert from degree to radians
    SZ_rad = ee.Image.constant((meanZenith).multiply(math.pi).divide(180))
    SA_rad = ee.Image.constant((meanAzimuth).multiply(math.pi).divide(180))

    # Creat terrain layers and covert from degree to radians
    slp = ee.Terrain.slope(DEM)
    slp_rad = ee.Terrain.slope(DEM).multiply(math.pi).divide(180)
    asp_rad = ee.Terrain.aspect(DEM).multiply(math.pi).divide(180)

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
def topoCorr_SCSc_MOD(img):
    img_plus_ic = img

    # masking flat, shadowed, and incorrect pixels (these get excluded from the topographic correction)
    mask = img_plus_ic.select('slope').gte(5) \
        .And(img_plus_ic.select('TC_illumination').gte(0)) \
        .And(img_plus_ic.select('sur_refl_b02').gt(-0.1))
    img_plus_ic_mask = ee.Image(img_plus_ic.updateMask(mask))

    # Specify Bands to topographically correct
    bandList = ee.List(['sur_refl_b01', 'sur_refl_b02'])

    # This function quantifies the linear relation between illumination and reflectance and corrects for it
    def apply_SCSccorr(band):
        out = img_plus_ic_mask.select('TC_illumination', band).reduceRegion(
            reducer=ee.Reducer.linearFit(),  # Compute coefficients: a(slope), b(offset), c(b/a)
            geometry=aoi_CH_simplified,  # trim off the outer edges of the image for linear relationship
            scale=250,
            maxPixels=1e6,
            bestEffort=True,
            tileScale=16
        )

        out_c = ee.Number(out.get('offset')).divide(ee.Number(out.get('scale')))

        # apply the SCSc correction
        SCSc_output = img_plus_ic_mask.expression("((image * (cosB * cosZ + cvalue)) / (ic + cvalue))", {
            'image': img_plus_ic_mask.select([band, ]),
            'ic': img_plus_ic_mask.select('TC_illumination'),
            'cosB': img_plus_ic_mask.select('cosS'),
            'cosZ': img_plus_ic_mask.select('cosZ'),
            'cvalue': out_c
        })

        return ee.Image(SCSc_output)

    # list all bands without topographic correction (to be added to the TC image)
    bandsWithoutTC = ee.List(
        ['QC_250m', 'num_observations', 'clouds_MOD09GA_state_1km', 'cloudAndCloudShadowMask', 'terrainShadowMask',
         'terrainShadowFraction'])

    # Take care of dependencies between switches
    if terrainShadowDetection is False:
        # remove the bands from the co-registration
        bandsWithoutTC = bandsWithoutTC.remove('terrainShadowMask').remove('terrainShadowFraction')

    if cloudMasking is False:
        # remove the bands from the co-registration
        bandsWithoutTC = bandsWithoutTC.remove('clouds_MOD09GA_state_1km')

    # add all bands and properties to the TC bands
    img_SCSccorr = ee.ImageCollection.fromImages(
        bandList.map(apply_SCSccorr)).toBands().rename(bandList)
    img_SCSccorr = img_SCSccorr.addBands(img_plus_ic.select(bandsWithoutTC))
    img_SCSccorr = img_SCSccorr.copyProperties(img_plus_ic, img_plus_ic.propertyNames())

    # flatten both lists into one
    bandList_IC = ee.List([bandList, bandsWithoutTC]).flatten()

    # unmasked the uncorrected pixel using the orignal image
    return ee.Image(img_SCSccorr).unmask(img_plus_ic.select(bandList_IC)).addBands(mask.rename('TC_mask'))


# SWITCH
if topoCorrection is True:
    print('--- Topographic correction applied ---')

    # The topographic correction operates at the DEM scale and projection
    # Therefore, we need to rescale the DEM
    DEM = DEM \
        .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1024) \
        .reproject(crs=MOD09GQ.first().select('sur_refl_b01').projection())

    # apply the topographic correction function
    MOD09GQ = MOD09GQ.map(topoCorr_MOD) \
        .map(topoCorr_SCSc_MOD)

    # print('MOD09GQ size after mosaic', MOD09GQ.size())
    print('MOD09GQ topoCorrected - first', MOD09GQ.first())

    # Map.addLayer(MOD09GQ.first(), vis_fci, 'MOD09GQ TC', False)


##############################
# EXPORT

# This function converts the data type of the topographically corrected images
def dataType(image):
    return image.addBands(image.select(['sur_refl_b01', 'sur_refl_b02']).round().toInt16(), None, True)


# data type conversion
MOD09GQ = MOD09GQ.map(dataType)

# convert image collection to image (used in export)
img_exp = ee.Image(MOD09GQ.first())
# Map.addLayer(img_exp, vis_fci, 'MOD09GQ export', False)

# converting the data type of converted spectral bands back to int16
img_exp = img_exp.addBands(img_exp.select(['sur_refl_b01', 'sur_refl_b02']).round().toInt16(), None, True)

# extract the image properties
img_exp_properties = ee.FeatureCollection([ee.Feature(img_exp.select([]))])

# extract the date and time
# advance to T10:15:00 for Switzerland (45degN) as the GEE MODIS Terra time is set to T00:00:00 UTC due to its global mosaic
sensing_date = img_exp.date().advance(10.25, 'hour').format('YYYY-MM-dd_hh-mm-ss').getInfo()
sensing_date_read = sensing_date[0:10] + '_T' + sensing_date[11:19]

# define the filenames
fname_all = 'MOD09GQ_' + sensing_date_read + '_All'
fname_250m = 'MOD09GQ_' + sensing_date_read + '_Bands-250m'  # ['sur_refl_b01', 'sur_refl_b02']
fname_masks = 'MOD09GQ_' + sensing_date_read + '_Masks-250m'  # ['terrainShadowMask', 'terrainShadowFraction', 'cloudAndCloudShadowMask', 'TC_mask', 'clouds_MOD09GA_state_1km']
fname_TSF = 'MOD09GQ_' + sensing_date_read + '_TSF-250m'  # ['terrainShadowFraction']
fname_QAbands = 'MOD09GQ_' + sensing_date_read + '_Bands-QA'  # ['QC_250m', 'num_observations']
fname_properties = 'MOD09GQ_' + sensing_date_read + '_properties'  # ["SolarAzimuth", "SolarZenith", "percentData", "percentMasked", "system:asset_size", "system:footprint", "system:time_start", "system:time_end", "system:index"]

# define the export aoi
# the full mosaic image geometry covers larger areas outside Switzerland that are not needed
aoi_img = img_exp.geometry()
# therefore it is clipped with rectangle aoi of Switzerland to keep the geometry simple
# the alternative clip with aoi_CH would be computationally heavier
aoi_exp = aoi_img.intersection(aoi_CH_simplified)  # alternativ': aoi_CH
# print('aoi_exp', aoi_exp)
# Map.addLayer(aoi_exp, {}, 'aoi export', False)


# SWITCH export
if exportAllToAsset is True:
    ee.batch.Export.image.toAsset(
        image=img_exp,
        scale=10,
        description=fname_all,
        crs='EPSG:2056',
        region=aoi_exp,
        maxPixels=1e10,
        assetId=fname_all,
    )

# SWITCH export
if export250mBands is True:
    # Export 250 m spectral bands
    task = ee.batch.Export.image.toDrive(
        image=img_exp.select(['sur_refl_b01', 'sur_refl_b02']),
        scale=250,
        description=fname_250m,
        crs='EPSG:2056',
        region=aoi_exp,
        maxPixels=1e10,
        folder='eeExports',
        skipEmptyTiles=True,
        fileFormat='GeoTIFF',
        formatOptions={'cloudOptimized': True}
    )
    task.start()

# SWITCH export
if exportMasks is True:
    # Export masks
    task = ee.batch.Export.image.toDrive(
        image=img_exp.select(['terrainShadowMask', 'cloudAndCloudShadowMask', 'TC_mask', 'clouds_MOD09GA_state_1km']),
        scale=250,
        description=fname_masks,
        crs='EPSG:2056',
        region=aoi_exp,
        maxPixels=1e10,
        folder='eeExports',
        skipEmptyTiles=True,
        fileFormat='GeoTIFF',
        formatOptions={'cloudOptimized': True}
    )
    task.start()

# SWITCH export
if exportTSF is True:
    # Export masks
    task = ee.batch.Export.image.toDrive(
        image=img_exp.select(['terrainShadowFraction']),
        scale=500,
        description=fname_TSF,
        crs='EPSG:2056',
        region=aoi_exp,
        maxPixels=1e10,
        folder='eeExports',
        skipEmptyTiles=True,
        fileFormat='GeoTIFF',
        formatOptions={'cloudOptimized': True}
    )
    task.start()

# SWITCH export
if exportQAbands is True:
    # Export QA layers
    task = ee.batch.Export.image.toDrive(
        image=img_exp.select(['QC_250m']).addBands(img_exp.select(['num_observations']).uint16()),
        scale=250,
        description=fname_QAbands,
        crs='EPSG:2056',
        region=aoi_exp,
        maxPixels=1e10,
        folder='eeExports',
        skipEmptyTiles=True,
        fileFormat='GeoTIFF',
        formatOptions={'cloudOptimized': True}
    )
    task.start()

# SWITCH export
if exportProperties is True:
    # Export image properties
    task = ee.batch.Export.table.toDrive(
        collection=img_exp_properties,
        description=fname_properties,
        fileFormat='CSV'
    )
    task.start()
