import ee 
import math
ee.Initialize()

# Pre-processing pipeline for daily MODIS Terra surface reflectance observations
# in the red and NIR spectral bands in 250 m (MODIS product: MOD13Q1) over Switzerland


##############################
# INTRODUCTION
#
# This script provides a tool to preprocess MODIS product MOD13Q1 over Switzerland.
# It can detect terrain shadows,topographically correct images and export the results.
#


##############################
# CONTENT

# This script includes the following steps:
# 1. Detecting terrain shadows
# 2. Applying a topographic correction (SCSc-correction) to the spectral bands
# 3. Exporting spectral bands, additional layers and relevant properties
#
# The script is set up to export one MOD13Q1 image every 16 days.



##############################
# SWITCHES
# The switches enable / disable the execution of individual steps in this script

terrainShadowDetection = True          # options': True, False - defines if a cast shadow mask is applied
topoCorrection = True                  # options': True, False - defines if a topographic correction is applied to the image swath

# Export switches
exportAllToAsset = False               # options': True, False - defines if image with all bands is exported as an asset
exportVIsBands = True                  # options': True, 'False - defines if 250 m vegetation index bands are exported': 'NDVI', 'EVI'
export250mBands = True                 # options': True, 'False - defines if 250 m spectral bands are exported': 'sur_refl_b01','sur_refl_b02', 'sur_refl_b03', 'sur_refl_b07'
exportMasks = True                     # options': True, 'False - defines if masks are exported': 'terrainShadowMask', 'TC_mask'
exportTSF = True                       # options': True, 'False - defines if the terrain shadow layer is exported': 'terrainShadowFraction'
exportQAbands = True                   # options': True, 'False - defines if MODIS quality bands are exported': 'ViewZenith', 'SolarZenith', 'SolarAzimuth', 'RelativeAzimuth'	, 'DayOfYear'	, 'SummaryQA'
exportProperties = True                # options': True, False - defines if image properties are exported


##############################
# TIME
end_date = ee.Date('2023-02-11')            # ee.Date(Date.now())
start_date = end_date.advance(-16, 'day')   # this script is set up to export only one MOD13Q1 image every 16 days


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
vis_fci = {'bands': ['sur_refl_b07',  'sur_refl_b02',  'sur_refl_b01'], 'min': 0, 'max': [3000, 4000, 3000]}


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

# MOD13Q1.061
# https:#developers.google.com/earth-engine/datasets/catalog/MODIS_061_MOD13Q1
MOD13Q1 = ee.ImageCollection('MODIS/061/MOD13Q1') \
              .filter(ee.Filter.date(start_date, end_date))

# MOD09GA.061
# https:#developers.google.com/earth-engine/datasets/catalog/MODIS_061_MOD09GA
MOD09GA = ee.ImageCollection("MODIS/061/MOD09GA") \
              .filter(ee.Filter.date(start_date, end_date))


# Merge qa bands from other collections to this MOD13Q1 collection
MOD13Q1 = MOD13Q1.linkCollection(MOD09GA,['SolarAzimuth'])


# print('MOD13Q1 size', MOD13Q1.size())
# Map.addLayer(MOD13Q1, {'bands': ['sur_refl_b07',  'sur_refl_b02',  'sur_refl_b01'], 'min': 0, 'max': [3000, 4000, 3000]}, 'MOD13Q1 original', True)


##############################
# FUNCTIONS

# This function adds the masked-pixel-percentage (clouds, cloud shadows, QA masks) as a property to each image
def addMaskedPixelCount(image):
  # Count the number of all non-masked pixels
  statsMasked = image.select('sur_refl_b02').reduceRegion(
    reducer=ee.Reducer.count(),
    geometry=aoi_CH_simplified,
    scale=250,
    bestEffort=True,
    maxPixels=1e10,
    tileScale=4
  )
  dataPixels = statsMasked.getNumber('sur_refl_b02')

  # Remove the mask and count all pixels
  statsAll = image.select('sur_refl_b02').unmask().reduceRegion(
    reducer=ee.Reducer.count(),
    geometry=aoi_CH_simplified,
    scale=250,
    bestEffort=True,
    maxPixels=1e10,
    tileScale=4
  )
  allPixels = statsAll.getNumber('sur_refl_b02')

  # Calculate the percentages and add the properties
  percData = (dataPixels.divide(allPixels)).multiply(1000).round().divide(10)
  percMasked = ee.Number(100).subtract(percData)

  return image.set({
    'percentData': percData,          # percentage of unmasked pixel
    'percentMasked': percMasked       # masked pixels include clouds, cloud shadows and QA pixels
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


# map the functions
MOD13Q1 = MOD13Q1.map(setSunAngles) \
                 .map(addMaskedPixelCount)


##############################
# TERRAIN SHADOWS

# This function detects terrain shadows
def addTerrainShadow(image):

  # get the solar position
  meanAzimuth = ee.Number(image.get('SolarAzimuth'))
  meanZenith = ee.Number(image.get('SolarZenith'))

  # Terrain shadow
  terrainShadow = ee.Terrain.hillShadow(DEM, meanAzimuth, meanZenith, 500, True)
  terrainShadow = terrainShadow.Not().rename('terrainShadowMask') # invert the binaries

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
  MOD13Q1 = MOD13Q1.map(addTerrainShadow)

  # # Map.addLayer(MOD13Q1.select('terrainShadowMask'), {}, 'MOD13Q1 terrain shadow mask', False)
  # Map.addLayer(MOD13Q1.select('terrainShadowFraction'), {}, 'MOD13Q1 terrain shadow fraction', False)




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
  img_plus_ic = ee.Image(img.addBands(ic.rename('TC_illumination')).addBands(cosZ.rename('cosZ')).addBands(cosS.rename('cosS')).addBands(slp.rename('slope')))
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
  bandList = ee.List(['sur_refl_b01','sur_refl_b02', 'sur_refl_b03', 'sur_refl_b07'])

  # This function quantifies the linear relation between illumination and reflectance and corrects for it
  def apply_SCSccorr(band):
    out = img_plus_ic_mask.select('TC_illumination', band).reduceRegion(
      reducer=ee.Reducer.linearFit(), # Compute coefficients: a(slope), b(offset), c(b/a)
      geometry=aoi_CH_simplified, # trim off the outer edges of the image for linear relationship
      scale=250,
      maxPixels=1e6,
      bestEffort=True,
      tileScale=16
    )
    out_c = ee.Number(out.get('offset')).divide(ee.Number(out.get('scale')))
    #apply the SCSc correction
    SCSc_output = img_plus_ic_mask.expression("((image * (cosB * cosZ + cvalue)) / (ic + cvalue))", {
      'image': img_plus_ic_mask.select([band, ]),
      'ic': img_plus_ic_mask.select('TC_illumination'),
      'cosB': img_plus_ic_mask.select('cosS'),
      'cosZ': img_plus_ic_mask.select('cosZ'),
      'cvalue': out_c
    })
    return ee.Image(SCSc_output)

  # list all bands without topographic correction (to be added to the TC image)
  bandsWithoutTC = ee.List(['NDVI', 'EVI', 'DetailedQA', 'ViewZenith', 'SolarZenith', 'SolarAzimuth', 'RelativeAzimuth'	, 'DayOfYear'	, 'SummaryQA', 'terrainShadowMask', 'terrainShadowFraction'])

  # Take care of dependencies between switches
  if terrainShadowDetection is False:
    # remove the bands from the co-registration
    bandsWithoutTC = bandsWithoutTC.remove('terrainShadowMask').remove('terrainShadowFraction')

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
      .reproject(crs=MOD13Q1.first().select('sur_refl_b01').projection())

  # apply the topographic correction function
  MOD13Q1 = MOD13Q1.map(topoCorr_MOD) \
                   .map(topoCorr_SCSc_MOD)
  # print('MOD13Q1 size after mosaic', MOD13Q1.size())

  # Map.addLayer(MOD13Q1.first(), vis_fci, 'MOD13Q1 TC', False)
  # if this error message "Tile error: Output of image computation" appears, aim for a higher zoom level



##############################
# EXPORT

# This function converts the data type of the topographically corrected images
def dataType(image):
  return image.addBands(image.select(['sur_refl_b01','sur_refl_b02', 'sur_refl_b03', 'sur_refl_b07']).round().toInt16(),  None, True)


# data type conversion
MOD13Q1 = MOD13Q1.map(dataType)


# convert image collection to image (used in export)
img_exp = ee.Image(MOD13Q1.first())
# Map.addLayer(img_exp, vis_fci, 'MOD13Q1 export', False)

# extract the image properties
img_exp_properties = ee.FeatureCollection([ee.Feature(img_exp.select([]))])

# extract the date and time
# advance to T10:15:00 for Switzerland (45degN) as the GEE MODIS Terra time is set to T00:00:00 UTC due to its global mosaic
sensing_date = img_exp.date().advance(10.25,'hour').format('YYYY-MM-dd_hh-mm-ss').getInfo()
sensing_date_read = sensing_date[0:10] + '_T' + sensing_date[11:19]

# define the filenames
fname_all = 'MOD13Q1_' + sensing_date_read + '_All'
fname_VIs = 'MOD13Q1_' + sensing_date_read + '_VIs-250m'            # ['NDVI', 'EVI']
fname_250m = 'MOD13Q1_' + sensing_date_read + '_Bands-250m'         # ['sur_refl_b01','sur_refl_b02', 'sur_refl_b03', 'sur_refl_b07']
fname_masks = 'MOD13Q1_' + sensing_date_read + '_Masks-250m'        # ['terrainShadowMask', 'TC_mask']
fname_TSF = 'MOD13Q1_' + sensing_date_read + '_TSF-250m'            # ['terrainShadowFraction']
fname_QAbands = 'MOD13Q1_' + sensing_date_read + '_Bands-QA'        # ['ViewZenith', 'SolarZenith', 'SolarAzimuth', 'RelativeAzimuth'	, 'DayOfYear'	, 'SummaryQA']
fname_properties = 'MOD13Q1_' + sensing_date_read + '_properties'   # ["SolarAzimuth", "SolarZenith", "percentData", "percentMasked", "system:asset_size", "system:footprint", "system:time_start", "system:time_end", "system:index"]


# define the export aoi
# the full mosaic image geometry covers larger areas outside Switzerland that are not needed
aoi_img = img_exp.geometry()
# therefore it is clipped with rectangle aoi of Switzerland to keep the geometry simple
# the alternative clip with aoi_CH would be computationally heavier
aoi_exp = aoi_img.intersection(aoi_CH_simplified) # alternativ': aoi_CH
# print('aoi_exp', aoi_exp)
# Map.addLayer(aoi_exp, {}, 'aoi export', False)

# SWITCH export
if exportAllToAsset is True:
  task = ee.batch.Export.image.toAsset(
    image=img_exp,
    scale=10,
    description=fname_all,
    crs='EPSG:2056',
    region=aoi_exp,
    maxPixels=1e10,
    assetId=fname_all,
  )
  task.start()


# SWITCH export
if exportVIsBands is True:
  # Export 250 m spectral bands
  task = ee.batch.Export.image.toDrive(
    image=img_exp.select(['NDVI', 'EVI']),
    scale=250,
    description=fname_VIs,
    crs= 'EPSG:2056',
    region= aoi_exp,
    maxPixels= 1e10,
    folder= 'eeExports',
    skipEmptyTiles= True,
    fileFormat= 'GeoTIFF',
    formatOptions= {'cloudOptimized': True}
  )
  task.start()


# SWITCH export
if export250mBands is True:
  # Export 250 m spectral bands
  task = ee.batch.Export.image.toDrive(
    image=img_exp.select(['sur_refl_b01','sur_refl_b02', 'sur_refl_b03', 'sur_refl_b07']),
    scale=250,
    description=fname_250m,
    crs= 'EPSG:2056',
    region= aoi_exp,
    maxPixels= 1e10,
    folder= 'eeExports',
    skipEmptyTiles= True,
    fileFormat= 'GeoTIFF',
    formatOptions= {'cloudOptimized': True}
  )
  task.start()


# SWITCH export
if exportMasks is True:
  # Export masks
  task = ee.batch.Export.image.toDrive(
    image=img_exp.select(['terrainShadowMask', 'TC_mask']),
    scale=250,
    description=fname_masks,
    crs= 'EPSG:2056',
    region= aoi_exp,
    maxPixels= 1e10,
    folder= 'eeExports',
    skipEmptyTiles= True,
    fileFormat= 'GeoTIFF',
    formatOptions= {'cloudOptimized': True}
  )
  task.start()


# SWITCH export
if exportTSF is True:
  # Export masks
  task = ee.batch.Export.image.toDrive(
    image=img_exp.select(['terrainShadowFraction']),
    scale=500,
    description=fname_TSF,
    crs= 'EPSG:2056',
    region= aoi_exp,
    maxPixels= 1e10,
    folder= 'eeExports',
    skipEmptyTiles= True,
    fileFormat= 'GeoTIFF',
    formatOptions= {'cloudOptimized': True}
  )
  task.start()


# SWITCH export
if exportQAbands is True:
  # Export QA layers
  task = ee.batch.Export.image.toDrive(
    image=img_exp.select(['ViewZenith', 'SolarZenith', 'SolarAzimuth', 'RelativeAzimuth'	, 'DayOfYear'	, 'SummaryQA']),
    scale=250,
    description=fname_QAbands,
    crs= 'EPSG:2056',
    region= aoi_exp,
    maxPixels= 1e10,
    folder= 'eeExports',
    skipEmptyTiles= True,
    fileFormat= 'GeoTIFF',
    formatOptions= {'cloudOptimized': True}
  )
  task.start()


# SWITCH export
if exportProperties is True:
  # Export image properties
  task = ee.batch.Export.table.toDrive(
    collection= img_exp_properties,
    description=fname_properties,
    fileFormat='CSV'
  )
  task.start()
