import os
import math
from datetime import datetime, timedelta
import json
import pandas as pd
import ee
import configuration as config



# Pre-processing pipeline for daily Sentinel-2 L1C top-of-atmosphere (toa) mosaics over Switzerland

##############################
# INTRODUCTION
# This script provides a tool to preprocess Sentinel-2 L1C top-of-atmosphere (toa) data over Switzerland.
# It can mask clouds and cloud shadows, detect terrain shadows, mosaic images from the same image swath,
# co-register images to the Sentinel-2 Global Reference Image, and topographically correct images
# and export the results.
#

##############################
# Discussion points
#
# - export aoi on extended Switzerland (rectangle) instead of admin boundaries (cost effective)
# - EECU seconds are not consistent for the same export at different times
# - data availibility threshold ('percentData') at 2%
# - export to asset and drive as options
# - export band names - keep as original (differences within the Landsat program, different script for L5+7?)
#


##############################
# CONTENT
# This script includes the following steps:
# 1. Masking clouds and cloud shadows
# 2. Detecting terrain shadows
# 3. Mosaicing of images from the same day (=same orbital track) over Switzerland
# 4. Registering the S2 Mosaic to the Sentinel-2 global reference image
# 5. Applying a topographic correction (SCSc-correction) to the spectral bands
# 6. Exporting spectral bands, additional layers and relevant properties
#
# The script is set up to export one mosaic image per day.


##############################
# SWITCHES
# The switches enable / disable the execution of individual steps in this script

# options': True, False - defines if individual clouds and cloud shadows are masked
cloudMasking = True
# options: True, False - defines if a cast shadow mask is applied
terrainShadowDetection = True
# options': True, False - defines if individual scenes get mosaiced to an image swath
swathMosaic = True
# options': True, False - defines if a the image coregistration is applied
coRegistration = True
# options': True, False - defines if a topographic correction is applied to the image swath
topoCorrection = True

# Export switches
# options': True, False - defines if image with all bands is exported as an asset
exportAllToAsset = False
# options': True, 'False - defines if 10-m-bands are exported': 'B2','B3','B4','B8'
export10mBands = True
# options': True, 'False - defines if 20-m-bands are exported': 'B5','B6','B7','B8A','B11','B12'
export20mBands = False
# options': True, 'False - defines if 60-m-bands are exported': 'B1','B9','B10'
export60mBands = False
# options': True, 'False - defines if registration layers are exported': 'reg_dx','reg_dy', 'reg_confidence'
exportRegLayers = True
# options': True, 'False - defines if masks are exported': 'terrainShadowMask','cloudAndCloudShadowMask'
exportMasks = True
# options': True, 'False - defines if S2 cloud probability layer is exported': 'cloudProbability'
exportS2cloud = True

def write_asset_as_empty(collection, day_to_process, remark):
    print('Cutting asset create for {} / {}'.format(collection, day_to_process))
    print('Reason: {}'.format(remark))
    collection_name = os.path.basename(collection)
    df = pd.DataFrame([(collection_name, day_to_process, remark)])
    df.to_csv(config.EMPTY_ASSET_LIST, mode='a', header=False, index=False)

def generate_asset_mosaic_for_single_date(day_to_process: str, collection: str, task_description: str) -> None:

    ##############################
    # TIME
    # define a date or use the current date: ee.Date(Date.now())
    start_date = ee.Date(day_to_process)
    end_date = ee.Date(day_to_process).advance(1, 'day')

    ##############################
    # SPACE

    # Official swisstopo boundaries
    # source: https:#www.swisstopo.admin.ch/de/geodata/landscape/boundaries3d.html#download
    # processing: reprojected in QGIS to epsg32632
    aoi_CH = ee.FeatureCollection("users/wulf/SATROMO/swissBOUNDARIES3D_1_4_TLM_LANDESGEBIET_epsg32632").geometry()

    # Region (extended Switzerland) to simplify processing
    aoi_CH_rectangle = ee.Geometry.Rectangle(5.9, 45.7, 10.6, 47.9)
    # clipping on complex shapefiles costs more processing resources and can cause memory issues


    ##############################
    # VISUALISATION
    vis_nfci = {'bands': ['B12', 'B8', 'B4'], 'min': [500, 500, 500], 'max': [4000, 5000, 3000]}

    ##############################
    # REFERENCE DATA

    # Sentinel-2 Global Reference Image (contains the red spectral band in 10 m resolution)
    # source: https:#s2gri.csgroup.space
    # processing: GDAL merge and warp (reproject) to epsg32632
    S2_gri = ee.Image("users/wulf/SATROMO/S2_GRI_CH_epsg32632")
    # Map.addLayer(S2_gri, {min: 0, max: 2000, bands:'b1'}, 'Sentinel-2 global reference image', False)

    # SwissALTI3d - very precise digital terrain model in a 10 m resolution
    # source: https:#www.swisstopo.admin.ch/de/geodata/height/alti3d.html#download (inside CH)
    # source: https:#www.swisstopo.admin.ch/de/geodata/height/dhm25.html#download (outside CH)
    # processing: resampling both to 10 m resolution, GDAL merge of SwissALTI3d on DHM25, GDAL warp (reproject) to epsg32632
    DEM_sa3d = ee.Image("users/wulf/SATROMO/SwissALTI3d_20kmBuffer_epsg32632")
    # Map.addLayer(DEM_sa3d, {min: 0, max: 4000}, 'swissALTI3d', False)

    ##############################
    # SATELLITE DATA

    # Sentinel-2
    S2_toa = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') \
        .filter(ee.Filter.bounds(aoi_CH)) \
        .filter(ee.Filter.date(start_date, end_date))

    image_list_size = S2_toa.size().getInfo()
    if image_list_size == 0:
        write_asset_as_empty(collection, day_to_process, 'No scene')
        return

    image_list = S2_toa.toList(S2_toa.size())
    for i in range(image_list_size):
        image = ee.Image(image_list.get(i))

        # EE asset ids for Sentinel-2 L2 assets have the following format: 20151128T002653_20151128T102149_T56MNN.
        #  Here the first numeric part represents the sensing date and time, the second numeric part represents the product generation date and time,
        #  and the final 6-character string is a unique granule identifier indicating its UTM grid reference
        image_id = image.id().getInfo()
        image_sensing_timestamp = image_id.split('_')[0]
        # first numeric part represents the sensing date, needs to be used in publisher
        print("generating json {} of {} ({})".format(i+1, image_list_size, image_sensing_timestamp))

        # Generate the filename
        filename = config.PRODUCT_S2_LEVEL_2A['prefix'] + '_' + image_id
        # Export Image Properties into a json file
        file_name = filename + "_properties" + "_run" + day_to_process.replace("-", "") + ".json"
        json_path = os.path.join(config.PROCESSING_DIR, file_name)
        with open(json_path, "w") as json_file:
            json.dump(image.getInfo(), json_file)

    length_without_clouds = S2_toa.size().getInfo()
    if length_without_clouds == 0:
        write_asset_as_empty(collection, day_to_process, 'No candidate scene')
        return

    # .filter(ee.Filter.eq('SENSING_ORBIT_NUMBER', 65))     # orbits covering parts of Switzerland (W-E): 8, 108, 65, 22

    # S2cloudless
    S2_clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
        .filter(ee.Filter.bounds(aoi_CH)) \
        .filter(ee.Filter.date(start_date, end_date))

    ###########################
    # WATER MASK
    # The water mask is used to limit a buffering operation on the cast shadow mask.
    # Here, it helps to better distinguish between dark areas and water bodies.
    # This distinction is also used to limit the cloud shadow propagation.
    # EU-Hydro River Network Database 2006-2012 data is derived from this data source:
    # https:#land.copernicus.eu/en/products/eu-hydro/eu-hydro-river-network-database#download
    # processing: reprojected in QGIS to epsg32632

    # Lakes
    lakes = ee.FeatureCollection("users/wulf/SATROMO/CH_inlandWater")

    # vector-to-image conversion based on the area attribute
    lakes_img = lakes.reduceToImage(
        properties=['AREA'],
        reducer=ee.Reducer.first()
    )

    # Make a binary mask and clip to area of interest
    lakes_binary = lakes_img.gt(0).unmask().clip(aoi_CH_rectangle)
    # Map.addLayer(lakes_binary, {min:0, max:1}, 'lake mask', False)

    # Rivers
    rivers = ee.FeatureCollection("users/wulf/SATROMO/CH_RiverNet")

    # vector-to-image conversion based on the area attribute.
    rivers_img = rivers.reduceToImage(
        properties=['AREA_GEO'],
        reducer=ee.Reducer.first()
    )

    # Make a binary mask and clip to area of interest
    rivers_binary = rivers_img.gt(0).unmask().clip(aoi_CH_rectangle)
    # Map.addLayer(rivers_binary, {min:0, max:1}, 'river mask', False)

    # combine both water masks
    water_binary = rivers_binary.Or(lakes_binary)


    # Map.addLayer(water_binary, {min:0, max:1}, 'water mask', False)

    ##############################
    # FUNCTIONS

    # This function detects clouds and cloud shadows, masks all spectral bands for them, and adds the mask as an additional layer
    def maskCloudsAndShadows(image):
        # get the solar position
        meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
        meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

        # get the cloud probability
        clouds = ee.Image(image.get('cloud_mask')).select('probability')
        # the maximum cloud probability threshold is set at 50
        isNotCloud = clouds.lt(50)
        cloudMask = isNotCloud.Not()
        # Opening operation: individual pixels are deleted (localMin) and buffered (localMax) to also capture semi-transparent cloud edges
        cloudMask = cloudMask.focalMin(50, 'circle', 'meters', 1, None).focalMax(100, 'circle', 'meters', 1, None)

        # Find dark pixels but exclude lakes and rivers (otherwise projected shadows would cover large parts of water bodies)
        darkPixels = image.select(['B8', 'B11', 'B12']).reduce(ee.Reducer.sum()).lt(2500).subtract(water_binary).clamp(0, 1)

        # Project shadows from clouds. This step assumes we're working in a UTM projection.
        shadowAzimuth = ee.Number(90).subtract(ee.Number(meanAzimuth))
        # shadow distance is tied to the solar zenith angle (minimum shadowDistance is 30 pixel)
        shadowDistance = ee.Number(meanZenith).multiply(0.7).floor().int().max(30)

        # With the following algorithm, cloud shadows are projected.
        isCloud = cloudMask.directionalDistanceTransform(shadowAzimuth, shadowDistance)
        isCloud = isCloud.reproject(crs=image.select('B2').projection(), scale=100)

        cloudShadow = isCloud.select('distance').mask()

        # combine projectedShadows & darkPixel and buffer the cloud shadow
        cloudShadow = cloudShadow.And(darkPixels).focalMax(100, 'circle', 'meters', 1, None)

        # combined mask for clouds and cloud shadows
        cloudAndCloudShadowMask = cloudShadow.Or(cloudMask)

        # mask spectral bands for clouds and cloudShadows
        image_out = image.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B9', 'B10', 'B11', 'B12']) \
            .updateMask(cloudAndCloudShadowMask.Not())

        # adding the additional S2 L2A layers, S2 cloudProbability and cloudAndCloudShadowMask as additional bands
        image_out = image_out.addBands(clouds.rename(['cloudProbability'])) \
            .addBands(cloudAndCloudShadowMask.rename(['cloudAndCloudShadowMask']))

        return image_out


    # This function detects and adds terrain shadows
    def addTerrainShadow(image):
        # get the solar position
        meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
        meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

        # Find dark pixels but exclude lakes
        darkPixels = image.select(['B8', 'B11', 'B12']).reduce(ee.Reducer.sum()).lt(2500)

        # Terrain shadow
        terrainShadow = ee.Terrain.hillShadow(DEM_sa3d, meanAzimuth, meanZenith, 100, True)
        terrainShadow = terrainShadow.Not()  # invert the binaries

        # buffering the terrain shadow
        terrainShadow_buffer = terrainShadow.focalMax(200, 'circle', 'meters', 1, None)

        # extracting the terrain shadow buffer
        shadowBuffer = terrainShadow_buffer.subtract(terrainShadow)

        # removing dark water pixels from the buffer (as water is part of the darkPixels class, we exclude it from the buffer)
        shadowBuffer = shadowBuffer.subtract(water_binary).clamp(0, 1)

        # add the new buffer
        terrainShadow_bufferNoWater = terrainShadow.add(shadowBuffer).clamp(0, 1)

        # combining castShadow and darkPixels (only from the buffer region)
        terrainShadow_darkPixels = terrainShadow_bufferNoWater.And(darkPixels).Or(terrainShadow).rename('terrainShadowMask')

        # add the additonal terrainShadow band
        image = image.addBands(terrainShadow_darkPixels)

        return image


    # This function adds the masked-pixel-percentage (clouds, cloud shadows, QA masks) as a property to each image
    def addMaskedPixelCount(image):
        # Count the number of all non-masked pixels
        statsMasked = image.select('B2').reduceRegion(
            reducer= ee.Reducer.count(),
            geometry= image.geometry(),
            scale= 100,
            bestEffort= True,
            maxPixels= 1e10,
            tileScale= 4
        )
        dataPixels = statsMasked.getNumber('B2')

        # Remove the mask and count all pixels
        statsAll = image.select('B2').unmask().reduceRegion(
            reducer= ee.Reducer.count(),
            geometry= image.geometry(),
            scale= 100,
            bestEffort= True,
            maxPixels= 1e10,
            tileScale= 4
        )
        allPixels = statsAll.getNumber('B2')

        # Calculate the percentages and add the properties
        percData = (dataPixels.divide(allPixels)).multiply(1000).round().divide(10)
        percMasked = ee.Number(100).subtract(percData)

        return image.set({
            'percentData': percData,  # percentage of unmasked pixel
            'percentMasked': percMasked  # masked pixels include clouds, cloud shadows and QA pixels
        })


    # This function masks all bands to the same extent as the 20 m and 60 m bands
    def maskEdges(s2_img):
        return s2_img.updateMask(
            s2_img.select('B8A').mask().updateMask(s2_img.select('B9').mask()))


    # This function sets the date as an additional property to each image
    def set_date(img):
        date = img.date().format('YYYY-MM-dd')
        return img.set('date', date)


    ##############################
    # PROCESSING

    # Map the date and edges functions
    S2_toa = S2_toa.map(maskEdges) \
        .map(set_date)

    # Join S2 toa with cloud probability dataset to add cloud mask.
    S2_toaWithCloudMask = ee.Join.saveFirst('cloud_mask').apply(
        primary=S2_toa,
        secondary=S2_clouds,
        condition=ee.Filter.equals(leftField='system:index', rightField='system:index')
    )

    # SWITCH
    if cloudMasking is True:
        print('--- Cloud and cloud shadow masking applied ---')
        # apply the cloud mapping and masking functions
        S2_toa = ee.ImageCollection(S2_toaWithCloudMask).map(maskCloudsAndShadows)

    # SWITCH
    if terrainShadowDetection is True:
        print('--- Terrain shadow detection applied ---')
        # apply the terrain shadow function
        S2_toa = S2_toa.map(addTerrainShadow)

    ###########################/
    # MOSAIC
    # This step mosaics overlapping Sentinel-2 tiles acquired on the same day

    # 'distinct' removes duplicates from a collection based on a property.
    distinctDates_S2_toa = S2_toa.distinct('date').sort('date')

    # define the filter
    filter = ee.Filter.equals(leftField='date', rightField='date')

    # 'ee.Join.saveAll' Returns a join that pairs each element from the first collection with a group of matching elements from the second collection
    # the matching images are stored in a new property called 'date_match'
    join = ee.Join.saveAll('date_match')

    # 'apply' Joins to collections.
    joinCol_S2_toa = join.apply(distinctDates_S2_toa, S2_toa, filter)


    # This function mosaics image acquired on the same day (same image swath)
    def mosaic_collection(img):
        # create a collection of the date-matching images
        col = ee.ImageCollection.fromImages(img.get('date_match'))

        # extract collection properties to assign to the mosaic
        time_start = col.aggregate_min('system:time_start')
        time_end = col.aggregate_max('system:time_end')
        index_list = col.aggregate_array('system:index')
        index_list = index_list.join(',')
        scene_count = col.size()

        # get the unified geometry of the collection (outer boundary)
        col_geo = col.geometry().dissolve()

        # clip the mosaic to set a geometry to it
        mosaic = col.mosaic().clip(col_geo).copyProperties(img, ["system:time_start", "system:index", "date", "month",
                                                                 "SENSING_ORBIT_NUMBER", "PROCESSING_BASELINE",
                                                                 "SPACECRAFT_NAME", "MEAN_SOLAR_ZENITH_ANGLE",
                                                                 "MEAN_SOLAR_AZIMUTH_ANGLE"])

        # set the extracted properties to the mosaic
        mosaic = mosaic.set('system:time_start', time_start) \
            .set('system:time_end', time_end) \
            .set('index_list', index_list) \
            .set('scene_count', scene_count)

        # reset the projection to epsg:32632 as mosaic changes it to epsg:4326 (otherwise the registration fails)
        mosaic = ee.Image(mosaic).setDefaultProjection('epsg:32632', None, 10)
        return mosaic


    # SWITCH
    if swathMosaic is True:
        print('--- Image swath mosaicing applied ---')
        # apply the mosaicing and maskPixelCount function
        S2_toa = ee.ImageCollection(joinCol_S2_toa.map(mosaic_collection)).map(addMaskedPixelCount)
        # filter for data availability
        S2_toa = S2_toa.filter(ee.Filter.gte('percentData', 2))
        length_without_clouds = S2_toa.size().getInfo()
        if length_without_clouds == 0:
            write_asset_as_empty(collection, day_to_process, 'cloudy')
            return

        S2_toa = S2_toa.first()


    ##############################
    # REGISTER

    # This function co-registers Sentinel-2 images to the Sentinel-2 global reference image
    def S2regFunc(image):
        # Use bicubic resampling during registration.
        imageOrig = image.resample('bicubic')

        # Choose to register using only the 'R' band.
        imageRedBand = imageOrig.select('B4')

        # Determine the displacement by matching only the 'R' bands.
        displacement = imageRedBand.displacement(
            referenceImage=S2_gri,
            maxOffset=10,
            patchWidth=300,
            stiffness=8
        )

        # Extract relevant displacement parameters
        reg_dx = displacement.select('dx').rename('reg_dx')
        reg_dy = displacement.select('dy').rename('reg_dy')
        reg_confidence = displacement.select('confidence').rename('reg_confidence')

        # Use the computed displacement to register all original bands.
        registered = image.displace(displacement) \
            .addBands(reg_dx) \
            .addBands(reg_dy) \
            .addBands(reg_confidence)
        return registered

    # SWITCH
    if coRegistration is True:
        print('--- Image swath co-registration applied ---')
        # apply the registration function
        S2_toa = S2regFunc(S2_toa)

    ##############################
    # TOPOGRAPHIC CORRECTION
    # This step compensates for the effects of terrain elevation, slope, and solar illumination variations.
    # The method is based on Soenen et al. 2005 (https:#ieeexplore.ieee.Org/document/1499030)

    # This function calculates the illumination condition during the time of image acquisition
    def topoCorr_S2(img):
        # Extract image metadata about solar position and covert from degree to radians
        SZ_rad = ee.Image.constant(ee.Number(img.get('MEAN_SOLAR_ZENITH_ANGLE'))).multiply(math.pi).divide(180)
        SA_rad = ee.Image.constant(ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE'))).multiply(math.pi).divide(180)

        # Creat terrain layers and covert from degree to radians
        slp = ee.Terrain.slope(DEM_sa3d)
        slp_rad = ee.Terrain.slope(DEM_sa3d).multiply(math.pi).divide(180)
        asp_rad = ee.Terrain.aspect(DEM_sa3d).multiply(math.pi).divide(180)

        # Calculate the Illumination Condition
        # slope part of the illumination condition
        cosZ = SZ_rad.cos()
        cosS = slp_rad.cos()
        slope_illumination = cosS.select('slope').multiply(cosZ)

        # aspect part of the illumination condition
        sinZ = SZ_rad.sin()
        sinS = slp_rad.sin()
        cosAziDiff = (SA_rad.subtract(asp_rad)).cos()
        aspect_illumination = sinZ.multiply(sinS).multiply(cosAziDiff)

        # full illumination condition
        ic = slope_illumination.add(aspect_illumination)

        # Add the illumination condition to original image
        img_plus_ic = ee.Image(
            img.addBands(ic.rename('TC_illumination')).addBands(cosZ.rename('cosZ')).addBands(cosS.rename('cosS')).addBands(
                slp.rename('slope')))
        return img_plus_ic


    # This function applies the sun-canopy-sensor+C topographic correction (Soenen et al. 2005)
    def topoCorr_SCSc_S2(img):
        img_plus_ic = img

        # masking flat, shadowed, and incorrect pixels (these get excluded from the topographic correction)
        mask = img_plus_ic.select('slope').gte(5) \
            .And(img_plus_ic.select('TC_illumination').gte(0)) \
            .And(img_plus_ic.select('B8').gt(-0.1))
        img_plus_ic_mask = ee.Image(img_plus_ic.updateMask(mask))

        # Specify Bands to topographically correct
        bandList = ee.List(['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12'])

        # This function quantifies the linear relation between illumination and reflectance and corrects for it
        def apply_SCSccorr(band):
            # Compute coefficients': a(slope), b(offset), c(b/a)
            out = img_plus_ic_mask.select('TC_illumination', band).reduceRegion(
                reducer=ee.Reducer.linearFit(),
                geometry=ee.Geometry(img.geometry().buffer(-5000)),
                # trim off the outer edges of the image for linear relationship
                scale=20,
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
            ['B1', 'B9', 'B10', 'cloudProbability', 'cloudAndCloudShadowMask', 'terrainShadowMask', 'TC_illumination',
             'reg_dx', 'reg_dy', 'reg_confidence'])

        # Take care of dependencies between switches
        if coRegistration is False:
            # remove the bands from the co-registration
            bandsWithoutTC = bandsWithoutTC.remove('reg_dx').remove('reg_dy').remove('reg_confidence')

        if terrainShadowDetection is False:
            # remove the bands from the co-registration
            bandsWithoutTC = bandsWithoutTC.remove('terrainShadowMask')

        if cloudMasking is False:
            # remove the bands from the co-registration
            bandsWithoutTC = bandsWithoutTC.remove('cloudProbability').remove('cloudAndCloudShadowMask')

        bandsWithoutTC.getInfo()
        # add all bands and properties to the TC bands
        img_SCSccorr = ee.ImageCollection.fromImages(bandList.map(apply_SCSccorr)).toBands().rename(bandList)
        img_SCSccorr = img_SCSccorr.addBands(img_plus_ic.select(bandsWithoutTC))
        img_SCSccorr = img_SCSccorr.copyProperties(img_plus_ic, img_plus_ic.propertyNames())

        # flatten both lists into one
        bandList_IC = ee.List([bandList, bandsWithoutTC]).flatten()

        # unmasked the uncorrected pixel using the orignal image
        return ee.Image(img_SCSccorr).unmask(img_plus_ic.select(bandList_IC)).addBands(mask.rename('TC_mask'))

    # SWITCH
    if topoCorrection is True:
        print('--- Topographic correction applied ---')
        # apply the topographic correction function
        S2_toa = topoCorr_S2(S2_toa)
        S2_toa = topoCorr_SCSc_S2(S2_toa)

    ##############################
    # EXPORT

    # extract the date and time (it is same time for all images in the mosaic)
    sensing_date = S2_toa.get('system:index').getInfo()[0:15]
    sensing_date_read = sensing_date[0:4] + '-' + sensing_date[4:6] + '-' + sensing_date[6:8] \
                        + '_' + sensing_date[8:11] + '-' + sensing_date[11:13] + '-' + sensing_date[13:15]

    # define the filenames
    fname_10m = 'S2-L1C_Mosaic_' + sensing_date_read + '_Bands-10m'
    fname_20m = 'S2-L1C_Mosaic_' + sensing_date_read + '_Bands-20m'
    fname_60m = 'S2-L1C_Mosaic_' + sensing_date_read + '_Bands-60m'
    fname_reg = 'S2-L1C_Mosaic_' + sensing_date_read + '_Registration-10m'
    fname_masks = 'S2-L1C_Mosaic_' + sensing_date_read + '_Masks-10m';  # cloud mask + cloud shadow mask, terrain shadow mask
    fname_properties = 'S2-L1C_Mosaic_' + sensing_date_read + '_properties'
    fname_cloudP = 'S2_Mosaic_' + sensing_date_read + '_CloudProbability-10m'

    # define the export aoi
    # the full mosaic image geometry covers larger areas outside Switzerland that are not needed
    aoi_img = S2_toa.geometry()
    # therefore it is clipped with rectangle aoi of Switzerland to keep the geometry simple
    # the alternative clip with aoi_CH would be computationally heavier
    aoi_exp = aoi_img.intersection(aoi_CH_rectangle)  # alternativ: aoi_CH

    # SWITCH export
    if exportAllToAsset is True:
        task = ee.batch.Export.image.toAsset(
            image=S2_toa,
            scale=10,
            description=fname_10m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e10,
            assetId=fname_10m,
        )
        task.start()

    # SWITCH export
    if export10mBands is True:
        print('Launching export for 10m bands')
        band_list = ['B2', 'B3', 'B4', 'B8']
        if exportMasks:
            band_list.extend(['terrainShadowMask', 'cloudAndCloudShadowMask'])
        if exportRegLayers:
            band_list.extend(['reg_dx', 'reg_dy', 'reg_confidence'])
        if exportS2cloud:
            band_list.extend(['cloudProbability'])
        print('Band list: {}'.format(band_list))
        # Export COG 10m bands
        task = ee.batch.Export.image.toAsset(
            image=S2_toa.select(band_list),
            scale=10,
            description=task_description,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e10,
            assetId=collection + '/' + fname_10m,
        )
        task.start()

    # SWITCH export
    if export20mBands is True:
        # Export COG 20m bands
        task = ee.batch.Export.image.toDrive(
            image=S2_toa.select(['B5', 'B6', 'B7', 'B8A', 'B11', 'B12']),
            scale=20,
            description=fname_20m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e10,
            assetId=fname_20m
        )
        task.start()

    # SWITCH export
    if export60mBands is True:
        task = ee.batch.Export.image.toDrive(
            image=S2_toa.select(['B1', 'B9', 'B10']),
            scale=60,
            description=fname_60m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e10,
            assetId=fname_60m
        )
        task.start()


if __name__ == '__main__':
    start_date = datetime(2023, 1, 25)
    end_date = datetime(2023, 1, 25)
    delta = timedelta(days=1)
    while start_date <= end_date:
        print(start_date.strftime("%Y-%m-%d"))
        generate_asset_mosaic_for_single_date(day_to_process=start_date, collection='projects/satromo-exolabs/assets/col1')
        start_date += delta
