import ee
from .step0_utils import write_asset_as_empty
from main_functions import main_utils
import math


# Pre-processing pipeline for daily Landsat 5/7 surface reflectance (sr) mosaics over Switzerland
# TODO :
# - export Spatial resolution wise to asset as for S2 SR -> Decision
# - multiply / cast 32bit/float bands to 16int
# - rename asset export


##############################
# INTRODUCTION
#
# This script provides a tool to preprocess Landsat 5/7 SR (surface reflectance) mosaics over Switzerland.
# It can mask clouds and cloud shadows, detect terrain shadows, mosaic images from the same image swath,
# topographically correct images and export the results.
#


##############################
# CONTENT

# This script includes the following steps:
# 1. Masking clouds and cloud shadows
# 2. Detecting terrain shadows
# 3. Mosaicing of images from the same day (=same orbital track) over Switzerland
# 4. Applying a topographic correction (SCSc-correction) to the spectral bands
# 5. Exporting spectral bands, additional layers and relevant properties
#
# The script is set up to export one mosaic image per day.

def generate_l57_sr_mosaic_for_single_date(day_to_process: str, collection: str, task_description: str) -> None:
    ##############################
    # SWITCHES
    # The switches enable / disable the execution of individual steps in this script

    # Export switches
    # options': True, False - defines if individual clouds and cloud shadows are masked
    cloudMasking = True
    # options': True, False - defines if a cast shadow mask is applied
    terrainShadowDetection = True
    # options': True, False - defines if individual scenes get mosaiced to an image swath
    swathMosaic = True
    # options': True, False - defines if a topographic correction is applied to the image swath
    topoCorrection = True

    # options': True, False - defines if image with all bands is exported as an asset
    exportAllToAsset = True
    # options': True, 'False - defines if 30 m spectral bands are exported': 'B1','B2','B3','B4','B5','B7'
    export30mBands = True
    # options': True, 'False - defines if 100 m thermal bands are exported': 'B6_VCID_1','B6_VCID_2'
    export60mBands = True
    exportMasks = True  # options': True, 'False - defines if masks are exported': 'terrainShadowMask','cloudAndCloudShadowMask', 'TC_mask'
    # options': True, 'False - defines if Landsat QA bands are exported': 'QA_PIXEL', 'QA_RADSAT'
    exportQAbands = True
    # options': True, False - defines if image properties are exported
    exportProperties = True

    ##############################
    # TIME
    # define a date or use the current date: ee.Date(Date.now())
    start_date = ee.Date(day_to_process)
    end_date = ee.Date(day_to_process).advance(1, 'day')

    ##############################
    # SPACE

    # Official swisstopo boundaries
    # source: https:#www.swisstopo.admin.ch/de/geodata/landscape/boundaries3d.html#download
    # processing: layer Landesgebiet dissolved  in QGIS and reprojected to epsg32632
    aoi_CH = ee.FeatureCollection(
        "projects/satromo-prod/assets/res/swissBOUNDARIES3D_1_5_TLM_LANDESGEBIET_dissolve_epsg32632").geometry()

    # Simplified and buffered shapefile of Switzerland to simplify processing
    aoi_CH_simplified = ee.FeatureCollection(
        "projects/satromo-prod/assets/res/CH_boundaries_buffer_5000m_epsg32632").geometry()
    # clipping on complex shapefiles costs more processing resources and can cause memory issues

    ##############################
    # REFERENCE DATA

    # SwissALTI3d - very precise digital terrain model in a 10 m resolution
    # source: https:#www.swisstopo.admin.ch/de/geodata/height/alti3d.html#download (inside CH)
    # source: https:#www.swisstopo.admin.ch/de/geodata/height/dhm25.html#download (outside CH)
    # processing: GDAL warp (reproject) to epsg32632 while resampling DHM25 to 10 m resolution, GDAL merge of SwissALTI3d on DHM25
    DEM_sa3d = ee.Image("projects/satromo-prod/assets/res/SwissALTI3d_20kmBuffer_epsg32632")
    # # Map.addLayer(DEM_sa3d, {min: 0, max: 4000}, 'swissALTI3d', False)

    ##############################
    # SATELLITE DATA

    # Landsat 5
    L5_sr = ee.ImageCollection("LANDSAT/LT05/C02/T1_L2") \
        .filter(ee.Filter.bounds(aoi_CH)) \
        .filter(ee.Filter.date(start_date, end_date)) \
        .filter(ee.Filter.lt('GEOMETRIC_RMSE_MODEL', 10)) \
        .filter(ee.Filter.Or(
            ee.Filter.eq('IMAGE_QUALITY', 9),
            ee.Filter.eq('IMAGE_QUALITY_OLI', 9)))

    # Landsat 7
    L7_sr = ee.ImageCollection("LANDSAT/LE07/C02/T1_L2") \
        .filter(ee.Filter.bounds(aoi_CH)) \
        .filter(ee.Filter.date(start_date, end_date)) \
        .filter(ee.Filter.lt('GEOMETRIC_RMSE_MODEL', 10)) \
        .filter(ee.Filter.Or(
            ee.Filter.eq('IMAGE_QUALITY', 9),
            ee.Filter.eq('IMAGE_QUALITY_OLI', 9)))

    L57_sr = L7_sr.merge(L5_sr)

    # Define if we have imagery for the selected day
    image_list_size = L57_sr.size().getInfo()
    if image_list_size == 0:
        write_asset_as_empty(collection, day_to_process, 'No candidate scene')
        return

    # Map.addLayer(L57_sr, {'bands': ['SR_B7',  'SR_B4',  'SR_B3'], 'min': 8000, 'max': 21000}, 'L57 original', False)

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
    lakes = ee.FeatureCollection("projects/satromo-prod/assets/res/CH_inlandWater")

    # vector-to-image conversion based on the area attribute
    lakes_img = lakes.reduceToImage(
        properties=['AREA'],
        reducer=ee.Reducer.first()
    )

    # Make a binary mask and clip to area of interest
    lakes_binary = lakes_img.gt(0).unmask().clip(aoi_CH_simplified)
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
        image_mask = image.select('cloudAndCloudShadowMask').gt(
            0).Or(image.select('terrainShadowMask').gt(0))

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
        percMasked = (dataPixels.divide(allPixels)).multiply(
            1000).round().divide(10)
        percData = ee.Number(100).subtract(percMasked)

        return image.set({
            'percentData': percData,  # percentage of useful pixel
            # less useful pixels including clouds, cloud shadows and terrain shadows
            'percentMasked': percMasked
        })

    ##############################
    # CLOUD MASKING

    # This function masks clouds & cloud shadows based on the QA quality bands of Landsat

    def maskCloudsAndShadowsL57sr(image):
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

    # SWITCH
    if cloudMasking is True:
        print('--- Cloud and cloud shadow masking applied ---')
        # apply the masking function
        L57_sr = L57_sr.map(maskCloudsAndShadowsL57sr)
        # Map.addLayer(L57_sr, vis_nfci, 'L57 cloud masked', False)

    ##############################
    # TERRAIN SHADOWS

    # This function detects terrain shadows

    def addTerrainShadow(image):
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

    # SWITCH
    if terrainShadowDetection is True:
        print('--- Terrain shadow detection applied ---')
        # apply the terrain shadow function
        L57_sr = L57_sr.map(addTerrainShadow)
        # Map.addLayer(L57_sr.select('terrainShadowMask'), {}, 'L57 terrain shadow mask', False)

    # /
    # MOSAIC

    # This step mosaics overlapping Landsat X tiles acquired on the same day

    # 'distinct' removes duplicates from a collection based on a property.
    distinctDates_L57_sr = L57_sr.distinct(
        'DATE_ACQUIRED').sort('DATE_ACQUIRED')

    # define the filter
    filter = ee.Filter.equals(
        leftField='DATE_ACQUIRED', rightField='DATE_ACQUIRED')

    # 'ee.Join.saveAll' Returns a join that pairs each element from the first collection with a group of matching elements from the second collection
    # the matching images are stored in a new property called 'date_match'
    join = ee.Join.saveAll('date_match')

    # 'apply' Joins to collections.
    joinCol_L57_sr = join.apply(distinctDates_L57_sr, L57_sr, filter)

    # function to mosaic matching images of the same day

    def mosaic_collection(img):
        orig = img

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

        mosaic = col.mosaic().clip(col_geo).copyProperties(img, ["system:time_start", "system:index", "SPACECRAFT_ID",
                                                                 "WRS_PATH", "SCENE_CENTER_TIME", "SENSOR_ID",
                                                                 "SUN_ELEVATION", "SUN_AZIMUTH", "EARTH_SUN_DISTANCE",
                                                                 "COLLECTION_CATEGORY", "COLLECTION_NUMBER",
                                                                 "DATA_SOURCE_ELEVATION", "DATE_ACQUIRED"])
        # Getting swisstopo Processor Version
        processor_version = main_utils.get_github_info()

        # Copy the "DATE_ACQUIRED" property and store it as "date" since date is need by Master Gilians check function
        date_acquired = mosaic.get("DATE_ACQUIRED")
        mosaic = mosaic.set("date", date_acquired)

        # set the extracted properties to the mosaic
        mosaic = mosaic.set('system:time_start', time_start) \
            .set('system:time_end', time_end) \
            .set('index_list', index_list) \
            .set('scene_count', scene_count) \
            .set('SWISSTOPO_PROCESSOR', processor_version['GithubLink']) \
            .set('SWISSTOPO_RELEASE_VERSION', processor_version['ReleaseVersion'])

        return mosaic

    # SWITCH
    if swathMosaic is True:
        print('--- Image swath mosaicing applied ---')

        # apply the mosaicing and maskPixelCount function
        L57_sr = ee.ImageCollection(joinCol_L57_sr.map(mosaic_collection))
        L57_sr = L57_sr.map(addMaskedPixelCount)

        # print('L57_sr size after mosaic', L57_sr.size())

        # display the mosaic
        # imgMosaic = ee.Image(L57_sr.first())
        # Map.addLayer(imgMosaic, vis_nfci, 'L57 mosaic', False)

        # filter for data availability: "'percentData', 2 " is 98% cloudfree. "'percentData', 20 " is 80% cloudfree.
        L57_sr = L57_sr.filter(ee.Filter.gte('percentData', 2))
        length_without_clouds = L57_sr.size().getInfo()
        if length_without_clouds == 0:
            write_asset_as_empty(collection, day_to_process, 'cloudy')
            return
        # This is the If condition the return just the line after the end the step0 script ends the process if 'percentData' is greater.
        # It's after the mosaic because the threshold (98% here) is applied on the whole mosaic and not per scene:
        # we decide together for the whole swath if we want to process it or not.



    ##############################
    # TOPOGRAPHIC CORRECTION
    # This step compensates for the effects of terrain elevation, slope, and solar illumination variations.
    # The method is based on Soenen et al. 2005 (https:#ieeexplore.ieee.Org/document/1499030)

    # This function calculates the illumination condition during the time of image acquisition

    def topoCorr_L57(img):
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

    def topoCorr_SCSc_L57(img):
        img_plus_ic = img

        # masking flat, shadowed, and incorrect pixels (these get excluded from the topographic correction)
        mask = img_plus_ic.select('slope').gte(5) \
            .And(img_plus_ic.select('TC_illumination').gte(0.1)) \
            .And(img_plus_ic.select('SR_B4').gt(-0.1))
        img_plus_ic_mask = ee.Image(img_plus_ic.updateMask(mask))

        # Specify Bands to topographically correct
        bandList = ee.List(
            ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'])

        # This function quantifies the linear relation between illumination and reflectance and corrects for it
        def apply_SCSccorr(band):
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
            ['ST_B6', 'cloudAndCloudShadowMask', 'QA_PIXEL', 'QA_RADSAT', 'terrainShadowMask'])

        # Take care of dependencies between switches
        if terrainShadowDetection is False:
            # remove the bands from the co-registration
            bandsWithoutTC = bandsWithoutTC.remove('terrainShadowMask')

        if cloudMasking is False:
            # remove the bands from the co-registration
            bandsWithoutTC = bandsWithoutTC.remove('cloudAndCloudShadowMask')

        # add all bands and properties to the TC bands
        img_SCSccorr = ee.ImageCollection.fromImages(
            bandList.map(apply_SCSccorr)).toBands().rename(bandList)

        img_SCSccorr = img_SCSccorr.addBands(
            img_plus_ic.select(bandsWithoutTC))

        img_SCSccorr = img_SCSccorr.copyProperties(
            img_plus_ic, img_plus_ic.propertyNames())

        # flatten both lists into one
        bandList_IC = ee.List([bandList, bandsWithoutTC]).flatten()

        # unmasked the uncorrected pixel using the orignal image
        return ee.Image(img_SCSccorr).unmask(img_plus_ic.select(bandList_IC)).addBands(mask.rename('TC_mask'))

    # SWITCH
    if topoCorrection is True:
        print('--- Topographic correction applied ---')
        # apply the topographic correction function
        L57_sr = L57_sr.map(topoCorr_L57)
        L57_sr = L57_sr.map(topoCorr_SCSc_L57)
        # print('L57_sr size after mosaic', L57_sr.size())
        # Map.addLayer(L57_sr.first(), vis_nfci, 'L57 mosaic TC', False)

    ##############################
    # EXPORT

    # This function converts the data type of the topographically corrected images

    def dataType(image):
        return image.addBands(
            image.select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'ST_B6']).round().toUint16(), None, True)

    # data type conversion
    L57_sr = L57_sr.map(dataType)

    # convert image collection to image (used in export)
    img_exp = ee.Image(L57_sr.first())
    # Map.addLayer(img_exp, vis_nfci, 'L57 export', False)

    # Add Source to fullfill: https://www.usgs.gov/information-policies-and-instructions/usgs-visual-identity-system

    img_exp = img_exp.set(
        'DATA_SOURCE', "Landsat image courtesy of the U.S. Geological Survey")

    # extract the image properties
    img_exp_properties = ee.FeatureCollection([ee.Feature(img_exp.select([]))])

    # extract the date and time
    sensing_date = img_exp.date().format('YYYY-MM-dd_hh-mm-ss').getInfo()
    sensing_date_read = sensing_date[0:10] + '_T' + sensing_date[11:19]

    # define the filenames
    fname_all = 'L57-sr_Mosaic_' + sensing_date_read + '_All'
    # ['SR_B1','SR_B2','SR_B3','SR_B4','SR_B5','SR_B6','SR_B7']
    fname_30m = 'L57-sr_Mosaic_' + sensing_date_read + '_Bands-30m'
    fname_60m = 'L57-sr_Mosaic_' + sensing_date_read + '_Band-60m'  # ['ST_B6']
    # ['terrainShadowMask','cloudAndCloudShadowMask', 'TC_mask']
    fname_masks = 'L57-sr_Mosaic_' + sensing_date_read + '_Masks-30m'
    # ["system:time_start", "system:index", "DATE_ACQUIRED", "SPACECRAFT_ID", "WRS_PATH", "SCENE_CENTER_TIME", "SENSOR_ID", "SUN_ELEVATION", "SUN_AZIMUTH", "EARTH_SUN_DISTANCE", "COLLECTION_CATEGORY", "COLLECTION_NUMBER", "DATA_SOURCE_ELEVATION"]
    fname_properties = 'L57-sr_Mosaic_' + sensing_date_read + '_properties'
    fname_QAbands = 'L57-sr_Mosaic_' + sensing_date_read + \
        '_Bands-QA'  # ['QA_PIXEL', 'QA_RADSAT']

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
        task = ee.batch.Export.image.toAsset(
            image=img_exp,
            scale=30,
            description=task_description + "_" + fname_all,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e10,
            assetId=collection + '/' + fname_all,
        )
        task.start()

    # # SWITCH export
    # if export30mBands is True:
    #     # Export 30 m spectral bands
    #     task = ee.batch.Export.image.toDrive(
    #         image=img_exp.select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7']),
    #         scale=30,
    #         description=fname_30m,
    #         crs='EPSG:2056',
    #         region=aoi_exp,
    #         maxPixels=1e10,
    #         folder='eeExports',
    #         skipEmptyTiles=True,
    #         fileFormat='GeoTIFF',
    #         formatOptions={'cloudOptimized': True}
    #     )
    #     task.start()

    # # SWITCH export
    # if export60mBands is True:
    #     # Export 60 m thermal band
    #     task = ee.batch.Export.image.toDrive(
    #         image=img_exp.select(['ST_B6']),
    #         scale=60,
    #         description=fname_60m,
    #         crs='EPSG:2056',
    #         region=aoi_exp,
    #         maxPixels=1e10,
    #         folder='eeExports',
    #         skipEmptyTiles=True,
    #         fileFormat='GeoTIFF',
    #         formatOptions={'cloudOptimized': True}
    #     )
    #     task.start()

    # # SWITCH export
    # if exportMasks is True:
    #     # Export masks
    #     task = ee.batch.Export.image.toDrive(
    #         image=img_exp.select(['terrainShadowMask', 'cloudAndCloudShadowMask', 'TC_mask']),
    #         scale=30,
    #         description=fname_masks,
    #         crs='EPSG:2056',
    #         region=aoi_exp,
    #         maxPixels=1e10,
    #         folder='eeExports',
    #         skipEmptyTiles=True,
    #         fileFormat='GeoTIFF',
    #         formatOptions={'cloudOptimized': True}
    #     )
    #     task.start()

    # # SWITCH export
    # if exportQAbands is True:
    #     # Export QA layers
    #     task = ee.batch.Export.image.toDrive(
    #         image=img_exp.select(['QA_PIXEL', 'QA_RADSAT']),
    #         scale=30,
    #         description=fname_QAbands,
    #         crs='EPSG:2056',
    #         region=aoi_exp,
    #         maxPixels=1e10,
    #         folder='eeExports',
    #         skipEmptyTiles=True,
    #         fileFormat='GeoTIFF',
    #         formatOptions={'cloudOptimized': True}
    #     )
    #     task.start()

    # # SWITCH export
    # if exportProperties is True:
    #     # Export image properties
    #     task = ee.batch.Export.table.toDrive(
    #         collection=img_exp_properties,
    #         description=fname_properties,
    #         fileFormat='CSV'
    #     )
    #     task.start()
