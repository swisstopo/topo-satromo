import ee
from .step0_utils import write_asset_as_empty
from main_functions import main_utils
import math

# Pre-processing pipeline for daily OLCI  mosaics over Switzerland
# TODO :
# - export Spatial resolution wise to asset as for S2 SR -> Decision
# - multiply / cast 32bit/float bands to 16int
# - rename asset export

# Pre-processing pipeline for daily Sentinel-3 OLI radiance data
# in 300 m (MODIS product: S3_OLCI) over Switzerland


##############################
# INTRODUCTION
#
# This script provides a tool to preprocess Sentinel-3 OLI over Switzerland.
# It can mask clouds and cloud shadows, detect terrain shadows,
# topographically correct images and export the results.
#


##############################
# CONTENT

# This script includes the following steps:
# 1. Masking clouds and cloud shadows
# 2. Mosaicing of images from the same day over Switzerland
# 3. Detecting terrain shadows
# 4. Applying a topographic correction (SCSc-correction) to the spectral bands
# 5. Exporting spectral bands, additional layers and relevant properties
#
# The script is set up to export one image per day.

def generate_s3_toa_mosaic_for_single_date(day_to_process: str, collection: str, task_description: str) -> None:
    ##############################
    # SWITCHES
    # The switches enable / disable the execution of individual steps in this script

    # options': True, False - defines if individual scenes / swaths get mosaiced
    dailyMosaic = True

    # options': True, False - defines if individual clouds and cloud shadows are masked
    cloudMasking = True

    # options': True, False - defines if a cast shadow mask is applied
    terrainShadowDetection = True

    # options': True, False - defines if a topographic correction is applied to the image swath
    topoCorrection = True

    # Export switches
    # options': True, False - defines if image with all bands is exported as an asset
    exportAllToAsset = True
    # options': True, 'False - defines if 250 m spectral bands are exported': 'Oa01_radiance', ... , 'Oa21_radiance'
    export300mBands = True
    # options': True, 'False - defines if masks are exported': 'terrainShadowMask','cloudAndCloudShadowMask', 'TC_mask', 'clouds_QA'
    exportMasks = True
    # options': True, 'False - defines if the terrain shadow layer is exported': 'terrainShadowFraction'
    exportTSF = True
    # options': True, 'False - defines if MODIS quality bands are exported': 'QC_250m', 'num_observations'
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
        "users/wulf/SATROMO/swissBOUNDARIES3D_1_5_TLM_LANDESGEBIET_dissolve_epsg32632").geometry()

    # Simplified and buffered shapefile of Switzerland to simplify processing
    aoi_CH_simplified = ee.FeatureCollection(
        "users/wulf/SATROMO/CH_boundaries_buffer_5000m_epsg32632").geometry()
    # clipping on complex shapefiles costs more processing resources and can cause memory issues

    ##############################
    # VISUALISATION
    # vis_fci = {'bands': ['Oa18_radiance',  'Oa08_radiance',
    #                     'Oa06_radiance'], 'min': 0.1, 'max': 1, 'gamma': 1.2}

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

    DEM = DEM \
        .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1024) \
        .reproject(ee.Projection("EPSG:4326").atScale(300))
    # .reproject({crs: S3_OLCI.first().select('Oa01_radiance').projection()})
    # .setDefaultProjection(crs, crsTransform, scale)

    ##############################
    # SATELLITE DATA

    # Sentinel-3 OLCI
    # https:#developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S3_OLCI
    S3_OLCI = ee.ImageCollection('COPERNICUS/S3/OLCI') \
        .filterBounds(aoi_CH_simplified) \
        .filter(ee.Filter.date(start_date, end_date))

    # Define if we have imagery for the selected day
    image_list_size = S3_OLCI.size().getInfo()
    if image_list_size == 0:
        write_asset_as_empty(collection, day_to_process, 'No candidate scene')
        return

    # print('S3_OLCI size', S3_OLCI.size())
    ##############################
    # DATA CONVERSION

    # This function masks clouds & cloud shadows using the QA quality bands of Landsat

    def scaleRadiance(image):

        # Select all bands and apply band-specific scale factors.
        radScaled = image.select(['Oa01_radiance', 'Oa02_radiance', 'Oa03_radiance', 'Oa04_radiance', 'Oa05_radiance', 'Oa06_radiance', 'Oa07_radiance',
                                  'Oa08_radiance', 'Oa09_radiance', 'Oa10_radiance', 'Oa11_radiance', 'Oa12_radiance', 'Oa13_radiance', 'Oa14_radiance',
                                  'Oa15_radiance', 'Oa16_radiance', 'Oa17_radiance', 'Oa18_radiance', 'Oa19_radiance', 'Oa20_radiance', 'Oa21_radiance']) \
            .multiply(ee.Image([0.0139465, 0.0133873, 0.0121481, 0.0115198, 0.0100953, 0.0123538, 0.00879161, 0.00876539, 0.0095103, 0.00773378,
                                0.00675523, 0.0071996, 0.00749684, 0.0086512, 0.00526779, 0.00530267, 0.00493004, 0.00549962, 0.00502847, 0.00326378, 0.00324118]))

        # apply the masks
        radScaled = radScaled.addBands(image.select('quality_flags')) \
            .copyProperties(image, image.propertyNames())

        return radScaled

    # This function sets the date as an additional property to each image

    def set_date(img):
        date = img.date().format('YYYY-MM-dd')
        return img.set('date', date)

    # map the function
    S3_OLCI = S3_OLCI.map(scaleRadiance) \
        .map(set_date)
    # Map.addLayer(S3_OLCI, vis_fci, 'S3_OLCI original', False)

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
    # SOLAR GEOMETRY

    # This functions calculates the solar position angles for a given date and location

    def addSunToImage(position, date):

        # time (get day of year, hour, minutes of the acquisition time)
        DoY = ee.Number.parse(date.format('D'))
        hour = ee.Number.parse(date.format('H'))
        minutes = ee.Number.parse(date.format('m'))

        # space (get latitude and longitude, in radians)
        coordinates = position.coordinates()
        longitude = ee.Number(coordinates.get(0))
        lon = ee.Number(coordinates.get(0)).multiply(math.pi/180)
        lat = ee.Number(coordinates.get(1)).multiply(math.pi/180)

        # the fractional year (y) is calculated, in radians
        # ! set leap years to 366
        y = ee.Number(2*math.pi/365).multiply(DoY.subtract(1))

        # from y, we can estimate the equation of time (E) in minutes
        E = ee.Number(229.18).multiply(
            ee.Number(0.000075)
            .add(ee.Number(0.001868).multiply(y.cos()))
            .subtract(ee.Number(0.032077).multiply(y.sin()))
            .subtract(ee.Number(0.014615).multiply(y.multiply(2).cos()))
            .subtract(ee.Number(0.040849).multiply(y.multiply(2).sin()))
        )

        # from y, we can estimate the solar declination angle (declin) in radians
        declin = ee.Number(0.006918) \
            .subtract(ee.Number(0.399912).multiply(y.cos())) \
            .add(ee.Number(0.070257).multiply(y.sin())) \
            .subtract(ee.Number(0.006758).multiply(y.multiply(2).cos())) \
            .add(ee.Number(0.000907).multiply(y.multiply(2).sin())) \
            .subtract(ee.Number(0.002697).multiply(y.multiply(3).cos())) \
            .add(ee.Number(0.00148).multiply(y.multiply(3).cos()))

        # the True solar time is calculated
        SolarTime = hour.multiply(60).add(
            minutes).add(longitude.multiply(4)).add(E)

        # solar hour angle (w), in radians,
        w = SolarTime.divide(4).subtract(180).multiply(math.pi/180)

        # the solar zenith angle (Phi) is based on the hour angle (w), latitude (lat) and solar declination (declin)
        Phi = (lat.sin().multiply(declin.sin()).add(
            lat.cos().multiply(declin.cos()).multiply(w.cos()))).acos()

        # the solar azimuth angle (Theta) is based on this equation
        Theta = (lat.sin().multiply(Phi.cos()).subtract(declin.sin())) \
            .divide(lat.cos().multiply(Phi.sin())).multiply(-1).acos().add(math.pi*2).mod(math.pi*2)

        # return the solar angles in degree
        return ee.Algorithms.Dictionary(['SolarAzimuth', Theta.multiply(180/math.pi), 'SolarZenith', Phi.multiply(180/math.pi)])

    # This functions adds the solar position angles as additional attributes

    def calSunAngles(image):
        phiAndTheta = addSunToImage(
            aoi_CH_simplified.centroid(), ee.Date(image.get('system:time_start')))
        return image.set(phiAndTheta)

    # map the function
    S3_OLCI = S3_OLCI.map(calSunAngles)

    ##############################
    # PIXEL STATS

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
        percMasked = (dataPixels.divide(allPixels)).multiply(
            1000).round().divide(10)
        percData = ee.Number(100).subtract(percMasked)

        return image.set({
            'percentData': percData,          # percentage of useful pixel
            # less useful pixels including clouds, cloud shadows and terrain shadows
            'percentMasked': percMasked
        })

    ##############################
    # CLOUD MASKING

    # This function masks clouds & cloud shadows using the QA quality bands of Landsat

    def maskCloudsAndShadowsS3(image):

        # get the solar position
        meanAzimuth = ee.Number(image.get('SolarAzimuth'))
        meanZenith = ee.Number(image.get('SolarZenith'))

        # extract the MOD09GA state_1km-QA_band
        QA = image.select('quality_flags')

        # Make a mask to get bit 10, the internal_cloud_algorithm_flag bit.
        qaCloud = QA.bitwiseAnd(math.pow(2, 27)).rightShift(27).eq(0)  # .Not()

        # Find dark pixels but exclude lakes and rivers (otherwise projected shadows would cover large parts of water bodies)
        darkPixels = image.select(['Oa18_radiance']).reduce(
            ee.Reducer.sum()).lt(0.2).subtract(water_binary).clamp(0, 1)

        # Project shadows from clouds. This step assumes we're working in a UTM projection.
        shadowAzimuth = ee.Number(90).subtract(ee.Number(meanAzimuth))
        # shadow distance is tied to the solar zenith angle (minimum shadowDistance is 10 pixel)
        shadowDistance = ee.Number(meanZenith).multiply(
            0.4).floor().int().max(10)

        # With the following algorithm, cloud shadows are projected.
        isCloud = qaCloud.directionalDistanceTransform(
            shadowAzimuth, shadowDistance)
        isCloud = isCloud.reproject(crs=image.select(
            'Oa18_radiance').projection(), scale=1000)

        cloudShadow = isCloud.select('distance').mask()

        # combine projectedShadows & darkPixel and buffer the cloud shadow
        cloudShadow = cloudShadow.And(darkPixels)

        # combined mask for clouds and cloud shadows
        cloudAndCloudShadowMask = cloudShadow.Or(qaCloud.Not())

        # apply the masks
        image = image.updateMask(qaCloud) \
            .addBands(qaCloud.Not().rename('clouds_QA')) \
            .addBands(cloudAndCloudShadowMask.rename(['cloudAndCloudShadowMask'])) \
            .copyProperties(image, image.propertyNames())

        return image

    # SWITCH
    if cloudMasking is True:
        print('--- Cloud and cloud shadow masking applied ---')
        # apply the masking function
        S3_OLCI = S3_OLCI.map(maskCloudsAndShadowsS3) \
            .map(addMaskedPixelCount)
        # Map.addLayer(S3_OLCI, vis_fci, 'S3_OLCI cloud masked', True)

    # /
    # MOSAIC

    # This step mosaics overlapping Landsat X tiles acquired on the same day

    # 'distinct' removes duplicates from a collection based on a property.
    distinctDates_S3_OLCI = S3_OLCI.distinct('date').sort('date')

    # define the filter
    filter = ee.Filter.equals(leftField='date', rightField='date')

    # 'ee.Join.saveAll' Returns a join that pairs each element from the first collection with a group of matching elements from the second collection
    # the matching images are stored in a new property called 'date_match'
    join = ee.Join.saveAll('date_match')

    # 'apply' Joins to collections.
    joinCol_S3_OLCI = join.apply(distinctDates_S3_OLCI, S3_OLCI, filter)

    # function to mosaic matching images of the same day

    def mosaic_collection(img):
        orig = img

        # create a collection of the date-matching images
        col = ee.ImageCollection.fromImages(
            img.get('date_match')).sort('system:time_start', False)

        # extract collection properties to assign to the mosaic
        time_start = col.aggregate_min('system:time_start')
        time_end = col.aggregate_max('system:time_end')
        index_list = col.aggregate_array('system:index')
        index_list = index_list.join(',')
        scene_count = col.size()

        # get the unified geometry of the collection (outer boundary)
        col_geo = col.geometry().dissolve()

        # clip the mosaic to set a geometry to it
        mosaic = col.mosaic().setDefaultProjection(ee.Image(img).select('Oa01_radiance').projection()) \
            .clip(col_geo) \
            .copyProperties(img, ["spacecraft", "relative_orbit_num", "processing_time", "groundTrackDirection", "PRODUCT_ID", 'SolarAzimuth', 'SolarZenith','date'])

        # set the extracted properties to the mosaic

        # Getting swisstopo Processor Version
        processor_version = main_utils.get_github_info()

        # set the extracted properties to the mosaic
        mosaic = mosaic.set('system:time_start', time_start) \
            .set('system:time_end', time_end) \
            .set('index_list', index_list) \
            .set('scene_count', scene_count) \
            .set('SWISSTOPO_PROCESSOR', processor_version['GithubLink']) \
            .set('SWISSTOPO_RELEASE_VERSION', processor_version['ReleaseVersion'])

        return mosaic

    # SWITCH
    if dailyMosaic is True:
        print('--- Image swath mosaicing applied ---')
        # apply the mosaicing and maskPixelCount function
        S3_OLCI = ee.ImageCollection(joinCol_S3_OLCI.map(
            mosaic_collection)).map(addMaskedPixelCount)
        # print('S3_OLCI size after mosaic', S3_OLCI.size())
        # display the mosaic
        # imgMosaic = ee.Image(S3_OLCI.first())
        # Map.addLayer(S3_OLCI, vis_fci, 'S3_OLCI mosaic', False)
        
        # filter for data availability: "'percentData', 2 " is 98% cloudfree. "'percentData', 20 " is 80% cloudfree.
        S3_OLCI = S3_OLCI.filter(ee.Filter.gte('percentData', 2))         
        length_without_clouds = S3_OLCI.size().getInfo()
        if length_without_clouds == 0:
            write_asset_as_empty(collection, day_to_process, 'cloudy')
            return
        # This is the If condition the return just the line after the end the step0 script ends the process if 'percentData' is greater.
        # It's after the mosaic because the threshold (98% here) is applied on the whole mosaic and not per scene:
        # we decide together for the whole swath if we want to process it or not.


    ##############################
    # TERRAIN SHADOWS

    # This function detects terrain shadows

    def addTerrainShadow(image):
        # get the solar position
        meanAzimuth = ee.Number(image.get('SolarAzimuth'))
        meanZenith = ee.Number(image.get('SolarZenith'))
        # Terrain shadow
        terrainShadow = ee.Terrain.hillShadow(
            DEM, meanAzimuth, meanZenith, 500, True)
        terrainShadow = terrainShadow.Not().rename(
            'terrainShadowMask')  # invert the binaries
        # Get information about the MODIS projection.
        imageProjection = image.select('Oa01_radiance').projection()
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
        S3_OLCI = S3_OLCI.map(addTerrainShadow)
        # # Map.addLayer(S3_OLCI.select('terrainShadowMask'), {}, 'S3_OLCI terrain shadow mask', False)
        # Map.addLayer(S3_OLCI.select('terrainShadowFraction'), {}, 'S3_OLCI terrain shadow fraction', False)

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
        img_plus_ic = ee.Image(img.addBands(ic.rename('TC_illumination')).addBands(
            cosZ.rename('cosZ')).addBands(cosS.rename('cosS')).addBands(slp.rename('slope')))
        return img_plus_ic

    # This function applies the sun-canopy-sensor+C topographic correction (Soenen et al. 2005)

    def topoCorr_SCSc_MOD(img):
        img_plus_ic = img
        # masking flat, shadowed, and incorrect pixels (these get excluded from the topographic correction)
        mask = img_plus_ic.select('slope').gte(5) \
            .And(img_plus_ic.select('TC_illumination').gte(0)) \
            .And(img_plus_ic.select('Oa08_radiance').gt(-0.1))
        img_plus_ic_mask = ee.Image(img_plus_ic.updateMask(mask))
        #
        # Specify Bands to topographically correct (focus on vegetation)
        bandList = ee.List(['Oa03_radiance', 'Oa04_radiance', 'Oa05_radiance', 'Oa06_radiance', 'Oa07_radiance', 'Oa08_radiance',
                            'Oa09_radiance', 'Oa10_radiance', 'Oa11_radiance', 'Oa12_radiance', 'Oa15_radiance', 'Oa18_radiance'])

        # This function quantifies the linear relation between illumination and reflectance and corrects for it

        def apply_SCSccorr(band):
            out = img_plus_ic_mask.select('TC_illumination', band).reduceRegion(
                reducer=ee.Reducer.linearFit(),  # Compute coefficients=a(slope), b(offset), c(b/a)
                # trim off the outer edges of the image for linear relationship
                geometry=aoi_CH_simplified,
                scale=300,
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
        bandsWithoutTC = ee.List(['quality_flags', 'clouds_QA',
                                  'cloudAndCloudShadowMask', 'terrainShadowMask', 'terrainShadowFraction'])

        # Take care of dependencies between switches
        if terrainShadowDetection is False:
            # remove the bands from the co-registration
            bandsWithoutTC = bandsWithoutTC.remove(
                'terrainShadowMask').remove('terrainShadowFraction')

        if cloudMasking is False:
            # remove the bands from the co-registration
            bandsWithoutTC = bandsWithoutTC.remove('clouds_QA')

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

        # The topographic correction operates at the DEM scale and projection
        # Therefore, we need to rescale the DEM
        DEM = DEM \
            .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1024)
        # Request the data at the scale and projection of the MODIS image.
        # .reproject(ee.Projection("EPSG:4326").atScale(300)) \
        DEM = DEM.reproject(crs=S3_OLCI.first().select(
            'Oa01_radiance').projection())

        # apply the topographic correction function
        S3_OLCI = S3_OLCI.map(topoCorr_MOD) \
            .map(topoCorr_SCSc_MOD)
        # print('S3_OLCI size after mosaic', S3_OLCI.size())
        # Map.addLayer(S3_OLCI.first(), vis_fci, 'S3_OLCI TC', False)

    ##############################
    # EXPORT

    # This function converts the data type of the topographically corrected images

    def dataType(image):
        return image.addBands(image.select(['Oa03_radiance', 'Oa04_radiance', 'Oa05_radiance', 'Oa06_radiance', 'Oa07_radiance', 'Oa08_radiance',
                                            'Oa09_radiance', 'Oa10_radiance', 'Oa11_radiance', 'Oa12_radiance', 'Oa15_radiance', 'Oa18_radiance'])
                              .round().toInt16(),  None, True)

    # data type conversion
    S3_OLCI = S3_OLCI.map(dataType)

    # convert image collection to image (used in export)
    img_exp = ee.Image(S3_OLCI.first())
    # Map.addLayer(img_exp, vis_fci, 'S3_OLCI export', False)

    # extract the image properties
    img_exp_properties = ee.FeatureCollection([ee.Feature(img_exp.select([]))])

    # extract the date and time
    sensing_date = img_exp.date().format('YYYY-MM-dd_hh-mm-ss').getInfo()
    sensing_date_read = sensing_date[0:10] + '_T' + sensing_date[11:19]

    # define the filenames
    fname_all = 'S3_OLCI_' + sensing_date_read + '_All'
    # ['Oa01_radiance', ..., 'Oa21_radiance']
    fname_300m = 'S3_OLCI_' + sensing_date_read + '_Bands-300m'
    # ['terrainShadowMask', 'terrainShadowFraction', 'cloudAndCloudShadowMask', 'TC_mask', 'clouds_QA']
    fname_masks = 'S3_OLCI_' + sensing_date_read + '_Masks-300m'
    fname_TSF = 'S3_OLCI_' + sensing_date_read + \
        '_TSF-300m'             # ['terrainShadowFraction']
    fname_QAbands = 'S3_OLCI_' + sensing_date_read + \
        '_Bands-QA'        # ['quality_flags']
    # ["SolarAzimuth", "SolarZenith", "percentData", "percentMasked", "system:asset_size", "system:footprint", "system:time_start", "system:time_end", "system:index"]
    fname_properties = 'S3_OLCI_' + sensing_date_read + '_properties'

    # Add Source to fullfill Copernicus requirements: 
    img_exp = img_exp.set(
            'DATA_SOURCE', "Contains modified Copernicus Sentinel data "+day_to_process[:4])

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
            scale=300,
            description=task_description + "_" + fname_all,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e10,
            assetId=collection + '/' + fname_all,
        )
        task.start()

    # # SWITCH export
    # if export300mBands is True:
    #     # Export 250 m spectral bands
    #     task = ee.batch.Export.image.toDrive(
    #         image=img_exp.select(['Oa01_radiance', 'Oa02_radiance', 'Oa03_radiance', 'Oa04_radiance', 'Oa05_radiance', 'Oa06_radiance', 'Oa07_radiance',
    #                               'Oa08_radiance', 'Oa09_radiance', 'Oa10_radiance', 'Oa11_radiance', 'Oa12_radiance', 'Oa13_radiance', 'Oa14_radiance',
    #                               'Oa15_radiance', 'Oa16_radiance', 'Oa17_radiance', 'Oa18_radiance', 'Oa19_radiance', 'Oa20_radiance', 'Oa21_radiance']),
    #         scale=300,
    #         description=fname_300m,
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
    #         image=img_exp.select(
    #             ['terrainShadowMask', 'cloudAndCloudShadowMask', 'TC_mask', 'clouds_QA']),
    #         scale=300,
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
    # if exportTSF is True:
    #     # Export masks
    #     task = ee.batch.Export.image.toDrive(
    #         image=img_exp.select(['terrainShadowFraction']),
    #         scale=500,
    #         description=fname_TSF,
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
    #         image=img_exp.select(['QC_250m']).addBands(
    #             img_exp.select(['num_observations']).uint16()),
    #         scale=250,
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
