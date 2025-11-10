import ee
import numpy as np
from main_functions import main_utils
from .step0_utils import write_asset_as_empty

# Pre-processing pipeline for daily Sentinel-2 L2A surface reflectance (sr) mosaics over Switzerland

##############################
# INTRODUCTION
# This script provides a tool to preprocess Sentinel-2 L2A surface reflectance (sr) data over Switzerland.
# It can mask clouds and cloud shadows, detect terrain shadows, mosaic images from the same image swath,
# co-register images to the Sentinel-2 Global Reference Image, and export the results.
#

##############################
# CONTENT
# The switches enable / disable the execution of individual steps in this script

# This script includes the following steps:
# 1. Masking clouds and cloud shadows
# 2. Detecting terrain shadows
# 3. Mosaicing of images from the same day (=same orbital track) over Switzerland
# 4. Registering the S2 Mosaic to the Sentinel-2 global reference image
# 5. Exporting spectral bands, additional layers and relevant properties
#
# The script is set up to export one mosaic image per day.


def generate_s2_sr_mosaic_for_single_date(day_to_process: str, collection: str, task_description: str) -> None:
    ##############################
    # SWITCHES
    # The switches enable / disable the execution of individual steps in this script

    # options': True, False - defines if individual clouds and cloud shadows are masked
    cloudMasking = True
    # options: True, False - defines if the CloudScore+ dataset should be used (if False': s2cloudless)
    cloudScorePlus = True
    # options: True, False - defines if a cast shadow mask is applied
    terrainShadowDetection = False
    # options: True, False - defines if a cast shadow mask is applied from the precalculated mask
    terrainShadowDetectionPrecalculated = True
    # options': True, False - defines if individual scenes get mosaiced to an image swath
    swathMosaic = True
    # options': True, False - defines if the coregistration is applied
    coRegistration = False
    # options': True, False - defines if the coregistration is applied
    coRegistrationPrecalculated = True

    # Export switches
    # options': True, 'False - defines if 10-m-bands are exported': 'B2','B3','B4','B8'
    export10mBands = True
    # options': True, 'False - defines if 20-m-bands are exported':  select from 'B5','B6','B7','B8A','B11','B12'below
    export20mBands = True
    # options': True, 'False - defines if 60-m-bands are exported': 'B1','B9','B10'
    # export60mBands = False  # NOTEJS: ununsed, export function commented in the script below
    # options': True, 'False - defines if registration layers are exported': 'reg_dx','reg_dy', 'reg_confidence'
    exportRegLayers = True
    # options': True, 'False - defines if masks are exported': 'terrainShadowMask','cloudAndCloudShadowMask'
    exportMasks = True
    # options': True, 'False - defines if S2 cloud probability layer is exported': 'cloudProbability'
    exportS2cloud = True

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
    aoi_CH = ee.FeatureCollection(
        "projects/satromo-prod/assets/res/swissBOUNDARIES3D_1_5_TLM_LANDESGEBIET_dissolve_epsg32632").geometry()
    aoi_CH_simplified = ee.FeatureCollection(
        "projects/satromo-prod/assets/res/CH_boundaries_buffer_5000m_epsg32632").geometry()

    ##############################
    # REFERENCE DATA

    # Sentinel-2 Global Reference Image (contains the red spectral band in 10 m resolution))
    # source: https:#s2gri.csgroup.space
    # processing: GDAL merge and warp (reproject) to epsg32632
    S2_gri = ee.Image("projects/satromo-prod/assets/res/S2_GRI_CH_epsg32632")

    # swissSURFACE3D- very precise digital Surface model in a 10 m resolution
    # source: https://www.swisstopo.admin.ch/de/hoehenmodell-swisssurface3d (inside CH) and the area at "Meiringen" and outside CH was filled with https://www.swisstopo.admin.ch/de/geodata/height/alti3d.html#download
    # source: https://www.swisstopo.admin.ch/de/hoehenmodell-swissaltiregio
    # processing: by F. Gandor in FME
    DEM_sa3d = ee.Image(
        "projects/satromo-prod/assets/res/SS3DR_SA3DRegio_10m_20kmBuffer_epsg32632")

    # SRTM 30 - digital elevation model (slope and aspect) used for the atmospheric correction in sen2cor in a 30 m resolution
    # source: https://developers.google.com/earth-engine/datasets/catalog/USGS_SRTMGL1_003
    # processing: ee.Terrain.slope(DEM) and ee.Terrain.aspect(DEM) converted to radians
    slope = ee.Image('projects/satromo-prod/assets/res/SRTM30m_slope_radians_epsg32632')
    aspect = ee.Image('projects/satromo-prod/assets/res/SRTM30m_aspect_radians_epsg32632')

    # Terrain - very precise digital surface  model in a 10 m resolution
    # source: https://code.earthengine.google.com/ccfa64fe9827c93e2986e693983332e2
    # processing: The shadow masks are  combined into a single image with multiple bands as asset per DOY.
    terrain_shadow_collection = "projects/satromo-prod/assets/col/TERRAINSHADOW_SWISS/"

    # DX DY - Precalculated DX DY shifts
    # source: https://github.com/SARcycle/AROSICS/
    # processing: The DX DY are  combined into a single image with multiple bands as asset per DATE.
    dxdy_collection = "projects/satromo-432405/assets/COL_S2_SR_DXDY"

    ##############################
    # SATELLITE DATA

    # MULTIPLE ORBITS per day: For 2025 starting in March, ESA runs S2A and S2C in parallel resulting in multiple orbits per day

    # Sentinel-2
    S2_sr_orbits= ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
        .filter(ee.Filter.bounds(aoi_CH)) \
        .filter(ee.Filter.date(start_date, end_date))

    # unique SENSING_ORBIT_NUMBER
    unique_orbits = S2_sr_orbits.aggregate_array('SENSING_ORBIT_NUMBER') \
        .distinct() \
        .getInfo()

    # check if we have no orbits / s2_sr data for that specific day
    if not unique_orbits:
        write_asset_as_empty(collection, day_to_process, 'No candidate scene')
        return

    # For multiple orbits set a cloudy scene counter to zero
    cloudy_scene_counter = 0

    # Loop over all orbits
    for orbit in unique_orbits:

        # Print if unique_orbit has more than 1 element
        if len(unique_orbits) > 1:
            print(f"Processing orbit: {orbit} of {day_to_process}")


        # S2 CloudScore+
        S2_csp = ee.ImageCollection('GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED') \
            .filter(ee.Filter.bounds(aoi_CH)) \
            .filter(ee.Filter.date(start_date, end_date))

        # S2cloudless
        S2_clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
            .filter(ee.Filter.bounds(aoi_CH)) \
            .filter(ee.Filter.date(start_date, end_date))

        # Sentinel-2
        S2_sr = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
            .filter(ee.Filter.bounds(aoi_CH)) \
            .filter(ee.Filter.date(start_date, end_date)) \
            .filter(ee.Filter.eq('SENSING_ORBIT_NUMBER', orbit)) \
            .linkCollection(S2_csp, ['cs', 'cs_cdf']) \
            .linkCollection(S2_clouds, ['probability'])
        
        # Are all relevant scenes available for this date and orbit?
        unique_tiles = S2_sr.distinct('MGRS_TILE')
        image_list_size = unique_tiles.size().getInfo()

        # Is a scene available for this date at all -> Yes: continue / No: abort ('No candidate scene')
        if image_list_size == 0:
            write_asset_as_empty(collection, day_to_process, 'No candidate scene')
            return

        # Are all tiles (by distinct tile id) for the overpass available -> Yes: continue / No: abort ('Tile upload incomplete')
        SENSING_ORBIT_NUMBER = S2_sr.first().get('SENSING_ORBIT_NUMBER').getInfo()
        if image_list_size < 4 and SENSING_ORBIT_NUMBER == 8:
            write_asset_as_empty(collection, day_to_process,
                                'Tile upload incomplete')
            return  # exit if condition met
        if image_list_size < 11 and SENSING_ORBIT_NUMBER == 108:
            write_asset_as_empty(collection, day_to_process,
                                'Tile upload incomplete')
            return
        if image_list_size < 11 and SENSING_ORBIT_NUMBER == 65:
            write_asset_as_empty(collection, day_to_process,
                                'Tile upload incomplete')
            return
        if image_list_size < 4 and SENSING_ORBIT_NUMBER == 22:
            write_asset_as_empty(collection, day_to_process,
                                'Tile upload incomplete')
            return


        # Get image_list_size for the cloud probability dataset
        if cloudScorePlus is True:
            image_list_size_cloud = S2_sr.select('cs').size().getInfo()
        else:
            image_list_size_cloud = S2_sr.select('probability').size().getInfo()

        # Are CloudScore+ datasets for all tiles available -> Yes: continue / No: abort ('Cloud probability data missing')
        if image_list_size_cloud < 4 and SENSING_ORBIT_NUMBER == 8:
            write_asset_as_empty(collection, day_to_process,
                                'Cloud probability data missing')
            return
        if image_list_size_cloud < 11 and SENSING_ORBIT_NUMBER == 108:
            write_asset_as_empty(collection, day_to_process,
                                'Cloud probability data missing')
            return
        if image_list_size_cloud < 11 and SENSING_ORBIT_NUMBER == 65:
            write_asset_as_empty(collection, day_to_process,
                                'Cloud probability data missing')
            return
        if image_list_size_cloud < 4 and SENSING_ORBIT_NUMBER == 22:
            write_asset_as_empty(collection, day_to_process,
                                'Cloud probability data missing')
            return

        # image_list = S2_sr.toList(S2_sr.size())
        # for i in range(image_list_size):
        #     image = ee.Image(image_list.get(i))

        #     # EE asset ids for Sentinel-2 L2 assets have the following format: 20151128T002653_20151128T102149_T56MNN.
        #     #  Here the first numeric part represents the sensing date and time, the second numeric part represents the product generation date and time,
        #     #  and the final 6-character string is a unique granule identifier indicating its UTM grid reference
        #     image_id = image.id().getInfo()
        #     image_sensing_timestamp = image_id.split('_')[0]
        #     # first numeric part represents the sensing date, needs to be used in publisher
        #     print("generating json {} of {} ({})".format(
        #         i + 1, image_list_size, image_sensing_timestamp))

        #     # Generate the filename
        #     filename = config.PRODUCT_S2_LEVEL_2A['product_name'] + '_' + image_id
        #     # Export Image Properties into a json file
        #     file_name = filename + "_properties" + "_run" + \
        #         day_to_process.replace("-", "") + ".json"
        #     json_path = os.path.join(config.PROCESSING_DIR, file_name)
        #     with open(json_path, "w") as json_file:
        #         json.dump(image.getInfo(), json_file)

        ###########################
        # WATER MASK
        # The water mask is used to limit a buffering operation on the cast shadow mask.
        # Here, it helps to better distinguish between dark areas and water bodies.
        # This distinction is also used to limit the cloud shadow propagation.
        # EU-Hydro River Network Database 2006-2012 data is derived from this data source:
        # https:#land.copernicus.eu/en/products/eu-hydro/eu-hydro-river-network-database#download
        # processing: reprojected in QGIS to epsg32632

        # Lakes
        lakes = ee.FeatureCollection(
            "projects/satromo-prod/assets/res/CH_inlandWater")

        # vector-to-image conversion based on the area attribute
        lakes_img = lakes.reduceToImage(
            properties=['AREA'],
            reducer=ee.Reducer.first()
        )

        # Make a binary mask and clip to area of interest
        lakes_binary = lakes_img.gt(0).unmask().clip(aoi_CH_simplified)

        # Rivers
        rivers = ee.FeatureCollection(
            "projects/satromo-prod/assets/res/CH_RiverNet")

        # vector-to-image conversion based on the area attribute.
        rivers_img = rivers.reduceToImage(
            properties=['AREA_GEO'],
            reducer=ee.Reducer.first()
        )

        # Make a binary mask and clip to area of interest
        rivers_binary = rivers_img.gt(0).unmask().clip(aoi_CH_simplified)

        # combine both water masks
        water_binary = rivers_binary.Or(lakes_binary)

        ##############################
        # FUNCTIONS

        # This function detects clouds and cloud shadows, masks all spectral bands for them, and adds the mask as an additional layer
        # CloudScore+
        def maskCloudsAndShadowsCloudScorePlus(image):
            # Use 'cs' or 'cs_cdf'
            # cs: Pixel quality score based on spectral distance from a (theoretical) clear reference
            # cs_cdf: Value of the cumulative distribution function of possible cs values for the estimated cs value
            QA_BAND = 'cs_cdf'

            # invert the cloud score bands to represent cloudy with 1 and clear with 0
            # inherently CloudScore+ shows the clearness of a pixel, but we would like to look at cloudyness
            invertedImage = image.expression('1 - b("cs")', {'cs': image.select('cs')}).rename('cs') \
                .addBands(image.expression('1 - b("cs_cdf")', {'cs_cdf': image.select('cs_cdf')}).rename('cs_cdf'))

            # replace the cloud score bands with the inverted ones
            bandNames = image.bandNames()
            bandsToDelete = ['cs', 'cs_cdf']
            bandsToKeep = bandNames.filter(
                ee.Filter.inList('item', bandsToDelete).Not())

            # Replace 'cs' and 'cs_cdf' bands in the original 'image' with the inverted versions
            image = image \
                .select(bandsToKeep) \
                .addBands(invertedImage.select(['cs']).rename('cs')) \
                .addBands(invertedImage.select(['cs_cdf']).rename('cs_cdf'))

            # get the cloud probability

            # clouds = image.select(QA_BAND)
            # get the cloud probability casted to uint8 0-100
            clouds = image.select(QA_BAND).multiply(100).toUint8()

            # The threshold for masking; values between 0.50 and 0.35 generally work well.
            # Lower values will remove thin clouds, haze, cirrus & shadows.
            CLOUD_THRESHOLD = 40  # casted to 100 from 0.4
            CLOUDSHADOW_THRESHOLD = 20  # casted to 100 from 0.2

            # applying the maximum cloud probability threshold
            isNotCloud = clouds.lt(CLOUD_THRESHOLD)

            # get the solar position
            meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
            meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

            # define potential cloud shadow values
            cloudShadowMask = clouds.lt(CLOUD_THRESHOLD).And(
                clouds.gte(CLOUDSHADOW_THRESHOLD))

            # Project shadows from clouds. This step assumes we're working in a UTM projection.
            shadowAzimuth = ee.Number(90).subtract(ee.Number(meanAzimuth))
            # shadow distance is tied to the solar zenith angle (minimum shadowDistance is 30 pixel)
            shadowDistance = ee.Number(meanZenith).multiply(
                0.7).floor().int().max(30)

            # With the following algorithm, cloud shadows are projected.
            isCloud = isNotCloud.directionalDistanceTransform(
                shadowAzimuth, shadowDistance)
            isCloud = isCloud.reproject(
                crs=image.select('B2').projection(), scale=100)

            cloudShadow = isCloud.select('distance').mask()

            # combine projected Shadows & potential cloud shadow values
            cloudShadow = cloudShadow.And(cloudShadowMask)

            # combine mask for clouds and cloud shadows
            cloudAndCloudShadowMask = cloudShadow.Or(isNotCloud.Not())

            # Opening operation: individual pixels are deleted (localMin) and buffered (localMax) to also capture semi-transparent cloud edges
            cloudAndCloudShadowMask = cloudAndCloudShadowMask \
                .focalMin(50, 'circle', 'meters', 1, None) \
                .focalMax(100, 'circle', 'meters', 1, None)

            # mask spectral bands for clouds and cloudShadows
            # image_out = image.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B9', 'B11', 'B12']) \
            #     .updateMask(cloudAndCloudShadowMask.Not())  # NOTE: disabled because we want the clouds in the asset

            # filtering out the old cloud probability band to avoid conflicts
            all_bands = image.bandNames()
            bands_to_keep = all_bands.filter(ee.Filter.neq('item', 'probability'))
            image = image.select(bands_to_keep)

            # adding the additional S2 L2A layers, S2 cloudProbability and cloudAndCloudShadowMask as additional bands
            image = image.addBands(clouds.rename(['cloudProbability'])) \
                .addBands(cloudAndCloudShadowMask.rename(['cloudAndCloudShadowMask']))

            return image.set({
                'cloud_detection_algorithm': 'CloudScore+',
                'cloud_mask_threshold': str(CLOUD_THRESHOLD) + ' / ' + str(CLOUDSHADOW_THRESHOLD)
            })

        # This function detects clouds and cloud shadows, masks all spectral bands for them, and adds the mask as an additional layer
        # S2cloudless
        def maskCloudsAndShadowsSTwoCloudless(image):
            # get the solar position
            meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
            meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

            # get the cloud probability
            clouds = image.select('probability')
            # the maximum cloud probability threshold is set at 50
            CLOUD_THRESHOLD = 50
            isNotCloud = clouds.lt(CLOUD_THRESHOLD)
            cloudMask = isNotCloud.Not()
            # Opening operation: individual pixels are deleted (localMin) and buffered (localMax) to also capture semi-transparent cloud edges
            cloudMask = cloudMask.focalMin(50, 'circle', 'meters', 1, None).focalMax(
                100, 'circle', 'meters', 1, None)

            # Find dark pixels but exclude lakes and rivers (otherwise projected shadows would cover large parts of water bodies)
            darkPixels = image.select(['B8', 'B11', 'B12']).reduce(
                ee.Reducer.sum()).lt(2500).subtract(water_binary).clamp(0, 1)

            # Project shadows from clouds. This step assumes we're working in a UTM projection.
            shadowAzimuth = ee.Number(90).subtract(ee.Number(meanAzimuth))
            # shadow distance is tied to the solar zenith angle (minimum shadowDistance is 30 pixel)
            shadowDistance = ee.Number(meanZenith).multiply(
                0.7).floor().int().max(30)

            # With the following algorithm, cloud shadows are projected.
            isCloud = cloudMask.directionalDistanceTransform(
                shadowAzimuth, shadowDistance)
            isCloud = isCloud.reproject(
                crs=image.select('B2').projection(), scale=100)

            cloudShadow = isCloud.select('distance').mask()

            # combine projectedShadows & darkPixel and buffer the cloud shadow
            cloudShadow = cloudShadow.And(darkPixels).focalMax(
                100, 'circle', 'meters', 1, None)

            # combined mask for clouds and cloud shadows
            cloudAndCloudShadowMask = cloudShadow.Or(cloudMask)

            # mask spectral bands for clouds and cloudShadows
            # image_out = image.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B9', 'B11', 'B12']) \
            #     .updateMask(cloudAndCloudShadowMask.Not())  # NOTE: disabled because we want the clouds in the asset

            # filtering out the old cloud probability band to avoid conflicts
            all_bands = image.bandNames()
            bands_to_keep = all_bands.filter(ee.Filter.neq('item', 'probability'))
            image = image.select(bands_to_keep)

            # adding the additional S2 L2A layers, S2 cloudProbability and cloudAndCloudShadowMask as additional bands
            image = image.addBands(clouds.rename(['cloudProbability'])) \
                .addBands(cloudAndCloudShadowMask.rename(['cloudAndCloudShadowMask']))

            return image.set({
                'cloud_detection_algorithm': 's2cloudless',
                'cloud_mask_threshold': CLOUD_THRESHOLD         # threshold for cloud mask
            })

        # This function calculates and adds the illumination angle
        def addIlluminationAngel(image):
            # get the solar position
            meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
            meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

            # Create an empty image to apply the expression
            empty_image = ee.Image().float()

            # Calculate illumination angle
            illumination_cos = empty_image.expression(
                'cos(sz) * cos(ps) + sin(sz) * sin(ps) * cos(sa - pa)',
                {
                    'sz': ee.Number(meanZenith).multiply(np.pi).divide(180),  # Convert solar zenith to radians
                    'sa': ee.Number(meanAzimuth).multiply(np.pi).divide(180),  # Convert solar azimuth to radians
                    'ps': slope,
                    'pa': aspect
                }
            )
            # The result is the cosine of the illumination angle
            # To get the angle itself -> acos
            illumination_angle_r = illumination_cos.acos()
            illumination_angle = illumination_angle_r.multiply(180).divide(np.pi)

            # Round to full numbers, convert to int, and cap at 90
            illumination_angle = illumination_angle.round().toInt().clamp(0, 90).rename('terrainShadowMask')

            # add the additonal terrainShadow band
            image = image.addBands(illumination_angle)

            return image

        # This function detects and updates terrain shadows
        def addTerrainShadow(image):
            # get the solar position
            meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
            meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

            # Terrain shadow
            terrainShadow = ee.Terrain.hillShadow(
                DEM_sa3d, meanAzimuth, meanZenith, 100, True)
            terrainShadow = terrainShadow.Not() # invert the binaries

            # Update the existing terrainShadowMask band
            updatedMask = image.select('terrainShadowMask').where(terrainShadow, 100)

            # Replace the existing terrainShadowMask band
            image = image.addBands(updatedMask, ['terrainShadowMask'], True)

            return image

        # This updates terrain shadows from precalcuated terrain
        def addTerrainShadow_predefined(image, start_date, terrain_shadow_collection, S2_sr):

            # Define the day of year
            doy = ee.Date(start_date).getRelative('day', 'year').add(1)

            # Get the date string and create an ee.Date object
            date_string = ee.Date(start_date).format('YYYY-MM-dd').getInfo()
            midnight_date = ee.Date(date_string)

            # Get the ee.Date object in UNIX TIME
            midnight_unix = midnight_date.millis()

            # Load the terrain shadow image for the DOY
            terrain_shadow_asset = ee.Image(
                terrain_shadow_collection + str(doy.getInfo()))

            # Extract Unix time from the first image in the Sentinel-2 collection
            sysindex = S2_sr.first()
            index = sysindex.get('system:index').getInfo()

            date_time_part = ee.String(index).split('_').get(0)
            date_time_part_without_t = ee.String(date_time_part).replace('T', '')
            date = ee.Date.parse('yyyyMMddHHmmss', date_time_part_without_t)
            unix_time = ee.Number(date.millis()).subtract(midnight_unix)

            # Extract band names from the asset and remove the prefix "shadow_"
            band_names = terrain_shadow_asset.bandNames().map(
                lambda band_name: ee.String(band_name).replace('shadow_', ''))

            # Find the band with the smallest difference in Unix time
            def find_closest_band(current, previous):
                current_time = ee.Number.parse(current)
                previous_time = ee.Number.parse(previous)
                current_diff = current_time.subtract(unix_time).abs()
                previous_diff = previous_time.subtract(unix_time).abs()
                return ee.Algorithms.If(current_diff.lt(previous_diff), current, previous)

            closest_band_name = ee.String(band_names.iterate(
                find_closest_band, band_names.get(0)))

            band_image = terrain_shadow_asset.select(
                'shadow_' + closest_band_name.getInfo())

            # Update the existing terrainShadowMask band
            updatedMask = image.select('terrainShadowMask').where(band_image, 100)

            # Replace the existing terrainShadowMask band
            image = image.addBands(updatedMask, ['terrainShadowMask'], True)

            return image

        # This function adds the masked-pixel-percentage (clouds, cloud shadows, QA masks) as a property to each image
        def addMaskedPixelCount(image):
            # count the number of pixels that are masked by cloud or shadows
            image_mask = image.select('cloudAndCloudShadowMask').gt(
                0).Or(image.select('terrainShadowMask').gt(99))
            statsMasked = image_mask.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=image.geometry().intersection(aoi_CH_simplified),
                scale=100,
                bestEffort=True,
                maxPixels=1e10,
                tileScale=4
            )
            dataPixels = statsMasked.getNumber('cloudAndCloudShadowMask')

            # get the total number of valid pixel
            image_mask = image.select('cloudAndCloudShadowMask').gte(0)
            statsAll = image_mask.unmask().reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=image.geometry().intersection(aoi_CH_simplified),
                scale=100,
                bestEffort=True,
                maxPixels=1e10,
                tileScale=4
            )
            allPixels = statsAll.getNumber('cloudAndCloudShadowMask')

            # Calculate the percentages and add the properties
            percMasked = (dataPixels.divide(allPixels)).multiply(
                1000).round().divide(10)
            percData = ee.Number(100).subtract(percMasked)

            return image.set({
                'percent_data': percData,  # percentage of unmasked pixel
                # masked pixels include clouds, cloud shadows and non-illuminated pixels (terrain shadows)
                'percent_masked': percMasked
            })

        # This function buffers (inward) the tile geometry by 500m
        # necessary because the CloudScore+ dataset has edge effects
        def clip_outermost_rows(image):
            img_geometry = image.geometry()  # Get the geometry of each image

            # Buffer the geometry inward by 500 meters
            buffered_geometry = img_geometry.buffer(-500)
            # Clip the image to the outer bounds
            return image.clip(buffered_geometry)

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
        S2_sr = S2_sr.map(clip_outermost_rows) \
            .map(maskEdges) \
            .map(set_date)

        # SWITCH
        if cloudMasking is True:
            # apply the cloud mapping and masking functions
            if cloudScorePlus is True:
                print('--- Cloud and cloud shadow masking applied: CloudScore+ ---')
                S2_sr = ee.ImageCollection(
                    S2_sr).map(maskCloudsAndShadowsCloudScorePlus)
            else:
                print('--- Cloud and cloud shadow masking applied: s2cloudless ---')
                S2_sr = ee.ImageCollection(
                    S2_sr).map(maskCloudsAndShadowsSTwoCloudless)

        # Add the illumination angle as terrainShadowMask band
        S2_sr = S2_sr.map(addIlluminationAngel)

        # SWITCH
        if terrainShadowDetection is True:
            print('--- Terrain shadow detection applied ---')
            # apply the terrain shadow function
            S2_sr = S2_sr.map(addTerrainShadow)

        if terrainShadowDetectionPrecalculated is True:
            print('--- Terrain shadow from precalculated shadow applied  ---')
            # apply the terrain shadows
            S2_sr = S2_sr.map(lambda image: addTerrainShadow_predefined(
                image, start_date, terrain_shadow_collection, S2_sr))

        # MOSAIC
        # This step mosaics overlapping Sentinel-2 tiles acquired on the same day

        # 'distinct' removes duplicates from a collection based on a property.
        distinctDates_S2_sr = S2_sr.distinct('date').sort('date')

        # define the filter
        filter = ee.Filter.equals(leftField='date', rightField='date')

        # 'ee.Join.saveAll' Returns a join that pairs each element from the first collection with a group of matching elements from the second collection
        # the matching images are stored in a new property called 'date_match'
        join = ee.Join.saveAll('date_match')

        # 'apply' Joins to collections.
        joinCol_S2_sr = join.apply(distinctDates_S2_sr, S2_sr, filter)

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
                                                                    "MEAN_SOLAR_AZIMUTH_ANGLE", "cloud_detection_algorithm",
                                                                    "cloud_mask_threshold"])

            # Getting swisstopo Processor Version
            processor_version = main_utils.get_github_info()

            # Set TerrainShadow Properties
            if terrainShadowDetectionPrecalculated:
                terrainshadow_method = terrain_shadow_collection
            else:
                terrainshadow_method = 'ee.Terrain.hillShadow'

            # Set TerrainShadow Properties
            if coRegistrationPrecalculated:
                coreg_method = dxdy_collection
            else:
                coreg_method = 'GEE displacement'

            # set the extracted properties to the mosaic
            mosaic = mosaic.set('system:time_start', time_start) \
                .set('system:time_end', time_end) \
                .set('index_list', index_list) \
                .set('scene_count', scene_count) \
                .set('COREGISTRATION', coreg_method) \
                .set('TERRAIN_SHADOW', terrainshadow_method) \
                .set('SWISSTOPO_PROCESSOR', processor_version['GithubLink']) \
                .set('SWISSTOPO_RELEASE_VERSION', processor_version['ReleaseVersion'])

            # reset the projection to epsg:32632 as mosaic changes it to epsg:4326 (otherwise the registration fails)
            mosaic = ee.Image(mosaic).setDefaultProjection('epsg:32632', None, 10)

            return mosaic

        # SWITCH
        if swathMosaic is True:
            print('--- Image swath mosaicing applied ---')
            # apply the mosaicing function
            S2_sr = ee.ImageCollection(joinCol_S2_sr.map(
                mosaic_collection)).map(addMaskedPixelCount)
            # filter for data availability: "'percent_data', 2 " is 98% cloudfree. "'percent_data', 20 " is 80% cloudfree.
            S2_sr = S2_sr.filter(ee.Filter.gte('percent_data', 20))
            length_without_clouds = S2_sr.size().getInfo()
            if length_without_clouds == 0:
                # check if the first scene is cloudy increase the counter in this case. if we have two scenes with clouds assign cloudy
                if len(unique_orbits) > 1:
                    cloudy_scene_counter = cloudy_scene_counter+ 1
                    if cloudy_scene_counter == 1:
                        print(f"Orbit {SENSING_ORBIT_NUMBER} is cloudy")
                        continue
                    if cloudy_scene_counter == 2:
                        print(f"Orbit {SENSING_ORBIT_NUMBER} is cloudy")
                        write_asset_as_empty(collection, day_to_process, 'cloudy')
                        return
                else:
                    write_asset_as_empty(collection, day_to_process, 'cloudy')
                    return
            # This is the If condition the return just the line after the end the step0 script ends the process if 'percent_data' is greater.
            # It's after the mosaic because the threshold (80% here) is applied on the whole mosaic and not per scene:
            # we decide together for the whole swath if we want to process it or not.

            S2_sr = S2_sr.first()

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
            reg_dx = reg_dx.multiply(100).round().toInt16()
            reg_dy = displacement.select('dy').rename('reg_dy')
            reg_dy = reg_dy.multiply(100).round().toInt16()
            reg_confidence = displacement.select(
                'confidence').rename('reg_confidence')
            reg_confidence = reg_confidence.multiply(100).round().toUint8()

            # Compute image offset and direction.
            reg_offset = reg_dx.hypot(reg_dy).rename('reg_offset')
            reg_angle = reg_dx.atan2(reg_dy).rename('reg_offsetAngle')

            # Use the computed displacement to register all original bands.
            registered = image.displace(displacement) \
                .addBands(reg_dx) \
                .addBands(reg_dy) \
                .addBands(reg_confidence) \
                .addBands(reg_offset) \
                .addBands(reg_angle)

            return registered

        def S2regprecalcFunc(image, day, collection, orbit):
            # Load the collection
            dxdy_coll = ee.ImageCollection(collection)

            # Define the precise start and end timestamps for '2023-10-01'
            start_datetime = day+'T00:00:00'
            end_datetime = day+'T23:59:59'


            # Filter the collection by the precise date and time range and SENSING_ORBIT_NUMBER
            filtered_collection = dxdy_coll.filterDate(
                start_datetime, end_datetime).filter(ee.Filter.eq('SENSING_ORBIT_NUMBER', orbit))

            # Is a dx dy available for this date -> Yes: continue / No: abort ('No dx dy available')
            image_list_size = filtered_collection.size().getInfo()
            if image_list_size == 0:
                write_asset_as_empty(
                    collection, day, 'No dx dy available')
                return

            # Get the first image that meets the criteria
            dxdy = filtered_collection.first()

            # Check if the image exists
            if dxdy:
                # Get the image ID
                dxdy_id = dxdy.get('system:id').getInfo()
                print('-> dxdy ID:', dxdy_id)
            else:
                print('ERROR: No precalculated dxdy  found for the specified date.')

            # Extract relevant displacement parameters
            # Select the bands 'reg_dx' and 'reg_dy' and divide by 100
            displacement = dxdy.select(['reg_dx', 'reg_dy']).divide(100)

            # Extract relevant displacement parameters
            reg_dx = dxdy.select('reg_dx')
            reg_dy = dxdy.select('reg_dy')
            reg_confidence = dxdy.select(
                'reg_dy').rename('reg_confidence')
            # TODO This band is not needed change whole processing chain since now all are 0, till the export
            reg_confidence = reg_confidence.multiply(0).round().toUint8()

            # # Use bicubic resampling during registration.
            # imageOrig = image.resample('bicubic')

            # # Choose to register using only the 'R' band.
            # imageRedBand = imageOrig.select('B4')

            # # Determine the displacement by matching only the 'R' bands.
            # displacement = imageRedBand.displacement(
            #     referenceImage=S2_gri,
            #     maxOffset=10,
            #     patchWidth=300,
            #     stiffness=8
            # )

            # # Extract relevant displacement parameters
            # reg_dx = displacement.select('dx').rename('reg_dx')
            # reg_dx = reg_dx.multiply(100).round().toInt16()
            # reg_dy = displacement.select('dy').rename('reg_dy')
            # reg_dy = reg_dy.multiply(100).round().toInt16()
            # reg_confidence = displacement.select(
            #     'confidence').rename('reg_confidence')
            # reg_confidence = reg_confidence.multiply(100).round().toUint8()

            # Compute image offset and direction.
            reg_offset = reg_dx.hypot(reg_dy).rename('reg_offset')
            reg_angle = reg_dx.atan2(reg_dy).rename('reg_offsetAngle')

            # Use the computed displacement to register all original bands.
            registered = image.displace(displacement) \
                .addBands(reg_dx) \
                .addBands(reg_dy) \
                .addBands(reg_confidence) \
                .addBands(reg_offset) \
                .addBands(reg_angle)

            return registered

        # SWITCH
        if coRegistration is True:
            print('--- Image swath co-registration applied ---')
            # apply the registration function
            S2_sr = S2regFunc(S2_sr)
        if coRegistrationPrecalculated is True:
            print('--- Image swath co-registration from precalculated dx dy is applied ---')
            # apply the registration function

            S2_sr = S2regprecalcFunc(S2_sr, day_to_process, dxdy_collection,orbit)

        ##############################
        # EXPORT

        # extract the date and time (it is same time for all images in the mosaic)
        sensing_date = S2_sr.get('system:index').getInfo()[0:15]
        sensing_date_read = sensing_date[0:4] + '-' + \
            sensing_date[4:6] + '-' + sensing_date[6:15]

        # Add Source to fullfill Copernicus requirements:
        S2_sr = S2_sr.set(
            'DATA_SOURCE', "Contains modified Copernicus Sentinel data "+day_to_process[:4])

        # define the export aoi

        # # mask the zero values outside the satellite footprint
        # # Pixels are not zeros, return zeros
        # zeros = S2_sr.Not()
        # # Pixels are zeros, return ones
        # ones = zeros.Not()
        # # Vectorize the ones mask image
        # vectorized_ones = ones.reduceToVectors()

        # the full mosaic image geometry covers larger areas outside Switzerland that are not needed

        aoi_img = S2_sr.geometry()
        # therefore it is clipped with rectangle to keep the geometry simple
        # the alternative clip with aoi_CH would be computationally heavier
        aoi_exp = aoi_img.intersection(aoi_CH_simplified)  # alternativ': aoi_CH
        # aoi_exp = aoi_img.intersection(aoi_CH_simplified).intersection(
        #     vectorized_ones)  # alternativ': aoi_CH

        # SWITCH export
        if export10mBands is True:
            print('Launching export for 10m bands')
            # define the filenames
            fname_10m = 'S2-L2A_mosaic_' + sensing_date_read + '_bands-10m'
            band_list_10m = ['B2', 'B3', 'B4', 'B8']
            if exportMasks:
                band_list_10m.extend(
                    ['terrainShadowMask', 'cloudAndCloudShadowMask'])
            if exportRegLayers:
                band_list_10m.extend(['reg_dx', 'reg_dy', 'reg_confidence'])
            if exportS2cloud:
                band_list_10m.extend(['cloudProbability'])
            print('Band list: {}'.format(band_list_10m))
            # Export COG 10m bands
            task = ee.batch.Export.image.toAsset(
                image=S2_sr.select(band_list_10m).clip(
                    aoi_exp).set('pixel_size_meter', 10),
                scale=10,
                description=task_description + '_10m'+ ' Orbit: '+str(orbit),
                crs='EPSG:2056',
                region=aoi_exp,
                maxPixels=1e10,
                assetId=collection + '/' + fname_10m,
            )
            task.start()

        # SWITCH export
        if export20mBands is True:
            print('Launching export for 20m bands')
            # define the filenames
            fname_20m = 'S2-L2A_mosaic_' + sensing_date_read + '_bands-20m'
            band_list_20m = ['B8A', 'B11', 'B5']
            print('Band list: {}'.format(band_list_20m))
            # Export COG 20m bands
            task = ee.batch.Export.image.toAsset(
                image=S2_sr.select(band_list_20m).clip(
                    aoi_exp).set('pixel_size_meter', 20),
                scale=20,
                description=task_description + '_20m'+ ' Orbit: '+str(orbit),
                crs='EPSG:2056',
                region=aoi_exp,
                maxPixels=1e10,
                assetId=collection + '/' + fname_20m
            )
            task.start()

        """"
        # SWITCH export
        if export60mBands is True:
            print('Launching export for 60m bands')
            fname_60m = 'S2-L2A_Mosaic_' + sensing_date_read + '_Bands-60m'
            band_list_60m = ['B1', 'B9', 'B10']
            print('Band list: {}'.format(band_list_60m))
            task = ee.batch.Export.image.toAsset(
                image=S2_sr.select(band_list_60m).clip(aoi_exp),
                scale=60,
                description=task_description + '_60m',
                crs='EPSG:2056',
                region=aoi_exp,
                maxPixels=1e10,
                assetId=collection + '/' + fname_60m
            )
            task.start()
        """