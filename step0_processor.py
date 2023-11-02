import ee
from datetime import datetime, timedelta

ee.Initialize()


def generate_asset_mosaic_for_single_date(day_to_process: datetime, export_to_drive: bool = False,
                                          export_to_asset: bool = False) -> None:
    # Processing pipeline for S2 SR mosaics
    ##############################
    # Discussion points

    # 1. export of additional S2A bands? "AOT", "WVP", "SCL", "TCI_R", "TCI_G", "TCI_B", "MSK_CLDPRB", "MSK_SNWPRB", "QA10", "QA20", "QA60", "cloudProbability",
    # 2. export of dates and times with dashes (improved readibility)?
    # 3. export of registration layers with reg_offset, reg_offsetAngle (can be computed from dx and dy)
    # 4. cloudShadowMask includes cloud mask and cloud shadow mask. Better name needed?

    ##############################
    # Content

    # 1.
    # This script includes
    # 1. Cloud and cloud shadow masking
    # 2. Cloud Shadows - We filter cloud shadows and add the cloud shadow mask to the imagery
    # 3. Terrain Shadows - We filter terrain shadows and add them to the imagery
    # 4. Export - all spectral bands of the same spatial resolution (10, 20, 60) and data type,
    #           - all registration bands (dx, dy, confidence)
    #           - all masks (clouds, cloud shadows, terrain shadows, topo correction)
    #           - export of properties as csv
    # 5.
    # 6.
    #

    ##############################
    # TIME
    # start_date = ee.Date('2023-01-01')
    # end_date = ee.Date('2024-01-01')

    start_date = ee.Date(day_to_process.strftime('%Y-%m-%d'))
    end_date = ee.Date(day_to_process.strftime('%Y-%m-%d')).advance(1, 'day')

    ##############################
    # SPACE
    # Country
    #  #
    aoi_name = 'Switzerland'  # 'Liechtenstein'
    # https:#developers.google.com/earth-engine/datasets/catalog/USDOS_LSIB_SIMPLE_2017
    countries = ee.FeatureCollection("USDOS/LSIB/2017")
    country = countries.filterMetadata('COUNTRY_NA', 'equals', aoi_name)
    aoi = country.geometry()  # swiss.geometry()

    # Official swisstopo boundaries
    aoi_CH = ee.FeatureCollection("users/wulf/SATROMO/swissBOUNDARIES3D_1_4_TLM_LANDESGEBIET_epsg32632").geometry()

    # Region (extended Switzerland) to simplify processing
    aoi_rectangle = ee.Geometry.Rectangle(5.9, 45.7, 10.6, 47.9)
    # clipping on complex shapefiles cost more processing resources and can cause memory issues

    ##############################
    # Definitions
    i = 0  # index of individual images from the collection

    MAX_CLOUD_PROBABILITY = 50  # filter on the S2cloudless cloud propability value

    ##############################
    # Reference Image

    ################
    # Sentinel-2 Global Reference Image (contains the red spectral band)
    S2_gri = ee.Image("users/wulf/SATROMO/S2_GRI_CH_epsg32632")
    DEM_sa3d = ee.Image("users/wulf/SATROMO/SwissALTI3d_20kmBuffer")

    ##############################
    # Data

    # Sentinel-2
    S2_sr = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
        .filter(ee.Filter.bounds(aoi)) \
        .filter(ee.Filter.date(start_date, end_date))

    # S2cloudless
    S2_clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
        .filter(ee.Filter.bounds(aoi)) \
        .filter(ee.Filter.date(start_date, end_date))
    # .filter(ee.Filter.calendarRange(start_month, end_month, 'month'))

    ###########################
    # Water mask

    # Lakes
    # lakes = ee.FeatureCollection("users/michaelbrechbuehler/eu-hydro")
    lakes = ee.FeatureCollection("users/wulf/SATROMO/CH_inlandWater")

    # Make an image out of the land area attribute.
    lakes_img = lakes.reduceToImage(
        properties=['AREA'],
        reducer=ee.Reducer.first()
    )

    # Make a binary mask and clip to area of intest
    lakes_binary = lakes_img.gt(0).unmask().clip(aoi_rectangle)
    # Map.addLayer(lakes_binary, {min:0, max:1}, 'lake mask', False)

    # Rivers
    rivers = ee.FeatureCollection("users/wulf/SATROMO/CH_RiverNet")
    # print('rivers',rivers.first())
    # Make an image out of the land area attribute.
    rivers_img = rivers.reduceToImage(
        properties=['AREA_GEO'],
        reducer=ee.Reducer.first()
    )

    # Make a binary mask and clip to area of intest
    rivers_binary = rivers_img.gt(0).unmask().clip(aoi_rectangle)
    # Map.addLayer(rivers_binary, {min:0, max:1}, 'river mask', False)

    # combine both masks
    water_binary = rivers_binary.Or(lakes_binary)

    ##############################
    # Functions
    # detect, add, and mask clouds and cloud shadows
    def maskCloudsAndShadows(image):
        # get the solar position
        meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
        meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

        # get the cloud probability
        clouds = ee.Image(image.get('cloud_mask')).select('probability')
        isNotCloud = clouds.lt(MAX_CLOUD_PROBABILITY)
        cloudMask = isNotCloud.Not()
        cloudMask = cloudMask.focalMin(50, 'circle', 'meters', 1, None).focalMax(100, 'circle', 'meters', 1, None)

        # Find dark pixels but exclude lakes and rivers
        darkPixels = image.select(['B8', 'B11', 'B12']).reduce(ee.Reducer.sum()).lt(2500).subtract(water_binary).clamp(
            0, 1)

        # Project shadows from clouds. This step assumes we're working in a UTM projection.
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
        cloudShadowMask = cloudShadow.Or(cloudMask)

        # adding the S2 cloudProbability and cloudShadows as additional bands
        image = image.addBands(clouds.rename(['cloudProbability'])) \
            .addBands(cloudShadow.rename(['cloudShadowMask']))

        # mask for clouds and cloudShadows
        image = image.updateMask(cloudShadowMask.Not())

        return image

    # detect and add terrain shadows
    def addTerrainShadow(image):
        # get the solar position
        meanAzimuth = image.get('MEAN_SOLAR_AZIMUTH_ANGLE')
        meanZenith = image.get('MEAN_SOLAR_ZENITH_ANGLE')

        # Find dark pixels but exclude lakes
        darkPixels = image.select(['B8', 'B11', 'B12'])\
            .reduce(ee.Reducer.sum()).lt(2500).subtract(water_binary)\
            .clamp(0, 1)

        # Terrain shadow
        terrainShadow = ee.Terrain.hillShadow(DEM_sa3d, meanAzimuth, meanZenith, 100, True)
        terrainShadow = terrainShadow.reproject(image.select('B2').projection())
        terrainShadow = terrainShadow.Not()  # invert the binaries

        # buffering the terrain shadow
        terrainShadow_buffer = terrainShadow.focalMax(200, 'circle', 'meters', 1,
                                                      None)  # .focalMin(40, 'circle', 'meters', 1, None)

        # combining castShadow and darkPixels
        terrainShadow_darkPixels = terrainShadow_buffer.And(darkPixels).rename('terrainShadow')

        # add the additonal terrainShadow band
        image = image.addBands(terrainShadow_darkPixels)

        return image

    # The masks for the 10m bands sometimes do not exclude bad data at
    # scene edges, so we apply masks from the 20m and 60m bands as well.
    def maskEdges(s2_img):
        return s2_img.updateMask(
            s2_img.select('B8A').mask().updateMask(s2_img.select('B9').mask()))

    # This function sets the date as an additional property to each image
    def set_group(img):
        date = img.date().format('YYYY-MM-dd')  # -hh-mm-ss
        platform = img.get('SPACECRAFT_NAME')
        orbit = ee.String(img.get('SENSING_ORBIT_NUMBER')).replace('\\.', '')
        group = ee.List([date, platform, orbit]).join('_')
        img.set('date', date)
        return img.set('group', group)

    ##############################
    # Processing

    S2_sr = S2_sr.map(maskEdges)

    # Join S2 SR with cloud probability dataset to add cloud mask.
    S2_srWithCloudMask = ee.Join.saveFirst('cloud_mask').apply(
        primary=S2_sr,
        secondary=S2_clouds,
        condition=ee.Filter.equals(leftField='date', rightField='date')
    )

    # apply the cloud mapping and date setting functions
    S2_sr = ee.ImageCollection(S2_srWithCloudMask) \
        .map(maskCloudsAndShadows) \
        .map(addTerrainShadow) \
        .map(set_group)

    ###########################/
    # Mosaics overlapping Sentinel-2 tiles acquired on the same day

    # 'distinct' removes duplicates from a collection based on a property.
    distinctDates_S2_sr = S2_sr.distinct('group').sort('group')

    # define the filter
    filter = ee.Filter.equals(leftField='group', rightField='group')

    # 'ee.Join.saveAll' returns a join that pairs each element from the first collection with a group of
    # matching elements from the second collection.
    # The matching images are stored in a new property called 'date_match'.
    join = ee.Join.saveAll('group_match')

    # 'apply' Joins to collections.
    joinCol_S2_sr = join.apply(distinctDates_S2_sr, S2_sr, filter)

    # function to mosaic matching images of the same day
    def mosaic_collection(img):
        orig = img
        # create a collection of the date-matching images
        col = ee.ImageCollection.fromImages(img.get('group_match'))
        # get the unified geometry of the collection (outer boundary)
        col_geo = col.geometry().dissolve()
        # clip the mosaic to set a geometry to it
        time_start = col.aggregate_min('system:time_start')
        time_end = col.aggregate_max('system:time_end')
        index_list = col.aggregate_array('system:index')
        index_list = index_list.join(',')
        scene_count = col.size()
        mosaic = col.mosaic().clip(col_geo).copyProperties(img, ["date", "group", "month", "SENSING_ORBIT_NUMBER",
                                                                 "PROCESSING_BASELINE", "SPACECRAFT_NAME"])
        mosaic = mosaic.set('system:time_start', time_start)
        mosaic = mosaic.set('system:time_end', time_end)
        mosaic = mosaic.set('index_list', index_list)
        mosaic = mosaic.set('scene_count', scene_count)
        return mosaic

    # apply the mosaicing function
    S2_sr = ee.ImageCollection(joinCol_S2_sr.map(mosaic_collection))

    ##############################
    # REGISTER
    # function to register S2 images to S2_gri
    def S2regFunc(image):
        # Use bicubic resampling during registration.
        imageOrig = image.reproject('epsg:32632', None, 10).resample('bicubic')

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

        # Compute image offset and direction. Could be used if needed
        # reg_offset = reg_dx.hypot(reg_dy).rename('reg_offset')
        # reg_angle = reg_dx.atan2(reg_dy).rename('reg_offsetAngle')

        # Use the computed displacement to register all original bands.
        registered = image.displace(displacement) \
            .addBands(reg_dx) \
            .addBands(reg_dy) \
            .addBands(reg_confidence)

        return registered

    S2regCol = S2_sr.map(S2regFunc)
    ##############################
    # EXPORT
    img_exp = ee.Image(S2regCol.toList(S2regCol.size(), 0).get(0))

    # The time given (including the seconds) refers to the first image of the mosaic but is same for all images in one mosaic
    sensing_date = img_exp.get('system:index').getInfo()[0:15]
    sensing_date_read = sensing_date[0:4] + '-' + sensing_date[4:6] + '-' + sensing_date[6:8] \
                        + '_' + sensing_date[8:11] + '-' + sensing_date[11:13] + '-' + sensing_date[13:15]

    fname_10m = 'S2-L2A_Mosaic_' + sensing_date_read + '_Bands-10m'
    fname_20m = 'S2-L2A_Mosaic_' + sensing_date_read + '_Bands-20m'
    fname_60m = 'S2-L2A_Mosaic_' + sensing_date_read + '_Bands-60m'
    fname_reg = 'S2-L2A_Mosaic_' + sensing_date_read + '_Registration-10m'
    fname_masks = 'S2-L2A_Mosaic_' + sensing_date_read + '_Masks-10m'  # cloud mask + cloud shadow mask, terrain shadow mask
    fname_cloudP = 'S2-L2A_Mosaic_' + sensing_date_read + '_CloudProbability-10m'

    # to be clarified
    # "AOT", "WVP", "SCL", "TCI_R", "TCI_G", "TCI_B", "MSK_CLDPRB", "MSK_SNWPRB", "QA10", "QA20", "QA60", "cloudProbability",

    # the image geometry covers larger areas than actually needed
    aoi_img = img_exp.geometry()
    # therefore it is clipped with rectangle to keep the geometry simple
    # the alternative clip with aoi_CH would be computationally heavier
    aoi_exp = aoi_img.intersection(aoi_rectangle)  # alternativ': aoi_CH
    # print('aoi_exp', aoi_exp)

    if export_to_drive:
        # Export COG 10m bands
        task = ee.batch.Export.image.toDrive(
            image=img_exp.select(['B2', 'B3', 'B4', 'B8']),
            scale=10,
            description=fname_10m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            folder='eeExports',
            fileNamePrefix=fname_10m,
            fileFormat='GeoTIFF',
            formatOptions={
                'cloudOptimized': True
            }
        )
        task.start()

        # Export COG 20m bands
        task = ee.batch.Export.image.toDrive(
            image=img_exp.select(['B5', 'B6', 'B7', 'B8A', 'B11', 'B12']),
            scale=20,
            description=fname_20m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            folder='eeExports',
            fileNamePrefix=fname_20m,
            fileFormat='GeoTIFF',
            formatOptions={
                'cloudOptimized': True
            }
        )
        task.start()

        # Export COG 60m bands
        task = ee.batch.Export.image.toDrive(
            image=img_exp.select(['B1', 'B9']),
            scale=60,
            description=fname_60m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            folder='eeExports',
            fileNamePrefix=fname_60m,
            fileFormat='GeoTIFF',
            formatOptions={
                'cloudOptimized': True
            }
        )
        task.start()

        # Export COG Registration Layers
        task = ee.batch.Export.image.toDrive(
            image=img_exp.select(['reg_dx', 'reg_dy', 'reg_confidence']),  # ? also include reg_offset, reg_offsetAngle
            scale=10,
            description=fname_reg,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            folder='eeExports',
            fileNamePrefix=fname_reg,
            fileFormat='GeoTIFF',
            formatOptions={
                'cloudOptimized': True
            }
        )
        task.start()

        # Export COG Masks
        task = ee.batch.Export.image.toDrive(
            image=img_exp.select(['terrainShadow', 'cloudShadowMask']),
            scale=10,
            description=fname_masks,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            folder='eeExports',
            fileNamePrefix=fname_masks,
            fileFormat='GeoTIFF',
            formatOptions={
                'cloudOptimized': True
            }
        )
        task.start()

        # Export COG cloud probability
        task = ee.batch.Export.image.toDrive(
            image=img_exp.select(['cloudProbability']),
            scale=10,
            description=fname_cloudP,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            folder='eeExports',
            fileNamePrefix=fname_cloudP,
            fileFormat='GeoTIFF',
            formatOptions={
                'cloudOptimized': True
            }
        )
        task.start()

    if export_to_asset:
        # Export COG 10m bands
        task = ee.batch.Export.image.toAsset(
            image=img_exp.select(['B2', 'B3', 'B4', 'B8']),
            scale=10,
            description=fname_10m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            assetId='projects/satromo-exolabs/assets/col_s2_l2a_10m/' + fname_10m,
        )
        task.start()

        # Export COG 20m bands
        task = ee.batch.Export.image.toAsset(
            image=img_exp.select(['B5', 'B6', 'B7', 'B8A', 'B11', 'B12']),
            scale=20,
            description=fname_20m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            assetId='projects/satromo-exolabs/assets/col_s2_l2a_20m/' + fname_20m,
        )
        task.start()

        # Export COG 60m bands
        task = ee.batch.Export.image.toAsset(
            image=img_exp.select(['B1', 'B9']),
            scale=60,
            description=fname_60m,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            assetId='projects/satromo-exolabs/assets/col_s2_l2a_60m/' + fname_60m,
        )
        task.start()

        # Export COG Registration Layers
        task = ee.batch.Export.image.toAsset(
            image=img_exp.select(['reg_dx', 'reg_dy', 'reg_confidence']),  # ? also include reg_offset, reg_offsetAngle
            scale=10,
            description=fname_reg,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            assetId='projects/satromo-exolabs/assets/col_s2_l2a_displacement/' + fname_reg,
        )
        task.start()

        # Export COG Masks
        task = ee.batch.Export.image.toAsset(
            image=img_exp.select(['terrainShadow', 'cloudShadowMask']),
            scale=10,
            description=fname_masks,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            assetId='projects/satromo-exolabs/assets/col_s2_l2a_masks/' + fname_masks,
        )
        task.start()

        # Export COG cloud probability
        task = ee.batch.Export.image.toAsset(
            image=img_exp.select(['cloudProbability']),
            scale=10,
            description=fname_cloudP,
            crs='EPSG:2056',
            region=aoi_exp,
            maxPixels=1e13,
            assetId='projects/satromo-exolabs/assets/col_s2_cloudproba/' + fname_cloudP,
        )
        task.start()


if __name__ == '__main__':
    start_date = datetime(2023, 6, 16)
    end_date = datetime(2023, 6, 16)
    delta = timedelta(days=1)
    while start_date <= end_date:
        print(start_date.strftime("%Y-%m-%d"))
        generate_asset_mosaic_for_single_date(day_to_process=start_date, export_to_drive=False, export_to_asset=True)
        start_date += delta
