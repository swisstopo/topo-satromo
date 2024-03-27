import ee
import datetime
import configuration as config
from main_functions import main_utils

def process_PRODUCT_V1(roi,collection_ready,current_date_str):
    """
    Process swissEO VHI: Karte des Vegetationszustandes .ch.swisstopo.swisseo_vhi_v100

    Returns:
        None
    """
    product_name = config.PRODUCT_V1['product_name']
    print("********* processing {} *********".format(product_name))

    #ee Date
    current_date = ee.Date(current_date_str)

    # Filter the sensor collection based on date and region
    start_date = ee.Date(
        current_date).advance(-int(config.PRODUCT_V1['temporal_coverage'])+1, 'day')

    end_date = ee.Date(current_date).advance(1, 'day')

    sensor = (
        ee.ImageCollection(config.PRODUCT_V1['step0_collection'])
        .filterDate(start_date, end_date)
        .filterBounds(roi)
        # we need only the 10m bands!
        .filter(ee.Filter.stringEndsWith('system:index', '-10m'))
    )

    # above filters  assets which only end with_bands-10m, and use then this collection: see docu unter https://developers.google.com/earth-engine/guides/ic_filtering
    #     filtered = sensor.filter(ee.Filter.stringEndsWith('system:index', '-10m'))
    # -> now Use filtered as colelction

    # Get information about the available sensor data for the range
    sensor_stats = main_utils.get_collection_info(sensor)

    # Check if there is new sensor data compared to the stored dataset
    if main_utils.check_product_update(config.PRODUCT_V1['product_name'], sensor_stats[1]) is True:
        print("new imagery from: "+sensor_stats[1])

        # Create NDVI and NDVI max
        sensor = sensor.map(lambda image: main_utils.addINDEX(
            image, bands=config.PRODUCT_V1['band_names'][0], index_name="NDVI"))

        mosaic = sensor.qualityMosaic("NDVI")
        ndvi_max = mosaic.select("NDVI")

        # Multiply by 100 to move the decimal point two places back to the left and get rounded values,
        # then round then cast to get int16, Int8 is not a solution since COGTiff is not supported
        ndvi_max_int = ndvi_max.multiply(100).round().toInt16()

        # Mask outside
        ndvi_max_int = main_utils.maskOutside(
            ndvi_max_int, roi).unmask(config.NODATA)

        # Define item Name
        timestamp = datetime.datetime.strptime(current_date_str, '%Y-%m-%d')
        timestamp = timestamp.strftime('%Y-%m-%dT235959')

        # Generate the filename
        filename = config.PRODUCT_V1['product_name'] + \
            '_mosaic_' + timestamp + '_10m'
        print(filename)

        # extract collection properties to assign to the product
        time_start = sensor.aggregate_min('system:time_start')
        time_end = sensor.aggregate_max('system:time_end')
        index_list = sensor.aggregate_array('system:index')
        index_list = index_list.join(',')
        scene_count = sensor.size()
        ee_version = ee.__version__

        # set the properties
        ndvi_max_int = ndvi_max_int.set('system:time_start', time_start) \
            .set('system:time_end', time_end) \
            .set('collection', collection_ready)\
            .set('index_list', index_list) \
            .set('scene_count', scene_count) \
            .set('GEE_api_version', ee_version)

        # Start the export
        main_utils.prepare_export(roi, timestamp, filename, config.PRODUCT_V1['product_name'],
                                  config.PRODUCT_V1['spatial_scale_export'], ndvi_max_int,
                                  sensor_stats, current_date_str)
