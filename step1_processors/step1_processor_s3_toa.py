import ee
import datetime
import configuration as config
from main_functions import main_utils


def process_S3_LEVEL_1(roi, current_date):
    """
    Export the S3 Level 1 product.

    Returns:
        str: "no new imagery" if no new imagery found, None if new imagery is processed.
    """

    product_name = config.PRODUCT_S3_LEVEL_1['product_name']
    print("********* processing {} *********".format(product_name))

    # Filter the sensor collection based on date and region

    start_date = ee.Date(
        current_date).advance(-int(config.PRODUCT_S3_LEVEL_1['temporal_coverage'])+1, 'day')
    end_date = ee.Date(current_date).advance(1, 'day')

    collection = (
        ee.ImageCollection(config.PRODUCT_S3_LEVEL_1['step0_collection'])
        .filterDate(start_date, end_date)
        .filterBounds(roi)
    )
    # Get the number of images found in the collection
    num_images = collection.size().getInfo()
    # Check if there are any new imagery
    if num_images == 0:
        print("no new imagery")
        return 0

    # Get information about the available sensor data for the range
    sensor_stats = main_utils.get_collection_info(collection)

    # Check if there is new sensor data compared to the stored dataset
    if main_utils.check_product_update(config.PRODUCT_S3_LEVEL_1['product_name'], sensor_stats[1]) is True:
        # Get the list of images
        image_list = collection.toList(collection.size())
        print(str(image_list.size().getInfo()) + " new image(s) for: " +
              sensor_stats[1] + " to: "+current_date.format("YYYY-MM-dd").getInfo())

        # Print the names of the assets
        for i in range(num_images):
            image = ee.Image(image_list.get(i))
            asset_name = image.get('system:index').getInfo()
            print(f"Mosaic {i + 1} - Custom Asset Name: {asset_name}")

        # Export the different bands
        for i in range(num_images):
            # Generate the mosaic name and sensing date by geeting EE asset ids from the first image
            mosaic_id = ee.Image(image_list.get(i))
            mosaic_id = mosaic_id.id().getInfo()
            mosaic_sensing_timestamp = mosaic_id.split('_')[2]

            clipped_image = ee.Image(collection.toList(num_images).get(i))

            # Clip Image to ROI
            clip_temp = clipped_image.clip(roi)
            clipped_image = clip_temp

            # # Get the bounding box of clippedRoi
            clipped_image_bounding_box = clipped_image.geometry()

            # Get processing date
            # Get the current date and time
            now = datetime.datetime.now()

            # Convert it to a string in ISO 8601 format and remove the seconds
            processing_date = now.strftime("%Y-%m-%dT%H:%M")

            breakpoint()
            # TODO :
            # - decide if we store the bands in different collections based on spatial resolution
            # - then export accordingly, chnage the content below

            # Check if mosaic_id ends with "-10m"
            if mosaic_id.endswith("-10m"):

                # Export selected bands (B4, B3, B2, B8) as a single GeoTIFF with '_10M'
                multiband_export = clipped_image.select(
                    ['B4', 'B3', 'B2', 'B8'])

                # Replacing the collection Name  with the actual product name
                # multiband_export_name = mosaic_id
                multiband_export_name = mosaic_id.replace(
                    "S2-L2A", product_name)

                main_utils.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, multiband_export_name,
                                          config.PRODUCT_S2_LEVEL_2A['product_name'], 10,
                                          multiband_export, sensor_stats, processing_date)

                # Export terrain & shadow Mask
                masks_export = clipped_image.select(
                    ['terrainShadowMask', 'cloudAndCloudShadowMask', 'reg_confidence'])
                masks_export_name = mosaic_id.replace(
                    '_bands-10m', '_masks-10m')

                # Replacing the collection Name  with the actual product name
                masks_export_name = masks_export_name.replace(
                    "S2-L2A", product_name)

                main_utils.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, masks_export_name,
                                          config.PRODUCT_S2_LEVEL_2A['product_name'],
                                          10,
                                          masks_export, sensor_stats, processing_date)

                # Export Registration
                masks_export = clipped_image.select(
                    ['reg_dx', 'reg_dy'])
                masks_export_name = mosaic_id.replace(
                    '_bands-10m', '_registration-10m')

                # Replacing the collection Name  with the actual product name
                masks_export_name = masks_export_name.replace(
                    "S2-L2A", product_name)

                main_utils.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, masks_export_name,
                                          config.PRODUCT_S2_LEVEL_2A['product_name'],
                                          10,
                                          masks_export, sensor_stats, processing_date)

                # Export Cloudprobability
                masks_export = clipped_image.select(
                    ['cloudProbability'])
                masks_export_name = mosaic_id.replace(
                    '_bands-10m', '_cloudprobability-10m')
                # Replacing the collection Name  with the actual product name
                masks_export_name = masks_export_name.replace(
                    "S2-L2A", product_name)

                main_utils.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, masks_export_name,
                                          config.PRODUCT_S2_LEVEL_2A['product_name'],
                                          10,
                                          masks_export, sensor_stats, processing_date)

            # Check if mosaic_id ends with "-20m"
            elif mosaic_id.endswith("-20m"):
                # Export selected bands ('B8A', 'B11', 'B5') as a single GeoTIFF with '_20M'
                multiband_export = clipped_image.select(['B8A', 'B11', 'B5'])

                # Replacing the collection Name  with the actual product name
                # multiband_export_name = mosaic_id
                multiband_export_name = mosaic_id.replace(
                    "S2-L2A", product_name)

                main_utils.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, multiband_export_name,
                                          config.PRODUCT_S2_LEVEL_2A['product_name'], 20,
                                          multiband_export, sensor_stats, processing_date)
