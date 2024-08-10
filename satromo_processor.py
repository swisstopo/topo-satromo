# -*- coding: utf-8 -*-
import sys
from pydrive.auth import GoogleAuth
import csv
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import requests
import csv
import json
import os
import ee
import configuration as config
from step0_functions import get_step0_dict, step0_main
from step1_processors import step1_processor_l57_sr, step1_processor_l57_toa, step1_processor_l89_sr, step1_processor_l89_toa, step1_processor_s3_toa, step1_processor_vhi
from main_functions import main_utils
import pandas as pd
from google.cloud import storage


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2
        print("\nType 2 run PROCESSOR: We are on a local machine")
    else:
        run_type = 1
        print("\nType 1 run PROCESSOR: We are on GitHub")


def initialize_gee_and_drive():
    """
    Initializes Google Earth Engine (GEE) and Google Drive based on the run type.

    If the run type is 2, initializes GEE and authenticates using the service account key file.
    If the run type is 1, initializes GEE and authenticates using secrets from GitHub Action.

    Prints a success or failure message after initializing GEE.

    Note: This function assumes the required credentials and scopes are properly set.

    Returns:
        None
    """
    # Set scopes for Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]

    if run_type == 2:
        # Initialize GEE and authenticate using the service account key file

        # Read the service account key file
        with open(config.GDRIVE_SECRETS, "r") as f:
            data = json.load(f)

        # Authenticate with Google using the service account key file
        gauth = GoogleAuth()
        gauth.service_account_file = config.GDRIVE_SECRETS
        gauth.service_account_email = data["client_email"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )
    else:
        # Run other code using secrets from GitHub Action
        # This script is running on GitHub
        gauth = GoogleAuth()
        google_client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
        google_client_secret = json.loads(google_client_secret)
        gauth.service_account_email = google_client_secret["client_email"]
        google_client_secret_str = json.dumps(google_client_secret)

        # Write the JSON string to a temporary key file
        gauth.service_account_file = "keyfile.json"
        with open(gauth.service_account_file, "w") as f:
            f.write(google_client_secret_str)

        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )

    # Initialize Google Earth Engine
    credentials = ee.ServiceAccountCredentials(
        gauth.service_account_email, gauth.service_account_file
    )
    ee.Initialize(credentials)

    # Test if GEE initialization is successful
    image = ee.Image("NASA/NASADEM_HGT/001")
    title = image.get("title").getInfo()

    if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
        print("GEE initialization successful")
    else:
        print("GEE initialization FAILED")


def process_NDVI_MAX(roi):
    """
    Process the NDVI MAX product.

    Returns:
        int: 1 if new imagery is found and processing is performed, 0 otherwise.
    """
    product_name = config.PRODUCT_NDVI_MAX['product_name']
    print("********* processing {} *********".format(product_name))

    # Filter the sensor collection based on date and region
    start_date = ee.Date(current_date).\
        advance(-int(config.PRODUCT_NDVI_MAX['temporal_coverage'])+1, 'day')
    end_date = ee.Date(current_date).advance(1, 'day')

    # Filter the sensor collection based on date and region
    sensor = (
        ee.ImageCollection(config.PRODUCT_NDVI_MAX['step0_collection'])
        .filterDate(start_date, end_date)
        .filterBounds(roi)
    )

    # Get information about the available sensor data for the range
    sensor_stats = main_utils.get_collection_info(sensor)

    # Check if there is new sensor data compared to the stored dataset
    if main_utils.check_product_update(config.PRODUCT_NDVI_MAX['product_name'], sensor_stats[1]) is True:
        print("new imagery from: "+sensor_stats[1])

        # Create NDVI and NDVI max
        sensor = sensor.map(lambda image: main_utils.addINDEX(
            image, bands=config.PRODUCT_NDVI_MAX['band_names'][0], index_name="NDVI"))

        mosaic = sensor.qualityMosaic("NDVI")
        ndvi_max = mosaic.select("NDVI")

        # Multiply by 100 to move the decimal point two places back to the left and get rounded values,
        # then round then cast to get int16, Int8 is not a sultion since COGTiff is not supported
        ndvi_max_int = ndvi_max.multiply(100).round().toInt16()

        # Mask outside
        ndvi_max_int = main_utils.maskOutside(
            ndvi_max_int, roi).unmask(config.NODATA)

        # Define item Name
        timestamp = datetime.datetime.strptime(current_date_str, '%Y-%m-%d')
        timestamp = timestamp.strftime('%Y%m%dT235959')

        # Generate the filename
        filename = config.PRODUCT_NDVI_MAX['prefix'] + \
            '_' + timestamp + '_10m'
        print(filename)

        # Check if there is at least 1 scene to be defined (if minimal scene count is required) TODO: is this necessary?
        if sensor_stats[2] > 0:
            # Start the export
            main_utils.prepare_export(roi, timestamp, filename, config.PRODUCT_NDVI_MAX['product_name'],
                                      config.PRODUCT_NDVI_MAX['spatial_scale_export'], ndvi_max_int,
                                      sensor_stats, current_date_str)


def process_S2_LEVEL_2A(roi):
    """
    Export the S2 Level 2A product.

    Returns:
        str: "no new imagery" if no new imagery found, None if new imagery is processed.
    """

    product_name = config.PRODUCT_S2_LEVEL_2A['product_name']
    print("********* processing {} *********".format(product_name))

    # Filter the sensor collection based on date and region

    start_date = ee.Date(
        current_date).advance(-int(config.PRODUCT_S2_LEVEL_2A['temporal_coverage'])+1, 'day')
    end_date = ee.Date(current_date).advance(1, 'day')

    collection = (
        ee.ImageCollection(config.PRODUCT_S2_LEVEL_2A['step0_collection'])
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
    if main_utils.check_product_update(config.PRODUCT_S2_LEVEL_2A['product_name'], sensor_stats[1]) is True:
        # Get the list of images
        image_list = collection.toList(collection.size())
        print(str(image_list.size().getInfo()) + " new image(s) for: " +
              sensor_stats[1] + " to: "+current_date_str)

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


def process_S2_LEVEL_1C(roi):
    """
    Export the S2 Level 1C product.

    Returns:
        None
    """
    product_name = config.PRODUCT_S2_LEVEL_1C['product_name']
    print("********* processing {} *********".format(product_name))

    # Filter the sensor collection based on date and region
    start_date = ee.Date(
        current_date).advance(-int(config.PRODUCT_S2_LEVEL_1C['temporal_coverage'])+1, 'day')
    end_date = ee.Date(current_date).advance(1, 'day')

    collection = (
        ee.ImageCollection(config.PRODUCT_S2_LEVEL_1C['step0_collection'])
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
    if main_utils.check_product_update(config.PRODUCT_S2_LEVEL_1C['product_name'], sensor_stats[1]) is True:
        # Get the list of images
        image_list = collection.toList(collection.size())
        image_list_size = image_list.size().getInfo()
        print("{} new image(s) for: {} to {}".format(
            image_list_size, sensor_stats[1], current_date_str))

        # Generate the mosaic name and sensing date by geeting EE asset ids from the first image
        mosaic_id = ee.Image(image_list.get(0))
        mosaic_id = mosaic_id.id().getInfo()
        mosaic_sensing_timestamp = mosaic_id.split('_')[2]

        # Create a mosaic of the images for the specified date and time
        mosaic = collection.mosaic()

        # Clip Image to ROI
        # might add .unmask(config.NODATA)
        clipped_image = mosaic.clip(roi)

        # Intersect ROI and clipped mosaic
        # Create an empty list to hold the footprints
        footprints = ee.List([])

        # Function to extract footprint from each image and add to the list
        def add_footprint(image, lst):
            footprint = image.geometry()
            return ee.List(lst).add(footprint)

        # Map the add_footprint function over the collection to create a list of footprints
        footprints_list = collection.iterate(add_footprint, footprints)

        # Reduce the list of footprints into a single geometry using reduce
        combined_swath_geometry = ee.Geometry.MultiPolygon(footprints_list)

        # Clip the ROI with the combined_swath_geometry
        clipped_roi = roi.intersection(
            combined_swath_geometry, ee.ErrorMargin(1))

        # Get the bounding box of clippedRoi
        clipped_image_bounding_box = clipped_roi.bounds()

        # Export selected bands (B4, B3, B2, B8) as a single GeoTIFF with '_10M'
        multiband_export = clipped_image.select(['B4', 'B3', 'B2', 'B8'])
        multiband_export_name = mosaic_id

        main_utils.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, multiband_export_name,
                                  config.PRODUCT_S2_LEVEL_1C['product_name'], config.PRODUCT_S2_LEVEL_1C['spatial_scale_export'],
                                  multiband_export, sensor_stats, current_date_str)

        # Export QA60 band as a separate GeoTIFF with '_QA60'
        masks_export = clipped_image.select(
            ['terrainShadowMask', 'cloudAndCloudShadowMask'])
        masks_export_name = mosaic_id.replace('_bands-10m', '_masks-10m')
        main_utils.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, masks_export_name,
                                  config.PRODUCT_S2_LEVEL_1C['product_name'],
                                  config.PRODUCT_S2_LEVEL_1C['spatial_scale_export_mask'], masks_export,
                                  sensor_stats, current_date_str)


def process_NDVI_MAX_TOA(roi):
    """
    Process the NDVI MAX product for TOA.

    Returns:
        None
    """
    product_name = config.PRODUCT_NDVI_MAX_TOA['product_name']
    print("********* processing {} *********".format(product_name))

    # Filter the sensor collection based on date and region
    start_date = ee.Date(
        current_date).advance(-int(config.PRODUCT_NDVI_MAX_TOA['temporal_coverage'])+1, 'day')

    end_date = ee.Date(current_date).advance(1, 'day')

    sensor = (
        ee.ImageCollection(config.PRODUCT_NDVI_MAX_TOA['step0_collection'])
        .filterDate(start_date, end_date)
        .filterBounds(roi)
    )

    # Get information about the available sensor data for the range
    sensor_stats = main_utils.get_collection_info(sensor)

    # Check if there is new sensor data compared to the stored dataset
    if main_utils.check_product_update(config.PRODUCT_NDVI_MAX_TOA['product_name'], sensor_stats[1]) is True:
        print("new imagery from: "+sensor_stats[1])

        # Create NDVI and NDVI max
        sensor = sensor.map(lambda image: main_utils.addINDEX(
            image, bands=config.PRODUCT_NDVI_MAX_TOA['band_names'][0], index_name="NDVI"))

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
        timestamp = timestamp.strftime('%Y%m%dT235959')

        # Generate the filename
        filename = config.PRODUCT_NDVI_MAX_TOA['prefix'] + \
            '_' + timestamp + '_10m.tif'
        print(filename)

        # Start the export
        main_utils.prepare_export(roi, timestamp, filename, config.PRODUCT_NDVI_MAX['product_name'],
                                  config.PRODUCT_NDVI_MAX['spatial_scale_export'], ndvi_max_int,
                                  sensor_stats, current_date_str)


if __name__ == "__main__":
    # Test if we are on Local DEV Run or if we are on PROD
    determine_run_type()

    # Authenticate with GEE and GDRIVE
    initialize_gee_and_drive()

    # Get current date
    current_date_str = datetime.datetime.today().strftime('%Y-%m-%d')

    # Get the current date
    current_date = datetime.datetime.today()

    # Subtract X day back from the current date to procoess not todays but the  date in the past: This is to overcome the delay
    delay = 3  # in days
    previous_date = current_date - datetime.timedelta(days=delay)

    # Convert the previous date to a string in the format 'YYYY-MM-DD' and set it to current date
    current_date_str = previous_date.strftime('%Y-%m-%d')
    print("Processing :", current_date_str)

    # For debugging
    # --------------
    current_date_str = "2023-10-31"

    print("*****************************\n")
    print("using a manual set Date: " + current_date_str)
    print("*****************************\n")

    # For CLI
    # --------------
    # satromo_processor.py
    from configuration import arg_date_str

    # Check if current_date_str is set by the command line
    if arg_date_str:

        # Use the default date
        current_date_str = arg_date_str
        print(f'Using command line set date: {arg_date_str}')

    # Define date to be used
    current_date = ee.Date(current_date_str)

    roi = ee.Geometry.Rectangle(config.ROI_RECTANGLE)

    # Retrieve the step0 information from the config object and store it in a dictionary
    step0_product_dict = get_step0_dict()
    # Print the dictionary containing collection names and their details
    print(step0_product_dict)

    # Process the step0 collections to determine which ones are ready for processing
    collections_ready_for_processors = step0_main(
        step0_product_dict, current_date_str)
    # Print the list of collections that are ready for processing
    print(collections_ready_for_processors)

    for collection_ready in collections_ready_for_processors:
        print('Collection ready: {}'.format(collection_ready))
        for product_to_be_processed in step0_product_dict[collection_ready][0]:
            print('Launching product {}'.format(product_to_be_processed))
            if product_to_be_processed == 'PRODUCT_NDVI_MAX':  # TODO Needs to be checked if needed
                roi = ee.Geometry.Rectangle(config.ROI_RECTANGLE)
                result = process_NDVI_MAX(roi)

            elif product_to_be_processed == 'PRODUCT_S2_LEVEL_2A':
                # ROI is only taking effect when testing. On prod we will use the clipping as defined in step0_processor_s2_sr
                # border = ee.FeatureCollection(
                #     "USDOS/LSIB_SIMPLE/2017").filter(ee.Filter.eq("country_co", "SZ"))
                # roi = border.geometry().buffer(config.ROI_BORDER_BUFFER)
                # roi = ee.Geometry.Rectangle(
                #     [7.075402, 46.107098, 7.100894, 46.123639])
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                # roi = ee.Geometry.Rectangle(
                #     [8.10, 47.18, 8.20, 47.25])  # 6221 Rickenbach
                # roi = ee.Geometry.Rectangle(
                #     [7.938447, 47.514378, 8.127522, 47.610846])
                result = process_S2_LEVEL_2A(roi)

            elif product_to_be_processed == 'PRODUCT_VHI':
                roi = ee.Geometry.Rectangle(config.ROI_RECTANGLE)
                # roi = ee.Geometry.Rectangle(
                #     [8.10, 47.18, 8.20, 47.25])  # 6221 Rickenbach
                result = step1_processor_vhi.process_PRODUCT_VHI(
                    roi, collection_ready, current_date_str)

            elif product_to_be_processed == 'PRODUCT_NDVI_MAX_TOA':
                roi = ee.Geometry.Rectangle(config.ROI_RECTANGLE)
                result = process_NDVI_MAX_TOA(roi)

            elif product_to_be_processed == 'PRODUCT_S2_LEVEL_1C':
                border = ee.FeatureCollection(
                    "USDOS/LSIB_SIMPLE/2017").filter(ee.Filter.eq("country_co", "SZ"))
                roi = border.geometry().buffer(config.ROI_BORDER_BUFFER)
                # roi = ee.Geometry.Rectangle( [ 7.075402, 46.107098, 7.100894, 46.123639])
                result = process_S2_LEVEL_1C(roi)

            elif product_to_be_processed == 'PRODUCT_L57_LEVEL_2':
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                result = step1_processor_l57_sr.process_L57_LEVEL_2(
                    roi, current_date)

            elif product_to_be_processed == 'PRODUCT_L57_LEVEL_1':
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                result = step1_processor_l57_toa.process_L57_LEVEL_1(
                    roi, current_date)

            elif product_to_be_processed == 'PRODUCT_L89_LEVEL_2':
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                result = step1_processor_l89_sr.process_L89_LEVEL_2(
                    roi, current_date)

            elif product_to_be_processed == 'PRODUCT_L89_LEVEL_1':
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                result = step1_processor_l89_toa.process_L89_LEVEL_1(
                    roi, current_date)

            elif product_to_be_processed == 'PRODUCT_S3_LEVEL_1':
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                result = step1_processor_s3_toa.process_S3_LEVEL_1(
                    roi, current_date)

            elif product_to_be_processed == 'PRODUCT_MSG_CLIMA':
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                result = "PRODUCT_MSG_CLIMA:  step0 only"

            elif product_to_be_processed == 'PRODUCT_MSG':
                # roi = ee.Geometry.Rectangle(
                #     [9.49541, 47.22246, 9.55165, 47.26374,])  # Liechtenstein
                result = "PRODUCT_MSG:  step0 only"

            else:
                raise BrokenPipeError('Inconsitent configuration')

            print("Result:", result)

print("Processing done!")
