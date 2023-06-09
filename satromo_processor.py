# -*- coding: utf-8 -*-
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


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2
        print("\nType 2 run PROCESSOR: We are on DEV")
    else:
        run_type = 1
        print("\nType 1 run PROCESSOR: We are on PROD")


def get_github_info():
    """
    Retrieves GitHub repository information and generates a GitHub link based on the latest commit.

    Returns:
        A dictionary containing the GitHub link. If the request fails or no commit hash is available, the link will be None.
    """
    # Enter your GitHub repository information
    owner = config.GITHUB_OWNER
    repo = config.GITHUB_REPO

    # Make a GET request to the GitHub API to retrieve information about the repository
    response = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/commits/main")

    github_info = {}

    if response.status_code == 200:
        # Extract the commit hash from the response
        commit_hash = response.json()["sha"]

        # Generate the GitHub link
        github_link = f"https://github.com/{owner}/{repo}/commit/{commit_hash}"
        github_info["GithubLink"] = github_link

    else:
        github_info["GithubLink"] = None

    # Make a GET request to the GitHub API to retrieve information about the repository releases
    response = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/releases/latest")

    if response.status_code == 200:
        # Extract the release version from the response
        release_version = response.json()["tag_name"]
    else:
        release_version = "0.0.0"

    github_info["ReleaseVersion"] = release_version

    return github_info


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


def maskOutside(image, aoi):
    """
    Masks the areas outside the specified region of interest (AOI) in an image.

    Args:
        image: The image to be masked.
        aoi: The region of interest (AOI) to keep in the image.

    Returns:
        The image with the areas outside the AOI masked.
    """
    # Create a constant image with a value of 1, clip it to the AOI, and use it as a mask
    # add .not() after mask() to mask inside
    mask = ee.Image.constant(1).clip(aoi).mask()

    # Apply the mask to the image
    return image.updateMask(mask)

# Function to analyse the number of sceneds first and last day


def get_collection_info(collection):
    """
    Retrieves information about an image collection.

    Args:
        collection: The image collection to retrieve information from.

    Returns:
        A tuple containing the first date, last date, and total number of images in the collection.
    """
    # Sort the collection by date in ascending order
    sorted_collection = collection.sort('system:time_start')

    # Get the first and last image from the sorted collection
    first_image = sorted_collection.first()
    last_image = sorted_collection.sort('system:time_start', False).first()

    # Get the dates of the first and last image
    first_date = ee.Date(first_image.get('system:time_start')
                         ).format('YYYY-MM-dd').getInfo()
    last_date = ee.Date(last_image.get('system:time_start')
                        ).format('YYYY-MM-dd').getInfo()

    # Get the count of images in the filtered collection
    image_count = collection.size()

    # Get the scenes count
    total_scenes = image_count.getInfo()

    # Return the first date, last date, and total number of scenes
    return first_date, last_date, total_scenes


def get_quadrants(roi):
    """
    Divide a region of interest into quadrants.

    Parameters:
    roi (ee.Geometry): Region of interest.

    Returns:
    dict: Dictionary with the quadrants (quadrant1, quadrant2, quadrant3, quadrant4).
    """
    # Calculate the bounding box of the region
    bounds = roi.bounds()

    # Get the coordinates of the bounding box
    bbox = bounds.coordinates().getInfo()[0]

    # Extract the coordinates
    min_x, min_y = bbox[0]
    max_x, max_y = bbox[2]

    # Calculate the midpoints
    mid_x = (min_x + max_x) / 2
    mid_y = (min_y + max_y) / 2

    # Define the quadrants
    quadrant1 = ee.Geometry.Rectangle(min_x, min_y, mid_x, mid_y)
    quadrant2 = ee.Geometry.Rectangle(mid_x, min_y, max_x, mid_y)
    quadrant3 = ee.Geometry.Rectangle(min_x, mid_y, mid_x, max_y)
    quadrant4 = ee.Geometry.Rectangle(mid_x, mid_y, max_x, max_y)

    return {
        "quadrant1": quadrant1,
        "quadrant2": quadrant2,
        "quadrant3": quadrant3,
        "quadrant4": quadrant4
    }


def start_export(image, scale, description, region, filename_prefix, crs):
    """
    Starts an export task to export an image to Google Drive.
    

    Args:
        image: The image to be exported.
        scale: The scale of the exported image.
        description: The description of the export task.
        region: The region of interest (ROI) to export.
        filename_prefix: The prefix to be used for the exported file.
        crs: The coordinate reference system (CRS) of the exported image.

    Returns:
        None
    """

    # only reproject but without scale use this code, based on https://developers.google.com/earth-engine/guides/exporting#setting_scal
    # projection = image.projection().getInfo()
    # task = ee.batch.Export.image.toDrive(
    #     image=image,
    #     description=description,
    #     "region "= "region",
    #     fileNamePrefix=filename_prefix,
    #     crs=crs,
    #     maxPixels=1e13,
    #     fileFormat ="GeoTIFF",
    #     crsTransform = projection['transform']
    # )

    # Export in GEE native Geotiff / Projection  with proper scale   and reprojetc in satromo_publish/postprocessing with gdal
    task = ee.batch.Export.image.toDrive(
        image=image,
        description=description,
        scale=scale,
        region=region,
        fileNamePrefix=filename_prefix,        
        maxPixels=1e13,
        crs ="EPSG:3857",
        fileFormat ="GeoTIFF",        
    )
    task.start()

    # Get Task ID
    task_id = task.status()["id"]
    print("Exporting  with Task ID:", task_id + f" file {filename_prefix}...")

    # Save Task ID and filename to a text file
    header = ["Task ID", "Filename"]
    data = [task_id, filename_prefix]

    # Check if the file already exists
    file_exists = os.path.isfile(config.GEE_RUNNING_TASKS)

    with open(config.GEE_RUNNING_TASKS, "a", newline="") as f:
        writer = csv.writer(f)

        # Write the header if the file is newly created
        if not file_exists:
            writer.writerow(header)

        # Write the data
        writer.writerow(data)


def check_product_update(product_name, date_string):
    """
    Check if the given product has a newer "LastSceneDate" than the provided date.

    Parameters:
    product_name (str): Name of the product to check.
    date_string (str): Date in the format "YYYY-MM-DD" for comparison.

    Returns:
    bool: True if date_String has a newer Date than "LastSceneDate" stored in the product,
    True if the product is not found, False otherwise.
    """
    target_date = datetime.datetime.strptime(date_string, "%Y-%m-%d").date()

    with open(config.LAST_PRODUCT_UPDATES, "r", newline="", encoding="utf-8") as f:
        dict_reader = csv.DictReader(f, delimiter=",")
        for row in dict_reader:
            if row["Product"] == product_name:
                last_scene_date = datetime.datetime.strptime(
                    row["LastSceneDate"], "%Y-%m-%d").date()
                return last_scene_date < target_date
    return True


def update_product_status_file(input_dict, output_file):
    """
    Write a dictionary to a CSV file. If the file exists, the data is appended to it.
    If the file does not exist, a new file is created with a header. The function also
    updates the dictionary entry for the "Product" field.

    Args:
        input_dict (dict): Dictionary to be written to the file.
        output_file (str): Path of the output file.

    Returns:
        None
    """
    # Get the field names from the input dictionary
    fieldnames = list(input_dict.keys())

    if os.path.isfile(output_file):
        # If the file already exists, update the existing data or append new data
        with open(output_file, "r+", newline="", encoding="utf-8") as f:
            dict_reader = csv.DictReader(f, delimiter=",")
            lines = list(dict_reader)
            product_exists = False
            for i, line in enumerate(lines):
                if line["Product"] == input_dict["Product"]:
                    lines[i] = input_dict
                    product_exists = True
                    break
            if not product_exists:
                lines.append(input_dict)

            # Move the file pointer to the beginning
            f.seek(0)
            dict_writer = csv.DictWriter(
                f, fieldnames=fieldnames, delimiter=",", quotechar='"', lineterminator="\n"
            )
            dict_writer.writeheader()
            dict_writer.writerows(lines)

            # Truncate the file to remove any remaining data
            f.truncate()
    else:
        # If the file doesn't exist, create a new file and write the header and data
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            dict_writer = csv.DictWriter(
                f, fieldnames=fieldnames, delimiter=",", quotechar='"', lineterminator="\n"
            )
            dict_writer.writeheader()
            dict_writer.writerow(input_dict)

    # Return None
    return None


def prepare_export(roi, filename, image, sensor_stats, current_date_str):
    """
    Prepare the export of the image by splitting it into quadrants and starting the export tasks.
    It also generates product status information, updates the product status file,
    and writes the product description to a CSV file.

    Args:
        roi (ee.Geometry): Region of interest for the export.
        filename (str): Base filename for the exported files.
        image (ee.Image): Image to be exported.
        sensor_stats (list): List containing sensor statistics.
        current_date_str (str): Current date in string format.

    Returns:
        None
    """

    # Get current Processor Version from GitHub
    processor_version = get_github_info()

    # Define the quadrants to split into 4 regions
    quadrants = get_quadrants(roi)

    for quadrant_name, quadrant in quadrants.items():
        # Create filename for each quadrant
        filename_q = filename + quadrant_name
        # Start the export for each quadrant
        start_export(image, int(config.PRODUCT_NDVI_MAX['spatial_scale_export']),
                     "P:" + config.PRODUCT_NDVI_MAX['product_name'] + " I:" + filename, quadrant, filename_q, config.OUTPUT_CRS)

    # Generate product status information
    product_status = {
        'Product': config.PRODUCT_NDVI_MAX['product_name'],
        'LastSceneDate': sensor_stats[1],
        'RunDate': current_date_str,
        'Status': "RUNNING"
    }

    # Update the product status file
    update_product_status_file(product_status, config.LAST_PRODUCT_UPDATES)

    # Write the product description to a CSV file
    header = ["Product", "Item", "DateFirstScene", "DateLastScene",
              "NumberOfScenes", "DateItemGeneration", "ProcessorHashLink", "ProcessorReleaseVersion"]
    data = [config.PRODUCT_NDVI_MAX['product_name'], filename, str(sensor_stats[0]), str(
        sensor_stats[1]), str(sensor_stats[2]), current_date_str, processor_version["GithubLink"], processor_version["ReleaseVersion"]]

    with open(os.path.join(config.PROCESSING_DIR, filename + ".csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(data)

    # Return None
    return None


def addNDVI(image, bands):
    """
    Add the Normalized Difference Vegetation Index (NDVI) band to the image.

    Args:
        image (ee.Image): Input image to add the NDVI band.
        bands (dict): Dictionary containing band names for NIR and RED.

    Returns:
        ee.Image: Image with the NDVI band added.
    """

    # Extract the band names for NIR and RED from the input dictionary
    NIR = bands['NIR']
    RED = bands['RED']

    # Compute the NDVI using the normalizedDifference() function and rename the band to "NDVI"
    ndvi = image.normalizedDifference([NIR, RED]).rename("NDVI")

    # Add the NDVI band to the image using the addBands() function
    image_with_ndvi = image.addBands(ndvi)

    # Return the image with the NDVI band added
    return image_with_ndvi


def process_NDVI_MAX():
    """
    Process the NDVI MAX product.

    Returns:
        int: 1 if new imagery is found and processing is performed, 0 otherwise.
    """

    print("********* processing " +
          config.PRODUCT_NDVI_MAX['product_name']+" *********")

    # Filter the sensor collection based on date and region
    sensor = (
        ee.ImageCollection(config.PRODUCT_NDVI_MAX['image_collection'])
        .filterDate(current_date.advance(-int(config.PRODUCT_NDVI_MAX['temporal_coverage']), 'day'), current_date)
        .filterBounds(roi)
    )

    # Get information about the available sensor data
    sensor_stats = get_collection_info(sensor)

    # Check if there is new sensor data compared to the stored dataset
    if check_product_update(config.PRODUCT_NDVI_MAX['product_name'], sensor_stats[1]) is True:
        print("new imagery from: "+sensor_stats[1])

        # Generate the filename
        filename = config.PRODUCT_NDVI_MAX['prefix']+sensor_stats[0].replace(
            "-", "")+"-"+sensor_stats[1].replace("-", "")+"_run"+current_date_str.replace("-", "")
        print(filename)

        # Create NDVI and NDVI max
        sensor = sensor.map(lambda image: addNDVI(
            image, bands=config.PRODUCT_NDVI_MAX['band_names'][0]))

        mosaic = sensor.qualityMosaic("NDVI")
        ndvi_max = mosaic.select("NDVI")
        ndvi_max_int = ndvi_max.multiply(10000).int()
        ndvi_max_int = maskOutside(ndvi_max_int, roi).unmask(config.NODATA)

        # Check if there is at least 1 scene to be defined (if minimal scene count is required) TODO: is this necessary?
        if sensor_stats[2] > 0:
            # Start the export
            prepare_export(roi, filename, ndvi_max_int,
                           sensor_stats, current_date_str)

            return 1
        else:
            # TODO: If there are not enough scenes, quit processing
            return 0
    else:
        print("no new imagery")
        return 0


if __name__ == "__main__":
    # Test if we are on Local DEV Run or if we are on PROD
    determine_run_type()

    # Authenticate with GEE and GDRIVE
    initialize_gee_and_drive()

    # Get current date
    current_date_str = datetime.datetime.today().strftime('%Y-%m-%d')
    global current_date
    current_date = ee.Date(current_date_str)

    # Set ROI
    # Either use a rectangle
    roi = ee.Geometry.Rectangle(config.ROI_RECTANGLE)

    # Or borders on country level (might be useful to reduce file size)
    # border = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017").filter(
    #     ee.Filter.eq("country_co", config.ROI_NAME))
    # roi = border.geometry()

    # Generate PRODUCT
    result = process_NDVI_MAX()
    print("Result:", result)

print("done!!!!")
