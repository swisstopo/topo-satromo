import configuration as config
import requests
import ee
import datetime
import csv
import os
import json
import pandas as pd
import dateutil


def is_date_in_empty_asset_list(collection, check_date_str):
    """
    Check if a given date for a collection is in the empty asset list.

    Args:
    collection_basename (str): The basename of the collection.
    check_date_str (str): The date to check in string format.
    config (object): Configuration object containing EMPTY_ASSET_LIST path.

    Returns:
    bool: True if the date is found in the empty asset list, False otherwise.
    """
    try:
        collection_basename = os.path.basename(collection)
        # Read the empty asset list
        df = pd.read_csv(config.EMPTY_ASSET_LIST)

        # Filter the dataframe for the given collection and date
        df_selection = df[(df.collection == collection_basename) &
                          (df.date == check_date_str)]

        # Check if any rows match the criteria
        if len(df_selection) > 0:
            print(check_date_str+' is in empty_asset_list for '+collection)
            return True
        else:
            return False

    except Exception as e:
        print(f"Error checking empty asset list: {e}")
        return False  # Return False in case of any error to allow further processing


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


def get_product_from_techname(techname):
    """
    This function searches for a dictionary in the 'config' module that contains 
    'product_name' with a specified value and returns it.

    Parameters:
    techname (str): The value of 'product_name' to search for. 
                    For example, 'ch.swisstopo.swisseo_s2-sr_v100'.

    Returns:
    dict: The dictionary that contains 'product_name' with the value of 'techname'. 
          If no such dictionary is found, it returns None.
    """

    # Initialize the variable to None
    var = None

    # Iterate over all attributes in the config module
    for attr_name in dir(config):
        attr_value = getattr(config, attr_name)

        # Check if the attribute is a dictionary
        if isinstance(attr_value, dict):
            # Check if the dictionary contains 'product_name' with the desired value
            if attr_value.get('product_name') == techname:
                var = attr_value
                break  # Exit the loop once the dictionary is found

    return var


def addINDEX(image, bands, index_name):
    """
    Add an Index (eg NDVI) band to the image based on two bands.

    Args:
        image (ee.Image): Input image to add the index band.
        bands (dict): Dictionary containing band names for NIR and RED.
        index_name (str): Name of the index used as band name

    Returns:
        ee.Image: Image with the index band added.
    """

    # Extract the band names for NIR and RED from the input dictionary
    NIR = bands['NIR']
    RED = bands['RED']

    # Compute the index using the normalizedDifference() function and rename the band to "NDVI"
    index = image.normalizedDifference([NIR, RED]).rename(index_name)

    # Add the index band to the image using the addBands() function
    image_with_index = image.addBands(index)

    # Return the image with the NDVI band added
    return image_with_index


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
        Returns (None, None, 0) for empty collections.
    """
    # Sort the collection by date in ascending order
    breakpoint()
    sorted_collection = collection.sort('system:time_start')

    # Get the first and last image from the sorted collection
    first_image = sorted_collection.first()
    last_image = sorted_collection.sort('system:time_start', False).first()
    
    try:
        # Get the count of images in the collection
        image_count = collection.size().getInfo()
        # Get the dates of the first and last image
        first_date = ee.Date(first_image.get('system:time_start')).format('YYYY-MM-dd').getInfo()
        last_date = ee.Date(last_image.get('system:time_start')).format('YYYY-MM-dd').getInfo()
    except ee.EEException:
        image_count = 0
        # Handle cases where date information might be missing
        first_date = None
        last_date = None

    # Return the first date, last date, and total number of scenes
    return first_date, last_date, image_count


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
    Starts an export task to export an image to Google Drive or Google Cloud Storage


    Args:
        image: The image to be exported.
        scale: The scale of the exported image.
        description: The description of the export task.
        region: The region of interest (ROI) to export.
        filename_prefix: The prefix to be used for the exported file.
        crs: The coordinate reference system (CRS) of the exported image.
        GCS=False : If set to true, an GCS will be used for output

    Returns:
        None
    """

    # Export in GEE
    # TODO Getting S2_mosaic.projection() makes no sense, it will always be a computed image, with 1 degree scale and EPSG 4326, unless manually reprojected.
    #  Use projection() from one of the original images instead, e.g., S2_collection.first().projection(), *after the aoi/date filters but before mapping any transformation function* then
    #  work with the corresponding CrsTtransform derived from it  crs:'EPSG:32632',   crsTransform: '[10,0,0,0,10,0]'

    if config.GDRIVE_TYPE == "GCS":
        # print("GCS export")
        task = ee.batch.Export.image.toCloudStorage(
            image=image,
            description=description,
            scale=scale,
            region=region,
            fileNamePrefix=filename_prefix,
            maxPixels=1e13,
            crs=crs,
            fileFormat="GeoTIFF",
            bucket=config.GCLOUD_BUCKET
        )
    else:
        # print("Drive export")
        task = ee.batch.Export.image.toDrive(
            image=image,
            description=description,
            scale=scale,
            region=region,
            fileNamePrefix=filename_prefix,
            maxPixels=1e13,
            crs=crs,
            fileFormat="GeoTIFF"
        )
    # OPTION Export in GEE with UTM32
    # for images covering that UTM zone this will be the best, but for the neighbouring UTM zones, images will be reprojected. So, for mosaics for larger areas spanning multiple UTM zones maybe some alternative projection is more convenient.
    # task = ee.batch.Export.image.toDrive(
    #    image=image,
    #    description=description,
    #    #scale=scale,
    #    "region=region,"
    #    fileNamePrefix=filename_prefix,
    #    maxPixels=1e13,
    #    crs = 'EPSG:32632',
    #    crsTransform = '[10,0,300000,0,-10,5200020]',
    #    fileFormat ="GeoTIFF"
    # )

    # OPTION: only reproject but without scale use this code, based on https://developers.google.com/earth-engine/guides/exporting#setting_scal
    # projection = image.projection().getInfo()
    # task = ee.batch.Export.image.toDrive(
    #     image=image,
    #     description=description,
    #     "region "= "region",
    #     fileNamePrefix=filename_prefix,
    #     crs=crs,
    #     maxPixels=1e13,
    #     fileFormat = "GeoTIFF",
    #     crsTransform = projection['transform']
    # )

    task.start()

    # Get Task ID
    task_id = task.status()["id"]
    print("Exporting  with Task ID:", task_id +
          f" file {filename_prefix} to {config.GDRIVE_TYPE}...")

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


def check_product_status(product_name):
    """
    Check if the given product has a "Status" marked as complete

    Parameters:
    product_name (str): Name of the product to check.

    Returns:
    bool: True if "Status" has a value equal to 'complete'
    False otherwise
    """

    with open(config.LAST_PRODUCT_UPDATES, "r", newline="", encoding="utf-8") as f:
        dict_reader = csv.DictReader(f, delimiter=",")
        for row in dict_reader:
            if row["Product"] == product_name:
                return row['Status'] == 'complete'
    return False


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


def prepare_export(roi, productitem, productasset, productname, scale, image, sensor_stats, current_date_str):
    """
    Prepare the export of the image by splitting it into quadrants and starting the export tasks.
    It also generates product status information, updates the product status file,
    and writes the product description to a CSV file.

    Args:
        roi (ee.Geometry): Region of interest for the export.
        productitem (str): Timestamp of assets YYYYMMDThhmmss, "YYYYMMDDT235959" for a day 
        productasset (str): Base filename for the exported files.
        productname (str): Product name of the exported files.
        scale (str): Scalenumber in [m] of the exported file
        image (ee.Image): Image to be exported.
        sensor_stats (list): List containing sensor statistics.
        current_date_str (str): Current date in string format.

    Returns:
        None
    """
    breakpoint()
    # Get current Processor Version from GitHub
    processor_version = get_github_info()

    # Define the quadrants to split into 4 regions
    quadrants = get_quadrants(roi)

    for quadrant_name, quadrant in quadrants.items():
        # Create filename for each quadrant
        filename_q = productasset + quadrant_name
        # Start the export for each quadrant

        start_export(image, int(scale),
                     productasset, quadrant, filename_q, config.OUTPUT_CRS)

    # Generate product status information
    product_status = {
        'Product': productname,
        'LastSceneDate': sensor_stats[1],
        'RunDate': current_date_str,
        'Status': "RUNNING"
    }

    # Update the product status file
    update_product_status_file(product_status, config.LAST_PRODUCT_UPDATES)

    # Get Product info from config
    product = get_product_from_techname(productname)

    # Update the product  file
    header = ["Product", "Item", "Asset", "DateFirstScene", "DateLastScene",
              "NumberOfScenes", "DateItemGeneration", "ProcessorHashLink", "ProcessorReleaseVersion", "GeocatID"]
    data = [productname, productitem, productasset, str(sensor_stats[0]), str(
        sensor_stats[1]), str(sensor_stats[2]), current_date_str, processor_version["GithubLink"], processor_version["ReleaseVersion"], product['geocat_id']]

    # Create swisstopo_data dictionary
    swisstopo_data = {"header": header, "data": data}

    # Create swisstopo_data dictionary with uppercase keys
    swisstopo_data = {key.upper(): value for key, value in zip(header, data)}

    # Adding extracting image info
    image_info = ee.Image(image).getInfo()

    # Convert keys to uppercase and add prefix
    image_info_gee = {"GEE_" + key.upper(): value for key,
                      value in image_info.items()}

    # Add swisstopo_data to image_info_gee
    image_info_gee["SWISSTOPO"] = swisstopo_data

    # Export the dictionary as JSON
    with open(os.path.join(config.PROCESSING_DIR, productasset + "_metadata.json"), 'w') as json_file:
        json.dump(image_info_gee, json_file)

    return None


def get_collection_info_landsat(collection):
    """
    Retrieves information about an image collection for the line of Landsat satellites

    Args:
        collection: The landsat image collection to retrieve information from.

    Returns:
        A tuple containing the first date, last date, and total number of images in the collection.
        Returns (None, None, 0) for empty collections.
    """
    # Sort the collection by date in ascending order
    index_list = collection.aggregate_array('system:index')

    dates_list = [dateutil.parser.parse(i.split('_')[-1]) for i in index_list.getInfo()]

    # Get the first and last image and size of image collection
    image_count = len(dates_list) if len(dates_list)>0 else 0
    first_date = min(dates_list) if image_count>0 else None
    last_date = max(dates_list) if image_count>0 else None

    # Return the first date, last date, and total number of scenes
    return first_date, last_date, image_count