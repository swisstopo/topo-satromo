from typing import Union, Any
from urllib.parse import urlparse
import boto3
import pystac
from pystac import Link
from pystac.stac_io import DefaultStacIO
from pystac.extensions.eo import Band, EOExtension
import re
import os
import pyproj
import rasterio
from shapely.geometry import Polygon, mapping, shape, MultiPolygon
from datetime import datetime
# From https://alexwlchan.net/2017/07/listing-s3-keys/
from botocore import UNSIGNED
from botocore.config import Config
import platform
import subprocess
import configuration as config

# For providing a simple but state of the art STAC Catalogue to Testers on integration,A catalog JSON is generated whcih can be used in radiant earth STAC BROWSER https://radiantearth.github.io/stac-browser
# all inspired from https://pystac.readthedocs.io/en/latest/tutorials/how-to-create-stac-catalogs.html#Creating-a-STAC-of-imagery-from-Spacenet-5-data needs heavy clean up

# Generall Settings
# *****************

os.environ["AWS_NO_SIGN_REQUEST"] = "true"

# Define the LV95 and WGS84 coordinate systems
lv95 = pyproj.CRS.from_epsg(2056)  # LV95 EPSG code
wgs84 = pyproj.CRS.from_epsg(4326)  # WGS84 EPSG code

# Create transformer objects
transformer_lv95_to_wgs84 = pyproj.Transformer.from_crs(
    lv95, wgs84, always_xy=True)

provider_sat = pystac.Provider(
    name="European Union/ESA/Copernicus",
    roles=["producer", "licensor"],
    url="https://sentinel.esa.int/web/sentinel/user-guides/sentinel-2-msi",
)

provider_host = pystac.Provider(
    name="Swiss government",
    roles=["host"],
    url="https://www.admin.ch/gov/en/start.html",
)
provider_processor = pystac.Provider(
    name="Federal Office of Topography swisstopo",
    roles=["processor"],
    url="https://www.swisstopo.ch/",
)


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file. And determine platform

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    global GDRIVE_SOURCE
    global S3_DESTINATION
    global GDRIVE_MOUNT
    global os_name

    # Get the operating system name
    os_name = platform.system()

    # Set SOURCE , DESTINATION and MOUNTPOINTS

    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2
        print("\nType 2 run PUBLISHER: We are on DEV")
        GDRIVE_SOURCE = config.GDRIVE_SOURCE_DEV
        GDRIVE_MOUNT = config.GDRIVE_MOUNT_DEV
        S3_DESTINATION = config.STAC_DESTINATION_DEV
    else:
        run_type = 1
        print("\nType 1 run PUBLISHER: We are on INT")
        GDRIVE_SOURCE = config.GDRIVE_SOURCE_INT
        GDRIVE_MOUNT = config.GDRIVE_MOUNT_INT
        S3_DESTINATION = config.STAC_DESTINATION_INT


def initialize_s3():
    """
    Initialize S3 RCLONE  authentication.

    This function authenticates S3 either using a service account key file
    via GitHub secrets depending on the run type.

    Returns:
    None
    """

    if run_type == 2:
        print("using local rclone installation")
    else:
        # Initialize S3 using GitHub secrets

        # Write rclone config to a file
        rclone_config = os.environ.get('RCONF_SECRET')
        rclone_config_file = "rclone.conf"
        with open(rclone_config_file, "w") as f:
            f.write(rclone_config)

        # Write rclone config to a file
        rclone_config = os.environ.get('RCONF_SECRET')
        rclone_config_file = "rclone.conf"
        with open(rclone_config_file, "w") as f:
            f.write(rclone_config)


def move_files_with_rclone(source, destination):
    """
    Move files using the rclone command.

    Parameters:
    source (str): Source path of the files to be moved.
    destination (str): Destination path to move the files to.

    Returns:
    None
    """
    # Run rclone command to move files
    # See hint https://forum.rclone.org/t/s3-rclone-v-1-52-0-or-after-permission-denied/21961/2

    if run_type == 2:
        rclone = os.path.join("secrets", "rclone")
        rclone_conf = os.path.join("secrets", "rclone.conf")
    else:
        rclone = "rclone"
        rclone_conf = "rclone.conf"
    command = [rclone, "move", "--config", rclone_conf, "--s3-no-check-bucket",
               source, destination]

    subprocess.run(command, check=True)

    print("SUCCESS: moved " + source + " to " + destination)


class CustomStacIO(DefaultStacIO):
    """
    A custom STAC IO class that extends DefaultStacIO for handling STAC resource reading and writing.

    This class is designed to read and write STAC resources from various sources, including Amazon S3.

    Attributes:
        s3 (boto3.resource): An instance of the boto3 S3 resource for S3-related operations.
    """

    def __init__(self):
        """
        Initializes the CustomStacIO instance.
        """
        self.s3 = boto3.resource("s3")

    def read_text(self, source: Union[str, Link], *args: Any, **kwargs: Any) -> str:
        """
        Read the text content of a STAC resource.

        Args:
            source (Union[str, Link]): The source of the STAC resource.
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            str: The text content of the STAC resource.
        """
        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]

            obj = self.s3.Object(bucket, key)
            return obj.get()["Body"].read().decode("utf-8")
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(
        self, dest: Union[str, Link], txt: str, *args: Any, **kwargs: Any
    ) -> None:
        """
        Write text content to a STAC resource.

        Args:
            dest (Union[str, Link]): The destination of the STAC resource.
            txt (str): The text content to be written.
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]
            self.s3.Object(bucket, key).put(Body=txt, ContentEncoding="utf-8")
        else:
            super().write_text(dest, txt, *args, **kwargs)


def get_s3_keys(bucket, prefix):
    """
    Generate all the object keys in an S3 bucket that match the specified prefix.

    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix to filter S3 object keys.

    Yields:
        str: The keys (object names) in the S3 bucket that match the specified prefix.

    Raises:
        KeyError: If there is an issue with listing S3 objects and the response does not contain a "NextContinuationToken".

    Note:
        This function uses a generator to yield S3 object keys one at a time, which is memory-efficient for large S3 buckets.

    Example:
        for key in get_s3_keys("my-bucket", "path/to/files/"):
            print(key)
    """
    # Create an S3 client with unsigned requests (for public buckets)
    s3 = boto3.client("s3", region_name="eu-central-2",
                      config=Config(signature_version=UNSIGNED))

    # Define initial list_objects_v2 parameters
    kwargs = {"Bucket": bucket, "Prefix": prefix}

    # Continue listing objects until there are no more pages
    while True:
        # Request the next page of S3 objects
        resp = s3.list_objects_v2(**kwargs)

        # Yield the keys (object names) from the current page
        for obj in resp["Contents"]:
            yield obj["Key"]

        # Check if there is a continuation token for the next page
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            # No more pages, break the loop
            break


def get_chip_id(uri, pattern):
    """
    Extract and return a chip ID from a URI using a regular expression pattern.

    Args:
        uri (str): The URI from which to extract the chip ID.
        pattern (str): The regular expression pattern to match against the URI.

    Returns:
        str or None: The extracted chip ID if a match is found, or None if no match is found.

    Example:
        uri = "https://example.com/chip123/resource"
        pattern = r'chip(\d+)'
        chip_id = get_chip_id(uri, pattern)
        chip_id
        'chip123'
    """
    match = re.match(pattern, uri)
    if match:
        return match.string
    else:
        return None


def get_bbox_and_footprint(raster_uri):
    """
    Extract the bounding box and footprint geometry from a raster dataset.

    Args:
        raster_uri (str): The URI of the raster dataset.

    Returns:
        tuple: A tuple containing the bounding box coordinates as [left, bottom, right, top] and the footprint geometry.

    Example:
         uri = "path/to/raster.tif"
         bbox, footprint = get_bbox_and_footprint(uri)
         bbox
        [longitude_left, latitude_bottom, longitude_right, latitude_top]
         footprint
        <shapely.geometry.polygon.Polygon object at 0x...>
    """
    with rasterio.Env():
        with rasterio.open(raster_uri) as ds:
            bounds = ds.bounds
            # Convert the bounding box coordinates from LV95 to WGS84
            left, bottom = transformer_lv95_to_wgs84.transform(
                bounds.left, bounds.bottom)
            right, top = transformer_lv95_to_wgs84.transform(
                bounds.right, bounds.top)

            bbox = [left, bottom, right, top]
            footprint = Polygon(
                [
                    [left, bottom],
                    [left, top],
                    [right, top],
                    [right, bottom],
                ]
            )

            return (bbox, mapping(footprint))


def ndvimax_get_start_end(img_uri):
    """
    Extract start and end dates in UTC format from an image URI.

    Args:
        img_uri (str): The URI of the image.

    Returns:
        tuple: A tuple containing the start date and end date in UTC format.

    Example:
         uri = "https://example.com/image_20220101_run.tif"
         start_date, end_date = ndvimax_get_start_end(uri)
         start_date
        datetime.datetime(2022, 1, 1, 0, 0)
         end_date
        datetime.datetime(2022, 1, 1, 23, 59, 59)
    """
    # Find the substring before "_run"
    start_index = img_uri.find("_run")

    if start_index != -1:
        # Extract the substring before "_run"
        date_str = img_uri[start_index - 17:start_index]

        # Split the date string into start and end date parts
        start_date_str = date_str[:8]
        end_date_str = date_str[9:]

        # Convert the date strings to UTC format
        from datetime import datetime

        start_date_utc = datetime.strptime(start_date_str, '%Y%m%d')
        end_date_utc = datetime.strptime(end_date_str, '%Y%m%d')

        # print('Start Date (UTC):', start_date_utc)
        # print('End Date (UTC):', end_date_utc)
        return (start_date_utc, end_date_utc)
    else:
        print('"_run" not found in the URI')


if __name__ == "__main__":

    # Starting STAC
    print("Starting STAC generation")

    # Test if we are on Local DEV Run or if we are on PROD
    determine_run_type()

    # get s3 connection
    initialize_s3()

    # Get Products
    product = config.STAC_PRODUCT

    # Now, we can create a Catalog
    catalog = pystac.Catalog(id="Erdbeobachtungs-SAtellitendaten fürs TRockenheitsMOnitoring (SATROMO) STAC INTEGRATION",
                                description="INTEGRATION *not for operational use*  ACCESS to automated spatial satellite products, indices, and analysis-ready datasets for  drought monitoring and more. Further info under https://github.com/swisstopo/topo-satromo.Contains modified Copernicus Sentinel data [2017-2023] for Sentinel data")

    # We loops through the products and add them as collection and descripe each product, currently two.
    for i in product:
        print(i)

        if i == "NDVI-MAX":
            product_bands = [
                Band.create(
                    name="NDVI-MAX-30DAY", description="Maximumm NDVI last 30 days, Range -100 to 100 divide by 1000 to get NDVI values, nodata is 9999", common_name="ndvi-max")
            ]
            product_name = "NDVI-MAX"
            pattern = r'.*' + re.escape(product_name) + \
                r'.*\.' + re.escape("tif") + r'$'
            item_common_metadata_gsd = 10
            item_common_metadata_platform = "SENTINEL-2"
            item_common_metadata_instruments = "MSI"

            collection_id = "NDVI MAX[TEST]"
            collection_description = "Sentinel 2 NDVI MAX last 30 Days for  Switzerland"
        else:
            product_bands = [
                Band.create(
                    name="B4", description="664.5nm (S2A) / 665nm (S2B)", common_name="red"),
                Band.create(
                    name="B3", description="560nm (S2A) / 559nm (S2B)", common_name="green"),
                Band.create(
                    name="B2", description="496.6nm (S2A) / 492.1nm (S2B)", common_name="blue"),
                Band.create(
                    name="B8", description="835.1nm (S2A) / 833nm (S2B)", common_name="nir")
            ]
            product_name = "S2_LEVEL_2A"
            pattern = r'.*' + re.escape(product_name) + \
                r'.*10M.*\.' + re.escape("tif") + r'$'

            item_common_metadata_gsd = 10
            item_common_metadata_platform = "SENTINEL-2"
            item_common_metadata_instruments = "MSI"

            collection_id = "S2-Level-2A-SR"
            collection_description = "The Copernicus Sentinel-2 mission consists of two polar-orbiting satellites that are positioned in the same sun-synchronous orbit, with a phase difference of 180°. It aims to monitor changes in land surface conditions. The satellites have a wide swath width (290 km) and a high revisit time. Sentinel-2 is equipped with an optical instrument payload that samples 13 spectral bands: This collection cover four bands at 10 mspatial resolution [https://dataspace.copernicus.eu/explore-data/data-collections/sentinel-data/sentinel-2]."

        # Let’s make a STAC of imagery over the dataset as part of the SATROMO INT Bucket.
        # As a first step, we can list out the imagery and extract IDs from each of the tiff
        # In this section, the code is creating a list of URIs (Uniform Resource Identifiers) for satellite imagery files stored in the Amazon S3 bucket named "satromoint." These URIs represent satellite data files associated with the product_name (either "NDVI-MAX" or "S2_LEVEL_2A"). The get_s3_keys function is used to retrieve these URIs, and the prefix parameter specifies a path within the bucket where these files are located.
        satromo_uris = list(
            get_s3_keys(
                bucket="satromoint", prefix=f"data/{product_name}/"
            )
        )

        # In this part of the code, it iterates through the list of satellite imagery URIs obtained earlier. For each URI, it attempts to extract a unique identifier (satromo_id) from the URI using the previously defined pattern. If a valid satromo_id is extracted, it creates a dictionary entry in satromo_id_to_data with the img key. This entry associates the satromo_id with the full URL (STAC_BASE_URL) needed to access the imagery.The purpose of this code block is to prepare a mapping of unique identifiers (satromo_id) to the URLs of satellite imagery files. This mapping will be used to organize and reference the imagery when creating a STAC (SpatioTemporal Asset Catalog) for the dataset.
        satromo_id_to_data = {}
        counter = 0  # Initialize a counter to start numbering from 0

        for uri in satromo_uris:

            satromo_id = get_chip_id(uri, pattern)

            if satromo_id is not None:

                satromo_id_to_data[counter] = {
                    "img": config.STAC_BASE_URL+"{}".format(uri)}
                counter += 1  # Increment the counter for the next entry

        # ITEMS
        # ***********************************************

        # We’ll create core Items for our imagery, but mark them with the eo extension as we did above, and store the eo data in a Collection.
        # Note that the image CRS is in WGS:84 (Lat/Lng). If it wasn’t, we’d have to reproject the footprint to WGS:84 in order to be compliant with the spec
        #  (which can easily be done with pyproj).
        # Here we’re taking advantage of rasterio’s ability to read S3 URIs, which only grabs the GeoTIFF metadata and does not pull the whole file down.

        # Let’s turn each of those chips into a STAC Item that represents the image.
        satromo_id_to_items = {}

        for chip_id in satromo_id_to_data:
            img_uri = satromo_id_to_data[chip_id]["img"]
            # print("Processing {}".format(img_uri))

            # Get the bounding box and footprint of the image
            bbox, footprint = get_bbox_and_footprint(img_uri)

            # Extract the image identifier from the URI
            img = img_uri.split(
                f"data/{product_name}/", 1)[-1].split("/", 1)[0]
            # print(img)

            # Create a STAC Item for the image
            item = pystac.Item(
                id=img,
                geometry=footprint,
                bbox=bbox,
                datetime=datetime.strptime(img[:-6] + "235959" if img.endswith(
                    "240000") else img, "%Y%m%dT%H%M%S"),  # since datetime has an issue with240000
                properties={},
            )

            # Set common metadata for the STAC image Item
            item.common_metadata.gsd = item_common_metadata_gsd
            item.common_metadata.platform = item_common_metadata_platform
            item.common_metadata.instruments = [
                item_common_metadata_instruments]
            # Set start and end datetime specifically for "NDVI-MAX" products
            if i == "NDVI-MAX":
                ndvimax_dates = ndvimax_get_start_end(img_uri)
                item.common_metadata.start_datetime = ndvimax_dates[0]
                item.common_metadata.end_datetime = ndvimax_dates[1]

            # ASSETS
            # ***********************************************

            # Add "tif" asset
            eo = EOExtension.ext(item, add_if_missing=True)
            eo.bands = product_bands

            tif_asset_uri = img_uri
            tif_asset = pystac.Asset(
                href=tif_asset_uri, media_type=pystac.MediaType.COG, roles=["data"])
            item.add_asset(key="Product", asset=tif_asset)
            eo = EOExtension.ext(item.assets["Product"])
            eo.bands = product_bands

            # Add "csv" asset
            # Assuming CSV has the same name but with .csv extension
            csv_asset_uri = img_uri.replace(".tif", ".csv")
            # You can specify the media type for CSV
            csv_asset = pystac.Asset(
                href=csv_asset_uri, media_type="text/csv", roles=["metadata"])
            item.add_asset(key="ProcessingLog", asset=csv_asset)

            # Add QA60 assets and JSON
            if i == "S2_LEVEL_2A":
                # QA60 Add "tif" asset
                tif_asset_uri = img_uri.replace("10M", "QA60")
                tif_asset = pystac.Asset(
                    href=tif_asset_uri, media_type=pystac.MediaType.COG, roles=["data"])
                item.add_asset(key="CloudMask", asset=tif_asset)

                # QA60 Add "csv" asset
                # Assuming CSV has the same name but with .csv extension
                csv_asset_uri = img_uri.replace(".tif", ".csv")
                # You can specify the media type for CSV
                csv_asset = pystac.Asset(
                    href=csv_asset_uri, media_type="text/csv", roles=["metadata"])
                item.add_asset(key="CloudMaskProcessingLog", asset=csv_asset)

                # Add "json" assets
                json_asset_baseurl = config.STAC_BASE_URL+"data/"+i+"/"+img+"/"

                # Initialize a list to store JSON asset URIs
                json_asset_uris = []

                # Iterate through satromo_uris to find JSON files
                for uri in satromo_uris:
                    # Check if the URI has a ".json" extension
                    if uri.endswith(".json"):
                        # Construct the JSON asset URI based on json_asset_baseurl
                        json_asset_uri = os.path.join(
                            config.STAC_BASE_URL, uri)

                        # Add the JSON asset URI to the list
                        json_asset_uris.append(json_asset_uri)

                # Now, you can iterate through json_asset_uris to create assets for STAC items
                for json_asset_uri in json_asset_uris:
                    # Create the JSON asset
                    json_asset = pystac.Asset(
                        href=json_asset_uri, media_type="application/json", roles=["metadata"])

                    # get the tile id
                    tile_id = re.search(r'_([^_]+)_properties_', json_asset_uri).group(
                        1) if re.search(r'_([^_]+)_properties_', json_asset_uri) else None

                    # Add the JSON asset to the STAC item (assuming you have an 'item' variable)
                    item.add_asset(key="SentinelProcessingLog" +
                                   tile_id, asset=json_asset)

            # Add the created STAC Item to the dictionary
            satromo_id_to_items[chip_id] = item

        # Creating the Collection
        # All of these images are over Switzerland. In Sentinel, we have a couple regions that have imagery;
        # a good way to separate these collections of imagery. We can store all of the common eo metadata in the collection.

        # Calculate the footprints of all STAC Items and create a collection bounding box
        footprints = list(
            map(lambda i: shape(i.geometry).envelope, satromo_id_to_items.values()))
        collection_bbox = MultiPolygon(footprints).bounds

        # Create spatial extent metadata for the collection
        spatial_extent = pystac.SpatialExtent(bboxes=[collection_bbox])

        # Calculate the temporal extent of the collection
        datetimes = sorted(
            list(map(lambda i: i.datetime, satromo_id_to_items.values())))
        temporal_extent = pystac.TemporalExtent(
            intervals=[[datetimes[0], datetimes[-1]]])

        # Create the complete extent for the collection
        collection_extent = pystac.Extent(
            spatial=spatial_extent, temporal=temporal_extent)
        
        # Define the licenses and their respective links
        licenses = [
            {
                "license": "Legal notice on the use of Copernicus Sentinel Data and Service Information",
                "url": "https://scihub.copernicus.eu/twiki/pub/SciHubWebPortal/TermsConditions/Sentinel_Data_Terms_and_Conditions.pdf"
            },
            {
                "license": "swisstopo OGD ",
                "url": "https://www.swisstopo.admin.ch/en/home/meta/conditions/geodata.html"
            }
        ]

        # Create a STAC Collection with relevant metadata
        collection = pystac.Collection(
            id=collection_id,
            description=collection_description,
            extent=collection_extent,
            license="various",
            providers=[provider_sat, provider_host, provider_processor],
 
        )
        # Add the licenses to the collection
        collection.extra_fields["licenses"] = licenses

        # Add STAC Items to the Collection
        collection.add_items(satromo_id_to_items.values())

        # Add the Collection to the main Catalog
        catalog.add_child(collection)

    # Normalize and save the Catalog as a self-contained STAC
    catalog.normalize_and_save(
        root_href=config.STAC_FOLDER,
        catalog_type=pystac.CatalogType.SELF_CONTAINED,
    )

    # Move Description of item to destination DIR
    move_files_with_rclone(config.STAC_FOLDER, os.path.join(
        S3_DESTINATION, config.STAC_FOLDER))

    print("STAC Updated")
