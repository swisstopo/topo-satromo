import os
import configuration as config
import subprocess
import rasterio
import numpy as np
from rasterio.transform import from_origin
from shapely.geometry import shape
import fiona

import os
import subprocess
import fiona
from shapely.geometry import shape


def fill_buffer_switzerland(shapefile_dir, shapefile_name, target_width):
    """
    Creates a raster file from the given shapefile with a buffer around Switzerland.

    Args:
    shapefile_dir (str): The directory where the shapefile is located.
    shapefile_name (str): The name of the shapefile.
    target_width (int): The target width of the output raster.

    Returns:
    None

    Example usage:
    fill_buffer_switzerland("assets", "ch_buffer_5000m.shp", 1024)
    """
    # Path to your shapefile
    shapefile_path = os.path.join(shapefile_dir, shapefile_name)

    # Open the shapefile and read the first feature to get its geometry
    with fiona.open(shapefile_path, 'r') as shp:
        first_feature = next(iter(shp))
        geom = shape(first_feature['geometry'])

    # Calculate bounding box dimensions (width and height)
    minx, miny, maxx, maxy = geom.bounds
    orig_width = maxx - minx
    orig_height = maxy - miny

    # Calculate corresponding height to maintain aspect ratio
    target_height = int(orig_height / orig_width * target_width)

    # Prepare the command for gdal_rasterize
    command = [
        "gdal_rasterize",
        "-burn", "220", "-burn", "220", "-burn", "220",
        "-init", "255",
        "-l", shapefile_name.replace(".shp", ""),
        shapefile_path,
        "-ot", "Byte",
        "-of", "GTiff",
        "-ts", str(target_width), str(target_height),
        "output_thumbnailswissfill.tif"
    ]

    # Execute the command
    result = subprocess.run(command, check=True,
                            capture_output=True, text=True)

    # # Check and print the command output
    # if result.returncode == 0:
    #     print("Rasterization successful!")
    # else:
    #     print(f"Error: {result.stderr}")


def apply_overlay(input_file, output_file):
    try:
        # remove the filname extension:
        input_file_without_extension = input_file.split('.')[0]
        # Burn Rivers
        command = [
            "gdal_rasterize",
            "-b", "1", "-b", "2", "-b", "3",
            "-burn", "255", "-burn", "255", "-burn", "255",
            "-l", "overview_rivers_2056",     # Specify layer name
            config.OVERVIEW_RIVERS,
            input_file_without_extension+".tif"
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)

        # Burn Lakes
        command = [
            "gdal_rasterize",
            "-b", "1", "-b", "2", "-b", "3",
            "-burn", "255", "-burn", "255", "-burn", "255",
            "-l", "overview_lakes_2056",     # Specify layer name
            config.OVERVIEW_LAKES,
            input_file_without_extension+".tif"
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)

        # export to jpg
        command = [
            "gdal_translate",
            "-of", "JPEG",
            "--config", "GDAL_PAM_ENABLED", "NO",
            input_file_without_extension+".tif",
            output_file
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)

        # remove intermediate files
        [os.remove(file) for file in os.listdir()
            if file.startswith("output_thumbnail")]
        return (output_file)

    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return False


def create_thumbnail(inputfile_name, product):
    # product = metadata['SWISSTOPO']['PRODUCT']
    # inputfile_name = "ch.swisstopo.swisseo_s2-sr_v100_mosaic_2023-10-28T102039_bands-10m.tif"
    # thumbnail_name = "ch.swisstopo.swisseo_s2-sr_v100_mosaic_2023-10-28T102039_thumbnail.jpeg"

    # Thumbnail S2_SR 10m bands
    if product.startswith("ch.swisstopo.swisseo_s2-sr") and inputfile_name.endswith("bands-10m.tif"):
        # https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#visual
        # It should be called just   "thumbnail.jpg"
        # thumbnail_name = inputfile_name.replace(
        #     "bands-10m.tif", "thumbnail.jpeg")
        thumbnail_name = "thumbnail.jpg"
        try:
            # Export thumbnail
            command = [
                "gdal_translate",
                "-b", "1", "-b", "2", "-b", "3",
                "-of", "GTiff",
                "-outsize", "256", "256",
                inputfile_name,
                "output_thumbnail.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # to RGB
            command = [
                "gdal_translate",
                "output_thumbnail.tif",
                "output_thumbnailRGB.tif",
                "-b", "1", "-b", "2", "-b", "3",
                "-co", "COMPRESS=DEFLATE",
                "-co", "PHOTOMETRIC=RGB"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Get Min Max Value to strecth the image
            with rasterio.open("output_thumbnailRGB.tif") as rgb_range:
                min_value = str(round(rgb_range.read().min()))
                max_value = str(round(rgb_range.read().max()))

            # scale the view, using a gamma correction of 0.5
            command = [
                "gdal_translate",
                "output_thumbnailRGB.tif",
                "output_thumbnailRGB_scaled.tif",
                "-scale", min_value, max_value, "0", "65535",
                "-exponent", "0.5",
                "-co", "COMPRESS=DEFLATE",
                "-co", "PHOTOMETRIC=RGB"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # make a geotiff
            command = [
                "gdal_translate",
                "-of", "GTiff",
                "-ot", "Byte",
                "-outsize", "256", "256",
                "-scale", "0", "65535", "0", "255",
                "output_thumbnailRGB_scaled.tif",
                "output_thumbnailRGB_scaled255.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Fill Buffer Switzerland
            fill_buffer_switzerland("assets", "ch_buffer_5000m.shp", 1024)

            # Set 0 values to nodata
            command = [
                "gdal_translate",
                # "-a_nodata", "0,0,0", #This step is only needed of you have additional data with nodata
                "output_thumbnailRGB_scaled255.tif",
                "output_thumbnailRGB_scaled255nodata.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # overlay on Switzerland
            command = [
                "gdalwarp",
                "-overwrite",
                "-dstnodata", "255",
                "output_thumbnailswissfill.tif",
                "output_thumbnailRGB_scaled255nodata.tif",
                "output_thumbnailRGB_scaled255nodata_merged.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Get the dimensions/ As
            # TODO should be output_thumbnailRGB_scaled255nodata_merged.tif then also add -ts below
            with rasterio.open("output_thumbnailRGB_scaled255nodata.tif") as swiss:

                # Calculate the new width to maintain the aspect ratio with a height of 256 pixels
                new_width = str(int((swiss.width * 256) / swiss.height))

            # clip to extent of products
            with rasterio.open("output_thumbnailRGB.tif") as subset:
                bboxl = str(int(subset.bounds.left))
                bboxb = str(int(subset.bounds.bottom))
                bboxr = str(int(subset.bounds.right))
                bboxt = str(int(subset.bounds.top))
            command = [
                "gdalwarp",
                "-overwrite",
                "-te", bboxl, bboxb, bboxr, bboxt,
                # "-ts", new_width, "256",
                "output_thumbnailRGB_scaled255nodata_merged.tif",
                "output_thumbnailRGB_scaled255nodata_clipped.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Apply overlay and create JPG
            thumbnail_name = apply_overlay(
                "output_thumbnailRGB_scaled255nodata_clipped.tif", thumbnail_name)

        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            print("Output")
            print(e.output)
            return False

    # VHI Use case
    elif product.startswith("ch.swisstopo.swisseo_vhi") and (inputfile_name.endswith("forest-10m.tif") or inputfile_name.endswith("forest-30m.tif")):
        # https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#visual
        # It should be called just   "thumbnail.jpg"
        # thumbnail_name = inputfile_name.replace(
        #     "bands-10m.tif", "thumbnail.jpeg")
        thumbnail_name = "thumbnail.jpg"

        try:
            # Export thumbnail
            command = [
                "gdal_translate",
                "-b", "1",
                "-of", "GTiff",
                "-outsize", "256", "256",
                inputfile_name,
                "output_thumbnail.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Define color map
            color_map = {
                (0, 10): (181, 106, 41),    # [0,10] extremely dry - dark brown
                (11, 20): (206, 133, 64),   # (10,20] severely dry - brown
                (21, 30): (245, 205, 133),  # (20,30] moderately dry - beige
                (31, 40): (255, 245, 186),  # (30,40] mild dry - yellow
                (41, 50): (203, 255, 202),  # (40,50] normal - light green
                (51, 60): (82, 189, 159),   # (50,60] good - green
                (61, 100): (4, 112, 176),   # (60,100] excellent - blue
                (110, 110): (128, 128, 128),  # 110 missing data values- gray
                (255, 255): (255, 255, 255)  # 255 no data values- white
            }

            # Load TIFF file
            with rasterio.open('output_thumbnail.tif') as src:
                data = src.read(1)  # Assuming single band
                profile = src.profile
                # Update profile for 3 bands and uint8 dtype
                profile.update(count=3, dtype=rasterio.uint8)

            # Apply color mapping
            data_rgb = np.zeros(
                (3, data.shape[0], data.shape[1]), dtype=np.uint8)
            for value_range, color in color_map.items():
                mask = np.logical_and(
                    data >= value_range[0], data <= value_range[1])
                for i in range(3):
                    data_rgb[i][mask] = color[i]

            # Write RGB image
            with rasterio.open('output_thumbnailRGB.tif', 'w', **profile) as dst:
                dst.write(data_rgb)

            # Fill Buffer Switzerland
            fill_buffer_switzerland("assets", "ch_buffer_5000m.shp", 1024)

            # overlay on Switzerland
            command = [
                "gdalwarp",
                "-overwrite",
                "-dstnodata", "255,255,255",
                "output_thumbnailswissfill.tif",
                "output_thumbnailRGB.tif",
                "output_thumbnailRGB_merged.tif",
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Apply overlay and create JPG
            thumbnail_name = apply_overlay(
                "output_thumbnailRGB_merged.tif", thumbnail_name)

        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            return False


    # NDVIz Use case
    elif product.startswith("ch.swisstopo.swisseo_ndvi_z") and (inputfile_name.endswith("forest-10m.tif")):
        # https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#visual
        # It should be called just   "thumbnail.jpg"
        # thumbnail_name = inputfile_name.replace(
        #     "bands-10m.tif", "thumbnail.jpeg")
        thumbnail_name = "thumbnail.jpg"

        try:
            # Pre-process to combine no-data values
            with rasterio.open(inputfile_name) as src:
                data = src.read(1)
                profile = src.profile.copy()
                
                # Convert both 32700 and 32701 to 32701
                data[(data == 32700) | (data == 32701)] = 32701
                
                # Update profile to set no-data value
                profile.update(nodata=32701)
                
                # Write preprocessed file
                with rasterio.open('preprocessed.tif', 'w', **profile) as dst:
                    dst.write(data, 1)

            # Export thumbnail
            command = [
                "gdal_translate",
                "-b", "1",
                "-of", "GTiff",
                "-outsize", "256", "256",
                "-a_nodata", "32701",
                "preprocessed.tif",
                "output_thumbnail.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Define color map
            color_map = { #scaling factor 100
                (-550, -450): (125, 102, 8),    #dark brown
                (-449, -350): (169, 137, 11),   # 
                (-349, -250): (212, 172, 13),  # 
                (-249, -150): (230, 196, 62),  # 
                (-149, -50): (247, 220, 111),  # light yellow
                (-49, 0.5): (245, 245, 245),   # light grey
                (49, 150): (125, 206, 160),   # light green
                (149, 250): (80, 180, 122),  # 
                (249, 350): (34, 153, 84),  # 
                (349, 450): (27, 122, 67),  #
                (449, 550): (20, 90, 50),  # dark green
                (32700, 32700): (128, 128, 128),  # missing data values- gray
                (32701, 32701): (255, 255, 255)  # no data values- white
            }

            # Load TIFF file
            with rasterio.open('output_thumbnail.tif') as src:
                data = src.read(1)  # Assuming single band
                profile = src.profile
                # Update profile for 3 bands and int16 dtype
                profile.update(count=3, dtype=rasterio.int16)

            # Apply color mapping
            data_rgb = np.zeros(
                (3, data.shape[0], data.shape[1]), dtype=np.int16)
            for value_range, color in color_map.items():
                mask = np.logical_and(
                    data >= value_range[0], data <= value_range[1])
                for i in range(3):
                    data_rgb[i][mask] = color[i]

            # Write RGB image
            with rasterio.open('output_thumbnailRGB.tif', 'w', **profile) as dst:
                dst.write(data_rgb)

            # Fill Buffer Switzerland
            fill_buffer_switzerland("assets", "ch_buffer_5000m.shp", 1024)

            # overlay on Switzerland
            command = [
                "gdalwarp",
                "-overwrite",
                "-dstnodata", "255,255,255",
                "output_thumbnailswissfill.tif",
                "output_thumbnailRGB.tif",
                "output_thumbnailRGB_merged.tif",
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)
        
            # Apply overlay and create JPG
            thumbnail_name = apply_overlay(
                "output_thumbnailRGB_merged.tif", thumbnail_name)

        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            return False
    else:
        return False




    # return the thumbnail
    return thumbnail_name
