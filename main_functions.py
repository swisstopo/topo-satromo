import os
import configuration as config
import subprocess
import rasterio


def create_thumbnail(inputfile_name, product):
    # product = metadata['SWISSTOPO']['PRODUCT']
    # inputfile_name = "ch.swisstopo.swisseo_s2-sr_v100_mosaic_2023-10-28T102039_bands-10m.tif"
    # thumbnail_name = "ch.swisstopo.swisseo_s2-sr_v100_mosaic_2023-10-28T102039_thumbnail.jpeg"

    # Thumbnail S2_SR 20m bands
    if product.startswith("ch.swisstopo.swisseo_s2-sr") and inputfile_name.endswith("bands-10m.tif"):
        #https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#visual
        #It should be called just   "thumbnail.jpg"  
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
            command = [
                "gdal_rasterize",
                "-burn", "220", "-burn", "220", "-burn", "220",
                "-init", "0",
                "-a_nodata", "0",
                "-l", "ch_buffer_5000m",
                config.BUFFER,
                "-ot", "Byte",
                "-of", "GTiff",
                "-ts", "1024", "1024",
                "output_thumbnailswissfill.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Set 0 values to nodata
            command = [
                "gdal_translate",
                "-a_nodata", "0,0,0",
                "output_thumbnailRGB_scaled255.tif",
                "output_thumbnailRGB_scaled255nodata.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # overlay on Switzerland
            command = [
                "gdalwarp",
                "-overwrite",
                "-dstnodata", "0",
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

            # Burn Rivers
            command = [
                "gdal_rasterize",
                "-b", "1", "-b", "2", "-b", "3",
                "-burn", "255", "-burn", "255", "-burn", "255",
                "-l", "overview_rivers_2056",     # Specify layer name
                config.OVERVIEW_RIVERS,
                "output_thumbnailRGB_scaled255nodata_clipped.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # Burn Lakes
            command = [
                "gdal_rasterize",
                "-b", "1", "-b", "2", "-b", "3",
                "-burn", "255", "-burn", "255", "-burn", "255",
                "-l", "overview_lakes_2056",     # Specify layer name
                config.OVERVIEW_LAKES,
                "output_thumbnailRGB_scaled255nodata_clipped.tif"
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # export to jpg
            command = [
                "gdal_translate",
                "-of", "JPEG",
                "--config", "GDAL_PAM_ENABLED", "NO",
                "output_thumbnailRGB_scaled255nodata_clipped.tif",
                thumbnail_name
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)

            # remove intermediate files
            [os.remove(file) for file in os.listdir()
             if file.startswith("output_thumbnail")]

        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            return False
    else:
        return False
    return thumbnail_name
