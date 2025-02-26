"""
VHI Map Grid Generator

This script generates grid images of Vegetation Health Index (VHI) maps for each month and year
within a specified range. The maps are fetched from a remote server, processed, and saved as images.

Author: David Oesch
Date: Jan 2025


Usage:
    python util_vhi_maps.py
    change in code the
    VHI_TYPE = "vegetation" or "forest"

Dependencies:
    - requests
    - numpy
    - PIL (Pillow)
    - concurrent.futures
    - tqdm
    - logging
    - json
    - calendar
"""

import requests
import numpy as np
from datetime import datetime, timedelta
import os
from PIL import Image, ImageDraw, ImageFont
import concurrent.futures
from tqdm import tqdm
import logging
import json
import calendar



class VHIMapGridGenerator:
    def __init__(self):
        self.years = range(2017, 2025)
        self.base_url = "https://data.geo.admin.ch/ch.swisstopo.swisseo_vhi_v100"
        self.map_size = (100, 100)
        self.max_workers = 4

        self.bbox = {
            'min_lon': 5.9559,
            'max_lon': 10.4921,
            'min_lat': 45.8179,
            'max_lat': 47.8084
        }

        self.vhi_colors = {
            (0, 9): "#b56a29",
            (10, 19): "#ce8540",
            (20, 29): "#f5cd85",
            (30, 39): "#fff5ba",
            (40, 49): "#cbffca",
            (50, 59): "#52bd9f",
            (60, 100): "#0470b0",
        }

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('map_generation.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def hex_to_rgb(self, hex_color):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    def get_color_for_vhi(self, vhi):
        if vhi is None or vhi == 110:
            return (255, 255, 255)

        for (min_val, max_val), color in self.vhi_colors.items():
            if min_val <= vhi <= max_val:
                return self.hex_to_rgb(color)
        return (255, 255, 255)

    def generate_dates(self):
        dates = []
        for year in self.years:
            for month in range(1, 13):
                num_days = calendar.monthrange(year, month)[1]
                for day in range(1, num_days + 1):
                    dates.append(datetime(year, month, day))
        return dates

    def convert_to_pixel_coords(self, lon, lat, width, height):
        x = int((lon - self.bbox['min_lon']) / (self.bbox['max_lon'] - self.bbox['min_lon']) * width)
        y = int((self.bbox['max_lat'] - lat) / (self.bbox['max_lat'] - self.bbox['min_lat']) * height)
        return x, y

    def fetch_map_data(self, date, year):
        try:
            date_str = date.strftime("%Y-%m-%d")
            url = f"{self.base_url}/{date_str}t235959/ch.swisstopo.swisseo_vhi_v100_{date_str}t235959_{VHI_TYPE}-warnregions.geojson"

            response = requests.get(url)
            if response.status_code != 200:
                return None

            return response.json()

        except Exception as e:
            self.logger.error(f"Error fetching data for {date_str}: {str(e)}")
            return None

    def process_coordinates(self, coordinates):
        pixel_coords = []
        try:
            if isinstance(coordinates[0][0], (int, float)):
                lon, lat = coordinates
                return [self.convert_to_pixel_coords(lon, lat, self.map_size[0], self.map_size[1])]
            elif isinstance(coordinates[0][0][0], (int, float)):
                for coord in coordinates[0]:
                    lon, lat = coord
                    pixel_coords.append(
                        self.convert_to_pixel_coords(lon, lat, self.map_size[0], self.map_size[1])
                    )
            else:
                for poly in coordinates:
                    for coord in poly[0]:
                        lon, lat = coord
                        pixel_coords.append(
                            self.convert_to_pixel_coords(lon, lat, self.map_size[0], self.map_size[1])
                        )
        except Exception as e:
            self.logger.error(f"Error processing coordinates: {str(e)}")
            return []

        return pixel_coords

    def create_map_image(self, geojson_data):
        if not geojson_data or 'features' not in geojson_data:
            return None

        image = Image.new('RGB', self.map_size, (255, 255, 255))
        draw = ImageDraw.Draw(image)

        for feature in geojson_data['features']:
            if 'geometry' not in feature or 'properties' not in feature:
                continue

            vhi = feature['properties'].get('vhi_mean')
            availability = feature['properties'].get('availability_percentage')

            coordinates = feature['geometry']['coordinates']
            pixel_coords = self.process_coordinates(coordinates)

            if len(pixel_coords) > 2:
                if vhi == 110 or availability < 20:
                    draw.polygon(pixel_coords, outline=(0, 0, 0))
                else:
                    color = self.get_color_for_vhi(vhi)
                    draw.polygon(pixel_coords, fill=color, outline=(0, 0, 0))

        return image

    def create_grid_image(self, maps_data, month):
        n_cols = len(self.years)
        num_days = calendar.monthrange(self.years[0], month)[1]
        n_rows = num_days

        padding_top = 30    # Space for year labels
        padding_left = 30   # Space for day labels

        total_width = n_cols * self.map_size[0] + padding_left
        total_height = n_rows * self.map_size[1] + padding_top
        grid_image = Image.new('RGB', (total_width, total_height), 'white')

        draw = ImageDraw.Draw(grid_image)
        try:
            font = ImageFont.truetype("arial.ttf", 20)
        except:
            font = ImageFont.load_default()

        # Add year labels at top
        for col, year in enumerate(self.years):
            x = padding_left + col * self.map_size[0] + self.map_size[0]//2 - 15
            draw.text((x, 10), str(year), fill='black', font=font)

        # Add day labels on left side
        for day in range(1, num_days + 1):
            y = padding_top + (day-1) * self.map_size[1] + self.map_size[1]//2 - 5
            draw.text((20, y), str(day), fill='black', font=font)

        # Add maps
        for day in range(1, num_days + 1):
            for col, year in enumerate(self.years):
                current_date = datetime(year, month, day)
                if (current_date, year) in maps_data:
                    map_image = self.create_map_image(maps_data[(current_date, year)])
                    if map_image:
                        x = padding_left + col * self.map_size[0]
                        y = padding_top + (day-1) * self.map_size[1]
                        grid_image.paste(map_image, (x, y))

        # Add the type  at the footer
        draw.text((10, total_height - self.map_size[1] + 10), f"{VHI_TYPE}", fill='black', font=font)

        # Add the name of the python program at the footer
        draw.text((10, total_height - 20), f"util_vhi_maps.py", fill='black', font=font)

        # Add month label at the top
        draw.text((10, 10), f"{datetime(1900, month, 1).strftime('%B')}", fill='black', font=font)

        return grid_image

    def add_legend(self, image):
        legend_width = 200
        legend_height = 150
        legend = Image.new('RGB', (legend_width, legend_height), 'white')
        draw = ImageDraw.Draw(legend)

        try:
            font = ImageFont.truetype("arial.ttf", 20)
        except:
            font = ImageFont.load_default()

    def run(self):
        self.logger.info("Starting VHI map grid generation...")

        os.makedirs("output", exist_ok=True)
        dates = self.generate_dates()

        maps_data = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_date = {
                executor.submit(self.fetch_map_data, date, date.year): (date, date.year)
                for date in dates
            }

            for future in tqdm(concurrent.futures.as_completed(future_to_date),
                             total=len(dates),
                             desc="Fetching maps"):
                date, year = future_to_date[future]
                try:
                    geojson_data = future.result()
                    if geojson_data is not None:
                        maps_data[(date, year)] = geojson_data
                except Exception as e:
                    self.logger.error(f"Error processing {date}: {str(e)}")

        for month in range(1, 13):
            self.logger.info(f"Creating grid image for month {month}...")
            grid_image = self.create_grid_image(maps_data, month)

            self.logger.info("Adding legend...")
            self.add_legend(grid_image)

            width, height = grid_image.size
            new_width = int(width * 1.8)
            grid_image = grid_image.resize((new_width, height))

            output_filename = f"output/vhi_map_grid_{datetime(1900, month, 1).strftime('%B')}_{VHI_TYPE}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            grid_image.save(output_filename, optimize=True, quality=85)
            self.logger.info(f"Saved output as {output_filename}")

if __name__ == "__main__":
    generator = VHIMapGridGenerator()
    global VHI_TYPE
    VHI_TYPE = "vegetation"
    generator.run()