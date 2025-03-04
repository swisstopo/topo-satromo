from pathlib import Path
import xarray as xr
import pandas as pd
import re
from datetime import datetime
import requests
import os
import tempfile
from io import BytesIO
import netCDF4
import subprocess

def download_netcdf_to_memory(url):
    """
    Download a netCDF file from a URL and return as a BytesIO object

    Args:
        url: URL to download

    Returns:
        BytesIO object containing the file data or None if download failed
    """
    try:
        #print(f"Streaming {url}")
        response = requests.get(url)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def get_monthly_files_for_date(parameters, satellite, channel, date_str, base_url):
    """
    Get monthly files that contain data for the specified date using streaming

    Args:
        parameters: List of parameters to get (e.g. ['SOL', 'SDL'])
        satellite: Satellite name (e.g. 'msg', 'mfg')
        channel: Channel name (e.g. 'ch02', 'ch05h')
        date_str: Date string in format YYYY-MM-DD for the day we want data for
        base_url: Base URL for the data

    Returns:
        Dictionary of BytesIO objects with parameter as key
    """
    # Parse the date
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    year = date_obj.year

    # Get the first day of the month
    first_day = date_obj.replace(day=1)
    first_day_str = first_day.strftime('%Y%m%d')  # Convert to YYYYMMDD for file naming

    # Construct the correct URL format
    # For MSG: MSG2004-2023
    # For MFG: MFG1991-2005
    if satellite.lower() == 'msg':
        dir_name = f"MSG2004-2023"
    elif satellite.lower() == 'mfg':
        dir_name = f"MFG1991-2005"
    else:
        raise ValueError(f"Unknown satellite: {satellite}")

    data_dict = {}

    for param in parameters:
        # Create URL for the monthly file
        # Correct filename format: msg.SOL.H_ch02.lonlat_20040101000000.nc
        url = f"{base_url}/{dir_name}/{satellite.lower()}.{param}.H_{channel}.lonlat_{first_day_str}000000.nc"

        # Download file into memory
        data = download_netcdf_to_memory(url)
        if data is not None:
            data_dict[param] = data
        else:
            data_dict[param] = None

    return data_dict

def export_geotiff(netcdf_path):
    """
    ExportLSTMAX from a NetCDF file to a GeoTIFF
    using subprocess calls to GDAL utilities. Multiplies values by 100 and converts to UInt16.

    Parameters:
    -----------
    netcdf_path : str
        Path to the NetCDF file
    output_path : str
        Path where the output GeoTIFF will be saved

    """
    # Open the NetCDF file to get information
    dataset = netCDF4.Dataset(netcdf_path, 'r')
    output_path = netcdf_path.replace(".nc", ".tif")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = os.path.join(temp_dir, "lst_max_raw.tif")
        temp_scaled_file = os.path.join(temp_dir, "lst_max_scaled.tif")

        # Extract the LST_MAX band
        gdal_translate_cmd = [
            'gdal_translate',
            '-a_srs', 'EPSG:4326',  # Set spatial reference
            '-b', '1',  # First and only band
            '-sds',  # Select subdataset if needed
            netcdf_path,
            temp_file
        ]

        subprocess.run(gdal_translate_cmd, check=True, capture_output=True, text=True)

        # Scale values by 100 and convert to UInt16
        gdal_calc_cmd = [
            'gdal_calc',
            '-A', temp_file,
            '--outfile=' + temp_scaled_file,
            '--calc=numpy.uint16(A*100)',
            '--NoDataValue=0',
            '--type=UInt16'
        ]

        subprocess.run(gdal_calc_cmd, check=True, capture_output=True, text=True)

        # Reproject to EPSG:2056 and save as Cloud-Optimized GeoTIFF (COG)
        gdalwarp_cmd = [
            'gdalwarp',
            '-s_srs', 'EPSG:4326',
            '-t_srs', 'EPSG:2056',
            '-of', 'COG',
            '-srcnodata', '0',
            '-co', 'COMPRESS=DEFLATE',
            '-co', 'PREDICTOR=2',
            '-ot', 'UInt16',
            '-r', 'bilinear',
            temp_scaled_file,
            output_path
        ]

        subprocess.run(gdalwarp_cmd, check=True, capture_output=True, text=True)

    dataset.close()
    print(f"COG Geotiff created at: {output_path}")

def export_netcdf(ds, satellite, parameter, channel, date_str, output_path):
    """
    Export dataset to netCDF with proper metadata

    Args:
        ds: Dataset to export
        satellite: Satellite name
        parameter: Parameter name
        channel: Channel name
        date_str: Date string in YYYY-MM-DD format
        output_path: Output path

    Returns:
        Path to output file
    """
    # Create output directory if it doesn't exist
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert date format for filename (YYYY-MM-DD to YYYYMMDD)
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    filename_date = date_obj.strftime('%Y%m%d')

    # Create output filename
    output_file = output_dir / f"{satellite.lower()}.{parameter}.H_{channel}.lonlat_{filename_date}000000.nc"

    # Add metadata
    ds.attrs.update({
        'title': f'Maximum Land Surface Temperature for {date_str}',
        'source': f'{satellite.upper()} satellite {channel} data',
        'created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'parameter': parameter,
        'satellite': satellite,
        'channel': channel
    })

    # Add parameter attributes
    if parameter == 'LST1max':
        ds[parameter].attrs.update({
            'long_name': 'Maximum Land Surface Temperature',
            'units': 'K',
            'standard_name': 'surface_temperature',
            'description': 'Daily maximum land surface temperature calculated using SOL and SDL data with emissivity of 0.98'
        })

    # Export to netCDF
    ds.to_netcdf(output_file, encoding={
        parameter: {
            'zlib': True,
            'complevel': 5,
            '_FillValue': -9999.0
        }
    })

    return output_file

def calc_LST_for_date(date_str, satellite, channel, base_url, output_path):
    """
    Calculate MAX LST for a specific date

    Args:
        date_str: Date string in format YYYY-MM-DD
        satellite: Satellite name (e.g. 'msg', 'mfg')
        channel: Channel name (e.g. 'ch02', 'ch05h')
        base_url: Base URL for the data
        output_path: Path to save output

    Returns:
        Path to output file
    """
    # Get data for the monthly files
    data_dict = get_monthly_files_for_date(['SOL', 'SDL'], satellite, channel, date_str, base_url)

    try:
        # Check if we have data for both parameters
        if data_dict['SOL'] is None or data_dict['SDL'] is None:
            print(f"No data found for {date_str}, {satellite}, {channel}")
            return None

        # Open datasets from BytesIO objects
        sol_ds = xr.open_dataset(data_dict['SOL'], engine='h5netcdf')
        sdl_ds = xr.open_dataset(data_dict['SDL'], engine='h5netcdf')

        # Parse target date
        target_date = pd.to_datetime(date_str)

        # Filter data for the specific date we want
        sol_ds = sol_ds.sel(time=slice(target_date, target_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)))
        sdl_ds = sdl_ds.sel(time=slice(target_date, target_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)))

        # Check if we have data for the target date
        if len(sol_ds.time) == 0 or len(sdl_ds.time) == 0:
            print(f"No data found for {date_str} in the monthly files")
            return None

        # Merge datasets
        ds = xr.merge([sol_ds, sdl_ds], compat='override')

        # Calculate LST
        Boltzmann = 5.670374419e-8
        Emissivity = 0.98
        ds['LST1'] = ((ds['SOL']-(1-Emissivity)*ds['SDL'])/Boltzmann/(Emissivity))**(1/4)

        # Calculate daily maximum
        daily_max = ds['LST1'].max(dim='time')

        # Create a new dataset with just the max value
        ds_max = xr.Dataset(
            data_vars={
                'LST1max': (('lat', 'lon'), daily_max.values)
            },
            coords={
                'time': [target_date],
                'lat': ds.lat,
                'lon': ds.lon
            }
        )

        # Export to netCDF with enhanced metadata
        output_file = export_netcdf(ds_max, satellite, 'LST1max', channel, date_str, output_path)

        # Close input datasets to free up resources
        sol_ds.close()
        sdl_ds.close()

        return output_file

    except Exception as e:
        print(f"Error processing data for {date_str}, {satellite}, {channel}: {e}")
        return None

def main():
    # Define paths
    BASE_URL = "https://data.geo.admin.ch/ch.meteoschweiz.landoberflaechentemperatur"
    OUTPUT_PATH = r"C:\temp\temp"

    # Define parameters with new YYYY-MM-DD format
    date_str = "2018-06-15"  # Changed from "20180615" to "2018-06-15"

    # Convert dates for comparison
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')

    # Calculate MAX LST for different satellite/channel combinations
    results = []

    # MSG CH02
    if date_obj >= datetime(2004, 1, 1) and date_obj <= datetime(2023, 12, 31):
        result = calc_LST_for_date(date_str, 'msg', 'ch02', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    # MSG CH05H
    if date_obj >= datetime(2004, 1, 1) and date_obj <= datetime(2023, 12, 31):
        result = calc_LST_for_date(date_str, 'msg', 'ch05h', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    # MFG CH02
    if date_obj >= datetime(1991, 1, 1) and date_obj <= datetime(2005, 12, 31):
        result = calc_LST_for_date(date_str, 'mfg', 'ch02', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    # MFG CH05H
    if date_obj >= datetime(1991, 1, 1) and date_obj <= datetime(2005, 12, 31):
        result = calc_LST_for_date(date_str, 'mfg', 'ch05h', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    print(f"Processed MAX LST for {date_str}. Output files:")
    for result in results:
        print(f" - {result}")

if __name__ == "__main__":
    main()