from pathlib import Path
import xarray as xr
import pandas as pd
import re
from datetime import datetime
import requests
import os
import tempfile

def download_netcdf(url, local_path=None):
    """
    Download a netCDF file from a URL

    Args:
        url: URL to download
        local_path: Local path to save the file, if None a temporary file is created

    Returns:
        Path to the downloaded file
    """
    if local_path is None:
        # Create a temporary file
        fd, local_path = tempfile.mkstemp(suffix='.nc')
        os.close(fd)

    # Download the file
    print(f"Downloading {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return local_path

def get_monthly_files_for_date(parameters, satellite, channel, date_str, base_url):
    """
    Get monthly files that contain data for the specified date

    Args:
        parameters: List of parameters to get (e.g. ['SOL', 'SDL'])
        satellite: Satellite name (e.g. 'msg', 'mfg')
        channel: Channel name (e.g. 'ch02', 'ch05h')
        date_str: Date string in format YYYYMMDD for the day we want data for
        base_url: Base URL for the data

    Returns:
        Dictionary of temporary files with parameter as key
    """
    # Parse the date
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    year = date_obj.year

    # Get the first day of the month
    first_day = date_obj.replace(day=1)
    first_day_str = first_day.strftime('%Y%m%d')

    # Construct the correct URL format
    # For MSG: MSG2004-2023
    # For MFG: MFG1991-2005
    if satellite.lower() == 'msg':
        dir_name = f"MSG2004-2023"
    elif satellite.lower() == 'mfg':
        dir_name = f"MFG1991-2005"
    else:
        raise ValueError(f"Unknown satellite: {satellite}")

    file_dict = {}
    temp_files = []

    for param in parameters:
        # Create URL for the monthly file
        # Correct filename format: msg.SOL.H_ch02.lonlat_20040101000000.nc
        url = f"{base_url}/{dir_name}/{satellite.lower()}.{param}.H_{channel}.lonlat_{first_day_str}000000.nc"

        try:
            local_file = download_netcdf(url)
            file_dict[param] = [local_file]
            temp_files.append(local_file)
        except requests.exceptions.HTTPError as e:
            print(f"Error downloading {url}: {e}")
            file_dict[param] = []

    return file_dict, temp_files

def export_netcdf(ds, satellite, parameter, channel, date_str, output_path):
    """
    Export dataset to netCDF with proper metadata

    Args:
        ds: Dataset to export
        satellite: Satellite name
        parameter: Parameter name
        channel: Channel name
        date_str: Date string
        output_path: Output path

    Returns:
        Path to output file
    """
    # Create output directory if it doesn't exist
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create output filename
    output_file = output_dir / f"{satellite.lower()}.{parameter}.H_{channel}.lonlat_{date_str}000000.nc"

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
        date_str: Date string in format YYYYMMDD
        satellite: Satellite name (e.g. 'msg', 'mfg')
        channel: Channel name (e.g. 'ch02', 'ch05h')
        base_url: Base URL for the data
        output_path: Path to save output

    Returns:
        Path to output file
    """
    # Get monthly files for the date
    file_dict, temp_files = get_monthly_files_for_date(['SOL', 'SDL'], satellite, channel, date_str, base_url)

    try:
        # Check if we have files for both parameters
        if not file_dict['SOL'] or not file_dict['SDL']:
            print(f"No data found for {date_str}, {satellite}, {channel}")
            return None

        # Create datasets for each parameter
        sol_ds = xr.open_mfdataset(file_dict['SOL'], combine='by_coords')
        sdl_ds = xr.open_mfdataset(file_dict['SDL'], combine='by_coords')

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
        ds = xr.merge([sol_ds, sdl_ds])

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

    finally:
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except Exception as e:
                print(f"Error removing temp file {temp_file}: {e}")

def main():
    # Define paths
    BASE_URL = "https://data.geo.admin.ch/ch.meteoschweiz.landoberflaechentemperatur"
    OUTPUT_PATH = r"C:\temp\temp"

    # Define parameters
    date_str = "20040102"  # Example: January 2, 2004

    # Calculate MAX LST for different satellite/channel combinations
    results = []

    # MSG CH02
    if datetime.strptime(date_str, '%Y%m%d') >= datetime(2004, 1, 1) and datetime.strptime(date_str, '%Y%m%d') <= datetime(2023, 12, 31):
        result = calc_LST_for_date(date_str, 'msg', 'ch02', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    # MSG CH05H
    if datetime.strptime(date_str, '%Y%m%d') >= datetime(2004, 1, 1) and datetime.strptime(date_str, '%Y%m%d') <= datetime(2023, 12, 31):
        result = calc_LST_for_date(date_str, 'msg', 'ch05h', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    # MFG CH02
    if datetime.strptime(date_str, '%Y%m%d') >= datetime(1991, 1, 1) and datetime.strptime(date_str, '%Y%m%d') <= datetime(2005, 12, 31):
        result = calc_LST_for_date(date_str, 'mfg', 'ch02', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    # MFG CH05H
    if datetime.strptime(date_str, '%Y%m%d') >= datetime(1991, 1, 1) and datetime.strptime(date_str, '%Y%m%d') <= datetime(2005, 12, 31):
        result = calc_LST_for_date(date_str, 'mfg', 'ch05h', BASE_URL, OUTPUT_PATH)
        if result:
            results.append(result)

    print(f"Processed MAX LST for {date_str}. Output files:")
    for result in results:
        print(f" - {result}")

if __name__ == "__main__":
    main()