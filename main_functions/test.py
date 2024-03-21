import ee
import configuration as config
from pydrive.auth import GoogleAuth
import json
from oauth2client.service_account import ServiceAccountCredentials

# Set scopes for Google Drive
scopes = ["https://www.googleapis.com/auth/drive"]

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



aoi_CH = ee.FeatureCollection(
    "users/wulf/SATROMO/swissBOUNDARIES3D_1_4_TLM_LANDESGEBIET_epsg32632").geometry()

day_to_process = '2023-08-22'
start_date = ee.Date(day_to_process)
end_date = ee.Date(day_to_process).advance(1, 'day')

 # S2 CloudScore+
S2_csp = ee.ImageCollection('GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED') \
    .filter(ee.Filter.bounds(aoi_CH)) \
    .filter(ee.Filter.date(start_date, end_date))

 # Sentinel-2
S2_sr = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
    .filter(ee.Filter.bounds(aoi_CH)) \
    .filter(ee.Filter.date(start_date, end_date)) \
    .linkCollection(S2_csp, ['cs','cs_cdf'])

breakpoint()