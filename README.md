[![Run processor](https://github.com/swisstopo/topo-satromo/actions/workflows/run-processor.yml/badge.svg)](https://github.com/swisstopo/topo-satromo/actions/workflows/run-processor.yml) [![Run publisher](https://github.com/swisstopo/topo-satromo/actions/workflows/run-publisher.yml/badge.svg)](https://github.com/swisstopo/topo-satromo/actions/workflows/run-publisher.yml)
[![GitHub commit](https://img.shields.io/github/last-commit/swisstopo/topo-satromo)](https://github.com/swisstopo/topo-satromo/commits/main)

![Python Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10-blue.svg)
# SATROMO Processing chain

The "Erdbeobachtungs-SAtellitendaten fürs TRockenheitsMOnitoring" (SATROMO) consists of python code with ETL functionalities to operationally generate and provide AnalysisReadyData and indices from satellite sensors using GoogleEarthEngine, GithubAction and AWS S3 / Cloudfront 

## Disclaimer

**Note: This project is currently in the commissioning phase and is limited for operational use.**


|      | swissEO S2-SR | swissEO VHI |
|------------------|---------------|--------------|
| Data description  | [Product site](https://www.swisstopo.admin.ch/en/satelliteimage-swisseo-s2-sr) | [Product site](https://www.swisstopo.admin.ch/en/satelliteimage-swisseo-vhi) |
| Access to data    | [STAC](https://data.geo.admin.ch/browser/index.html#/collections/ch.swisstopo.swisseo_s2-sr_v100) | [STAC](https://data.geo.admin.ch/browser/index.html#/collections/ch.swisstopo.swisseo_vhi_v100) |


## Introduction and Project Description

This project aims to define automated spatial satellite products, indices, and analysis-ready datasets (ARD), as well as establish a geospatial data processing pipeline with traceable algorithms in the field of drought monitoring.
The aim is to have a serverless processing chain to derive and publish satellite sensor derived products for drought monitoring. 

There are two environments:
- DEV which is a local machine with python installes
- PROD which is GitHub Action based.

## Installation

To install and set up the project, follow the instructions below:

1. Clone the repository to your local machine:
```
git clone https://github.com/swisstopo/topo-satromo.git
```
2. Install the project dependencies using pip:
```
pip install -r requirements.txt
```
3. Make sure you have Python 3.x and pip installed on your system.

[Optional] If you are using a virtual environment, activate it:
```
source <virtual-environment>/bin/activate
```
This step is recommended to isolate the project dependencies from your system-wide Python installation.

4. By default, the configuration used is the file `configuration\dev_config.py`.
You can use another configuration file for your own setup. 
To choose another configuration, use a command-line argument to point towards 
the configuration file contained in the <configuration> folder.

For example, if you want the configuration file `configuration\myconfig.py` to be used for your processing,
you can define it using the following statement:

> python satromo_processor.py myconfig.py

Note that all configuration files have to be located in the configuration folder

5. You are now ready to use the project!

For the use on your local machine / DEV 
- You need to create a folder "secrets" containing
    - GoogleEarthEngine credentials: the GEE json `keyfile.json` according to your [Google Service Account](https://developers.google.com/earth-engine/guides/service_account) and a private key for the [service account](https://developers.google.com/earth-engine/guides/service_account#create-a-private-key-for-the-service-account). Ensure that service  account has the [Earth Engine API enabled](https://developers.google.com/earth-engine/cloud/earthengine_cloud_project_setup) and has been has been [registered](https://code.earthengine.google.com/register) for use with Earth Engine. Then you also need to activate your  GoogleDrive / Google developers console API activation for your ServiceAccount Project https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project=<YourprojectID> to access GDRIVE via API
    - rclone set up: To move files from GoogleDrive to your TargetDestination we use [rclone](https://rclone.org/). Make sure that either rclone is installed,  and path to it is set .set up `rclone.conf` :
        ```
        [<GEEDRIVE_DEV as definend in the configuration file>]
        type = drive
        scope = drive
        service_account_file = keyfile.json
        team_drive = 
        
        [<S3_DEV as definend in configuration file>]
        type = s3
        provider = AWS
        access_key_id = <your key>
        secret_access_key = <your access key>
        region = <your region>
        location_constraint = <your region>
        ```
        
For the use with GithubAction / INT
Adapt your GoogleEarthEngine and rclone credentials
- GoogleEarthEngine credentials: create a secret for github action called "GOOGLE_CLIENT_SECRET" copy paste `keyfile.json` into it
- rclone credentials: create a secret for github action called "RCONF_SECRET" copy paste`rclone.conf` into it

## Solution architecture
### Program flows  (`satromo_processor.py` and `satromo_publisher.py`)

![Diagram](satromo_processor.drawio.svg)


### Program flows VHI (`step1_processor_vhi.py` ) 
See [VHI flowchart](VHI_flowchart.md)


## Functionality in detail

For starters, we build a GithubAction & GEE python based ARD and Indices extractor for Switzerland. The SATROMO operational module is started on a pre-defined schedule using [GitHub Actions](https://github.com/features/actions).The "processor" run is using by default the [dev_config.py](configuration/dev_config.py]). A specific configuration can be started with `python satromo_processor.py my_config.py`.  During a "processor" run, the code:
1. Check the existence and completeness of personal collections in `step0`: post-processed Google Earth Engine (GEE) sensor collection with selected co-registered bands, topographic correction (TOA products), cloud & terrain shadow masks
2. triggers GEE extraction of products for the region of Switzerland using the personal collection using the configuration and last update information in `tools/last_updates.csv`
3. stores running tasks in `processing/running_tasks.csv` and an appropriate accompanying text file for each product which is split in quadrants to enable GEE exports
4. persists information about status of running GEE processes IDs of the extracted data. These pieces of information are stored directly in the repository at hand.  

A pre-defined time after the "processor" run, a "publisher" run starts, assuming all exports are done. In it, the publisher process:
1. reads the persisted information as to the most recently bprocessed  products based on IDs in `processing/running_tasks.csv`
2. merges and clips the products, e.g. ARD, indices etc. locally in GitHubAction runner. Be aware that the current limit is the disk space available on github (approximately 7 GB)
3. moves the product and its persisted information with rclone to S3
4. creates a static STAC Catalog
5. updates `tools/last_updates.csv`.
6. invalidates the STAC Catalog on Coudfront: [STAC BROWSER](https://data.geo.admin.ch/browser/index.html#/collections/ch.swisstopo.swisseo_s2-sr_v100?.language=en) fetches latest version of the STAC catalog

## Configuration of products

### Configuration in <>_config.py  
Edit [configuration/dev_config.py](configuration/dev_config.py) rename it to your needs. 

#### Additional personal collections
A personal collection contains a GEE collection `my_new_collection` which contains postprocessed sensor data. The postprocessing is done according to the ` <new_file>.<new_function_name>` . E.g. The product `ch.swisstopo-swisseo_vhi_v100` is based on the `COPERNICUS/S2_SR_HARMONIZED` collection which has been postprocessed with `step0_processor_s2_sr.generate_s2_sr_mosaic_for_single_date` and stored in the personal collection 'projects/username/assets/COL_S2_SR_HARMONIZED_SWISS'

The personal collection needs to be created in advance in the GEE GUI via Assets -> new ->  Image collection

**Note:if you want to ensure that other accounts can update/read the asset ( like satromo-int) you need to give them access via right click on asset which will then open https://console.cloud.google.com/iam-admin/iam?project=your-project and as well share "image collection" (like COL_S2_SR_HARMONIZED_SWISS).**

Adding a new collection in the tool require to adapt the configuration file. 

A new function specifically designed for this new collections should also be added in the step0_processors folder.
(If you don't need a new step0 processor function, you certainly don't need a new collection...)
Create the new function in a new file located in step0_processors folder.
In the configuration file, add the function to the configuration entry of your new collection:
The `cleaning_older_than` removes asset older teh defiend days to save GEE storage. 

```
step0: {
    ...
    my_new_collection: {
        step0_function: <new_file>.<new_function_name>,
        cleaning_older_than: <days> 
    }
}
```


#### Product definition based on personal collection
Products are defined  with the following mandatory parameters. more can be added, based on needs for the product generation.Do mind that for all products based on the same sensor the same personal collection should be used

```
my_new_product = {
    "prefix": <my_prefix>,
    "temporal_coverage": <number of days to take into account for product generation>,  
    "spatial_scale_export": <spatial resolution in m>,
    "product_name": <my_product_name>,
    "step0_collection": <my_new_collection to be used>
    ...
}

```
### Configuration post processing for each sensor step0_processor_<>.py
A new function specifically designed for each personal collections is stored in the [step0_processors](step0_processors) folder.
(If you don't need a new step0 processor function, you certainly don't need a new collection...)
Create the new function in a new file located in step0_processors folder.

## Single Scene Processing via Command Line

### Use Case:
For cases where certain data was not computed on production for some reason (e.g., missing cloud mask etc.). These instructions are suitable for processing scenes via command line on the local machine using an INT or PROD account from GEE with a local version of the code and writing to PROD of STAC and GEE ASSETS.

### Prerequisites

- **Installation**
  Install according to the instructions in the GitHub repository.

- **Copy MAIN**
  It is recommended to copy/check out MAIN with Github to ensure consistency in data processing.

- **Objective**
  The goal is to process a day, e.g., 2024-06-12, for a product.

- **Configure prod_config.py**
  Configure prod_config.py so that in case of Windows:
    - Use GEE INT geetest-credentials-int.secret
    - Set GDRIVE_SOURCE_DEV = "geedriveINT:"
    - Set GDRIVE_MOUNT_DEV = r'G:\\'
    - Ensure corresponding drives are mounted

- **Delete Assets**
  Delete affected (damaged) assets in GEE step0_collection before recalculating step0.

- **Remove empty_step0.csv asset entry for date**
  Delete all 'no candidate' entries for products in tools\empty_step0.csv if they are to be recalculated.

- **Set Date**
  Set "LastSceneDate" of the respective product before the date you want to process, e.g., before 2024-06-12, in tools/last_updates.txt.

- **Start Venv Environment**
  Start and activate the virtual environment:
    - Open Command Prompt
    - Change directory where satromo folder is located 
      ```
      c:
      cd C:\temp\topo-satromo (as an example)
      .venv\Scripts\activate
      ```

### Execution

1. For June 12, 2024, process the products as defined in prod_config.py:
```
(.venv) C:\temp\topo-satromo>python satromo_processor.py prod_config.py 2024-06-12
```

2. GEE Asset creation complete and Start Export
Check after about 60 minutes, if the corresponding assets (x2) are present in GEE.
```
(.venv) C:\temp\topo-satromo>python satromo_processor.py prod_config.py 2024-06-12
```

3. Start Publish Export
Check after about 60 minutes, if the corresponding files (6x4) are present in Google Drive, or keep running the job below.
```
(.venv) C:\temp\topo-satromo>python satromo_publish.py prod_config.py
```

### Update step0_empty_assets.csv List on GitHub

Update the step0_empty_assets.csv directly on GitHub PROD: https://github.com/swisstopo/topo-satromo/blob/main/tools/step0_empty_assets.csv

## Technologies

- Python, GEE, GDAL, RCLONE
- GitHub Actions at pre-defined times (essentially CRON jobs): GitHub Actions are free when effected from an open repository.
- GitHub Secrets for GEE, S3 credentials
- `gee` and `requests` (or similar) Python packages

## Roadmap

- [X] Add Changelog
- [X] Implement STAC Cataloge description  
- [X] Implement Co-Registration correction
- [X] Implement Topografic shadow information
- [ ] Products
    - [X] R1 Rohdaten Level-2A 
    - [ ] N1 Vitalität – NDVI Anomalien
    - [ ] M1 Vitalität – NDMI Anomalien
    - [ ] N2 Veränderung – NDVI Anomalien
    - [ ] B2 Natürliche Störungen – NBR
    - [X] V1 Vegetation Health Index

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the BSD-3-Clause License. See `LICENSE.txt` for more information.

## Credits and Acknowledgments

* [Google Earth Engine API](https://github.com/google/earthengine-api)
* [GDAL](https://github.com/OSGeo/gdal)
* [RCLONE](https://github.com/rclone/rclone)


## Contact Information

David Oesch - david.oesch[ a t]swisstopo.ch
Joan Sturm - joan.sturm[ a t]swisstopo.ch

Project Link: [https://github.com/swisstopo/satromo](https://github.com/swisstopo/satromo)





