"""
util_upload_s3.py

This script provides utility functions for interacting with Amazon S3, including
uploading files and managing authentication. It supports both local (development)
and GitHub Action (production) environments by dynamically determining the run type
and configuring S3 credentials accordingly.

Key Features:
    - Determines the run type (local or GitHub) based on the presence of a secrets file.
    - Initializes an S3 client using appropriate credentials for the environment.
    - Uploads individual files or all CSV files from a specified directory to an S3 bucket.

Functions:
    - determine_run_type(): Identifies whether the script is running locally or on GitHub.
    - initialize_s3(): Authenticates and initializes the S3 client based on the run type.
    - upload_file_to_s3(local_file_path, bucket_name, s3_folder=''): Uploads a single file to S3.
    - upload_all_csv_from_tools(): Uploads all CSV files from the 'tools' directory to S3.

Usage:
    - Run the script directly to upload all CSV files from the 'tools' directory to the configured S3 bucket.
    - Modify the script to upload specific files or directories as needed.

Environment:
    - Development: Uses a local secrets file for authentication.
    - Production: Uses environment variables for authentication (e.g., in GitHub Actions).

Dependencies:
    - boto3: AWS SDK for Python.
    - configuration: Custom configuration module for accessing secrets and bucket information.

Example:
    $ python util_upload_s3.py
"""

import os
import json
import sys
import glob
import boto3
# Add parent directory to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)
import configuration as config


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.CMS_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    if os.path.exists(config.CMS_SECRETS):
        run_type = 2
        print("\nType 2 run PROCESSOR: We are on a local machine")
    else:
        run_type = 1
        print("\nType 1 run PROCESSOR: We are on GitHub")


def initialize_s3():
    """
    Initializes S3 based on the run type.

    If the run type is 2, S3 authenticates using the service account key file.
    If the run type is 1, S3  authenticates using secrets from GitHub Action.

    Note: This function assumes the required credentials and scopes are properly set.

    Returns:
        None
    """
    global s3
    if run_type == 2:
        # Initialize GEE and authenticate using the service account key file

        # Read the service account key file
        with open(config.CMS_SECRETS, "r") as f:
            credentials = json.load(f)
            bucket_key=credentials['aws_access_key_id']
            bucket_secret=credentials['aws_secret_access_key']

    else:
        # Run other code using secrets from GitHub Action
        # This script is running on GitHub
        bucket_key=os.environ.get('CMS_KEY')
        bucket_secret=os.environ.get('CMS_SECRET')

    # Create an S3 client using the credentials
    s3 = boto3.client('s3',
        aws_access_key_id=bucket_key,
        aws_secret_access_key=bucket_secret,
        #region_name='eu-central-2' #BIT sandbox
        region_name='eu-west-1' #PROD cms.geo.admin.ch
    )

    # # List all buckets
    # response = s3.list_buckets()

    # # Print the bucket names
    # for bucket in response['Buckets']:
    #     print(bucket['Name'])

    # breakpoint()

def upload_file_to_s3(local_file_path, bucket_name, s3_folder=''):

    # Construct the S3 key (path in the bucket)
    s3_key = os.path.join(s3_folder, os.path.basename(local_file_path))

    # Upload the file
    try:

        response = s3.upload_file(local_file_path, bucket_name, s3_key)
        #print(f"File uploaded successfully to {s3_key}")
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def upload_all_csv_from_tools():
    tools_dir = 'tools'
    target_s3_path = "Topo/umweltbeobachtung/tools/"
    failed_uploads = []

    csv_files = glob.glob(os.path.join(tools_dir, '*.csv'))

    if not csv_files:
        print("No CSV files found in tools/ directory.")
        return False

    for file_path in csv_files:
        filename = os.path.basename(file_path)
        print(f"Uploading {filename} ...")
        upload_ok = upload_file_to_s3(file_path, config.CMS_BUCKET, target_s3_path)
        if not upload_ok:
            print(f"Failed to upload {filename}.")
            failed_uploads.append(filename)

    if not failed_uploads:
        print("\nAll CSV files from tools/ uploaded successfully.")
        return True
    else:
        print("\nSome CSV files failed to upload:")
        for fname in failed_uploads:
            print(f" - {fname}")
        return False

if __name__ == "__main__":

    # Test if we are on a local machine or if we are on Github
    determine_run_type()

    # Authenticate with S3
    initialize_s3()

    # Upload all from tools
    upload_all_csv_from_tools()