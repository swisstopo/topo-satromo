import ee
import pandas as pd
from pydrive.auth import GoogleAuth
from oauth2client.service_account import ServiceAccountCredentials
import json
import os


def initialize_gee_and_drive(credentials_file):
    """
    Initializes Google Earth Engine (GEE) and Google Drive authentication.

    Args:
        credentials_file (str): Path to the service account credentials JSON file.

    Returns:
        bool: True if initialization is successful, False otherwise.
    """
    # Set scopes for Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]

    try:
        # Initialize Google Earth Engine
        ee.Initialize()

        # Authenticate with Google Drive
        gauth = GoogleAuth()
        gauth.service_account_file = credentials_file
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )
        gauth.ServiceAuth()

        print("Google Earth Engine and Google Drive authentication successful.")
        return True

    except Exception as e:
        print(
            f"Failed to initialize Google Earth Engine and Google Drive: {str(e)}")
        return False


def export_completed_tasks_to_csv(output_file):
    """
    Export information of completed tasks in Google Earth Engine to a CSV file.

    Args:
        output_file (str): Path to the output CSV file.

    Returns:
        None
    """
    try:
        # Get a list of all completed tasks
        completed_tasks = ee.batch.Task.list()

        # Extract information of completed tasks
        task_data = []
        for task in completed_tasks:
            task_status = ee.data.getTaskStatus(task.id)[0]
            task_info = {
                'state': task_status['state'],
                'description': task_status['description'],
                'priority': task_status['priority'],
                'creation_timestamp_ms': task_status['creation_timestamp_ms'],
                'update_timestamp_ms': task_status['update_timestamp_ms'],
                'start_timestamp_ms': task_status['start_timestamp_ms'],
                'task_type': task_status['task_type'],
                'destination_uris': task_status['destination_uris'],
                'attempt': task_status['attempt'],
                'batch_eecu_usage_seconds': task_status['batch_eecu_usage_seconds'],
                'id': task_status['id'],
                'name': task_status['name']
            }
            task_data.append(task_info)

        # Convert to DataFrame
        df = pd.DataFrame(task_data)

        # Export DataFrame to CSV
        df.to_csv(output_file, index=False)

        print(f"Information of completed tasks exported to '{output_file}'.")

    except Exception as e:
        print(f"Failed to export completed tasks to CSV: {str(e)}")


if __name__ == "__main__":
    # Path to the service account credentials JSON file
    credentials_file = r'C:\temp\topo-satromo\secrets\xxx'

    # Path to the output CSV file
    output_file = 'completed_tasks_selected_info.csv'

    # Initialize Google Earth Engine and Google Drive authentication
    if initialize_gee_and_drive(credentials_file):
        # Export information of completed tasks to CSV
        export_completed_tasks_to_csv(output_file)
