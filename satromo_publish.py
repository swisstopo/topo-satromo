# -*- coding: utf-8 -*-
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
import csv
import json
import os
import ee
import configuration as config
from collections import OrderedDict
import subprocess
import glob

# Set the CPL_DEBUG environment variable to enable verbose output
os.environ["CPL_DEBUG"] = "ON"

def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    if os.path.exists(config.GDRIVE_SECRETS):
        run_type = 2
        print("\nType 2 run PUBLISHER: We are on DEV")
    else:
        run_type = 1
        print("\nType 1 run PUBLISHER: We are on PROD")


def initialize_gee_and_drive():
    """
    Initialize Google Earth Engine (GEE), RCLONE and Google Drive authentication.

    This function authenticates GEE and Google Drive either using a service account key file
    or GitHub secrets depending on the run type.

    Returns:
    None
    """

    scopes = ["https://www.googleapis.com/auth/drive"]

    if run_type == 2:
        # Initialize GEE and Google Drive using service account key file

        # Authenticate using the service account key file
        with open(config.GDRIVE_SECRETS, "r") as f:
            service_account_key = json.load(f)

        # Authenticate Google Drive
        gauth = GoogleAuth()
        gauth.service_account_file = config.GDRIVE_SECRETS
        gauth.service_account_email = service_account_key["client_email"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )

        rclone_config_file = config.RCLONE_SECRETS
        google_secret_file = config.GDRIVE_SECRETS
    else:
        # Initialize GEE and Google Drive using GitHub secrets

        # Authenticate using the provided secrets from GitHub Actions
        gauth = GoogleAuth()
        google_client_secret = json.loads(
            os.environ.get('GOOGLE_CLIENT_SECRET'))
        gauth.service_account_email = google_client_secret["client_email"]
        gauth.service_account_file = "keyfile.json"
        with open(gauth.service_account_file, "w") as f:
            f.write(json.dumps(google_client_secret))
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )

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

        # Write GDRIVE Secrest config to a file
        google_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
        google_secret_file = "keyfile.json"
        with open(google_secret_file, "w") as f:
            f.write(google_secret)
        
        #Create mountpoint GDRIVE
        command = ["mkdir", config.GDRIVE_MOUNT]
        print(command)
        result=subprocess.run(command, check=True)

        #GDRIVE Mount
        command = ["rclone", "mount", "--config", "rclone.conf", #"--allow-other",
                    os.path.join(config.GDRIVE_SOURCE),config.GDRIVE_MOUNT,"--vfs-cache-mode", "full"]

        print(command)
        subprocess.Popen(command)

    # Create the Google Drive client
    global drive
    drive = GoogleDrive(gauth)

    # Initialize EE
    credentials = ee.ServiceAccountCredentials(
        gauth.service_account_email, gauth.service_account_file
    )
    ee.Initialize(credentials)

    # Test EE initialization
    image = ee.Image("NASA/NASADEM_HGT/001")
    title = image.get("title").getInfo()
    if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
        print("GEE initialization successful")
    else:
        print("GEE initialization FAILED")


def download_and_delete_file(file):
    """
    DEV/local machine only Download a file from Google Drive and delete it afterwards.

    Parameters:
    file (GoogleDriveFile): Google Drive file object to download and delete.

    Returns:
    None
    """

    # Download the file to local machine
    file.GetContentFile(os.path.join(config.RESULTS, file["title"]))
    print(f"File {file['title']} downloaded.")

    # Delete the file
    file.Delete()
    print(f"File {file['title']} DELETED on Google Drive.")


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
    command = ["rclone", "move", "--config", "rclone.conf", "--s3-no-check-bucket",
               source, destination]
    subprocess.run(command, check=True)

    print("SUCCESS: moved " + source + " to " + destination)

def merge_files_with_gdal_translate(source):
    """
    Merge with GDAL 

    Parameters:
    source (str): Source filename .
        
    Returns:
    None
    """

    #check local disk disk space
    command = ["df","-h"]
    print(command)
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)
    
    # Get the list of all quadrant files matching the pattern
    file_list = sorted(glob.glob(os.path.join(config.GDRIVE_MOUNT, source+"*.tif")))

    # Write the file names to _list.txt
    with open(source+"_list.txt", "w") as file:
        file.writelines([f"{filename}\n" for filename in file_list])
    
    #run gdal vrt
    command = ["gdalbuildvrt",
                "-input_file_list", source+"_list.txt",source+".vrt",
                "--config", "GDAL_CACHEMAX", "9999",
                "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES",
                ]
    #print(command)
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)

    #run gdal translate
    command = ["gdal_translate",
                source+".vrt", source+".tif", # rename to source+"_merged.tif" when doing reprojection afterwards
                "-of", "COG",
                "-co", "NUM_THREADS=ALL_CPUS",
                #"-co", "COMPRESS=LZW",
                "-co", "BIGTIFF=YES",
                "--config", "GDAL_CACHEMAX", "9999",
                "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES",
               #added thsiconfig to test. otherwise uncomment it and as well add compress=lzw above
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=2",
                ]
    #print(command)
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)

    print("SUCCESS: merged " + source+".tif")
    return(source+".tif")

def merge_files_with_gdal_warp(source):
    """
    Merge with GDAL 

    Parameters:
    source (str): Source filename .
        
    Returns:
    None
    """

    #check local disk disk space
    command = ["df","-h"]
    print(command)
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)
    
    # Get the list of all quadrant files matching the pattern
    file_list = sorted(glob.glob(os.path.join(config.GDRIVE_MOUNT, source+"*.tif")))

    # Write the file names to _list.txt
    with open(source+"_list.txt", "w") as file:
        file.writelines([f"{filename}\n" for filename in file_list])
    
    #run gdal vrt
    command = ["gdalbuildvrt",
                "-input_file_list", source+"_list.txt",source+".vrt",
                "--config", "GDAL_CACHEMAX", "9999",
                "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES",
                ]
    #print(command)
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)

    #run gdal translate
    command = ["gdalwarp",
                source+".vrt", source+".tif", # rename to source+"_merged.tif" when doing reprojection afterwards
                "-of", "COG",
                #"-cutline", "ch_buffer_5000m.shp",
                -cutline", config.BUFFER,
                "-crop_to_cutline", "-dstnodata", "-9999",
                "-co", "NUM_THREADS=ALL_CPUS",
                "-co", "BIGTIFF=YES",
                "--config", "GDAL_CACHEMAX", "9999",
                "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES",
               #otherwise use compress=lzw 
               # https://kokoalberti.com/articles/geotiff-compression-optimization-guide/ and https://digital-geography.com/geotiff-compression-comparison/
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=2",
                ]
    print(command)
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)
    
    print("Standard Output:")
    print(result.stdout)

    print("Standard Error:")
    print(result.stderr)

    print("SUCCESS: merged " + source+".tif")
    return(source+".tif")

def reproject_with_gdal(source):
    """
    Merge with GDAL 

    Parameters:
    source (str): Source filename .
        
    Returns:
    Filename reprojected
    """
       

    #run gdalwarp
    command = ["gdalwarp",
                source+"_merged.tif", source+".tif",
                "-t_srs", config.OUTPUT_CRS,
                "-tr","10.0", "10.0",
                "-of", "COG",
                "-co", "NUM_THREADS=ALL_CPUS",
                #"-co", "COMPRESS=LZW",
                "--config", "GDAL_CACHEMAX", "9999",
                "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES",
                #added thsiconfig to test. otherwise uncomment it and as well add compress=lzw above
                # https://kokoalberti.com/articles/geotiff-compression-optimization-guide/ and https://digital-geography.com/geotiff-compression-comparison/
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=2"
                ]
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)
    print("SUCCESS: reprojected " + source+".tif")
    return(source+".tif")

def reprojectmerge_with_gdal(source):
    """
    reproject & merge with GDALwarp (no use to gdal_translate, might be slower

    Parameters:
    source (str): Source filename .
        
    Returns:
    Filename reprojected
    """
    
    # Get the list of all quadrant files matching the pattern
    file_list = sorted(glob.glob(os.path.join(config.GDRIVE_MOUNT, source+"*.tif")))

    # Write the file names to _list.txt
    with open(source+"_list.txt", "w") as file:
        file.writelines([f"{filename}\n" for filename in file_list])
    
    #run gdal vrt
    command = ["gdalbuildvrt",
                "-input_file_list", source+"_list.txt",source+".vrt",
                "--config", "GDAL_CACHEMAX", "9999",
                "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES",
                ]
    #print(command)
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)   

    #run gdalwarp
    command = ["gdalwarp",
                source+".vrt", source+".tif",
                "-t_srs", config.OUTPUT_CRS,
                "-tr","10.0", "10.0",
                "-r","near",
                "-of", "COG",
                "-co", "NUM_THREADS=ALL_CPUS",
                "-co", "BIGTIFF=YES",
                "- overwrite", 
                "--config", "GDAL_CACHEMAX", "9999",
                "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES",
                #added thsiconfig to test. otherwise uncomment it and as well add compress=lzw 
                # https://kokoalberti.com/articles/geotiff-compression-optimization-guide/ and https://digital-geography.com/geotiff-compression-comparison/
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=2"
                ]
    result=subprocess.run(command, check=True, capture_output=True, text=True)
    print(result)
    print("SUCCESS: merge & reprojected " + source+".tif")
    return(source+".tif")

def extract_value_from_csv(filename, search_string, search_col, col_result):
    try:
        with open(filename, "r") as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                if row[search_col] == search_string:
                    return row[col_result]
            
            print(f"Entry not found for '{search_string}' in column '{search_col}'")
    except FileNotFoundError:
        print("File not found.")
    
    return None

def clean_up_gdrive(filename):
    """
    Deletes files in Google Drive that match the given filename.Writes Metadata of processing results

    Args:
        filename (str): The name of the file to be deleted.

    Returns:
        None
    """
    #  Find the file in Google Drive by its name
    file_list = drive.ListFile({
        "q": "title contains '"+filename+"' and trashed=false"
        }).GetList()

    # Check if the file is found
    if len(file_list) > 0:


        
        # Iterate through the files and delete them    
        for file in file_list:
            

            #Get the current Task id
            file_on_drive=file['title']
            file_task_id=extract_value_from_csv(config.GEE_RUNNING_TASKS,file_on_drive.replace(".tif", ""),"Filename","Task ID")

            # Check task status
            file_task_status = ee.data.getTaskStatus(file_task_id)[0]

            # Get the product and item
            file_product, file_item = extract_product_and_item(
                file_task_status['description'])
        
            # Delete the file
            file.Delete()
            print(f"File {file['title']} DELETED on Google Drive.")

            # Add DATA GEE PROCESSING info to stats
            write_file(file_task_status, config.GEE_COMPLETED_TASKS)

            # Remove the line from the RUNNING tasks file
            delete_line_in_file(config.GEE_RUNNING_TASKS, file_task_id)

    
        # Add DATA GEE PROCESSING info to Metadata of item,
        write_file_meta(file_task_status, os.path.join(
            config.PROCESSING_DIR, item+".csv"))
        
        # Move Metadata of item to Destination Dir,
        move_files_with_rclone(os.path.join(
                                config.PROCESSING_DIR, item+".csv"), os.path.join(config.S3_DESTINATION, product))

        # Update Status in RUNNING tasks file
        replace_running_with_complete(
            config.LAST_PRODUCT_UPDATES, file_product)
    return

def write_file(input_dict, output_file):
    """
    Write a dictionary to a CSV file. If the file exists, the data is appended
    to it. If the file does not exist, a new file is created with a header.

    Parameters:
    input_dict (dict): Dictionary to be written to file.
    output_file (str): Path of the output file.

    Returns:
    None
    """
    append_or_write = "a" if os.path.isfile(output_file) else "w"
    with open(output_file, append_or_write, encoding="utf-8", newline='') as f:
        dict_writer = csv.DictWriter(f, fieldnames=list(input_dict.keys()),
                                     delimiter=",", quotechar='"',
                                     lineterminator="\n")
        if append_or_write == "w":
            dict_writer.writeheader()
        dict_writer.writerow(input_dict)
    return


def delete_line_in_file(filepath, stringtoremove):
    """
    Delete lines containing a specific string from a file.

    Parameters:
    filepath (str): Path of the file to modify.
    stringtoremove (str): String to search for and remove from the file.

    Returns:
    None
    """
    with open(filepath, "r+") as file:
        lines = file.readlines()
        file.seek(0)
        file.truncate()
        for line in lines:
            if stringtoremove not in line.strip() and line.strip():
                file.write(line)
            elif not line.strip():
                file.write("\n")


def write_file_meta(input_dict, output_file):
    """
    Read the existing CSV file, append the input dictionary, and export it as a new CSV file.

    Parameters:
    input_dict (dict): Dictionary to be appended to the CSV file.
    output_file (str): Path of the output CSV file.

    Returns:
    None
    """
    existing_data = OrderedDict()
    if os.path.isfile(output_file):
        with open(output_file, "r", encoding="utf-8", newline='') as f:
            reader = csv.reader(f)
            existing_data = OrderedDict(zip(next(reader), next(reader)))

    existing_data.update(input_dict)

    with open(output_file, "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter=",", quotechar='"',
                            lineterminator="\n")

        writer.writerow(list(existing_data.keys()))
        writer.writerow(list(existing_data.values()))


def extract_product_and_item(task_description):
    """
    Extract the product and item information from a task description.

    Parameters:
    task_description (str): Description of the task containing product and item information.

    Returns:
    tuple: A tuple containing the extracted product and item information.
    """
    product_start_index = task_description.index('P:') + 2
    product_end_index = task_description.index(' I:')
    product = task_description[product_start_index:product_end_index]

    item_start_index = task_description.index('I:') + 2
    item = task_description[item_start_index:]

    return product, item


def replace_running_with_complete(input_file, item):
    """
    Replace 'RUNNING' with 'complete' in the specific item line of an input file.

    Parameters:
    input_file (str): Path to the input file.
    item (str): Item to identify the line to be modified.

    Returns:
    None
    """
    output_lines = []
    with open(input_file, 'r') as f:
        for line in f:
            if line.startswith(item):
                line = line.replace('RUNNING', 'complete')
            output_lines.append(line)

    with open(input_file, 'w') as f:
        f.writelines(output_lines)


if __name__ == "__main__":

    # Test if we are on Local DEV Run or if we are on PROD
    determine_run_type()

    # Authenticate with GEE and GDRIVE
    initialize_gee_and_drive()

    #empty temp files on GDrive
    file_list = drive.ListFile({'q': "trashed=true"}).GetList()
    for file in file_list:
        file.Delete()
        print('GDRIVE TRASH: Deleted file: %s' % file['title'])  

    # Read the status file
    with open(config.GEE_RUNNING_TASKS, "r") as f:
        lines = f.readlines()

    #Get the unique filename
    unique_filenames = set()

    for line in lines[1:]:  # Start from the second line
        _, filename = line.strip().split(',')
        filename = filename.split('quadrant')[0]  # Take the part before "quadrant"
        unique_filenames.add(filename.strip())

    unique_filenames=list(unique_filenames)
    
    # Check  if each quandrant is complete then process
    # Iterate over unique filenames
    for filename in unique_filenames:
        
        # Keep track of completion status
        all_completed = True
        for quadrant_num in range(1, 5): #You need to change this if we have more than 4 quadrants
            # Construct the filename with the quadrant
            full_filename = filename + "quadrant" + str(quadrant_num)

            # Find the corresponding task ID in the lines list
            task_id = None
            for line in lines[1:]:
                if full_filename in line:
                    task_id = line.strip().split(",")[0]
                    break

            if task_id:
                # Check task status
                task_status = ee.data.getTaskStatus(task_id)[0]

            if task_status["state"] != "COMPLETED":
                # Task is not completed
                all_completed = False
                print(f"{full_filename} - {task_status['state']}")

        # Check overall completion status
        if all_completed:
            if run_type == 2:
                # local machine run
                # Download DATA
                breakpoint()
                download_and_delete_file(filename)
            else:
                print(filename+" is ready to process")
                
                # Get the product and item
                product, item = extract_product_and_item(
                    task_status['description'])
        
                #merge files
                file_merged = merge_files_with_gdal_warp(filename)

                #reproject files: not needed, since we will prohject on export in satromo_processer.py on export
                #file_reprojected=reproject_with_gdal(filename)

                #move file to Destination: in case reproejction si done here: move file_reprojected
                move_files_with_rclone(
                                file_merged, os.path.join(config.S3_DESTINATION, product))
                
                #clean up GDrive and local drive
                #os.remove(file_merged)
                clean_up_gdrive(filename)
   
        else: 
            print(filename+" is NOT ready to process")       
   

    # Last step
    if run_type == 1:
        # Remove the key file so It wont be commited
        os.remove("keyfile.json")
        os.remove("rclone.conf")
    #empty temp files on GDrive
    file_list = drive.ListFile({'q': "trashed=true"}).GetList()
    for file in file_list:
        file.Delete()
        print('GDRIVE TRASH: Deleted file: %s' % file['title'])    
    print("PUBLISH Process done.")
