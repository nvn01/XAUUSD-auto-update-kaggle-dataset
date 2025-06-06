#!/usr/bin/env python3
import os
import sys
import time
import json
import logging
import traceback
import pandas as pd
import pyautogui
import subprocess
import shutil
from datetime import datetime
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi

# Load environment variables
load_dotenv()

# Kaggle credentials should be in .env file or environment variables
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

# Base directory of the script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# MetaTrader MQL4/Files directory where the exported files are saved
# This is the default location where MT4 saves files
MT4_FILES_DIR = r"C:\users\nvn\AppData\Roaming\MetaQuotes\Terminal\50CA3DFB510CC5A8F28B48D1BF2A5702\MQL4\Files"

# Project directories - use paths relative to the script location
DATA_FOLDER = os.path.join(BASE_DIR, "data")
NEW_DATA_FOLDER = os.path.join(BASE_DIR, "new_data")
MERGED_FOLDER = os.path.join(BASE_DIR, "merged_data")

# Kaggle dataset information
DATASET_SLUG = "novandraanugrah/xauusd-gold-price-historical-data-2004present"

def clean_folder(folder_path):
    """Clean the specified folder."""
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        logging.info(f"Cleaned folder: {folder_path}")
    else:
        os.makedirs(folder_path)
        logging.info(f"Created folder: {folder_path}")

def trigger_mt4_export():
    """Trigger the MT4 script by sending Ctrl+1 hotkey and pressing Enter to confirm"""
    try:
        # Check if DISPLAY is set
        if 'DISPLAY' not in os.environ:
            logging.warning("DISPLAY environment variable not set. Setting to :0")
            os.environ['DISPLAY'] = ':0'
        
        # Try different window names for MetaTrader
        window_names = ['MetaTrader', 'MetaTrader 4', 'MetaTrader4', 'MT4']
        window_found = False
        
        for name in window_names:
            try:
                logging.info(f"Searching for window with name: {name}...")
                result = subprocess.run(
                    ['xdotool', 'search', '--name', name], 
                    check=False,
                    capture_output=True,
                    text=True
                )
                
                if result.returncode == 0 and result.stdout.strip():
                    window_id = result.stdout.strip()
                    logging.info(f"Found window with ID: {window_id}")
                    subprocess.run(['xdotool', 'windowactivate', window_id], check=True)
                    window_found = True
                    break
            except Exception as e:
                logging.warning(f"Failed to find window {name}: {e}")
                continue
        
        if not window_found:
            logging.warning("Could not find MetaTrader window. Trying to send hotkey anyway...")
        
        # Wait for window to be in focus
        time.sleep(2)
        
        # Send Ctrl+1 hotkey to trigger the script
        logging.info("Sending Ctrl+1 hotkey...")
        pyautogui.hotkey('ctrl', '1')
        logging.info("Sent Ctrl+1 hotkey to MetaTrader")
        
        # Wait for the script dialog to appear
        time.sleep(3)
        
        # Press Enter to confirm the script execution
        logging.info("Pressing Enter to confirm script execution...")
        pyautogui.press('return')
        
        # Wait for the script to complete
        logging.info("Waiting for script execution to complete...")
        time.sleep(15)  # Increased wait time to ensure script completes
        return True
    except Exception as e:
        logging.error(f"Error triggering MT4 export: {e}")
        logging.error(traceback.format_exc())
        return False

def copy_files_from_mt4():
    """Copy the exported files from MT4 Files directory to our project folder"""
    try:
        # Create the new data folder if it doesn't exist
        os.makedirs(NEW_DATA_FOLDER, exist_ok=True)
        
        # Files to copy
        files_to_copy = ['XAU_1d_data.csv', 'XAU_1h_data.csv']
        
        for file in files_to_copy:
            source_path = os.path.join(MT4_FILES_DIR, file)
            dest_path = os.path.join(NEW_DATA_FOLDER, file)
            
            # Convert Windows path to Linux path if running on Linux
            if os.name != 'nt':  # If not Windows
                source_path = source_path.replace('\\', '/')
            
            # Check if source file exists
            if os.path.exists(source_path):
                shutil.copy2(source_path, dest_path)
                logging.info(f"Copied {file} from MT4 Files directory to {dest_path}")
            else:
                logging.warning(f"{file} not found in MT4 Files directory")
                
        return True
    except Exception as e:
        logging.error(f"Error copying files from MT4: {e}")
        return False

def download_kaggle_dataset():
    """Download the dataset and metadata from Kaggle."""
    logging.info("Downloading existing dataset from Kaggle...")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET_SLUG, path=DATA_FOLDER, unzip=True)
    api.dataset_metadata(DATASET_SLUG, path=DATA_FOLDER)
    logging.info(f"Dataset and metadata downloaded to {DATA_FOLDER}")

def merge_datasets(existing_file, new_file, output_file):
    """Merge existing and new datasets."""
    logging.info(f"Merging {os.path.basename(existing_file)} with {os.path.basename(new_file)}...")
    
    # Read the files
    existing_data = pd.read_csv(existing_file)
    new_data = pd.read_csv(new_file)
    
    # Convert date columns to datetime
    date_column = "Date" if "Date" in existing_data.columns else "Open time"
    
    if date_column in existing_data.columns:
        existing_data[date_column] = pd.to_datetime(existing_data[date_column])
    if date_column in new_data.columns:
        new_data[date_column] = pd.to_datetime(new_data[date_column])
    
    # Concatenate and remove duplicates
    merged_data = pd.concat([existing_data, new_data])
    merged_data.drop_duplicates(subset=date_column, inplace=True)
    merged_data.sort_values(by=date_column, inplace=True)
    
    # Save merged data
    merged_data.to_csv(output_file, index=False)
    logging.info(f"Merged dataset saved to {output_file}")

def copy_metadata(src_folder, dest_folder):
    """Copy the metadata file to the destination folder and ensure it's properly formatted."""
    metadata_file = os.path.join(src_folder, "dataset-metadata.json")
    dest_file = os.path.join(dest_folder, "dataset-metadata.json")
    
    if os.path.exists(metadata_file):
        try:
            # Read the metadata file content
            with open(metadata_file, 'r') as f:
                content = f.read()
            
            # Try to parse the content as JSON
            try:
                # First try to parse it directly
                metadata = json.loads(content)
            except json.JSONDecodeError:
                # If that fails, it might be a string representation of JSON
                # Remove the outer quotes and try again
                if content.startswith('"') and content.endswith('"'):
                    content = content[1:-1].replace('\\"', '\"')
                    metadata = json.loads(content)
                else:
                    raise
            
            # Ensure the metadata has the correct ID field
            if 'id' not in metadata and 'datasetSlug' in metadata:
                metadata['id'] = f"{metadata.get('ownerUser', 'novandraanugrah')}/{metadata['datasetSlug']}"
            
            # Write the properly formatted metadata to the destination folder
            with open(dest_file, 'w') as f:
                json.dump(metadata, f)
                
            logging.info(f"Copied and reformatted metadata file from {metadata_file} to {dest_folder}")
        except Exception as e:
            logging.error(f"Error processing metadata file: {e}")
            logging.warning("Creating a new metadata file with basic information")
            create_basic_metadata(dest_file)
    else:
        logging.warning(f"Metadata file not found in {src_folder}. Creating a new one.")
        create_basic_metadata(dest_file)

def create_basic_metadata(dest_file):
    """Create a basic metadata file with the required fields."""
    metadata = {
        "id": DATASET_SLUG,
        "title": "XAUUSD Gold Price Historical Data 2004–Present",
        "licenses": [{"name": "unknown"}]
    }
    with open(dest_file, 'w') as f:
        json.dump(metadata, f)
    logging.info(f"Created new metadata file at {dest_file}")

def upload_to_kaggle(version_notes, max_retries=3):
    """Upload the updated dataset to Kaggle with retry logic."""
    logging.info("Uploading dataset to Kaggle...")
    
    # Verify metadata file exists
    metadata_file = os.path.join(MERGED_FOLDER, "dataset-metadata.json")
    if not os.path.exists(metadata_file):
        logging.error(f"Metadata file not found at {metadata_file}. Cannot upload without metadata.")
        return False
    
    # Retry loop for upload
    for attempt in range(max_retries):
        try:
            # Temporarily disable any proxy settings if they exist
            original_http_proxy = os.environ.pop("HTTP_PROXY", None)
            original_https_proxy = os.environ.pop("HTTPS_PROXY", None)
            
            api = KaggleApi()
            api.authenticate()
            
            # Log the metadata file content for debugging
            with open(metadata_file, 'r') as f:
                logging.info(f"Metadata content: {f.read()}")
            
            api.dataset_create_version(
                folder=MERGED_FOLDER,
                version_notes=version_notes,
                dir_mode=True
            )
            
            # Restore proxy settings if they existed
            if original_http_proxy:
                os.environ["HTTP_PROXY"] = original_http_proxy
            if original_https_proxy:
                os.environ["HTTPS_PROXY"] = original_https_proxy
                
            logging.info("Dataset successfully updated on Kaggle.")
            return True
        except Exception as e:
            # Restore proxy settings if they existed
            if original_http_proxy:
                os.environ["HTTP_PROXY"] = original_http_proxy
            if original_https_proxy:
                os.environ["HTTPS_PROXY"] = original_https_proxy
                
            logging.error(f"Upload attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = 30 * (attempt + 1)  # Increasing wait time with each retry
                logging.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                logging.error("Maximum retry attempts reached. Upload failed.")
                logging.error(traceback.format_exc())
                return False
    
    return False

def main():
    """Main function to run the workflow"""
    logging.info(f"\n=== Starting XAUUSD data export and Kaggle upload at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    # Create necessary directories
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(NEW_DATA_FOLDER, exist_ok=True)
    os.makedirs(MERGED_FOLDER, exist_ok=True)
    
    try:
        # Step 1: Download existing dataset from Kaggle
        logging.info("\nStep 1: Downloading existing dataset from Kaggle...")
        try:
            download_kaggle_dataset()
            logging.info("Kaggle dataset downloaded successfully.")
        except Exception as e:
            logging.error(f"Failed to download Kaggle dataset: {e}")
            logging.warning("Continuing with the process using local data if available.")
        
        # Step 2: Trigger MT4 to export the data
        logging.info("\nStep 2: Triggering MT4 to export XAUUSD data...")
        mt4_export_success = trigger_mt4_export()
        if not mt4_export_success:
            logging.warning("Failed to trigger MT4 export. Will try to continue with existing data.")
        else:
            logging.info("MT4 export completed successfully.")
            
        # Step 3: Copy files from MT4 Files directory to our project folder
        logging.info("\nStep 3: Copying files from MT4 Files directory...")
        mt4_copy_success = copy_files_from_mt4()
        if not mt4_copy_success:
            logging.warning("Failed to copy files from MT4. Will try to continue with existing data.")
        else:
            logging.info("Files copied successfully.")
        
        # Check if we have any data to work with
        has_existing_data = any(os.path.exists(os.path.join(DATA_FOLDER, f"XAU_{tf}_data.csv")) for tf in ["1d", "1h"])
        has_new_data = any(os.path.exists(os.path.join(NEW_DATA_FOLDER, f"XAU_{tf}_data.csv")) for tf in ["1d", "1h"])
        
        if not has_existing_data and not has_new_data:
            logging.error("No data available to process. Exiting.")
            return
        
        # Step 4: Merge datasets
        logging.info("\nStep 4: Merging datasets...")
        timeframes = ["1d", "1h"]
        merged_files_count = 0
        
        for tf in timeframes:
            # Existing dataset from Kaggle
            existing_file = os.path.join(DATA_FOLDER, f"XAU_{tf}_data.csv")
            # New data from MT4
            new_file = os.path.join(NEW_DATA_FOLDER, f"XAU_{tf}_data.csv")
            # Output merged file
            merged_file = os.path.join(MERGED_FOLDER, f"XAU_{tf}_data.csv")
            
            if os.path.exists(existing_file) and os.path.exists(new_file):
                try:
                    merge_datasets(existing_file, new_file, merged_file)
                    merged_files_count += 1
                except Exception as e:
                    logging.error(f"Error merging {tf} datasets: {e}")
            elif os.path.exists(new_file):
                # If no existing file exists, just copy the new file
                shutil.copy(new_file, merged_file)
                logging.info(f"Copied {os.path.basename(new_file)} to merged folder (no existing file to merge)")
                merged_files_count += 1
            elif os.path.exists(existing_file):
                # If no new file exists, just copy the existing file
                shutil.copy(existing_file, merged_file)
                logging.info(f"Copied {os.path.basename(existing_file)} to merged folder (no new file to merge)")
                merged_files_count += 1
        
        if merged_files_count == 0:
            logging.error("No files were merged or copied to the merged folder. Exiting.")
            return
        
        # Step 5: Copy metadata file
        logging.info("\nStep 5: Copying metadata file...")
        try:
            copy_metadata(DATA_FOLDER, MERGED_FOLDER)
        except Exception as e:
            logging.warning(f"Failed to copy metadata: {e}")
            logging.warning("Continuing without metadata...")
        
        # Step 6: Upload to Kaggle
        logging.info("\nStep 6: Uploading updated dataset to Kaggle...")
        version_notes = f"Updated with data as of {datetime.now().strftime('%Y-%m-%d')}"
        try:
            if upload_to_kaggle(version_notes):
                logging.info("Dataset successfully updated on Kaggle!")
            else:
                logging.error("Failed to update dataset on Kaggle.")
        except Exception as e:
            logging.error(f"Error uploading to Kaggle: {e}")
            logging.error(traceback.format_exc())
        
        logging.info("Process completed!")
            
    except Exception as e:
        logging.error(f"An error occurred during execution: {e}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    try:
        # Set up logging to file
        log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kaggle_xau_upload.log")
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add console handler to see logs in terminal
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger('').addHandler(console)
        
        logging.info("Starting XAUUSD data export and Kaggle upload script")
        main()
        logging.info("Script completed successfully")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        logging.error(traceback.format_exc())
        print(f"Error: {e}")
        sys.exit(1)
