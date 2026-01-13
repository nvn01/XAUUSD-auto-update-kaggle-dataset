#!/usr/bin/env python3
import os
import sys
import time
import json
import logging
import traceback
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_kaggle_config():
    """
    Manually create kaggle.json if it doesn't exist, using credentials from .env.
    This resolves issues where the library fails to pick up environment variables.
    """
    username = os.getenv("KAGGLE_USERNAME")
    # Support both old KAGGLE_KEY and new KAGGLE_API_TOKEN
    key = os.getenv("KAGGLE_KEY") or os.getenv("KAGGLE_API_TOKEN")
    
    if not username or not key:
        logging.warning("KAGGLE_USERNAME or KAGGLE_KEY/KAGGLE_API_TOKEN not found in .env. Auth might fail.")
        return

    # Standard configuration paths
    # Linux: ~/.config/kaggle/kaggle.json or ~/.kaggle/kaggle.json
    # We will try to match what the error message suggested: /root/.config/kaggle
    
    home = os.path.expanduser("~")
    config_dir = os.path.join(home, ".config", "kaggle")
    config_file = os.path.join(config_dir, "kaggle.json")
    
    # Also create in legacy path just in case
    legacy_dir = os.path.join(home, ".kaggle")
    legacy_file = os.path.join(legacy_dir, "kaggle.json")

    # If neither exists, create one
    if not os.path.exists(config_file) and not os.path.exists(legacy_file):
        try:
            logging.info(f"Creating Kaggle config at {config_file}...")
            os.makedirs(config_dir, exist_ok=True)
            with open(config_file, 'w') as f:
                json.dump({"username": username, "key": key}, f)
            # Set permissions to 600 (required by some versions)
            os.chmod(config_file, 0o600)
            
            # Also set env vars just to be double sure
            os.environ["KAGGLE_USERNAME"] = username
            os.environ["KAGGLE_KEY"] = key
        except Exception as e:
            logging.warning(f"Failed to create config file: {e}")

# Run setup
setup_kaggle_config()

from kaggle.api.kaggle_api_extended import KaggleApi

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

DATASET_SLUG = "novandraanugrah/xauusd-gold-price-historical-data-2004present"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(BASE_DIR, "data")
MERGED_FOLDER = os.path.join(BASE_DIR, "merged_data")

# Setup Logging
log_file = os.path.join(BASE_DIR, "kaggle_xau_upload.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def fetch_new_data(last_date):
    """
    Fetch new 1-minute data from the database starting after last_date.
    Returns a DataFrame formatted for Kaggle.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Find instrument_id for XAUUSD (or Gold)
        cursor.execute("SELECT id FROM market.instruments WHERE symbol = 'XAUUSD'")
        res = cursor.fetchone()
        if not res:
            logging.error("Instrument XAUUSD not found in database.")
            return None
        instrument_id = res[0]

        logging.info(f"Fetching data for instrument_id: {instrument_id} after {last_date}")

        # Query 1m data
        query = """
            SELECT ts, open, high, low, close, volume 
            FROM market.timeframe_1m 
            WHERE instrument_id = %s AND ts > %s
            ORDER BY ts ASC
        """
        cursor.execute(query, (instrument_id, last_date))
        rows = cursor.fetchall()
        
        conn.close()

        if not rows:
            logging.info("No new data found in database.")
            return pd.DataFrame()

        # Create DataFrame
        df = pd.DataFrame(rows, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        
        # Format columns key to match Kaggle dataset expectations (assuming Date, Open, High, Low, Close, Volume)
        # Note: The Kaggle dataset likely expects 'Date' or 'Local time' format.
        # Based on user context, we will standardize to 'Date'
        df.rename(columns={'ts': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
        
        # Ensure Date is timezone-naive or formatted correctly if needed.
        # Assuming Kaggle uses a specific format, but pandas to_csv handles iso mostly fine.
        # Check if original was timezone aware.
        # If the DB returns timezone-aware datetimes, we might want to keep them or convert.
        # For simplicity, we keep as is, but Ensure it matches previous conventions if possible.
        
        return df

    except Exception as e:
        logging.error(f"Error fetching data from DB: {e}")
        logging.error(traceback.format_exc())
        return None

def download_kaggle_dataset():
    """Download the dataset and metadata from Kaggle."""
    logging.info("Downloading existing dataset from Kaggle...")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET_SLUG, path=DATA_FOLDER, unzip=True)
    
    # We also need metadata to upload back
    # api.dataset_metadata(DATASET_SLUG, path=DATA_FOLDER) # This saves dataset-metadata.json
    # Note: dataset_metadata method downloads to specific file usually, let's verify if needed or we recreate.
    # Actually dataset_create_version needs it.
    
    # Workaround: manually create or download metadata if not present by default unzip
    # But usually we just need to preserve it.
    # Let's ensure we have metadata.
    if not os.path.exists(os.path.join(DATA_FOLDER, 'dataset-metadata.json')):
         # Try to get it
         try:
             # This call might be slightly different in various versions, but standard is:
             # api.dataset_metadata(DATASET_SLUG, path=DATA_FOLDER)
             # If that fails, we can construct one.
             subprocess.run(['kaggle', 'datasets', 'metadata', DATASET_SLUG, '-p', DATA_FOLDER], check=False)
         except:
             pass

    logging.info(f"Dataset downloaded to {DATA_FOLDER}")

def merge_and_save(original_file, new_df, output_file):
    """
    Merge original CSV with new DataFrame and save to output.
    """
    if os.path.exists(original_file):
        logging.info(f"Reading original file: {original_file}")
        # Try reading with semicolon delimiter
        try:
             # Check if file has headers or not. The error suggests it might be reading the first line as data or the header is combined
             # "Date;Open;High;Low;Close;Volume"
             orig_df = pd.read_csv(original_file, sep=';')
        except Exception as e:
             logging.warning(f"Failed to read with semicolon sep: {e}. Trying default.")
             orig_df = pd.read_csv(original_file)
        
        # Detect Date column
        date_col = 'Date'
        # Check if index 0 is 'Date' by any chance or if columns like 'Date' exist
        if 'Date' not in orig_df.columns:
             # Fallback: maybe the header is missing or different case?
             # Based on error: Index(['Date;Open;High;Low;Close;Volume'], dtype='object')
             # It seems if sep is wrong it loads everything into one column.
             # If we fixed sep, we should see columns.
             if len(orig_df.columns) == 1:
                  # Force semicolon again if read_csv failed to detect
                  logging.info("Only 1 column detected. Forcing semicolon split.")
                  orig_df = pd.read_csv(original_file, sep=';')

        if date_col not in orig_df.columns and 'Open time' in orig_df.columns:
            date_col = 'Open time'
            
        logging.info(f"Columns found: {orig_df.columns}")
            
        # Parse dates with specific format if standard fails
        # Format seen in error: 2004.06.11 07:18
        try:
            orig_df[date_col] = pd.to_datetime(orig_df[date_col], format='%Y.%m.%d %H:%M')
        except:
             logging.warning("Standard format failed, trying auto-parse")
             orig_df[date_col] = pd.to_datetime(orig_df[date_col])
             
        # Ensure new_df matches format for merging (which is CSV write default)
        new_df['Date'] = pd.to_datetime(new_df['Date'])
        
        # Rename new_df date col if needed to match original
        if date_col != 'Date':
             new_df.rename(columns={'Date': date_col}, inplace=True)
        
        # Get last date from original
        if not orig_df.empty:
            last_date = orig_df[date_col].max()
            logging.info(f"Last date in existing dataset: {last_date}")
            
            # Filter new data to be strictly after last data
            # Ensure timezones match for comparison
            if new_df['Date'].dt.tz is not None and orig_df[date_col].dt.tz is None:
                 new_df['Date'] = new_df['Date'].dt.tz_localize(None)
            elif new_df['Date'].dt.tz is None and orig_df[date_col].dt.tz is not None:
                 orig_df[date_col] = orig_df[date_col].dt.tz_localize(None) # Or localize new

            new_rows = new_df[new_df[date_col] > last_date]
            
            if new_rows.empty:
                logging.info("No new rows to add after merging.")
                # We still might want to copy strictly to output if we want to ensure latest file is there
                # But logical to just copy original
                full_df = orig_df
            else:
                logging.info(f"Appending {len(new_rows)} new rows.")
                full_df = pd.concat([orig_df, new_rows])
        else:
            full_df = new_df
    else:
        full_df = new_df
        date_col = 'Date'

    # Deduplicate and sort
    full_df.drop_duplicates(subset=date_col, keep='last', inplace=True)
    full_df.sort_values(by=date_col, inplace=True)
    
    # Save
    full_df.to_csv(output_file, index=False)
    logging.info(f"Saved merged dataset to {output_file}. Total rows: {len(full_df)}")

def setup_metadata(source_folder, dest_folder):
    """Ensure proper metadata file exists in destination."""
    meta_src = os.path.join(source_folder, "dataset-metadata.json")
    meta_dest = os.path.join(dest_folder, "dataset-metadata.json")
    
    if os.path.exists(meta_src):
        # Read and potentially fix ID if needed, or just copy
        with open(meta_src, 'r') as f:
            data = json.load(f)
        
        # Ensure ID matches our slug
        owner, slug = DATASET_SLUG.split('/')
        if data.get('id') != DATASET_SLUG:
            data['id'] = DATASET_SLUG
            
        with open(meta_dest, 'w') as f:
            json.dump(data, f, indent=4)
    else:
        # Create basic
        data = {
            "title": "XAUUSD Gold Price Historical Data 2004–Present",
            "id": DATASET_SLUG,
            "licenses": [{"name": "CC0-1.0"}] 
        }
        with open(meta_dest, 'w') as f:
            json.dump(data, f, indent=4)

def main():
    logging.info("=== Starting XAUUSD Auto Updater (PostgreSQL Version) ===")
    
    # 1. Prepare Folders
    if os.path.exists(MERGED_FOLDER):
        import shutil
        shutil.rmtree(MERGED_FOLDER)
    os.makedirs(MERGED_FOLDER)
    os.makedirs(DATA_FOLDER, exist_ok=True)
    
    # 2. Download from Kaggle
    try:
        download_kaggle_dataset()
    except Exception as e:
        logging.error(f"Kaggle download failed: {e}")
        # If download fails, we can't really proceed unless we have local backup?
        # User said "download existing dataset... check the gap... merge". 
        # So we must proceed only if we have data.
        if not os.listdir(DATA_FOLDER):
             logging.error("No local data found. Exiting.")
             return

    # 3. Process each relevant file
    # User mentioned only "1 minute timeframe" is relevant now.
    # Kaggle dataset likely has XAU_1m_data.csv. Let's look for it.
    
    target_file_name = "XAU_1m_data.csv"
    local_file_path = os.path.join(DATA_FOLDER, target_file_name)
    
    if not os.path.exists(local_file_path):
        logging.warning(f"{target_file_name} not found in downloaded data. Checking for other files...")
        # Fallback or check what files exist
        files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]
        if not files:
            logging.error("No CSV files found.")
            return
        # Just pick the first one or logic? User said "XAU_1m_data.csv" in text implies 1m.
        # But previous code had 1d and 1h. User said "change the timeframe on kaggle all just in one minutes".
        # So we assume the main file is now 1m.
        if len(files) == 1:
            target_file_name = files[0]
            local_file_path = os.path.join(DATA_FOLDER, target_file_name)
        else:
             # Try to match '1m'
             matches = [f for f in files if '1m' in f]
             if matches:
                 target_file_name = matches[0]
                 local_file_path = os.path.join(DATA_FOLDER, target_file_name)
             else:
                 target_file_name = files[0] # Blind guess
                 local_file_path = os.path.join(DATA_FOLDER, target_file_name)

    logging.info(f"Targeting file: {target_file_name}")

    # 4. Determine missing range
    # Get last date from CSV
    try:
        if os.path.exists(local_file_path):
            try:
                df_existing = pd.read_csv(local_file_path, sep=';')
            except:
                df_existing = pd.read_csv(local_file_path)
            
            date_col = 'Date'
            if date_col not in df_existing.columns:
                 # Check if headers are messed up or another name
                 if len(df_existing.columns) == 1:
                      # Try force reading again if comma failed previously (though we tried semi first)
                      pass
                 elif 'Open time' in df_existing.columns:
                      date_col = 'Open time'
            
            # Parse with explicit format first
            try:
                df_existing[date_col] = pd.to_datetime(df_existing[date_col], format='%Y.%m.%d %H:%M')
            except:
                df_existing[date_col] = pd.to_datetime(df_existing[date_col])
                
            last_date = df_existing[date_col].max()
        else:
            last_date = datetime(2000, 1, 1)
    except Exception as e:
        logging.error(f"Error reading existing CSV: {e}")
        last_date = datetime(2000, 1, 1) # Default far past

    # If last_data is default (2000), it means we failed to parse or file is empty.
    # In this case, we shouldn't try to fetch EVERYTHING from the DB unless explicitly wanted.
    # But for now, let's respect the last_date. 
    
    # MEMORY OPTIMIZATION:
    # Instead of fetching everything, let's verify if we need to.
    logging.info(f"Last detected date: {last_date}")
    
    # 5. Fetch from DB
    new_data = fetch_new_data(last_date)
    
    # 6. Merge
    output_path = os.path.join(MERGED_FOLDER, target_file_name)
    has_updates = False
    
    if new_data is not None and not new_data.empty:
        # Optimization: Only load original if we actually have new data to merge
        # And we already loaded it partially to check date.
        # To avoid reading whole file again if it's huge, we can append if we are sure.
        # But for safety, let's read/concat.
        merge_and_save(local_file_path, new_data, output_path)
        has_updates = True
    else:
        logging.info("No new data to merge. Skipping upload.")
        return

    # 7. Metadata
    setup_metadata(DATA_FOLDER, MERGED_FOLDER)

    # 8. Upload
    if has_updates:
        try:
            api = KaggleApi()
            api.authenticate()
            version_notes = f"Auto-update: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            # 401 Unauthorized usually means Key/User is wrong OR the token is expired/invalid.
            # It can also happen if we try to upload to a dataset we don't own (slug mismatch).
            # Double check slug: novandraanugrah/xauusd-gold-price-historical-data-2004present
            
            logging.info(f"Uploading to {DATASET_SLUG}...")
            api.dataset_create_version(
                folder=MERGED_FOLDER,
                version_notes=version_notes,
                dir_mode=True
            )
            logging.info("Upload initiated successfully.")
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            # If 401, hint user
            if "401" in str(e):
                 logging.error("Check KAGGLE_USERNAME and KAGGLE_KEY in .env. Ensure they match your account.")
    
if __name__ == "__main__":
    main()
