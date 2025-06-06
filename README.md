# XAUUSD Gold Price Historical Data Updater

This project automates the process of updating the XAUUSD (Gold) price historical data on Kaggle. It retrieves the latest data from MetaTrader, merges it with existing data, and uploads the updated dataset to Kaggle.

## Dataset

The dataset is available on Kaggle: [XAUUSD Gold Price Historical Data (2004-Present)](https://www.kaggle.com/datasets/novandraanugrah/xauusd-gold-price-historical-data-2004present)

## Requirements

- Python 3.8+
- Poetry for dependency management (or pip with requirements.txt)
- MetaTrader 4 with appropriate script for data export
- Kaggle API credentials
- X11 display server for GUI automation (for MetaTrader interaction)

## Setup

1. Install Python virtual environment if not already installed:
   ```bash
   sudo apt-get install python3.11-venv
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   # Using Poetry
   poetry install
   
   # OR using pip
   pip install pandas pyautogui kaggle python-dotenv
   ```

4. Create a `.env` file with your Kaggle credentials:
   ```
   KAGGLE_USERNAME=your_username
   KAGGLE_KEY=your_key
   ```

5. Set up Kaggle API credentials:
   ```bash
   mkdir -p ~/.kaggle
   echo '{"username":"your_username","key":"your_key"}' > ~/.kaggle/kaggle.json
   chmod 600 ~/.kaggle/kaggle.json
   ```

6. Install required system packages for GUI automation:
   ```bash
   sudo apt-get install python3-tk python3-dev xdotool
   ```

## Project Structure

```
kaggle_xau/
├── data/                  # Directory for downloaded data
├── new_data/              # Directory for new data from MT4
├── merged_data/           # Directory for merged data to be uploaded
├── upload_xau_to_kaggle.py # Main script
├── pyproject.toml         # Poetry configuration
├── .env                   # Environment variables (gitignored)
└── README.md              # This file
```

## Usage

Run the script to update the dataset:

```bash
python upload_xau_to_kaggle.py
```

## Automated Updates with Cron

The project is configured to run automatically on weekdays at 10:00 AM using cron:

```bash
# View current cron jobs
crontab -l

# Edit cron jobs
crontab -e

# Example cron job (runs at 10:00 AM on weekdays)
0 10 * * 1-5 cd /home/nvn/Project/kaggle_xau && source .venv/bin/activate && DISPLAY=:0 python upload_xau_to_kaggle.py >> /home/nvn/Project/kaggle_xau/kaggle_xau_upload.log 2>&1
```

## Dataset Metadata

The dataset includes the following metadata:
- Title: XAUUSD Gold Price Historical Data 2004–Present
- License: CC BY 4.0 (Creative Commons Attribution)
- Temporal Coverage: 2004 to Present
- Geospatial Coverage: Global (Gold is traded globally)
- Creator: Novandra Anugrah

## Troubleshooting

- If the script fails to interact with MetaTrader, ensure the DISPLAY environment variable is set correctly
- Check the log file at `kaggle_xau_upload.log` for detailed error messages
- Verify that MetaTrader 4 is running and accessible
