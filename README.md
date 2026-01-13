# XAUUSD Gold Price Historical Data Updater

This project automates the process of updating the XAUUSD (Gold) price historical data on Kaggle. It retrieves the latest 1-minute data from a PostgreSQL database, merges it with existing Kaggle data, and uploads the updated dataset back to Kaggle.

## Dataset

The dataset is available on Kaggle: [XAUUSD Gold Price Historical Data (2004-Present)](https://www.kaggle.com/datasets/novandraanugrah/xauusd-gold-price-historical-data-2004present)

## Requirements

- Python 3.8+
- PostgreSQL Database with `market.timeframe_1m` table
- Kaggle API credentials
- Dependencies listed in `requirements.txt`

## Setup

1. **Clone and Install Dependencies**
    ```bash
    git clone <your-repo-url> xauusd-1m-updater
    cd xauusd-1m-updater
    
    # Create virtual env
    # If missing venv: sudo apt install python3.12-venv
    python3 -m venv .venv
    source .venv/bin/activate
    
    # Install dependencies
    pip install -r requirements.txt
    ```

2. **Configuration**
    Copy `.env.example` to `.env` and fill in your details:
    ```bash
    cp .env.example .env
    nano .env
    ```
    
    Required Environment Variables:
    - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`
    - `POSTGRES_USER`, `POSTGRES_PASSWORD`
    - `KAGGLE_USERNAME`, `KAGGLE_KEY`

## Usage

Run the script manually:
```bash
python upload_xau_to_kaggle.py
```

## Scheduling (Cron)

To run this script automatically every day at 07:00 AM WIB (UTC+7), usage `crontab -e` and add the following line.

Note: 07:00 AM WIB is 00:00 UTC.

```cron
# Run daily at 00:00 UTC (07:00 WIB)
0 0 * * * cd /home/nvn/xauusd-1m-updater && /home/nvn/xauusd-1m-updater/.venv/bin/python3 upload_xau_to_kaggle.py >> /home/nvn/xauusd-1m-updater/kaggle_xau_upload.log 2>&1
```

*Adjust the paths (`/home/nvn/xauusd-1m-updater`) according to your actual installation directory on the server.*

## Troubleshooting

- **Database Connection**: Ensure the server IP (`100.100.20.1` or similar) is accessible and firewalls allow port 5432.
- **Kaggle API**: Ensure credentials are correct. If you see "403 Forbidden", double check your key.
- **Logs**: Check `kaggle_xau_upload.log` for detailed execution logs.
