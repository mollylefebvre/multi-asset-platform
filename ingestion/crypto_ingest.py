import logging
import requests
import json
import time
from datetime import datetime
from google.cloud import storage
import gzip

#---------------------------
# CONFIG
#---------------------------
BUCKET_NAME = "ma-marketdata-dl-usc1-prod-7f3a9c2h"

URL = "https://api.coingecko.com/api/v3/coins/markets"

#---------------------------
# COIN LIST
#---------------------------
COIN_IDS = [
    'bitcoin',              # BTC
    'ethereum',             # ETH
    'solana',               # SOL
    'cardano',              # ADA
    'avalanche-2',          # AVAX (CoinGecko ID)
    'polkadot',             # DOT
    'chainlink',            # LINK
    'polygon',              # MATIC
    'uniswap',              # UNI
    'aave',                 # AAVE
    'tether',               # USDT
    'usd-coin',             # USDC
    'dogecoin',             # DOGE
    'toncoin',              # TON
    'render-token'          # RENDER
]

#---------------------------
# LOGGING SETUP
#---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#---------------------------
# STEP 1: FETCH DATA
#---------------------------
MAX_RETRIES = 5
INITIAL_DELAY = 1   # seconds
TIMEOUT = (3,10)    # (connect, read)

def fetch_crypto_data():
    params = {
        'vs_currency': 'usd',
        'ids':','.join(COIN_IDS),
        'order':'market_cap_desc',
        'sparkline': False
    }

    delay = INITIAL_DELAY

    for attempt in range(1, MAX_RETRIES+1):
        try:
            logging.info(f'Attempt {attempt} - Fetching Crypto data...')
            response = requests.get(URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            logging.info('Data fetched successfully')
            return response.json()
        #------------------------
        # HANDLE TIMEOUT
        #------------------------
        except requests.exceptions.Timeout:
            logging.warning(f'Timeout on attempt {attempt}')
        
        #------------------------
        # HANDLE HTTP ERRORS (4xx, 5xx)
        #------------------------
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code

            if status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 5))
                logging.warning(f'Rate limited. Waiting {retry_after}s ...')
                time.sleep(retry_after)
            # Retry only on server errors (5xx)
            if 500 <= status_code < 600:
                logging.warning(f'Server error {status_code} on attempt {attempt}')
            else:
                logging.error(f'Client error {status_code} - not retrying')
                raise
        #-----------------------
        # HANDLE OTHER ERRORS
        #-----------------------
        except requests.exceptions.RequestException as e:
            logging.error(f'Request failed: {e}')
            raise
        
        #-----------------------
        # BACKOFF BEFORE RETRY
        #-----------------------
        if attempt < MAX_RETRIES:
            logging.info(f'Waiting {delay}s before retry...')
            time.sleep(delay)
            delay *=2      #exponential backoff

    #---------------------
    # FINAL FAILURE
    #---------------------       
    raise Exception('Failed to fetch crypto data after multiple retries')        

#---------------------------
# STEP 2: UPLOAD TO GCS
#---------------------------
def upload_to_gcs(data):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    blob_name = f'raw/crypto/prices/crypto_{timestamp}.json'
    blob = bucket.blob(blob_name)
    
    try:
        blob.upload_from_string(
            gzip.compress(json.dumps(data).encode('utf-8')),
            content_type='application/json',
            content_encoding='gzip'
        )
        print(f'Uploaded: gs://{BUCKET_NAME}/{blob_name}')
    except Exception as e:
        print(f'Upload failed: {e}')    

    

#---------------------------
# PIPELINE
#---------------------------
def main():
    print('Fetching crypto data ...')
    data = fetch_crypto_data()

    print('Uploading to GCS ...')
    upload_to_gcs(data)

    print('Done!')

if __name__ == '__main__':
    main()    