import logging
import pybreaker
import requests
import json
import time
from datetime import datetime, timezone, timedelta
from google.cloud import storage
import gzip

def get_default_timestamp():
    """
    Used only when no scheduler is present.
    Return the latest *safe* complete minute.
    """
    now =datetime.now(timezone.utc)
    return (now - timedelta(minutes=1)).replace(second=0, microsecond=0)

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
# INITIATE BREAKER
#---------------------------
breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=180)

#---------------------------
# STEP 1: FETCH DATA
#---------------------------
MAX_RETRIES = 5
INITIAL_DELAY = 1   # seconds
TIMEOUT = (3,10)    # (connect, read)

@breaker
def fetch_crypto_data(timestamp):

    def now():
        return datetime.now(timezone.utc).isoformat()
    
    run_ts = timestamp.isoformat()

    params = {
        'vs_currency': 'usd',
        'ids':','.join(COIN_IDS),
        'order':'market_cap_desc',
        'sparkline': False
    }

    delay = INITIAL_DELAY

    for attempt in range(1, MAX_RETRIES+1):
        try:
            logging.info(
                f"[EVENT {run_ts}] [PROC {now()}] [ATTEMPT {attempt}] Fetching Crypto data..."
            )

            response = requests.get(URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()

            logging.info(
                f"[EVENT {run_ts}] [PROC {now()}] [SUCCESS] Data fetched successfully on attempt {attempt}"
            )

            return response.json()
        #------------------------
        # TIMEOUT
        #------------------------
        except requests.exceptions.Timeout:
            logging.warning(
                f"[EVENT {run_ts}] [PROC {now()}] [ATTEMPT {attempt}] [TIMEOUT]"
            )
        #------------------------
        # HTTP ERRORS (4xx, 5xx)
        #------------------------
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code

            if status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', delay))

                wait_time = max(retry_after, delay)

                logging.warning(
                    f"[EVENT {run_ts}] [PROC {now()}] [ATTEMPT {attempt}] [RATE_LIMIT] "
                    f"Retry after {retry_after}s (retry_after={retry_after}, backoff={delay})"
                )
                time.sleep(wait_time)
                delay *= 2
                continue
            # Retry only on server errors (5xx)
            elif 500 <= status_code < 600:
                logging.warning(
                    f"[RUN {run_ts}] [ATTEMPT {attempt}] [SERVER_ERROR {status_code}]"
                )
                
            else:
                logging.error(
                    f"[EVENT {run_ts}] [PROC {now()}] [ATTEMPT {attempt}] [CLIENT_ERROR {status_code}]"
                )
                raise
        #-----------------------
        # OTHER ERRORS
        #-----------------------
        except requests.exceptions.RequestException as e:
            logging.warning(
                f"[EVENT {run_ts}] [PROC {now()}] [ATTEMPT {attempt}] [REQUEST_EXCEPTION] {e}"
            )
        
        #-----------------------
        # BACKOFF BEFORE RETRY
        #-----------------------
        if attempt < MAX_RETRIES:
            logging.info(f"[EVENT {run_ts}] [PROC {now()}] [ATTEMPT {attempt}] Waiting {delay}s before retry")
            time.sleep(delay)
            delay *=2      #exponential backoff

    #---------------------
    # FINAL FAILURE
    #---------------------
    logging.error(f"[EVENT {run_ts}] [PROC {now()}] [FAILED] Missing data after {MAX_RETRIES} attempts")      
    raise Exception('Failed to fetch crypto data after multiple retries')        

#---------------------------
# STEP 2: UPLOAD TO GCS
#---------------------------
def upload_to_gcs(data, timestamp):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    timestamp = timestamp.strftime('%Y%m%d_%H%M%S') 

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
def run_pipeline(timestamp=None):
    if timestamp is None:
        timestamp = get_default_timestamp()

    try:
        data = fetch_crypto_data(timestamp)
    except pybreaker.CircuitBreakerError:
        logging.warning(
            f"[EVENT {timestamp.isoformat()}] [PROC {datetime.now(timezone.utc).isoformat()}] Circuit breaker OPEN - skipping API call"
        )
        return 
    

    print('Uploading to GCS ...')
    upload_to_gcs(data, timestamp)

    print('Done!')

if __name__ == '__main__':
    run_pipeline()
    #later when scheduler --> run_pipeline(timestamp=execution_time_from_scheduler)    