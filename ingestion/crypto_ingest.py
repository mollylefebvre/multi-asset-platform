import requests
import json
from datetime import datetime
from google.cloud import storage

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
# STEP 1: FETCH DATA
#---------------------------
def fetch_crypto_data():
    params = {
        'vs_currency': 'usd',
        'ids':','.join(COIN_IDS),
        'order':'markect_cap_desc',
        'sparkline': False
    }

    response = requests.get(URL, params=params)
    response.raise_for_status()
    
    return response.json()

#---------------------------
# STEP 2: UPLOAD TO GCS
#---------------------------
def upload_to_gcs(data):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    #create unique filename
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    blob_name = f'raw/crypto/prices/crypto_{timestamp}.json'
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(data),
        content_type='application/json'
    )

    print(f'Uploaded: gs://{BUCKET_NAME}/{blob_name}')

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