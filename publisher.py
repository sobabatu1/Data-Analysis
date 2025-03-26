import requests
import time
from google.cloud import pubsub_v1
import json
import os
import datetime


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/sadiqolowojia/Desktop/gcp_project/capstone-396019-e9a887417d07.json"

def crypto_data(coin_name):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_name}"

    # fetch data from CoinGecko
    response = requests.get(url)

    raw_data = response.json()
    
    retrieval_time = datetime.datetime.utcnow().isoformat() + "Z"

    # extract and format data as desired
    data = {
        'coin_name': coin_name.capitalize(),
        'current_price_usd': raw_data['market_data']['current_price']['usd'],
        '24h_high_usd': raw_data['market_data']['high_24h']['usd'],
        '24h_low_usd': raw_data['market_data']['low_24h']['usd'],
        '24h_trade_volume_usd': raw_data['market_data']['total_volume']['usd'],
        'market_cap_usd': raw_data['market_data']['market_cap']['usd'],
        'market_change_percentage_24h': raw_data['market_data']['market_cap_change_percentage_24h'],
        'market_rank': raw_data['market_cap_rank'],
        'circulating_supply': raw_data['market_data']['circulating_supply'],
        'total_supply': raw_data['market_data']['total_supply'],
        'last_updated': raw_data['last_updated'],
        'retrieval_time': retrieval_time
    }
    
    return data

project_id = "capstone-396019"
topic_id = "electric"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

cryptocurrencies = ["bitcoin", "ethereum", "litecoin", "ripple"]

while True:
    for coin in cryptocurrencies:
        # print(f'Generating Log for {coin.capitalize()}: ')
        log = crypto_data(coin)
        message = json.dumps(log)
        # print(message)
        publisher.publish(topic_path, message.encode("utf-8"))
    time.sleep(20)