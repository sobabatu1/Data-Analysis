import pandas as pd
from prophet import Prophet
from pandas_gbq import read_gbq
import pickle


project_id = "capstone-396019"

# SQL query to fetch data from BigQuery
query = """
SELECT last_updated, current_price_usd
FROM `capstone-396019.logs_datasets.streaming_btc` 
ORDER BY last_updated
"""

# load data from BigQuery into pandas dataframe
df = read_gbq(query, project_id=project_id)

# preprocess dataframe for model training
df = df.rename(columns={'last_updated': 'ds', 'current_price_usd': 'y'})

# absolve timezone localization as it is not supported in prophet
df['ds'] = df['ds'].dt.tz_localize(None)

# train the model
model = Prophet(daily_seasonality=True)
model.fit(df)

# save trained model
with open('/Users/sadiqolowojia/Desktop/gcp_project/predictmodel.pkl', 'wb') as model_file:
    pickle.dump(model, model_file)
