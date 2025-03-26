import os
import json
import pickle
import pandas as pd
import pandas_gbq
import prophet
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.userstate import BagStateSpec, CombiningValueStateSpec, TimerSpec, on_timer
import pandas.core.indexes.numeric

schema = 'coin_name:STRING,current_price_usd:FLOAT,24h_high_usd:FLOAT,24h_low_usd:FLOAT,24h_trade_volume_usd:FLOAT,market_cap_usd:FLOAT,market_change_percentage_24h:FLOAT,market_rank:INTEGER,circulating_supply:FLOAT,total_supply:FLOAT,last_updated:TIMESTAMP,retrieval_time:TIMESTAMP,rolling_average:FLOAT,predicted_price:FLOAT,ds:STRING'
topic = "projects/capstone-396019/topics/crypto-data-stream"
output_table = "capstone-396019:logs_datasets.crypto-realtime-data"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/sadiqolowojia/Desktop/gcp_project/capstone-396019-eb868a15b809.json"

# convert json payload from pubsub to tuple, to align with stateful function
class ConvertJSONtoTuple(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        coin_name = record.pop('coin_name')
        yield (coin_name, record)

# DoFn to calculate rolling average of streaming data
class RollingAverageTransform(beam.DoFn):
    STATE_PREFIX = 'price_'
    TIMER_PREFIX = 'timer_'
    WINDOW_SIZE = 7  # Set window size for rolling average

    state_spec = BagStateSpec('prices', beam.coders.FloatCoder())
    timer_spec = TimerSpec('rolling_timer', beam.TimeDomain.WATERMARK)
    record_spec = BagStateSpec('record', beam.coders.FastPrimitivesCoder())
    
    def process(self, element, prices=beam.DoFn.StateParam(state_spec), record_state=beam.DoFn.StateParam(record_spec),timer=beam.DoFn.TimerParam(timer_spec)):
        _, record = element
        current_price = record['current_price_usd']
        prices.add(current_price)

        record_state.add(element)
        dt = datetime.fromisoformat(record["retrieval_time"].replace("Z", "+00:00"))
        timestamp = dt.timestamp()
        timer.set(beam.window.Timestamp(seconds=timestamp))

    @on_timer(timer_spec)
    def on_rolling_timer(self, record_state=beam.DoFn.StateParam(record_spec), prices=beam.DoFn.StateParam(state_spec)):
        
        record = list(record_state.read())[0]
        current_prices = list(prices.read())
        result = {'coin_name': record[0]}
        result.update(record[1])
        if len(current_prices) >= self.WINDOW_SIZE:
            rolling_avg = sum(current_prices[-self.WINDOW_SIZE:]) / self.WINDOW_SIZE
            update = {'rolling_average':rolling_avg}
            result.update(update)
            yield result

def load_model():
        # function to load prediction model for inference
        with open('/Users/sadiqolowojia/Desktop/gcp_project/predictmodel.pkl', 'rb') as model_file:
            model = pickle.load(model_file)
        return model

class RunPredictionInference(beam.DoFn):
    def process(self, element):
        model = load_model()
        
        # prepare data for prediction (based on the structure of incoming data)
        df = pd.DataFrame(element, index=[0])

        df['ds'] = df['last_updated']
        future = model.make_future_dataframe(periods=5)
        forecast = model.predict(future)
        prediction = forecast['yhat'][0]

        # add prediction to the original data
        df['predicted_price'] = prediction
        
        return df.to_dict('records')

# pipeline options 
beam_options_dict = {
'project': 'capstone-396019',
'runner': 'DataflowRunner',
'region':'us-central1',
'temp_location':'gs://beam-temp-bucket-092/temp_location',
'job_name': 'crypto-streaming-job',
'streaming': True
}
beam_options = PipelineOptions.from_dictionary(beam_options_dict)


with beam.Pipeline(options=beam_options) as p: (
        p 
        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=topic)
        | 'Tuple' >> beam.ParDo(ConvertJSONtoTuple())
        | 'SetCoder' >> beam.Map(lambda x: x).with_output_types(beam.typehints.KV[str, dict]).with_input_types(beam.typehints.KV[str, dict]) # set the coder explicitly to avoid deterministic warning
        | 'ComputeRollingAverage' >> beam.ParDo(RollingAverageTransform()).with_input_types(beam.typehints.Tuple[str, dict])
        | 'RunPrediction' >> beam.ParDo(RunPredictionInference())
        | 'Write to Table' >> beam.io.WriteToBigQuery(output_table,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


if __name__ == '__main__':    
    p.run()